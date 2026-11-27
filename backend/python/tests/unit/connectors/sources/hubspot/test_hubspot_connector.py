"""HubSpot connector against a fake HubSpot data source: what a sync writes, and who may read it."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from app.config.constants.arangodb import MimeTypes, ProgressStatus
from app.connectors.core.base.connector.connector_service import ConnectorInitError
from app.connectors.sources.hubspot import connector as connector_module
from app.connectors.sources.hubspot.connector import HubSpotAuthError, HubSpotConnector
from app.models.entities import (
    CommentRecord,
    DealRecord,
    FileRecord,
    MailRecord,
    MeetingRecord,
    ProductRecord,
    RecordType,
    TicketRecord,
    WebpageRecord,
)
from app.sources.client.hubspot.hubspot import HubSpotClient, HubSpotResponse
from app.sources.client.resilience import ResiliencePolicy

CONNECTOR_ID = "hubspot-1"
T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)
T1 = datetime(2026, 11, 2, tzinfo=timezone.utc)


def _obj(object_id: str, updated: datetime = T0, **props: object) -> dict:
    stamp = updated.isoformat().replace("+00:00", "Z")
    return {"id": object_id, "created_at": T0, "updated_at": updated, "archived": False,
            "properties": {"hs_lastmodifieddate": stamp, **props}}


class FakeHubSpot:
    """The HubSpotDataSource methods the connector calls, answering from in-memory data."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.failures: dict[str, list[HubSpotResponse]] = {}
        self.users = [
            {"id": "1", "email": "Admin@Acme.com", "super_admin": True},
            {"id": "2", "email": "rep@acme.com", "super_admin": False, "role_id": "r1"},
            {"id": "3", "email": "teammate@acme.com", "super_admin": False},
        ]
        self.owners = [
            {"id": "101", "user_id": "2", "email": "rep@acme.com", "first_name": "Rita", "last_name": "Rep"},
            {"id": "102", "user_id": "1", "email": "admin@acme.com", "first_name": "Ada", "last_name": None},
        ]
        self.archived_owners = [{"id": "103", "user_id": None, "user_id_including_inactive": "9", "archived": True}]
        self.teams = [{"id": "t1", "name": "West", "user_ids": ["2"], "secondary_user_ids": ["3"]}]
        # Only Enterprise accounts have roles; the default fake answers as the others do.
        self.roles: list[dict] | None = None
        self.objects: dict[str, list[dict]] = {
            "deals": [
                _obj("11", dealname="Fleet rollout", amount="48000", dealstage="s1", pipeline="default",
                     hubspot_owner_id="101", hs_all_team_ids="t1", description="<p>Wants <b>ETA</b> tracking</p>"),
                _obj("12", dealname="Unassigned deal"),
            ],
            "tickets": [_obj("21", subject="Login broken", hs_pipeline="0", hs_pipeline_stage="1",
                             hs_ticket_priority="HIGH", hubspot_owner_id="101", hs_shared_user_ids="3")],
            "tasks": [],
            "meetings": [],
            "notes": [],
            "calls": [],
            "emails": [],
            "contacts": [],
            "companies": [],
            "products": [],
        }
        self.archived: dict[str, list[dict]] = {}
        self.links = {("deals", "companies"): {"11": ["501"]}, ("deals", "contacts"): {"11": ["601"]},
                      ("tickets", "deals"): {"21": ["11"]}}
        self.details = {"companies": {"501": {"name": "Northwind", "domain": "northwind.example"}},
                        "contacts": {"601": {"firstname": "Maya", "lastname": "Chen", "email": "maya@northwind.example"}}}
        self.files = [{"id": "7", "name": "spec", "extension": "pdf", "size": 10, "created_at": T0, "updated_at": T0,
                       "archived": False}]

    def _answer(self, method: str, data: object) -> HubSpotResponse:
        queue = self.failures.get(method)
        if queue:
            return queue.pop(0)
        return HubSpotResponse(success=True, data=data)

    def _page(self, method: str, items: list, limit: int, after: str | None) -> HubSpotResponse:
        start = int(after or 0)
        page = {"results": items[start:start + limit]}
        if start + limit < len(items):
            page["paging"] = {"next": {"after": str(start + limit)}}
        return self._answer(method, page)

    def __getattr__(self, method: str):  # noqa: ANN204 - records every call by name
        handler = getattr(self, f"_{method}")

        async def call(*args: object, **kwargs: object) -> HubSpotResponse:
            self.calls.append((method, args, kwargs))
            return handler(*args, **kwargs)
        return call

    def _get_account_details(self) -> HubSpotResponse:
        return self._answer("get_account_details", {"portalId": 248, "uiDomain": "app-eu1.hubspot.com"})

    def _list_owners(self, limit: int = 100, after: str | None = None, archived: bool = False) -> HubSpotResponse:
        return self._page("list_owners", self.archived_owners if archived else self.owners, limit, after)

    def _list_users(self, limit: int = 100, after: str | None = None) -> HubSpotResponse:
        return self._page("list_users", self.users, limit, after)

    def _list_teams(self) -> HubSpotResponse:
        return self._answer("list_teams", {"results": self.teams})

    def _list_roles(self) -> HubSpotResponse:
        if self.roles is None:
            return HubSpotResponse(success=False, status_code=400, category="VALIDATION_ERROR",
                                   error="VALIDATION_ERROR: Account doesn't have access to roles. (HTTP 400)")
        return self._answer("list_roles", {"results": self.roles})

    def _list_pipelines(self, object_type: str) -> HubSpotResponse:
        stages = {"deals": [{"id": "s1", "label": "Qualified", "metadata": {}}],
                  "tickets": [{"id": "1", "label": "New", "metadata": {"ticketState": "OPEN"}}]}[object_type]
        return self._answer("list_pipelines", {"results": [{"id": "default" if object_type == "deals" else "0",
                                                            "label": "Sales", "stages": stages}]})

    def _list_objects(self, object_type: str, limit: int = 100, after: str | None = None,
                      properties: list | None = None, archived: bool = False) -> HubSpotResponse:
        items = self.archived.get(object_type, []) if archived else self.objects[object_type]
        return self._page("list_objects", items, limit, after)

    def _search_objects(self, object_type: str, filter_groups: list, sorts: list, properties: list,
                        limit: int, after: str | None) -> HubSpotResponse:
        since = int(filter_groups[0]["filters"][0]["value"])
        items = [o for o in self.objects[object_type] if o["updated_at"].timestamp() * 1000 >= since]
        return self._page("search_objects", items, limit, after)

    def _batch_read_associations(self, from_type: str, to_type: str, ids: list[str]) -> HubSpotResponse:
        links = self.links.get((from_type, to_type), {})
        results = [{"_from": {"id": i}, "to": [{"to_object_id": t} for t in links[i]]} for i in ids if i in links]
        return self._answer("batch_read_associations", {"results": results})

    def _batch_read_objects(self, object_type: str, ids: list[str], properties: list) -> HubSpotResponse:
        found = self.details.get(object_type, {})
        return self._answer("batch_read_objects", {"results": [{"id": i, "properties": found[i]} for i in ids if i in found]})

    def _search_files(self, limit: int, after: str | None, updated_at_gte: datetime | None, sort: list) -> HubSpotResponse:
        items = [f for f in self.files if updated_at_gte is None or f["updated_at"] >= updated_at_gte]
        return self._page("search_files", items, limit, after)

    def _get_object(self, object_type: str, object_id: str, properties: list) -> HubSpotResponse:
        for obj in self.objects[object_type]:
            if obj["id"] == object_id:
                return self._answer("get_object", obj)
        return HubSpotResponse(success=False, status_code=404, category="OBJECT_NOT_FOUND",
                               error="OBJECT_NOT_FOUND: Object not found (HTTP 404)")

    def _get_file_signed_url(self, file_id: str, expiration_seconds: int) -> HubSpotResponse:
        return self._answer("get_file_signed_url", {"url": "https://cdn.example/signed?token=secret"})


def _memory_sync_point() -> MagicMock:
    store: dict = {}

    async def read(key: str) -> dict:
        return dict(store.get(key) or {})

    async def update(key: str, data: dict) -> None:
        store[key] = dict(data)

    sync_point = MagicMock()
    sync_point.store = store
    sync_point.read_sync_point = AsyncMock(side_effect=read)
    sync_point.update_sync_point = AsyncMock(side_effect=update)
    return sync_point


@pytest.fixture
def hubspot() -> FakeHubSpot:
    return FakeHubSpot()


def _connector(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch, *, access: dict[str, list[str]] | None = None,
               auth_type: str = "API_TOKEN") -> HubSpotConnector:
    processor = MagicMock()
    processor.org_id = "org-1"
    for name in ("on_new_app_users", "on_new_user_groups", "on_new_record_groups", "on_new_records",
                 "delete_record_by_external_id", "reindex_existing_records"):
        setattr(processor, name, AsyncMock(return_value=None))
    connector = HubSpotConnector(MagicMock(), processor, MagicMock(), AsyncMock(), CONNECTOR_ID, "team", "user-1")

    async def rebuild() -> None:
        connector._client = HubSpotClient.build_with_token("pat-secret", auth_type=auth_type, refresh_token="refresh-secret")
        connector._data_source = hubspot

    connector._rebuild_client = AsyncMock(side_effect=rebuild)
    connector.records_sync_point = _memory_sync_point()
    connector._resilience = ResiliencePolicy(max_retries=2, base_delay=0.0)
    connector._resilience_loaded = True
    connector.notify = AsyncMock()
    filters = MagicMock()
    filters.is_enabled = MagicMock(side_effect=lambda key, default=True: default)
    filters.get_value = MagicMock(side_effect=lambda key, default=None: (access or {}).get(key, default))
    monkeypatch.setattr(connector_module, "load_connector_filters", AsyncMock(return_value=(filters, None)))
    return connector


def _written(connector: HubSpotConnector) -> dict[str, tuple]:
    out = {}
    for call in connector.data_entities_processor.on_new_records.await_args_list:
        assert call.kwargs == {"replace_permissions": True}
        for record, permissions in call.args[0]:
            out[record.external_record_id] = (record, sorted(p.external_id for p in permissions))
    return out


def _groups(connector: HubSpotConnector) -> dict[str, list[str]]:
    [call] = connector.data_entities_processor.on_new_user_groups.await_args_list
    return {group.source_user_group_id: sorted(u.email for u in members) for group, members in call.args[0]}


async def test_first_sync_writes_users_groups_and_records(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch)

    await connector.run_sync()

    [users_call] = connector.data_entities_processor.on_new_app_users.await_args_list
    assert sorted(u.email for u in users_call.args[0]) == ["admin@acme.com", "rep@acme.com", "teammate@acme.com"]
    groups = _groups(connector)
    assert {k: v for k, v in groups.items() if not k.startswith(("owner_", "all_")) or k == "all_users"} == {
        "super_admins": ["admin@acme.com"],
        "all_users": ["admin@acme.com", "rep@acme.com", "teammate@acme.com"],
        "user_1": ["admin@acme.com"],
        "user_2": ["rep@acme.com"],
        "user_3": ["teammate@acme.com"],
        "user_9": [],  # deactivated: rewritten empty so its grants lead nowhere
        "team_t1": ["rep@acme.com", "teammate@acme.com"],
    }
    # Nobody was given "All" or "Team" access, so everyone counts as "Owned".
    assert all(groups[f"all_{t}"] == [] for t in ("deals", "tickets", "tasks", "meetings"))
    assert groups["owner_2_deals"] == ["rep@acme.com"]
    assert groups["owner_9_tickets"] == []  # a deactivated owner's records stay readable to Super Admins only

    written = _written(connector)
    assert set(written) == {"0-3:11", "0-3:12", "0-5:21", "file:7"}
    deal, deal_grants = written["0-3:11"]
    assert isinstance(deal, DealRecord) and deal.record_type == RecordType.DEAL
    assert (deal.record_name, deal.amount, deal.owner_id) == ("Fleet rollout", 48000.0, "2")
    assert deal.weburl == "https://app-eu1.hubspot.com/contacts/248/record/0-3/11"
    assert deal.mime_type == MimeTypes.BLOCKS.value and deal.version == 0 and not deal.inherit_permissions
    assert deal_grants == ["all_deals", "owner_2_deals", "super_admins"]
    assert written["0-3:12"][1] == ["all_deals", "super_admins"]  # unassigned: no owner group

    ticket, ticket_grants = written["0-5:21"]
    assert isinstance(ticket, TicketRecord) and ticket.record_type == RecordType.TICKET
    assert ticket_grants == ["all_tickets", "owner_2_tickets", "super_admins", "user_3"]  # user 3: shared with
    assert (ticket.source_status, ticket.assignee_email) == ("New", "rep@acme.com")
    assert [r.external_record_id for r in ticket.related_external_records] == ["0-3:11"]

    file_record, file_grants = written["file:7"]
    assert isinstance(file_record, FileRecord) and file_record.record_name == "spec.pdf"
    assert file_record.mime_type == MimeTypes.PDF.value and file_grants == ["super_admins"]

    [groups_call] = connector.data_entities_processor.on_new_record_groups.await_args_list
    assert sorted(g.external_group_id for g, _ in groups_call.args[0]) == [
        "calls", "companies", "contacts", "deals", "emails", "files", "meetings", "notes", "products", "tasks", "tickets"]
    # Everyone may browse a folder; records do not inherit from it.
    assert all(sorted(p.external_id for p in grants) == ["all_users", "super_admins"] for _, grants in groups_call.args[0])
    assert connector.full_sync_coverage().complete


async def test_activities_people_and_products_become_records(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.objects["notes"] = [_obj("31", hs_note_body="<p>Asked for a <b>pilot</b> in Q3.</p><p>More later</p>",
                                     hubspot_owner_id="101")]
    hubspot.objects["calls"] = [_obj("32", hs_call_title="Discovery call", hs_call_body="Budget is approved",
                                     hs_call_duration="125000", hs_call_recording_url="https://rec.example/1",
                                     hs_timestamp="2026-11-01T10:00:00Z", hubspot_owner_id="101")]
    hubspot.objects["emails"] = [_obj("33", hs_email_subject="Re: pricing", hs_email_text="See the attached quote",
                                      hs_email_from_email="rep@acme.com", hs_email_to_email="maya@northwind.example;ops@northwind.example",
                                      hubspot_owner_id="101")]
    hubspot.objects["contacts"] = [_obj("601", firstname="Maya", lastname="Chen", email="maya@northwind.example",
                                        jobtitle="CTO", hubspot_owner_id="101")]
    hubspot.objects["companies"] = [_obj("501", name="Northwind", domain="northwind.example", industry="Logistics")]
    hubspot.objects["products"] = [_obj("701", name="Tracker", price="99", hs_sku="TRK-1")]
    hubspot.links[("notes", "deals")] = {"31": ["11"]}
    hubspot.links[("deals", "line_items")] = {"11": ["801"]}
    hubspot.details["line_items"] = {"801": {"name": "Tracker", "quantity": "40", "price": "99", "amount": "3960"}}
    connector = _connector(hubspot, monkeypatch, access={"notes_view_all": ["user:3"]})

    await connector.run_sync()

    written = _written(connector)
    note, note_grants = written["0-46:31"]
    assert isinstance(note, CommentRecord) and note.record_type == RecordType.COMMENT
    assert note.record_name == "Asked for a **pilot** in Q3." and note.author_source_id == "2"
    assert note_grants == ["all_notes", "owner_2_notes", "super_admins"]
    assert [r.external_record_id for r in note.related_external_records] == ["0-3:11"]
    call, _ = written["0-48:32"]
    assert isinstance(call, MeetingRecord) and (call.record_name, call.duration_minutes) == ("Discovery call", 2)
    assert call.recording_url == "https://rec.example/1"
    email, _ = written["0-49:33"]
    assert isinstance(email, MailRecord) and email.record_type == RecordType.MAIL
    assert email.to_emails == ["maya@northwind.example", "ops@northwind.example"]
    contact, contact_grants = written["0-1:601"]
    assert isinstance(contact, WebpageRecord) and contact.record_name == "Maya Chen"
    assert contact_grants == ["all_contacts", "owner_2_contacts", "super_admins"]
    assert isinstance(written["0-2:501"][0], WebpageRecord) and written["0-2:501"][1] == ["all_companies", "super_admins"]
    product, product_grants = written["0-7:701"]
    assert isinstance(product, ProductRecord) and (product.sku, product.list_price) == ("TRK-1", 99.0)
    assert product_grants == ["all_users", "super_admins"]  # no owner: every HubSpot user picks products
    groups = _groups(connector)
    assert groups["all_notes"] == ["teammate@acme.com"] and "all_products" not in groups

    body = json.loads(b"".join([c async for c in (await connector.stream_record(written["0-3:11"][0])).body_iterator]))
    assert "## Line items" in body["block_groups"][0]["data"]
    assert "- Tracker (qty 40, unit price 99, total 3960)" in body["block_groups"][0]["data"]


async def test_view_access_follows_the_admins_role_and_user_lists(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch, access={
        "deals_view_all": ["role:r1"],  # rep's role
        "deals_view_team": ["user:3", "user:1"],
        "tickets_view_team": ["user:3"],
    })

    await connector.run_sync()

    groups = _groups(connector)
    assert groups["all_deals"] == ["rep@acme.com"]
    # The teammate shares team t1 with the rep, so reads the rep's deals and tickets;
    # the admin has "Team" access but no team, so reads only their own.
    assert groups["owner_2_deals"] == ["rep@acme.com", "teammate@acme.com"]
    assert groups["owner_1_deals"] == ["admin@acme.com"]
    assert groups["owner_2_tickets"] == ["rep@acme.com", "teammate@acme.com"]
    assert groups["all_tickets"] == [] and groups["owner_2_tasks"] == ["rep@acme.com"]
    # Records grant the same groups whoever is in them; only memberships carry the setting.
    assert _written(connector)["0-3:11"][1] == ["all_deals", "owner_2_deals", "super_admins"]


async def test_all_wins_over_team(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch, access={"deals_view_all": ["user:3"], "deals_view_team": ["user:3"]})

    await connector.run_sync()

    groups = _groups(connector)
    assert groups["all_deals"] == ["teammate@acme.com"]
    assert groups["owner_2_deals"] == ["rep@acme.com"]


async def test_access_options_list_users_when_the_account_has_no_roles(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch)

    response = await connector.get_filter_options("deals_view_all", limit=2)

    assert [(o.id, o.label) for o in response.options] == [("user:1", "User: Admin@Acme.com"), ("user:2", "User: rep@acme.com")]
    assert response.has_more


async def test_access_options_list_roles_first_and_search(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.roles = [{"id": "r1", "name": "Sales rep"}]
    connector = _connector(hubspot, monkeypatch)

    everything = await connector.get_filter_options("tickets_view_team")
    found = await connector.get_filter_options("tickets_view_team", search="sales")

    assert [o.id for o in everything.options] == ["role:r1", "user:1", "user:2", "user:3"]
    assert [o.label for o in found.options] == ["Role: Sales rep"] and not found.has_more
    with pytest.raises(ValueError, match="no dynamic filter"):
        await connector.get_filter_options("team_access")


async def test_lists_every_page(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.objects["deals"] = [_obj(str(i), dealname=f"Deal {i}") for i in range(250)]
    connector = _connector(hubspot, monkeypatch)

    await connector.run_sync()

    assert len([k for k in _written(connector) if k.startswith("0-3:")]) == 250


async def test_streamed_text_names_the_linked_contacts_and_companies(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch)
    await connector.run_sync()
    deal = _written(connector)["0-3:11"][0]

    response = await connector.stream_record(deal)

    body = json.loads(b"".join([chunk async for chunk in response.body_iterator]))
    text = body["block_groups"][0]["data"]
    assert "# Fleet rollout" in text and "**Stage:** Qualified" in text and "**Owner:** Rita Rep <rep@acme.com>" in text
    assert "Wants **ETA** tracking" in text
    assert "- Northwind (northwind.example)" in text and "- Maya Chen (maya@northwind.example)" in text
    # The revision is the hash of exactly this text, so a stream after an unchanged sync matches it.
    from app.connectors.sources.hubspot.mapping import content_revision
    assert deal.external_revision_id == content_revision(text)


async def test_stream_of_a_deleted_object_is_404(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch)
    await connector.run_sync()
    deal = _written(connector)["0-3:11"][0]
    hubspot.objects["deals"] = []

    with pytest.raises(HTTPException) as raised:
        await connector.stream_record(deal)
    assert raised.value.status_code == 404


async def test_incremental_sync_reads_changes_and_deletes_archived(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch)
    await connector.run_sync()
    since = connector.records_sync_point.store["deals"]["lastModifiedMs"]
    connector.data_entities_processor.on_new_records.reset_mock()
    hubspot.calls.clear()
    later = datetime.fromtimestamp(since / 1000 + 60, tz=timezone.utc)
    hubspot.objects["deals"] = [_obj("11", updated=later, dealname="Fleet rollout v2", hubspot_owner_id="102")]
    hubspot.archived["deals"] = [{"id": "12", "archived": True, "archived_at": later},
                                 {"id": "13", "archived": True, "archived_at": T0}]

    await connector.run_sync()

    searches = [c for c in hubspot.calls if c[0] == "search_objects" and c[1] == ("deals",)]
    assert searches and searches[0][2]["filter_groups"][0]["filters"][0]["value"] == str(since)
    assert not [c for c in hubspot.calls if c[0] == "list_objects" and c[1] == ("deals",) and not c[2].get("archived")]
    deal, grants = _written(connector)["0-3:11"]
    assert deal.record_name == "Fleet rollout v2" and grants == ["all_deals", "owner_1_deals", "super_admins"]
    deleted = [c.args[1] for c in connector.data_entities_processor.delete_record_by_external_id.await_args_list]
    assert deleted == ["0-3:12"]  # archived before the last run: already handled then


async def test_a_failed_page_holds_the_checkpoint_and_marks_coverage_incomplete(
    hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch,
) -> None:
    hubspot.failures["batch_read_associations"] = [HubSpotResponse(
        success=False, status_code=500, error="ERROR: boom (HTTP 500)")]
    connector = _connector(hubspot, monkeypatch)

    await connector.run_sync()

    assert "0-3:11" not in _written(connector)
    assert connector.records_sync_point.store["deals"]["lastModifiedMs"] < T0.timestamp() * 1000
    assert not connector.full_sync_coverage().complete


async def test_throttling_is_retried(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.failures["list_teams"] = [HubSpotResponse(
        success=False, status_code=429, policy_name="TEN_SECONDLY_ROLLING", retry_after=0.0,
        error="RATE_LIMIT: slow down (HTTP 429)")]
    connector = _connector(hubspot, monkeypatch)

    await connector.run_sync()

    assert len([c for c in hubspot.calls if c[0] == "list_teams"]) == 2


async def test_daily_limit_stops_the_sync_and_notifies(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.failures["list_teams"] = [HubSpotResponse(
        success=False, status_code=429, policy_name="DAILY", error="RATE_LIMIT: daily limit (HTTP 429)")]
    connector = _connector(hubspot, monkeypatch)

    with pytest.raises(connector_module.HubSpotQuotaError):
        await connector.run_sync()
    connector.notify.assert_awaited_once()
    connector.data_entities_processor.on_new_records.assert_not_awaited()


async def test_a_rejected_static_token_stops_without_retrying(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.failures["get_account_details"] = [HubSpotResponse(
        success=False, status_code=401, category="INVALID_AUTHENTICATION",
        error="INVALID_AUTHENTICATION: Authentication credentials not found. (HTTP 401)")]
    connector = _connector(hubspot, monkeypatch)

    with pytest.raises(HubSpotAuthError):
        await connector.run_sync()

    assert len([c for c in hubspot.calls if c[0] == "get_account_details"]) == 1
    message = connector.notify.await_args.args[3]
    assert "Authentication credentials not found" in message and "secret" not in message
    logged = " ".join(str(a) for call in connector.logger.mock_calls for a in call.args)
    assert "pat-secret" not in logged and "refresh-secret" not in logged


async def test_an_oauth_401_refreshes_once_and_retries(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.failures["get_account_details"] = [HubSpotResponse(success=False, status_code=401, error="expired (HTTP 401)")]
    connector = _connector(hubspot, monkeypatch, auth_type="OAUTH")
    connector.config_service.get_config = AsyncMock(return_value={"credentials": {"access_token": "pat-secret"}})
    refresh = MagicMock(refresh_now=AsyncMock())
    monkeypatch.setattr(connector_module.startup_service, "get_token_refresh_service", lambda: refresh)

    await connector.run_sync()

    refresh.refresh_now.assert_awaited_once_with(CONNECTOR_ID, "HUBSPOT", "refresh-secret")
    assert len([c for c in hubspot.calls if c[0] == "get_account_details"]) == 2


async def test_connection_test_names_the_refusal(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    hubspot.failures["list_users"] = [HubSpotResponse(
        success=False, status_code=403, category="MISSING_SCOPES",
        error="MISSING_SCOPES: This app hasn't been granted all required scopes (HTTP 403)")]
    connector = _connector(hubspot, monkeypatch)

    with pytest.raises(ConnectorInitError, match="MISSING_SCOPES"):
        await connector.test_connection_and_access()


async def test_indexing_filter_off_stores_without_indexing(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(hubspot, monkeypatch)
    indexing = MagicMock(is_enabled=MagicMock(side_effect=lambda key, default=True: key != "deals"))
    sync = MagicMock(is_enabled=MagicMock(return_value=False))
    monkeypatch.setattr(connector_module, "load_connector_filters", AsyncMock(return_value=(sync, indexing)))

    await connector.run_sync()

    written = _written(connector)
    assert written["0-3:11"][0].indexing_status == ProgressStatus.AUTO_INDEX_OFF.value
    assert written["0-5:21"][0].indexing_status != ProgressStatus.AUTO_INDEX_OFF.value


async def test_deleted_meetings_are_not_listed(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    # Live HubSpot answers 400 to paging archived meetings (object type 0-47).
    connector = _connector(hubspot, monkeypatch)
    await connector.run_sync()
    hubspot.calls.clear()

    await connector.run_sync()

    archived_listings = {c[1][0] for c in hubspot.calls if c[0] == "list_objects" and c[2].get("archived")}
    assert archived_listings == {"deals", "tickets", "tasks", "notes", "calls", "emails", "contacts", "companies", "products"}


async def test_a_type_without_its_scope_is_skipped_and_the_rest_syncs(hubspot: FakeHubSpot, monkeypatch: pytest.MonkeyPatch) -> None:
    missing = HubSpotResponse(success=False, status_code=403, category="MISSING_SCOPES",
                              error="MISSING_SCOPES: This app hasn't been granted all required scopes (HTTP 403)")
    real_list = hubspot._list_objects

    def list_objects(object_type: str, **kwargs: object) -> HubSpotResponse:
        return missing if object_type == "emails" else real_list(object_type, **kwargs)
    hubspot._list_objects = list_objects
    connector = _connector(hubspot, monkeypatch)

    await connector.run_sync()

    assert {"0-3:11", "0-5:21", "file:7"} <= set(_written(connector))
    # Nothing is deleted on the strength of a listing that did not run.
    assert not connector.full_sync_coverage().complete
    [notice] = connector.notify.await_args_list
    assert "Emails" in notice.args[3] and "sales-email-read" in notice.args[3]
