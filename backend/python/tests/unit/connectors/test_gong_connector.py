"""Unit tests for the Gong connector's pure mapping logic.

These cover the paths that fail silently in production: leaking a call to an
external participant, never re-indexing a call whose participants changed, and
pagination that does not terminate.
"""

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from aiolimiter import AsyncLimiter

from app.config.constants.arangodb import ProgressStatus
from app.connectors.core.registry.filters import FilterCollection
from app.connectors.sources.gong.connector import (
    GongCall,
    GongCallMetaData,
    GongConnector,
    GongCrmContext,
    GongCrmObject,
    GongParty,
    GongTranscriptMonologue,
    GongTranscriptSentence,
    GongWorkspace,
)
from app.models.permission import EntityType, PermissionType


def _connector(
    user_email_by_id: dict[str, str] | None = None,
    indexing_filters: FilterCollection | None = None,
) -> GongConnector:
    """Build a connector shell with only the attributes the mappers touch.

    __init__ opens a SyncPoint (needs SECRET_KEY and the encryption service), and
    none of the logic under test goes near it.
    """
    connector = object.__new__(GongConnector)
    connector.connector_id = "gong-connector-1"
    connector.data_entities_processor = SimpleNamespace(org_id="org-1")
    connector.indexing_filters = indexing_filters or FilterCollection()
    connector._user_email_by_id = user_email_by_id or {}
    return connector


def _call(
    *,
    parties: list[GongParty] | None = None,
    context: list[GongCrmContext] | None = None,
    primary_user_id: str = "u-1",
) -> GongCall:
    return GongCall(
        metaData=GongCallMetaData(
            id="call-1",
            url="https://app.gong.io/call?id=call-1",
            title="Acme discovery call",
            started="2026-05-01T10:00:00Z",
            duration=1800,
            primaryUserId=primary_user_id,
            workspaceId="ws-1",
        ),
        parties=parties or [],
        context=context or [],
    )


def test_permissions_grant_owner_and_internal_but_never_external() -> None:
    call = _call(parties=[
        GongParty(id="p1", userId="u-1", emailAddress="rep@acme.com",
                  name="Rep", affiliation="Internal"),
        GongParty(id="p2", userId="u-2", emailAddress="manager@acme.com",
                  name="Manager", affiliation="Internal"),
        GongParty(id="p3", emailAddress="buyer@customer.com",
                  name="Buyer", affiliation="External"),
    ])

    permissions = _connector()._build_call_permissions(call)
    by_email = {p.email: p for p in permissions}

    assert "buyer@customer.com" not in by_email, "external party must not get access"
    assert by_email["rep@acme.com"].type == PermissionType.OWNER
    assert by_email["manager@acme.com"].type == PermissionType.READ
    assert all(p.entity_type == EntityType.USER for p in permissions)
    assert len(permissions) == 2


def test_unknown_affiliation_is_internal_only_when_gong_resolved_a_user() -> None:
    call = _call(parties=[
        GongParty(id="p1", userId="u-9", emailAddress="staff@acme.com",
                  affiliation="Unknown"),
        GongParty(id="p2", emailAddress="stranger@elsewhere.com",
                  affiliation="Unknown"),
    ], primary_user_id="")

    emails = {p.email for p in _connector()._build_call_permissions(call)}

    assert emails == {"staff@acme.com"}


def test_primary_user_email_falls_back_to_the_user_map() -> None:
    # The primary user did not appear in parties (Gong omits them at times).
    call = _call(parties=[
        GongParty(id="p3", emailAddress="buyer@customer.com", affiliation="External"),
    ])

    connector = _connector(user_email_by_id={"u-1": "rep@acme.com"})

    assert connector._primary_user_email(call) == "rep@acme.com"
    owner = _connector(user_email_by_id={"u-1": "rep@acme.com"})._build_call_permissions(call)
    assert [(p.email, p.type) for p in owner] == [
        ("rep@acme.com", PermissionType.OWNER)
    ]


def test_revision_id_tracks_late_arriving_parties_and_crm_links() -> None:
    """Gong attaches participants and CRM objects after a call ends. If those do
    not move the revision id, _process_record treats the call as unchanged and
    the new context is never indexed."""
    connector = _connector()
    bare = connector._build_call_record(_call(), "group-1", "ws-1")

    with_party = connector._build_call_record(
        _call(parties=[GongParty(id="p1", emailAddress="rep@acme.com")]),
        "group-1",
        "ws-1",
    )
    with_crm = connector._build_call_record(
        _call(context=[GongCrmContext(
            system="Salesforce",
            objects=[GongCrmObject(objectType="Opportunity", objectId="006")],
        )]),
        "group-1",
        "ws-1",
    )

    assert bare.external_revision_id != with_party.external_revision_id
    assert bare.external_revision_id != with_crm.external_revision_id


def test_call_record_maps_core_fields() -> None:
    record = _connector()._build_call_record(_call(), "group-1", "ws-1")

    assert record.external_record_id == "call-1"
    assert record.record_name == "Acme discovery call"
    assert record.weburl == "https://app.gong.io/call?id=call-1"
    assert record.duration_minutes == 30
    assert record.end_time == "2026-05-01T10:30:00Z"
    assert record.record_group_id == "group-1"
    assert record.indexing_status != ProgressStatus.AUTO_INDEX_OFF.value


def test_record_group_external_id_matches_the_group_not_the_raw_workspace_id() -> None:
    """DataSourceEntitiesProcessor._handle_record_group resolves a record's group
    by external_record_group_id alone, ignoring record_group_id. If it does not
    match the RecordGroup's external_group_id, the processor silently creates a
    duplicate group named after the raw id — or, when the field is empty, skips
    the BELONGS_TO edge entirely and leaves record_group_id dangling."""
    connector = _connector()

    named = GongWorkspace(id="ws-1", name="EMEA")
    assert connector._workspace_external_id(named) == "ws-1"

    # Fallback workspace: id is blank, so the group and its records must agree on
    # the stand-in rather than falling back to metaData.workspaceId.
    fallback = GongWorkspace(id="", name="Gong")
    external_id = connector._workspace_external_id(fallback)
    assert external_id == "gong"

    group, _ = connector._build_record_group(fallback)
    record = connector._build_call_record(_call(), group.id, external_id)

    assert group.external_group_id == external_id
    assert record.external_record_group_id == group.external_group_id
    assert record.external_record_group_id != _call().metaData.workspaceId


def test_checkpoint_without_an_offset_does_not_crash_the_window_comparison() -> None:
    """A naive isoformat value parses fine, then explodes on comparison with the
    timezone-aware window end."""
    connector = _connector()
    connector.logger = logging.getLogger("gong-test")
    connector.sync_point = SimpleNamespace(
        read_sync_point=lambda _key: _async({"last_sync_at": "2026-05-01T10:00:00"})
    )
    connector.sync_filters = FilterCollection()

    start = asyncio.run(connector._resolve_sync_start("ws-1"))

    assert start.tzinfo is not None
    assert start < datetime.now(tz=timezone.utc)


def test_disabling_the_calls_filter_marks_records_auto_index_off() -> None:
    filters = FilterCollection.from_dict(
        {"calls": {"value": False, "operator": "is", "type": "boolean"}}
    )
    record = _connector(indexing_filters=filters)._build_call_record(_call(), "group-1", "ws-1")

    assert record.indexing_status == ProgressStatus.AUTO_INDEX_OFF.value


def test_next_cursor_returns_none_when_the_page_is_last() -> None:
    # A cursor that never goes falsy turns every paginated loop into an infinite one.
    assert GongConnector._next_cursor({"records": {"cursor": "abc"}}) == "abc"
    assert GongConnector._next_cursor({"records": {"totalRecords": 3}}) is None
    assert GongConnector._next_cursor({"records": {"cursor": ""}}) is None
    assert GongConnector._next_cursor({}) is None
    assert GongConnector._next_cursor(None) is None


def test_transcript_markdown_labels_speakers_and_topics() -> None:
    call = _call(parties=[
        GongParty(id="spk-1", name="Rep", emailAddress="rep@acme.com"),
    ])
    monologues = [
        GongTranscriptMonologue(
            speakerId="spk-1",
            topic="Pricing",
            sentences=[
                GongTranscriptSentence(text="Let's talk numbers."),
                GongTranscriptSentence(text="Our starting tier is $50."),
            ],
        ),
        GongTranscriptMonologue(
            speakerId="spk-unknown",
            topic="Pricing",
            sentences=[GongTranscriptSentence(text="That works.")],
        ),
    ]

    markdown = _connector()._build_transcript_markdown(monologues, call)

    assert "### Pricing" in markdown
    assert markdown.count("### Pricing") == 1, "topic heading must not repeat"
    assert "**Rep:** Let's talk numbers. Our starting tier is $50." in markdown
    assert "**Unknown speaker:** That works." in markdown


def test_transcript_markdown_is_empty_without_monologues() -> None:
    assert _connector()._build_transcript_markdown([], None) == ""


def test_crm_markdown_renders_objects_and_their_fields() -> None:
    call = _call(context=[GongCrmContext(
        system="Salesforce",
        objects=[GongCrmObject(
            objectType="Opportunity",
            objectId="006XYZ",
            fields=[
                {"name": "name", "value": "Acme - New Business"},
                {"name": "stage", "value": "Negotiation"},
                {"name": "amount", "value": None},
            ],
        )],
    )])

    markdown = GongConnector._build_crm_markdown(call)

    assert "### Salesforce" in markdown
    assert "- **Opportunity**: Acme - New Business" in markdown
    assert "  - stage: Negotiation" in markdown
    assert "amount" not in markdown, "empty CRM fields should be dropped"


def test_participants_markdown_lists_every_party() -> None:
    call = _call(parties=[
        GongParty(id="p1", name="Rep", emailAddress="rep@acme.com",
                  title="AE", affiliation="Internal"),
        GongParty(id="p2", name="Buyer", emailAddress="buyer@customer.com",
                  affiliation="External"),
    ])

    markdown = GongConnector._build_participants_markdown(call)

    assert "| Rep | rep@acme.com | AE | Internal |" in markdown
    assert "| Buyer | buyer@customer.com | — | External |" in markdown


def test_iso_always_carries_an_offset() -> None:
    formatted = GongConnector._iso(datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc))

    assert formatted == "2026-05-01T10:00:00Z"


def test_a_failed_call_page_raises_instead_of_reporting_an_empty_page() -> None:
    """Returning ([], None) here would let _sync_workspace_calls advance its
    checkpoint past a window it never read, skipping those calls forever."""
    connector = _connector()
    connector.logger = logging.getLogger("gong-test")
    connector.rate_limiter = AsyncLimiter(100, 1)

    failing = SimpleNamespace(
        get_calls_extensive=lambda **_: _async(
            SimpleNamespace(
                success=False, data=None, error="429 Too Many Requests", message="failed"
            )
        )
    )

    async def _fresh() -> object:
        return failing

    connector._get_fresh_datasource = _fresh

    with pytest.raises(RuntimeError, match="429 Too Many Requests"):
        asyncio.run(connector._list_calls_page(
            workspace_id="ws-1",
            from_dt=datetime(2026, 5, 1, tzinfo=timezone.utc),
            to_dt=datetime(2026, 5, 2, tzinfo=timezone.utc),
            cursor=None,
        ))


async def _async(value: object) -> object:
    return value
