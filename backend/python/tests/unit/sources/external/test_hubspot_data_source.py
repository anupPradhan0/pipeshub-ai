"""HubSpot client and data source against a fake HubSpot at urllib3's request hook.

Faking below the SDK keeps its real request building, retry and exception mapping
in the test; nothing reaches the network.
"""

import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import urllib3
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.util.retry import Retry

from app.sources.client.hubspot.hubspot import HubSpotClient
from app.sources.external.hubspot.hubspot import HubSpotDataSource


class FakeHubSpot:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._queue: list[tuple[int, Any, dict[str, str]]] = []

    def reply(self, status: int, body: object, headers: dict[str, str] | None = None) -> None:
        self._queue.append((status, body, headers or {}))

    def make_request(self, pool: HTTPConnectionPool, conn: object, method: str, url: str, body: bytes | None = None,
                     headers: dict[str, str] | None = None, **kwargs: object) -> urllib3.HTTPResponse:
        self.calls.append({
            "host": pool.host,
            "method": method,
            "url": url,
            "body": json.loads(body) if body else None,
            "auth": (headers or {}).get("Authorization"),
        })
        status, payload, extra = self._queue.pop(0) if self._queue else (200, {}, {})
        return urllib3.HTTPResponse(
            body=io.BytesIO(json.dumps(payload).encode()),
            status=status,
            headers={"Content-Type": "application/json", **extra},
            preload_content=kwargs.get("preload_content", True),
            request_method=method,
            request_url=url,
        )


@pytest.fixture
def hubspot(monkeypatch: pytest.MonkeyPatch) -> FakeHubSpot:
    fake = FakeHubSpot()

    def _make_request(pool: HTTPConnectionPool, conn: object, method: str, url: str, **kwargs: object) -> urllib3.HTTPResponse:
        return fake.make_request(pool, conn, method, url, **kwargs)

    monkeypatch.setattr(HTTPConnectionPool, "_make_request", _make_request)
    monkeypatch.setattr(Retry, "sleep", lambda self, response=None: None)
    return fake


@pytest.fixture
def ds() -> HubSpotDataSource:
    return HubSpotDataSource(HubSpotClient.build_with_token("pat-test"))


ENDPOINTS = [
    ("list_objects", ("contacts",), {"properties": ["email", "firstname"], "after": "50"}, "GET",
     "/crm/v3/objects/contacts?limit=100&after=50&properties=email&properties=firstname&archived=False"),
    ("get_object", ("deals", "42"), {"associations": ["companies"]}, "GET",
     "/crm/v3/objects/deals/42?associations=companies&archived=False"),
    ("batch_read_objects", ("tickets", ["1", "2"]), {"properties": ["subject"]}, "POST",
     "/crm/v3/objects/tickets/batch/read?archived=False"),
    ("search_objects", ("companies",), {}, "POST", "/crm/v3/objects/companies/search"),
    ("list_associations", ("deals", "42", "contacts"), {}, "GET",
     "/crm/v4/objects/deals/42/associations/contacts?limit=500"),
    ("batch_read_associations", ("deals", "contacts", ["42"]), {}, "POST",
     "/crm/v4/associations/deals/contacts/batch/read"),
    ("list_properties", ("deals",), {}, "GET", "/crm/v3/properties/deals?archived=False"),
    ("list_pipelines", ("tickets",), {}, "GET", "/crm/v3/pipelines/tickets"),
    ("list_owners", (), {}, "GET", "/crm/v3/owners/?limit=100&archived=False"),
    ("list_users", (), {}, "GET", "/settings/v3/users/?limit=100"),
    ("list_teams", (), {}, "GET", "/settings/v3/users/teams"),
    ("search_files", (), {"ids": [7]}, "GET", "/files/v3/files/search?limit=100&ids=7"),
    ("get_file", ("7",), {}, "GET", "/files/v3/files/7"),
    ("get_file_signed_url", ("7",), {"expiration_seconds": 60}, "GET",
     "/files/v3/files/7/signed-url?expirationSeconds=60"),
]


@pytest.mark.parametrize(("method", "args", "kwargs", "http_method", "path"), ENDPOINTS, ids=[e[0] for e in ENDPOINTS])
async def test_each_endpoint_sends_its_request_with_the_token(
    hubspot: FakeHubSpot, ds: HubSpotDataSource, method: str, args: tuple, kwargs: dict, http_method: str, path: str,
) -> None:
    response = await getattr(ds, method)(*args, **kwargs)

    assert response.success, response.error
    call = hubspot.calls[0]
    assert call["host"] == "api.hubapi.com"
    assert (call["method"], call["url"]) == (http_method, path)
    assert call["auth"] == "Bearer pat-test"


async def test_request_bodies_are_in_hubspots_json_shape(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    filters = [{"filters": [{"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": "1700000000000"}]}]
    sorts = [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}]
    await ds.search_objects("deals", filter_groups=filters, sorts=sorts, properties=["dealname"], after="100")
    await ds.batch_read_objects("tickets", ["1", "2"], properties=["subject"])
    await ds.batch_read_associations("deals", "contacts", ["42"])

    search, batch, assoc = (call["body"] for call in hubspot.calls)
    assert search == {"filterGroups": filters, "sorts": sorts, "properties": ["dealname"], "limit": 100, "after": "100"}
    assert batch == {"inputs": [{"id": "1"}, {"id": "2"}], "properties": ["subject"], "propertiesWithHistory": []}
    assert assoc == {"inputs": [{"id": "42"}]}


async def test_pages_until_hubspot_stops_returning_a_cursor(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    hubspot.reply(200, {"results": [{"id": "1", "properties": {}, "archived": False}],
                        "paging": {"next": {"after": "1"}}})
    hubspot.reply(200, {"results": [{"id": "2", "properties": {}, "archived": False}]})

    ids, after = [], None
    while True:
        response = await ds.list_objects("contacts", after=after)
        assert response.success
        ids += [row["id"] for row in response.data["results"]]
        after = ((response.data.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            break

    assert ids == ["1", "2"]
    assert "after=1" in hubspot.calls[1]["url"]


async def test_401_carries_hubspots_category_and_message(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    hubspot.reply(401, {"status": "error", "message": "Authentication credentials not found.",
                        "category": "INVALID_AUTHENTICATION", "correlationId": "c-1"})

    response = await ds.list_owners()

    assert not response.success
    assert response.status_code == 401
    assert response.category == "INVALID_AUTHENTICATION"
    assert response.error == "INVALID_AUTHENTICATION: Authentication credentials not found. (HTTP 401, correlationId c-1)"
    assert "pat-test" not in response.error
    assert len(hubspot.calls) == 1


async def test_429_is_returned_at_once_with_retry_after_and_policy(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    hubspot.reply(429, {"status": "error", "message": "You have reached your secondly limit.",
                        "errorType": "RATE_LIMIT", "policyName": "SECONDLY"}, {"Retry-After": "2"})

    response = await ds.list_objects("deals")

    assert not response.success
    assert (response.status_code, response.category, response.policy_name, response.retry_after) == (
        429, "RATE_LIMIT", "SECONDLY", 2.0)
    assert len(hubspot.calls) == 1


async def test_5xx_on_a_read_is_retried(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    hubspot.reply(502, {"message": "Bad gateway"})
    hubspot.reply(200, {"results": []})

    response = await ds.list_objects("deals")

    assert response.success
    assert len(hubspot.calls) == 2


async def test_5xx_on_a_post_is_not_retried(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    hubspot.reply(503, {"message": "Service unavailable", "category": "SERVICE_UNAVAILABLE"})

    response = await ds.search_objects("deals")

    assert (response.success, response.status_code) == (False, 503)
    assert len(hubspot.calls) == 1


async def test_5xx_that_persists_surfaces_its_status(hubspot: FakeHubSpot, ds: HubSpotDataSource) -> None:
    for _ in range(4):
        hubspot.reply(500, {"message": "Internal error"})

    response = await ds.get_file("7")

    assert (response.success, response.status_code) == (False, 500)
    assert len(hubspot.calls) == 4


async def test_network_failure_is_a_failed_response(monkeypatch: pytest.MonkeyPatch, ds: HubSpotDataSource) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise urllib3.exceptions.NewConnectionError(None, "Connection refused")

    monkeypatch.setattr(HTTPConnectionPool, "_make_request", refuse)
    monkeypatch.setattr(Retry, "sleep", lambda self, response=None: None)

    response = await ds.list_teams()

    assert not response.success
    assert response.status_code is None
    assert "Connection refused" in response.error


async def test_a_programming_error_is_raised_not_returned(ds: HubSpotDataSource) -> None:
    with pytest.raises(TypeError):
        await ds.list_objects("deals", no_such_argument=True)  # type: ignore[call-arg]


async def test_calls_run_on_the_given_executor(hubspot: FakeHubSpot) -> None:
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="hubspot-lease") as executor:
        seen: list[str] = []
        original = hubspot.make_request

        def record_thread(*args: object, **kwargs: object) -> urllib3.HTTPResponse:
            seen.append(threading.current_thread().name)
            return original(*args, **kwargs)

        hubspot.make_request = record_thread  # type: ignore[method-assign]
        ds = HubSpotDataSource(HubSpotClient.build_with_token("pat-test"), executor=executor)
        await ds.list_teams()

    assert seen and seen[0].startswith("hubspot-lease")


def _config_service(config: dict | None) -> AsyncMock:
    service = AsyncMock()
    service.get_config.return_value = config
    return service


@pytest.mark.parametrize(("config", "token"), [
    ({"auth": {"authType": "OAUTH"}, "credentials": {"access_token": "oauth-token"}}, "oauth-token"),
    ({"auth": {"authType": "API_TOKEN", "apiToken": "service-key"}}, "service-key"),
])
async def test_build_from_services_reads_the_instance_token(config: dict, token: str) -> None:
    service = _config_service(config)

    client = await HubSpotClient.build_from_services(MagicMock(), service, "inst-1")

    assert client.get_client().access_token == token
    service.get_config.assert_awaited_once_with("/services/connectors/inst-1/config", raise_on_error=True)


@pytest.mark.parametrize("config", [
    None,
    {"auth": {"authType": "OAUTH"}, "credentials": {}},
    {"auth": {"authType": "API_TOKEN"}},
    {"auth": {"authType": "BASIC"}},
])
async def test_build_from_services_refuses_missing_or_unknown_auth(config: dict | None) -> None:
    with pytest.raises(ValueError):
        await HubSpotClient.build_from_services(MagicMock(), _config_service(config), "inst-1")
