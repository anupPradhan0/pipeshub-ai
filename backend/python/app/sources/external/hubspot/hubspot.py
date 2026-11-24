"""HubSpot data source: the SDK calls a sync connector needs, made awaitable.

Every method runs the synchronous SDK call on ``executor`` (pass the connector's
``self._thread_lease(n)``; ``None`` falls back to the loop's default executor) and
returns a ``HubSpotResponse``. A HubSpot refusal or a network failure comes back as
``success=False`` with the status and HubSpot's own reason; anything else (a bug,
a bad argument) raises.

``object_type`` is a CRM object type name or id: ``contacts``, ``companies``,
``deals``, ``tickets``, ``notes``, ``calls``, ``emails``, ``meetings``, ``tasks``,
or a custom object's ``2-123456``.
"""

import asyncio
import functools
import json
from collections.abc import Callable
from concurrent.futures import Executor
from datetime import datetime
from typing import Any

from hubspot.crm.associations.v4 import BasicApi as AssociationsApi
from hubspot.crm.associations.v4 import BatchApi as AssociationsBatchApi
from hubspot.crm.associations.v4.models import (
    BatchInputPublicFetchAssociationsBatchRequest,
    PublicFetchAssociationsBatchRequest,
)
from hubspot.crm.objects import BasicApi as ObjectsApi
from hubspot.crm.objects import BatchApi as ObjectsBatchApi
from hubspot.crm.objects import SearchApi as ObjectsSearchApi
from hubspot.crm.objects.models import (
    BatchReadInputSimplePublicObjectId,
    PublicObjectSearchRequest,
    SimplePublicObjectId,
)
from hubspot.crm.owners import OwnersApi
from hubspot.crm.pipelines import PipelinesApi
from hubspot.crm.properties import CoreApi as PropertiesApi
from hubspot.files import FilesApi
from hubspot.settings.users import TeamsApi, UsersApi
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from app.sources.client.hubspot.hubspot import HubSpotClient, HubSpotResponse
from app.sources.client.resilience import parse_retry_after
from app.utils.url_redaction import redact_urls_in_text

# (connect, read) seconds. The SDK waits forever by default, which would hold a
# leased worker thread on a stalled socket.
DEFAULT_REQUEST_TIMEOUT = (10.0, 60.0)
_MAX_ERROR_TEXT = 500


def _is_api_exception(exc: Exception) -> bool:
    # Each SDK sub-package generates its own ApiException with no shared base.
    return any(cls.__name__ == "ApiException" for cls in type(exc).__mro__)


def _error_response(exc: Exception) -> HubSpotResponse:
    if not _is_api_exception(exc):
        detail = redact_urls_in_text(str(exc))[:_MAX_ERROR_TEXT]
        return HubSpotResponse(success=False, error=f"{type(exc).__name__}: {detail}")

    status = getattr(exc, "status", None)
    body: Any = getattr(exc, "body", None)
    try:
        payload = json.loads(body) if body else {}
    except (TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    # Rate-limit bodies carry errorType instead of category.
    category = payload.get("category") or payload.get("errorType")
    message = payload.get("message") or getattr(exc, "reason", None) or "no message"
    message = redact_urls_in_text(str(message))[:_MAX_ERROR_TEXT]
    correlation = f", correlationId {payload['correlationId']}" if payload.get("correlationId") else ""
    headers = getattr(exc, "headers", None)
    return HubSpotResponse(
        success=False,
        error=f"{category or 'ERROR'}: {message} (HTTP {status}{correlation})",
        status_code=status,
        category=category,
        policy_name=payload.get("policyName"),
        retry_after=parse_retry_after(headers) if headers else None,
    )


def _to_data(result: object) -> object:
    to_dict = getattr(result, "to_dict", None)
    return to_dict() if callable(to_dict) else result


def _drop_none(**kwargs: object) -> dict[str, object]:
    return {key: value for key, value in kwargs.items() if value is not None}


class HubSpotDataSource:
    def __init__(
        self,
        client: HubSpotClient,
        executor: Executor | None = None,
        request_timeout: float | tuple[float, float] = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self._sdk = client.get_client()
        self._executor = executor
        self._request_timeout = request_timeout

    async def _call(self, fn: Callable[..., object], *args: object, **kwargs: object) -> HubSpotResponse:
        call = functools.partial(fn, *args, _request_timeout=self._request_timeout, **kwargs)
        try:
            result = await asyncio.get_running_loop().run_in_executor(self._executor, call)
        except Exception as exc:
            if not (_is_api_exception(exc) or isinstance(exc, Urllib3HTTPError)):
                raise
            return _error_response(exc)
        return HubSpotResponse(success=True, data=_to_data(result))

    # Every SDK property access builds a new ApiClient and connection pool, so build
    # each API once per data source.
    @functools.cached_property
    def _objects(self) -> ObjectsApi:
        return self._sdk.crm.objects.basic_api

    @functools.cached_property
    def _objects_batch(self) -> ObjectsBatchApi:
        return self._sdk.crm.objects.batch_api

    @functools.cached_property
    def _objects_search(self) -> ObjectsSearchApi:
        return self._sdk.crm.objects.search_api

    @functools.cached_property
    def _associations(self) -> AssociationsApi:
        return self._sdk.crm.associations.v4.basic_api

    @functools.cached_property
    def _associations_batch(self) -> AssociationsBatchApi:
        return self._sdk.crm.associations.v4.batch_api

    @functools.cached_property
    def _properties(self) -> PropertiesApi:
        return self._sdk.crm.properties.core_api

    @functools.cached_property
    def _pipelines(self) -> PipelinesApi:
        return self._sdk.crm.pipelines.pipelines_api

    @functools.cached_property
    def _owners(self) -> OwnersApi:
        return self._sdk.crm.owners.owners_api

    @functools.cached_property
    def _users(self) -> UsersApi:
        return self._sdk.settings.users.users_api

    @functools.cached_property
    def _teams(self) -> TeamsApi:
        return self._sdk.settings.users.teams_api

    @functools.cached_property
    def _files(self) -> FilesApi:
        return self._sdk.files.files_api

    # ---- account ----

    async def get_account_details(self) -> HubSpotResponse:
        """``portalId`` and ``uiDomain`` (``app.hubspot.com``, ``app-eu1.hubspot.com``), for links into HubSpot.

        The SDK has no API class for this endpoint; its generic ``call_api`` keeps the
        same retry, timeout and ``ApiException`` as every other call.
        """
        return await self._call(
            self._owners.api_client.call_api,
            "/account-info/v3/details",
            "GET",
            response_types_map={200: "object"},
            auth_settings=["oauth2"],
            _return_http_data_only=True,
        )

    # ---- CRM objects ----

    async def list_objects(
        self,
        object_type: str,
        *,
        limit: int = 100,
        after: str | None = None,
        properties: list[str] | None = None,
        associations: list[str] | None = None,
        archived: bool = False,
    ) -> HubSpotResponse:
        """One page of objects; the next cursor is ``data["paging"]["next"]["after"]``. Max limit 100."""
        return await self._call(
            self._objects.get_page,
            object_type,
            archived=archived,
            **_drop_none(limit=limit, after=after, properties=properties, associations=associations),
        )

    async def get_object(
        self,
        object_type: str,
        object_id: str,
        *,
        properties: list[str] | None = None,
        associations: list[str] | None = None,
        archived: bool = False,
    ) -> HubSpotResponse:
        return await self._call(
            self._objects.get_by_id,
            object_type,
            object_id,
            archived=archived,
            **_drop_none(properties=properties, associations=associations),
        )

    async def batch_read_objects(
        self,
        object_type: str,
        object_ids: list[str],
        *,
        properties: list[str] | None = None,
        archived: bool = False,
    ) -> HubSpotResponse:
        """Up to 100 ids. Ids HubSpot cannot find are listed in ``data["errors"]``, not raised."""
        request = BatchReadInputSimplePublicObjectId(
            inputs=[SimplePublicObjectId(id=object_id) for object_id in object_ids],
            properties=properties or [],
            properties_with_history=[],
        )
        return await self._call(self._objects_batch.read, object_type, request, archived=archived)

    async def search_objects(
        self,
        object_type: str,
        *,
        filter_groups: list[dict[str, Any]] | None = None,
        sorts: list[dict[str, str]] | None = None,
        properties: list[str] | None = None,
        query: str | None = None,
        limit: int = 100,
        after: str | None = None,
    ) -> HubSpotResponse:
        """CRM search, in HubSpot's JSON shape.

        ``filter_groups=[{"filters": [{"propertyName": "hs_lastmodifieddate", "operator": "GTE",
        "value": "<epoch ms>"}]}]``, ``sorts=[{"propertyName": "hs_lastmodifieddate",
        "direction": "ASCENDING"}]``. HubSpot stops paging at 10,000 results per query and
        indexes changes a few seconds late; narrow the filter rather than paging past it.
        """
        request = PublicObjectSearchRequest(
            filter_groups=filter_groups or [],
            sorts=sorts or [],
            properties=properties or [],
            query=query,
            limit=limit,
            after=after,
        )
        return await self._call(self._objects_search.do_search, object_type, request)

    # ---- associations (v4) ----

    async def list_associations(
        self,
        object_type: str,
        object_id: str,
        to_object_type: str,
        *,
        limit: int = 500,
        after: str | None = None,
    ) -> HubSpotResponse:
        return await self._call(
            self._associations.get_page,
            object_type,
            object_id,
            to_object_type,
            **_drop_none(limit=limit, after=after),
        )

    async def batch_read_associations(
        self,
        from_object_type: str,
        to_object_type: str,
        object_ids: list[str],
    ) -> HubSpotResponse:
        """Associations of up to 1,000 objects; each result pages on its own ``paging.next.after``."""
        request = BatchInputPublicFetchAssociationsBatchRequest(
            inputs=[PublicFetchAssociationsBatchRequest(id=object_id) for object_id in object_ids],
        )
        return await self._call(self._associations_batch.get_page, from_object_type, to_object_type, request)

    # ---- schema: property and pipeline labels ----

    async def list_properties(self, object_type: str, *, archived: bool = False) -> HubSpotResponse:
        return await self._call(self._properties.get_all, object_type, archived=archived)

    async def list_pipelines(self, object_type: str) -> HubSpotResponse:
        return await self._call(self._pipelines.get_all, object_type)

    # ---- people ----

    async def list_owners(
        self,
        *,
        limit: int = 100,
        after: str | None = None,
        email: str | None = None,
        archived: bool = False,
    ) -> HubSpotResponse:
        """Owners are the users CRM objects are assigned to (``hubspot_owner_id``)."""
        return await self._call(
            self._owners.get_page,
            archived=archived,
            **_drop_none(limit=limit, after=after, email=email),
        )

    async def list_users(self, *, limit: int = 100, after: str | None = None) -> HubSpotResponse:
        """Account users with their role and teams. Needs the ``settings.users.read`` scope."""
        return await self._call(self._users.get_page, **_drop_none(limit=limit, after=after))

    async def list_teams(self) -> HubSpotResponse:
        """Every team with its user ids. Needs ``settings.users.teams.read``."""
        return await self._call(self._teams.get_all)

    async def list_roles(self) -> HubSpotResponse:
        """Permission sets ("roles"). Only Enterprise accounts have them; others answer HTTP 400."""
        return await self._call(self._sdk.settings.users.roles_api.get_all)

    # ---- files ----

    async def search_files(
        self,
        *,
        limit: int = 100,
        after: str | None = None,
        ids: list[int] | None = None,
        updated_at_gte: datetime | None = None,
        sort: list[str] | None = None,
    ) -> HubSpotResponse:
        """Paging stops at 10,000 results."""
        return await self._call(
            self._files.do_search,
            **_drop_none(limit=limit, after=after, ids=ids, updated_at_gte=updated_at_gte, sort=sort),
        )

    async def get_file(self, file_id: str) -> HubSpotResponse:
        return await self._call(self._files.get_by_id, file_id)

    async def get_file_signed_url(self, file_id: str, *, expiration_seconds: int | None = None) -> HubSpotResponse:
        """A temporary download URL for a private file. Never log ``data["url"]``."""
        return await self._call(
            self._files.get_signed_url, file_id, **_drop_none(expiration_seconds=expiration_seconds)
        )
