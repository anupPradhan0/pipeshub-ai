"""HubSpot connector: CRM objects, activities, products and files, with HubSpot's record access."""

import asyncio
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from http import HTTPStatus
from logging import Logger
from typing import Any

from fastapi.responses import StreamingResponse

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    AppGroups,
    Connectors,
    MimeTypes,
    PermissionModel,
)
from app.connectors.core.base.connector.connector_service import (
    BaseConnector,
    ConnectorInitError,
)
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.base.error.stream_errors import (
    connector_not_ready,
    map_source_status,
    not_found_at_source,
)
from app.connectors.core.base.sync_point.sync_point import (
    FailedItems,
    SyncDataPointType,
    SyncPoint,
)
from app.connectors.core.base.token_service.startup_service import startup_service
from app.connectors.core.constants import CONNECTOR_EMAIL_IDENTITY_INFO, IconPaths
from app.connectors.core.registry.auth_builder import (
    AuthBuilder,
    AuthType,
    OAuthScopeConfig,
)
from app.connectors.core.registry.connector_builder import (
    AuthField,
    CommonFields,
    ConnectorBuilder,
    ConnectorScope,
    DocumentationLink,
    SyncStrategy,
)
from app.connectors.core.registry.filters import (
    FilterCategory,
    FilterCollection,
    FilterField,
    FilterOption,
    FilterOptionsResponse,
    FilterType,
    ListOperator,
    OptionSourceType,
    load_connector_filters,
)
from app.connectors.core.sync.full_sync_coverage import FullSyncCoverage
from app.connectors.sources.hubspot.common.apps import HubSpotApp
from app.connectors.sources.hubspot.mapping import (
    ALL_USERS_GROUP_ID,
    FILES_GROUP_ID,
    LINKED_PROPERTIES,
    OBJECT_SPECS,
    OWNED_SPECS,
    SCOPE_ALL,
    SCOPE_TEAM,
    SPECS_BY_TYPE_ID,
    SUPER_ADMINS_GROUP_ID,
    TEAM_GROUP_PREFIX,
    USER_GROUP_PREFIX,
    Directory,
    ObjectSpec,
    all_group_id,
    build_file_record,
    build_record,
    build_record_group,
    file_permissions,
    owner_group_id,
    record_permissions,
    render_markdown,
    to_ms,
)
from app.models.blocks import (
    BlockGroup,
    BlocksContainer,
    DataFormat,
    GroupSubType,
    GroupType,
)
from app.models.entities import (
    AppUser,
    AppUserGroup,
    Record,
    RecordGroupType,
    RecordType,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.services.notification.types import NotificationSeverity, NotificationType
from app.sources.client.hubspot.hubspot import HubSpotClient, HubSpotResponse
from app.sources.external.hubspot.hubspot import HubSpotDataSource
from app.utils.streaming import create_stream_record_response, stream_content
from app.utils.time_conversion import get_epoch_timestamp_in_ms

PAGE_SIZE = 100
# HubSpot's search stops at 10,000 results per query; restart from the last time seen before that.
SEARCH_RESULT_CAP = 10_000
# Search and the archive listing show a change a few seconds after it is made.
INDEX_LAG_MS = 5 * 60 * 1000
SDK_THREADS = 4
FILES_SYNC_KEY = "files"
ROLE_OPTION = "role:"
USER_OPTION = "user:"
HUBSPOT_SCOPES = [
    "oauth",
    "crm.objects.deals.read",
    "crm.objects.contacts.read",
    "crm.objects.companies.read",
    "crm.objects.owners.read",
    "crm.objects.tickets.read",
    "crm.objects.line_items.read",
    "crm.objects.products.read",
    "sales-email-read",
    "files.read",
    "settings.users.read",
    "settings.users.teams.read",
]
CONFIG_PATH = "/services/connectors/{connector_id}/config"


class HubSpotApiError(Exception):
    """A call HubSpot refused, or that never reached it. The message is HubSpot's own reason."""

    def __init__(self, operation: str, response: HubSpotResponse) -> None:
        super().__init__(f"{operation} failed: {response.error}")
        self.response = response


class HubSpotAuthError(HubSpotApiError):
    pass


class HubSpotQuotaError(HubSpotApiError):
    pass


def _next_after(data: Mapping[str, Any]) -> str | None:
    return ((data.get("paging") or {}).get("next") or {}).get("after")


def _access_filter_key(object_type: str, scope: str) -> str:
    return f"{object_type}_view_{scope}"


def _access_filters() -> list[FilterField]:
    """Per object type, who HubSpot lets see all records, or their teams' records."""
    fields = []
    for spec in OWNED_SPECS:
        for scope, label, meaning in (
            (SCOPE_ALL, "All", "every record of this type"),
            (SCOPE_TEAM, "Team", "the records owned by anyone on their teams"),
        ):
            fields.append(FilterField(
                name=_access_filter_key(spec.name, scope),
                display_name=f"{spec.group_name}: view access \"{label}\"",
                filter_type=FilterType.LIST,
                category=FilterCategory.SYNC,
                description=(
                    f"Roles or users whose {spec.group_name} view permission in HubSpot is \"{label}\". They read "
                    f"{meaning}. Anyone not listed reads only what they own or what is shared with them."
                ),
                option_source_type=OptionSourceType.DYNAMIC,
                allowed_operators=[ListOperator.IN.value],
            ))
    return fields


def _with_access_filters(builder: Any) -> Any:  # noqa: ANN401 - the registry's builder type is private
    for field in _access_filters():
        builder = builder.add_filter_field(field)
    for spec in OBJECT_SPECS:
        builder = builder.add_filter_field(FilterField(
            name=spec.name, display_name=f"Index {spec.group_name}", filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING, default_value=True,
        ))
    return builder


def _group_grant(external_id: str) -> Permission:
    return Permission(external_id=external_id, type=PermissionType.READ, entity_type=EntityType.GROUP)


@ConnectorBuilder("HubSpot")\
    .in_group(AppGroups.HUBSPOT.value)\
    .with_description("Sync deals, tickets, contacts, companies, products, activities and files from HubSpot")\
    .with_categories(["CRM", "Sales"])\
    .with_scopes([ConnectorScope.TEAM.value])\
    .with_permission_model(PermissionModel.RECORD_LEVEL)\
    .with_auth([
        AuthBuilder.type(AuthType.OAUTH).oauth(
            connector_name="HubSpot",
            authorize_url="https://app.hubspot.com/oauth/authorize",
            token_url="https://api.hubapi.com/oauth/v1/token",
            redirect_uri="connectors/oauth/callback/HubSpot",
            scopes=OAuthScopeConfig(personal_sync=[], team_sync=HUBSPOT_SCOPES, agent=[]),
            fields=[CommonFields.client_id("HubSpot"), CommonFields.client_secret("HubSpot")],
            icon_path=IconPaths.connector_icon(Connectors.HUBSPOT.value),
            app_group=AppGroups.HUBSPOT.value,
            app_description="HubSpot project-based app (OAuth) for syncing HubSpot CRM records",
            app_categories=["CRM", "Sales"],
            documentation_links=[DocumentationLink(
                "HubSpot project-based apps (OAuth)", "https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/oauth/working-with-oauth", "setup",
            )],
        ),
        AuthBuilder.type(AuthType.API_TOKEN).fields([
            AuthField(
                name="apiToken",
                display_name="Service key",
                placeholder="pat-...",
                description="A HubSpot service key with the read scopes listed in the docs",
                field_type="PASSWORD",
                required=True,
                max_length=2000,
                is_secret=True,
            ),
        ]),
    ])\
    .with_info(CONNECTOR_EMAIL_IDENTITY_INFO)\
    .with_resilience_config(rate_limit=9.0, max_retries=4, max_delay=60.0)\
    .configure(lambda builder: _with_access_filters(builder)
        .with_icon(IconPaths.connector_icon(Connectors.HUBSPOT.value))
        .with_realtime_support(False)
        .add_documentation_link(DocumentationLink(
            "HubSpot service keys", "https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/account-service-keys", "setup",
        ))
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 60)
        .with_sync_support(True)
        .with_agent_support(False)
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .add_filter_field(FilterField(name="files", display_name="Index Files", filter_type=FilterType.BOOLEAN,
                                      category=FilterCategory.INDEXING, default_value=True))
    )\
    .build_decorator()
class HubSpotConnector(BaseConnector):
    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str,
        created_by: str,
    ) -> None:
        super().__init__(
            HubSpotApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_name = Connectors.HUBSPOT
        self._client: HubSpotClient | None = None
        self._data_source: HubSpotDataSource | None = None
        self._directory: Directory | None = None
        self._coverage_complete = True
        self.sync_filters: FilterCollection | None = None
        self.indexing_filters: FilterCollection | None = None
        self.records_sync_point = SyncPoint(
            connector_id=connector_id,
            org_id=data_entities_processor.org_id,
            sync_data_point_type=SyncDataPointType.RECORDS,
            data_store_provider=data_store_provider,
        )

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str,
        created_by: str,
        data_entities_processor: DataSourceEntitiesProcessor,
        **kwargs: object,
    ) -> "HubSpotConnector":
        return cls(logger, data_entities_processor, data_store_provider, config_service, connector_id, scope, created_by)

    # ---- client ----

    async def init(self) -> bool:
        try:
            await self._rebuild_client()
        except ValueError as e:
            self.logger.error("HubSpot connector %s could not start: %s", self.connector_id, e)
            raise ConnectorInitError(f"{self.display_name} could not start: {e}. Check its sign-in settings.") from e
        return True

    async def _rebuild_client(self) -> None:
        """Read the token from config again: OAuth tokens last 30 minutes and are refreshed in the background."""
        self._client = await HubSpotClient.build_from_services(self.logger, self.config_service, self.connector_id)
        self._data_source = HubSpotDataSource(self._client, executor=self._thread_lease(SDK_THREADS))

    async def _refresh_after_unauthorized(self) -> bool:
        """After a 401: adopt a token stored since the call, else refresh with the one held before it."""
        client = self._client
        if client is None or client.auth_type != "OAUTH":
            return False
        held_access, held_refresh = client.get_client().access_token, client.refresh_token
        try:
            config = await self.config_service.get_config(
                CONFIG_PATH.format(connector_id=self.connector_id), use_cache=False
            )
            stored = ((config or {}).get("credentials") or {}).get("access_token")
            if not stored or stored == held_access:
                refresh_service = startup_service.get_token_refresh_service()
                if not held_refresh or not refresh_service:
                    self.logger.error("HubSpot returned 401 and there is no refresh token or refresh service")
                    return False
                await refresh_service.refresh_now(self.connector_id, self.connector_name.value, held_refresh)
            await self._rebuild_client()
        except Exception:
            self.logger.exception("Refreshing the HubSpot token after a 401 failed")
            return False
        self.logger.info("Refreshed the HubSpot access token after a 401")
        return True

    async def _api(self, operation: str, method: str, *args: object, **kwargs: object) -> Any:  # noqa: ANN401 - shape depends on the endpoint
        """One HubSpot call under the connector's rate limit. A refusal raises with HubSpot's reason."""
        if self._data_source is None:
            await self.init()
        policy = self.resilience
        refreshed, attempt = False, 0
        while True:
            if policy is not None:
                await policy.acquire()
            response: HubSpotResponse = await getattr(self._data_source, method)(*args, **kwargs)
            if response.success:
                return response.data
            if response.status_code == 401 and not refreshed and await self._refresh_after_unauthorized():
                refreshed = True
                continue
            throttled = response.status_code == 429 and response.policy_name != "DAILY"
            if throttled and policy is not None and attempt < policy.max_retries:
                delay = policy.note_retry(attempt, response.retry_after)
                attempt += 1
                self.logger.warning("HubSpot throttled %s: %s; retrying in %.1fs", operation, response.error, delay)
                await asyncio.sleep(delay)
                continue
            if response.status_code == 401:
                raise HubSpotAuthError(operation, response)
            if response.status_code == 429:
                raise HubSpotQuotaError(operation, response)
            raise HubSpotApiError(operation, response)

    async def _pages(self, operation: str, method: str, *args: object, **kwargs: object) -> AsyncIterator[list[dict[str, Any]]]:
        after = None
        while True:
            data = await self._api(operation, method, *args, after=after, **kwargs)
            yield list(data.get("results") or [])
            after = _next_after(data)
            if not after:
                return

    # ---- sync ----

    async def run_sync(self) -> None:
        self._coverage_complete = True
        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, "hubspot", self.connector_id, self.logger
        )
        try:
            await self._rebuild_client()
            directory = await self._read_directory()
            await self._sync_people(directory)
            self._directory = directory
            await self._sync_record_groups(directory)
            refused = []
            for spec in OBJECT_SPECS:
                try:
                    await self._sync_object_type(spec, directory)
                except HubSpotApiError as e:
                    # A connection made before a type was added lacks its scope; the other types still sync.
                    if isinstance(e, (HubSpotAuthError, HubSpotQuotaError)) or e.response.status_code != HTTPStatus.FORBIDDEN:
                        raise
                    self.logger.warning("HubSpot connector %s skipped %s: %s", self.connector_id, spec.name, e)
                    self._coverage_complete = False
                    refused.append(spec.group_name)
            await self._sync_files(directory)
            if refused:
                await self.notify(
                    NotificationType.CONNECTOR_SYNC_ERROR,
                    NotificationSeverity.WARNING,
                    self._notification_title("some data was not synced"),
                    f"HubSpot refused {self.display_name} access to {', '.join(refused)}. Sign in again, or give the "
                    f"service key the scopes {', '.join(s for s in HUBSPOT_SCOPES if s != 'oauth')}.",
                )
        except HubSpotAuthError as e:
            self.logger.error("HubSpot sync for connector %s stopped: %s", self.connector_id, e)
            await self.notify(
                NotificationType.CONNECTOR_AUTH_ERROR,
                NotificationSeverity.ERROR,
                self._notification_title("sign-in failed"),
                f"HubSpot rejected the credentials of {self.display_name}: {e.response.error}. Sign in again or replace the token.",
            )
            raise
        except HubSpotQuotaError as e:
            self.logger.error("HubSpot sync for connector %s stopped at the rate limit: %s", self.connector_id, e)
            await self.notify(
                NotificationType.CONNECTOR_SYNC_ERROR,
                NotificationSeverity.WARNING,
                self._notification_title("rate limit reached"),
                f"HubSpot's API limit stopped the sync of {self.display_name}: {e.response.error}. It resumes on the next run.",
            )
            raise

    async def run_incremental_sync(self) -> None:
        await self.run_sync()

    def full_sync_coverage(self) -> FullSyncCoverage:
        return FullSyncCoverage(complete=self._coverage_complete)

    async def _read_directory(self) -> Directory:
        """The account's link address, owners and pipeline labels: what a record's text and links need."""
        account = await self._api("read account details", "get_account_details")
        directory = Directory(
            portal_id=str(account.get("portalId")),
            ui_domain=str(account.get("uiDomain") or "app.hubspot.com"),
        )
        async for page in self._pages("list owners", "list_owners", limit=PAGE_SIZE):
            for owner in page:
                name = " ".join(p for p in (owner.get("first_name"), owner.get("last_name")) if p)
                directory.owners[str(owner["id"])] = {
                    "user_id": str(owner["user_id"]) if owner.get("user_id") else None,
                    "email": owner.get("email"),
                    "name": name or None,
                }
        for object_type in ("deals", "tickets"):
            data = await self._api(f"list {object_type} pipelines", "list_pipelines", object_type)
            for pipeline in data.get("results") or []:
                directory.pipelines[(object_type, str(pipeline["id"]))] = pipeline.get("label")
                for stage in pipeline.get("stages") or []:
                    metadata = stage.get("metadata") or {}
                    directory.stages[(object_type, str(stage["id"]))] = {
                        "label": stage.get("label"),
                        "state": metadata.get("ticketState"),
                    }
        return directory

    async def _sync_people(self, directory: Directory) -> None:
        """Users, then the groups records grant, all rewritten every run.

        A record grants groups, never users, so a user deactivated, moved to another team
        or given another role in HubSpot reads the right records after the next sync, even
        records that never change again.
        """
        app_users: dict[str, AppUser] = {}
        roles: dict[str, set[str]] = {}
        names = {owner["user_id"]: owner.get("name") for owner in directory.owners.values() if owner.get("user_id")}
        async for page in self._pages("list users", "list_users", limit=PAGE_SIZE):
            for user in page:
                user_id, email = str(user["id"]), (user.get("email") or "").strip().lower()
                if not email:
                    continue
                directory.users[user_id] = {"email": email, "super_admin": bool(user.get("super_admin"))}
                roles[user_id] = {str(r) for r in (user.get("role_ids") or []) + [user.get("role_id")] if r}
                app_users[user_id] = AppUser(
                    app_name=Connectors.HUBSPOT,
                    connector_id=self.connector_id,
                    source_user_id=user_id,
                    email=email,
                    full_name=names.get(user_id) or email,
                    is_active=True,
                )
        if app_users:
            await self.data_entities_processor.on_new_app_users(list(app_users.values()))

        async for page in self._pages("list deactivated owners", "list_owners", limit=PAGE_SIZE, archived=True):
            for owner in page:
                user_id = owner.get("user_id_including_inactive") or owner.get("user_id")
                if user_id and str(user_id) not in app_users:
                    directory.deactivated.add(str(user_id))

        teams = (await self._api("list teams", "list_teams")).get("results") or []
        # Secondary teams count, as in HubSpot. Nested teams (Enterprise) do not: the Teams API
        # does not report a team's parent, so a parent team's "Team" viewers miss child-team records.
        teams_of: dict[str, set[str]] = {}
        for team in teams:
            for uid in (team.get("user_ids") or []) + (team.get("secondary_user_ids") or []):
                teams_of.setdefault(str(uid), set()).add(str(team["id"]))

        groups: list[tuple[AppUserGroup, list[AppUser]]] = [(
            self._user_group(SUPER_ADMINS_GROUP_ID, "HubSpot Super Admins"),
            [u for uid, u in app_users.items() if directory.users[uid]["super_admin"]],
        )]
        groups.append((self._user_group(ALL_USERS_GROUP_ID, "HubSpot users"), list(app_users.values())))
        groups += [(self._user_group(f"{USER_GROUP_PREFIX}{uid}", f"HubSpot user {u.email}"), [u]) for uid, u in app_users.items()]
        groups += [(self._user_group(f"{USER_GROUP_PREFIX}{uid}", "HubSpot user (deactivated)"), []) for uid in directory.deactivated]
        for team in teams:
            member_ids = [str(i) for i in (team.get("user_ids") or []) + (team.get("secondary_user_ids") or [])]
            groups.append((
                self._user_group(f"{TEAM_GROUP_PREFIX}{team['id']}", f"HubSpot team {team.get('name') or team['id']}"),
                [app_users[i] for i in dict.fromkeys(member_ids) if i in app_users],
            ))
        for spec in OWNED_SPECS:
            scope_of = self._scopes(spec.name, roles)
            groups.append((
                self._user_group(all_group_id(spec.name), f"HubSpot: all {spec.group_name}"),
                [u for uid, u in app_users.items() if scope_of.get(uid) == SCOPE_ALL],
            ))
            team_viewers = [uid for uid in app_users if scope_of.get(uid) == SCOPE_TEAM]
            for owner_id in list(app_users) + sorted(directory.deactivated):
                owner_teams = teams_of.get(owner_id, set())
                members = [owner_id] if owner_id in app_users else []
                members += [v for v in team_viewers if v != owner_id and teams_of.get(v, set()) & owner_teams]
                groups.append((
                    self._user_group(owner_group_id(owner_id, spec.name), f"HubSpot: {spec.group_name} owned by {owner_id}"),
                    [app_users[m] for m in members],
                ))
        await self.data_entities_processor.on_new_user_groups(groups)
        self.logger.info(
            "HubSpot connector %s: %d users, %d deactivated, %d teams, %d groups",
            self.connector_id, len(app_users), len(directory.deactivated), len(teams), len(groups),
        )

    def _scopes(self, object_type: str, roles: dict[str, set[str]]) -> dict[str, str]:
        """Each user's view scope for one object type, from the admin's lists; "All" wins over "Team"."""
        scope_of: dict[str, str] = {}
        for scope in (SCOPE_TEAM, SCOPE_ALL):
            chosen = set((self.sync_filters.get_value(_access_filter_key(object_type, scope)) if self.sync_filters else None) or [])
            for uid, user_roles in roles.items():
                if f"{USER_OPTION}{uid}" in chosen or any(f"{ROLE_OPTION}{r}" in chosen for r in user_roles):
                    scope_of[uid] = scope
        return scope_of

    def _user_group(self, external_id: str, name: str) -> AppUserGroup:
        return AppUserGroup(
            app_name=Connectors.HUBSPOT, connector_id=self.connector_id, source_user_group_id=external_id, name=name,
        )

    async def _sync_record_groups(self, directory: Directory) -> None:
        # The folders hold no content of their own and records do not inherit from them;
        # every HubSpot user may browse them, and sees inside only what their grants reach.
        base = f"https://{directory.ui_domain}"
        groups = [
            build_record_group(spec.name, spec.group_name, spec.group_type, self.connector_id,
                               f"{base}/contacts/{directory.portal_id}/objects/{spec.type_id}/views/all/list")
            for spec in OBJECT_SPECS
        ]
        groups.append(build_record_group(FILES_GROUP_ID, "Files", RecordGroupType.FILE_SHARE, self.connector_id,
                                         f"{base}/files/{directory.portal_id}/"))
        await self.data_entities_processor.on_new_record_groups(
            [(group, [_group_grant(SUPER_ADMINS_GROUP_ID), _group_grant(ALL_USERS_GROUP_ID)]) for group in groups]
        )

    def _indexing_on(self, key: str) -> bool:
        return self.indexing_filters is None or self.indexing_filters.is_enabled(key, default=True)

    async def _sync_object_type(self, spec: ObjectSpec, directory: Directory) -> None:
        saved = await self.records_sync_point.read_sync_point(spec.name)
        since = (saved or {}).get("lastModifiedMs")
        # Taken before the listing, so a change made while it runs is read next time.
        started = get_epoch_timestamp_in_ms() - INDEX_LAG_MS
        newest = since or 0
        failed = FailedItems()
        if since is None:
            pages = self._pages(f"list {spec.name}", "list_objects", spec.name, limit=PAGE_SIZE, properties=spec.all_properties)
        else:
            pages = self._search_pages(spec, int(since))
        written = 0
        async for objects in pages:
            await self._rebuild_client()
            stamps = [to_ms((o.get("properties") or {}).get("hs_lastmodifieddate")) or to_ms(o.get("updated_at")) for o in objects]
            try:
                written += await self._write_objects(spec, objects, directory)
            except (HubSpotAuthError, HubSpotQuotaError):
                raise
            except Exception:
                self.logger.exception(
                    "HubSpot connector %s: writing %d %s (ids %s..%s) failed",
                    self.connector_id, len(objects), spec.name, objects[0].get("id"), objects[-1].get("id"),
                )
                self._coverage_complete = False
                for stamp in stamps:
                    failed.add(stamp)
            newest = max([newest, *[s for s in stamps if s]])
        checkpoint = started if since is None else min(newest, started)
        await self.records_sync_point.update_sync_point(spec.name, {"lastModifiedMs": failed.checkpoint(checkpoint)})
        self.logger.info("HubSpot connector %s: %d %s written, %d failed", self.connector_id, written, spec.name, failed.count)
        if since is not None and spec.lists_archived:
            await self._delete_archived(spec, int(since))

    async def _search_pages(self, spec: ObjectSpec, since: int) -> AsyncIterator[list[dict[str, Any]]]:
        """Objects modified at or after ``since``, oldest first, restarting the query before HubSpot's result cap."""
        lower = since
        while True:
            after, seen, newest = None, 0, lower
            while True:
                data = await self._api(
                    f"search {spec.name} modified since {lower}",
                    "search_objects",
                    spec.name,
                    filter_groups=[{"filters": [{"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": str(lower)}]}],
                    sorts=[{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
                    properties=spec.all_properties,
                    limit=PAGE_SIZE,
                    after=after,
                )
                results = list(data.get("results") or [])
                yield results
                seen += len(results)
                for obj in results:
                    newest = max(newest, to_ms((obj.get("properties") or {}).get("hs_lastmodifieddate")) or newest)
                after = _next_after(data)
                if not after:
                    return
                if seen + PAGE_SIZE >= SEARCH_RESULT_CAP:
                    break
            if newest <= lower:
                raise RuntimeError(f"More than {SEARCH_RESULT_CAP} HubSpot {spec.name} share one modified time; cannot page past them")
            lower = newest

    async def _read_linked(
        self, spec: ObjectSpec, object_ids: list[str]
    ) -> tuple[dict[str, dict[str, list[str]]], dict[str, dict[str, Any]]]:
        """Each object's linked ids by type, and the linked contacts, companies, deals and tickets themselves."""
        linked_ids: dict[str, dict[str, list[str]]] = {oid: {} for oid in object_ids}
        wanted: dict[str, set[str]] = {}
        for to_type in spec.links_to:
            data = await self._api(f"read {spec.name} links to {to_type}", "batch_read_associations", spec.name, to_type, object_ids)
            for result in data.get("results") or []:
                from_id = str((result.get("_from") or {}).get("id"))
                ids = [str(link["to_object_id"]) for link in result.get("to") or []]
                linked_ids.setdefault(from_id, {})[to_type] = ids
                wanted.setdefault(to_type, set()).update(ids[:25])
        details: dict[str, dict[str, Any]] = {}
        for to_type, ids in wanted.items():
            id_list = sorted(ids)
            for start in range(0, len(id_list), PAGE_SIZE):
                data = await self._api(
                    f"read linked {to_type}", "batch_read_objects", to_type, id_list[start:start + PAGE_SIZE],
                    properties=list(LINKED_PROPERTIES[to_type]),
                )
                for obj in data.get("results") or []:
                    details[f"{to_type}:{obj['id']}"] = obj
        return linked_ids, details

    async def _write_objects(self, spec: ObjectSpec, objects: list[dict[str, Any]], directory: Directory) -> int:
        if not objects:
            return 0
        batch = []
        for obj, record, _ in await self._build_records(spec, objects, directory):
            batch.append((record, record_permissions(spec.name, obj.get("properties") or {}, directory)))
        await self.data_entities_processor.on_new_records(batch, replace_permissions=True)
        return len(batch)

    async def _build_records(
        self, spec: ObjectSpec, objects: list[dict[str, Any]], directory: Directory
    ) -> list[tuple[dict[str, Any], Record, str]]:
        linked_ids, details = await self._read_linked(spec, [str(o["id"]) for o in objects])
        index = self._indexing_on(spec.name)
        built = []
        for obj in objects:
            ids = linked_ids.get(str(obj["id"])) or {}
            linked = {t: [details[f"{t}:{i}"] for i in v if f"{t}:{i}" in details] for t, v in ids.items()}
            markdown = render_markdown(spec, obj, linked, directory)
            record = build_record(spec, obj, markdown, ids, directory, self.connector_id, index=index)
            built.append((obj, record, markdown))
        return built

    async def _delete_archived(self, spec: ObjectSpec, since: int) -> None:
        """Delete records HubSpot archived (deleted or merged away) since the last run."""
        cutoff = since - INDEX_LAG_MS
        deleted = 0
        async for page in self._pages(f"list archived {spec.name}", "list_objects", spec.name,
                                      limit=PAGE_SIZE, properties=["hs_object_id"], archived=True):
            for obj in page:
                archived_at = to_ms(obj.get("archived_at"))
                if archived_at is not None and archived_at >= cutoff:
                    await self.data_entities_processor.delete_record_by_external_id(
                        self.connector_id, f"{spec.type_id}:{obj['id']}"
                    )
                    deleted += 1
        if deleted:
            self.logger.info("HubSpot connector %s: deleted %d archived %s", self.connector_id, deleted, spec.name)

    async def _sync_files(self, directory: Directory) -> None:
        saved = await self.records_sync_point.read_sync_point(FILES_SYNC_KEY)
        since = (saved or {}).get("lastModifiedMs")
        started = get_epoch_timestamp_in_ms() - INDEX_LAG_MS
        index = self._indexing_on("files")
        failed = FailedItems()
        lower, newest = since, since or 0
        while True:
            after, seen = None, 0
            updated_gte = datetime.fromtimestamp(lower / 1000, tz=timezone.utc) if lower else None
            while True:
                data = await self._api("search files", "search_files", limit=PAGE_SIZE, after=after,
                                       updated_at_gte=updated_gte, sort=["updatedAt"])
                files = [f for f in data.get("results") or [] if not f.get("archived")]
                stamps = [to_ms(f.get("updated_at")) for f in files]
                if files:
                    try:
                        await self.data_entities_processor.on_new_records(
                            [(build_file_record(f, directory, self.connector_id, index=index), file_permissions()) for f in files],
                            replace_permissions=True,
                        )
                    except Exception:
                        self.logger.exception("HubSpot connector %s: writing %d files failed", self.connector_id, len(files))
                        self._coverage_complete = False
                        for stamp in stamps:
                            failed.add(stamp)
                newest = max([newest, *[s for s in stamps if s]])
                seen += len(data.get("results") or [])
                after = _next_after(data)
                if not after or seen + PAGE_SIZE >= SEARCH_RESULT_CAP:
                    break
            if not after:
                break
            if lower is not None and newest <= lower:
                raise RuntimeError(f"More than {SEARCH_RESULT_CAP} HubSpot files share one updated time; cannot page past them")
            lower = newest
        checkpoint = started if since is None else min(newest, started)
        await self.records_sync_point.update_sync_point(FILES_SYNC_KEY, {"lastModifiedMs": failed.checkpoint(checkpoint)})

    # ---- content ----

    async def stream_record(
        self, record: Record, user_id: str | None = None, convertTo: str | None = None
    ) -> StreamingResponse:
        try:
            await self._rebuild_client()
        except ValueError as e:
            raise connector_not_ready(self.display_name) from e
        kind, _, object_id = record.external_record_id.partition(":")
        try:
            if record.record_type == RecordType.FILE:
                signed = await self._api(f"get a download link for file {object_id}", "get_file_signed_url",
                                         object_id, expiration_seconds=300)
                return create_stream_record_response(
                    stream_content(signed["url"], record_id=record.id, file_name=record.record_name,
                                   connector=self.display_name),
                    filename=record.record_name,
                    mime_type=record.mime_type,
                )
            spec = SPECS_BY_TYPE_ID.get(kind)
            if spec is None:
                raise not_found_at_source(self.display_name)
            obj = await self._api(f"read {spec.name} {object_id}", "get_object", spec.name, object_id,
                                  properties=spec.all_properties)
            if self._directory is None:
                self._directory = await self._read_directory()
            directory = self._directory
            [(_, _, markdown)] = await self._build_records(spec, [obj], directory)
        except HubSpotApiError as e:
            self.logger.warning("HubSpot connector %s: streaming %s failed: %s", self.connector_id, record.external_record_id, e)
            retry_after = str(int(e.response.retry_after)) if e.response.retry_after else None
            raise map_source_status(e.response.status_code, connector=self.display_name, retry_after=retry_after) from e
        container = BlocksContainer(
            block_groups=[BlockGroup(
                index=0,
                name=record.record_name,
                type=GroupType.TEXT_SECTION,
                sub_type=GroupSubType.CONTENT,
                source_group_id=record.external_record_id,
                format=DataFormat.MARKDOWN,
                weburl=record.weburl or "",
                requires_processing=True,
                data=markdown,
            )],
            blocks=[],
        )
        return StreamingResponse(iter([container.model_dump_json().encode("utf-8")]), media_type=MimeTypes.BLOCKS.value)

    async def get_signed_url(self, record: Record) -> str | None:
        # File links expire in minutes; stream_record asks for a fresh one on every read.
        return None

    # ---- setup, filters, lifecycle ----

    async def test_connection_and_access(self) -> bool:
        await self.init()
        try:
            await self._api("read account details", "get_account_details")
            await self._api("list users", "list_users", limit=1)
            await self._api("list deals", "list_objects", "deals", limit=1)
            await self._api("list tickets", "list_objects", "tickets", limit=1)
        except HubSpotApiError as e:
            self.logger.error("HubSpot connection test for connector %s failed: %s", self.connector_id, e)
            raise ConnectorInitError(
                f"HubSpot refused {self.display_name}: {e.response.error}. Check that the token has the read scopes "
                f"{', '.join(s for s in HUBSPOT_SCOPES if s != 'oauth')}."
            ) from e
        return True

    async def get_filter_options(
        self, filter_key: str, page: int = 1, limit: int = 20, search: str | None = None, cursor: str | None = None
    ) -> FilterOptionsResponse:
        if filter_key not in {_access_filter_key(spec.name, scope) for spec in OWNED_SPECS for scope in (SCOPE_ALL, SCOPE_TEAM)}:
            raise ValueError(f"HubSpot has no dynamic filter {filter_key!r}")
        await self._rebuild_client()
        options: list[FilterOption] = []
        try:
            roles = await self._api("list roles", "list_roles")
            options += [FilterOption(id=f"{ROLE_OPTION}{r['id']}", label=f"Role: {r.get('name') or r['id']}")
                        for r in roles.get("results") or []]
        except HubSpotApiError as e:
            if e.response.status_code != HTTPStatus.BAD_REQUEST:
                raise
            # Only Enterprise accounts have roles; the others pick users.
        async for page_users in self._pages("list users", "list_users", limit=PAGE_SIZE):
            options += [FilterOption(id=f"{USER_OPTION}{u['id']}", label=f"User: {u.get('email') or u['id']}")
                        for u in page_users]
        if search:
            options = [o for o in options if search.lower() in o.label.lower()]
        start = (max(page, 1) - 1) * limit
        return FilterOptionsResponse(
            success=True, options=options[start:start + limit], page=page, limit=limit,
            has_more=start + limit < len(options),
        )

    async def reindex_records(self, record_results: list[Record]) -> None:
        if record_results:
            await self.data_entities_processor.reindex_existing_records(record_results)

    def handle_webhook_notification(self, notification: dict[str, Any]) -> None:
        self.logger.debug("HubSpot connector %s ignores webhooks", self.connector_id)

    async def cleanup(self) -> None:
        self._data_source = None
        self._client = None
        await self._release_thread_lease()
