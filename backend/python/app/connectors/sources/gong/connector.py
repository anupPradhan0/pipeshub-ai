"""Gong connector — syncs calls with participant-derived permissions.

Transcript text and CRM context are not stored on the record; both are fetched
live in ``stream_record`` (same contract as the Zoom connector), which keeps the
sync phase to one API call per page of calls.
"""

from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any, Optional
from uuid import uuid4

from aiolimiter import AsyncLimiter  # pyright: ignore[reportMissingImports]
from fastapi.responses import StreamingResponse  # pyright: ignore[reportMissingImports]
from pydantic import BaseModel, Field  # pyright: ignore[reportMissingImports]

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    AppGroups,
    Connectors,
    MimeTypes,
    ProgressStatus,
)
from app.connectors.core.base.connector.connector_service import BaseConnector
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.base.sync_point.sync_point import (
    SyncDataPointType,
    SyncPoint,
)
from app.connectors.core.constants import (
    CONNECTOR_EMAIL_IDENTITY_INFO,
    IconPaths,
)
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
    MultiselectOperator,
    OptionSourceType,
    load_connector_filters,
)
from app.connectors.sources.gong.common.apps import GongApp
from app.models.blocks import (
    BlockGroup,
    BlocksContainer,
    DataFormat,
    GroupSubType,
    GroupType,
)
from app.models.entities import (
    AppUser,
    MeetingRecord,
    OriginTypes,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.services.notification.types import (
    NotificationSeverity,
    NotificationType,
)
from app.sources.client.gong.gong import GongClient
from app.sources.external.gong.gong import GongDataSource
from app.utils.time_conversion import get_epoch_timestamp_in_ms, parse_timestamp

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GONG_FILTER_KEY = "gong"
GONG_SYNC_KEY_PREFIX = "gong_calls_last_sync"

# How far back the very first sync reaches when the user sets no "created" filter.
GONG_DEFAULT_HISTORY_DAYS = 365

# Re-scan this much before the last checkpoint. Gong can attach a call to the
# workspace minutes after it ends, so a zero-overlap window drops calls that
# landed just behind the previous cursor.
GONG_SYNC_OVERLAP_HOURS = 24

# Gong's documented ceiling is 3 requests/second per company, with a separate
# daily call quota. Every outbound request goes through this limiter.
# ponytail: fixed rate, no adaptive backoff — the shared HTTPClient has no 429
# handling, so if daily-quota exhaustion becomes real, add Retry-After parsing there.
GONG_REQUESTS_PER_SECOND = 3

# Calls per page requested from /calls/extensive.
GONG_PAGE_SIZE = 100

# Pseudo-workspace used when /settings/workspaces is unavailable (the
# api:settings:read scope is optional). Calls still sync, ungrouped by workspace.
GONG_DEFAULT_WORKSPACE_ID = ""
GONG_DEFAULT_WORKSPACE_NAME = "Gong"

_INTERNAL_AFFILIATIONS = {"internal", "unknown"}


# ---------------------------------------------------------------------------
# Gong API response models
#
# Every model ignores unknown fields: the generated GongDataSource returns raw
# JSON, so a Gong-side schema addition must not break parsing.
# ---------------------------------------------------------------------------


class GongUser(BaseModel):
    """User object returned by GET /v2/users."""

    id: str = ""
    emailAddress: str = ""
    firstName: str = ""
    lastName: str = ""
    title: Optional[str] = None
    active: bool = True
    created: Optional[str] = None

    model_config = {"extra": "ignore"}

    @property
    def full_name(self) -> str:
        name = f"{self.firstName} {self.lastName}".strip()
        return name or self.emailAddress


class GongWorkspace(BaseModel):
    """Workspace object returned by GET /v2/settings/workspaces."""

    id: str = ""
    name: str = ""
    description: Optional[str] = None

    model_config = {"extra": "ignore"}


class GongCallMetaData(BaseModel):
    """``metaData`` block of a call returned by POST /v2/calls/extensive."""

    id: str = ""
    url: str = ""
    title: str = "Gong Call"
    started: Optional[str] = None
    scheduled: Optional[str] = None
    duration: Optional[int] = None  # seconds
    primaryUserId: str = ""
    direction: Optional[str] = None
    system: Optional[str] = None
    scope: Optional[str] = None
    media: Optional[str] = None
    language: Optional[str] = None
    workspaceId: Optional[str] = None
    isPrivate: Optional[bool] = None
    purpose: Optional[str] = None
    meetingUrl: Optional[str] = None

    model_config = {"extra": "ignore"}


class GongParty(BaseModel):
    """Entry in a call's ``parties`` array."""

    id: str = ""
    emailAddress: str = ""
    name: str = ""
    userId: Optional[str] = None
    title: Optional[str] = None
    affiliation: Optional[str] = None  # Internal | External | Unknown

    model_config = {"extra": "ignore"}

    @property
    def is_internal(self) -> bool:
        """Treat an unknown affiliation as internal only when Gong resolved the
        party to a Gong user — an unresolved unknown is an outside participant."""
        affiliation = (self.affiliation or "unknown").strip().lower()
        if affiliation == "internal":
            return True
        return affiliation in _INTERNAL_AFFILIATIONS and bool(self.userId)


class GongCrmObject(BaseModel):
    """CRM entity attached to a call (``context[].objects[]``)."""

    objectType: Optional[str] = None
    objectId: Optional[str] = None
    fields: list[dict[str, Any]] = Field(default_factory=list)
    timing: Optional[str] = None

    model_config = {"extra": "ignore"}

    def field_value(self, name: str) -> Optional[str]:
        for entry in self.fields:
            if entry.get("name") == name:
                value = entry.get("value")
                return None if value is None else str(value)
        return None


class GongCrmContext(BaseModel):
    """Entry in a call's ``context`` array (one per CRM system)."""

    system: Optional[str] = None
    objects: list[GongCrmObject] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


class GongCall(BaseModel):
    """Call object returned by POST /v2/calls/extensive."""

    metaData: GongCallMetaData = Field(default_factory=GongCallMetaData)
    parties: list[GongParty] = Field(default_factory=list)
    context: list[GongCrmContext] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


class GongTranscriptSentence(BaseModel):
    """Sentence inside a transcript monologue."""

    start: Optional[int] = None
    end: Optional[int] = None
    text: str = ""

    model_config = {"extra": "ignore"}


class GongTranscriptMonologue(BaseModel):
    """Contiguous block of speech by one speaker."""

    speakerId: Optional[str] = None
    topic: Optional[str] = None
    sentences: list[GongTranscriptSentence] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


# ---------------------------------------------------------------------------
# ConnectorBuilder
# ---------------------------------------------------------------------------


@ConnectorBuilder("Gong")\
    .in_group(AppGroups.GONG.value)\
    .with_description("Sync Gong calls, transcripts, and CRM context")\
    .with_categories(["Revenue Intelligence", "Meetings"])\
    .with_scopes([ConnectorScope.TEAM.value])\
    .with_auth([
        AuthBuilder.type(AuthType.BASIC_AUTH).fields([
            AuthField(
                name="accessKey",
                display_name="Access Key",
                placeholder="Enter your Gong Access Key",
                description=(
                    "Access Key from Gong Admin > Settings > Ecosystem > API"
                ),
                field_type="PASSWORD",
                max_length=2000,
                is_secret=True,
            ),
            AuthField(
                name="accessKeySecret",
                display_name="Access Key Secret",
                placeholder="Enter your Gong Access Key Secret",
                description=(
                    "Access Key Secret issued alongside the Access Key"
                ),
                field_type="PASSWORD",
                max_length=2000,
                is_secret=True,
            ),
            AuthField(
                name="baseUrl",
                display_name="API Base URL",
                placeholder="https://api.gong.io/v2",
                description=(
                    "Leave blank unless Gong issued your company a dedicated API "
                    "host. Shown as 'API Base URL' on the Gong API settings page."
                ),
                field_type="TEXT",
                required=False,
                default_value="https://api.gong.io/v2",
                max_length=512,
            ),
        ]),
        AuthBuilder.type(AuthType.OAUTH).oauth(
            connector_name="Gong",
            authorize_url="https://app.gong.io/oauth2/authorize",
            token_url="https://app.gong.io/oauth2/generate-customer-token",
            redirect_uri="connectors/oauth/callback/Gong",
            scopes=OAuthScopeConfig(
                personal_sync=[],
                team_sync=[
                    "api:calls:read:basic",
                    "api:calls:read:extensive",
                    "api:calls:read:transcript",
                    "api:users:read",
                    "api:settings:read",
                    "api:crm:read",
                ],
                agent=[],
            ),
            fields=[
                CommonFields.client_id("Gong API settings"),
                CommonFields.client_secret("Gong API settings"),
                AuthField(
                    name="baseUrl",
                    display_name="API Base URL",
                    placeholder="https://api.gong.io/v2",
                    description=(
                        "Gong returns a per-company API host after authorization "
                        "(api_base_url_for_customer). Enter it here if it differs "
                        "from the default; leave blank otherwise."
                    ),
                    field_type="TEXT",
                    required=False,
                    default_value="https://api.gong.io/v2",
                    max_length=512,
                ),
            ],
            icon_path=IconPaths.connector_icon(Connectors.GONG.value),
            app_group=AppGroups.GONG.value,
            app_description="OAuth application for syncing Gong calls and transcripts",
            app_categories=["Revenue Intelligence", "Meetings"],
        ),
    ])\
    .with_info(CONNECTOR_EMAIL_IDENTITY_INFO)\
    .configure(lambda builder: builder
        .with_icon(IconPaths.connector_icon(Connectors.GONG.value))
        .with_realtime_support(False)
        .add_documentation_link(DocumentationLink(
            "Gong API Credentials",
            "https://help.gong.io/docs/receive-access-to-the-api",
            "setup",
        ))
        .add_documentation_link(DocumentationLink(
            "Pipeshub Documentation",
            "https://docs.pipeshub.com/connectors/gong/gong",
            "pipeshub",
        ))
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 60)
        .with_sync_support(True)
        .with_agent_support(True)
        .add_filter_field(FilterField(
            name="workspace_ids",
            display_name="Workspaces",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Sync calls from selected Gong workspaces only.",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(CommonFields.created_date_filter(
            "Only sync calls that took place after this date."
        ))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .add_filter_field(FilterField(
            name="calls",
            display_name="Index Call Transcripts",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description="Enable indexing of Gong call transcripts.",
            default_value=True,
        ))
        .add_filter_field(FilterField(
            name="crm_context",
            display_name="Include CRM Context",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description=(
                "Include the CRM accounts and opportunities Gong associates with "
                "each call in the indexed content."
            ),
            default_value=True,
        ))
    )\
    .build_decorator()
class GongConnector(BaseConnector):
    """Gong connector — calls as meeting records, grouped by workspace."""

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
            GongApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_id = connector_id
        self.connector_name = Connectors.GONG
        self.external_client: Optional[GongClient] = None
        self.data_source: Optional[GongDataSource] = None

        self.sync_point = SyncPoint(
            connector_id=self.connector_id,
            org_id=self.data_entities_processor.org_id,
            sync_data_point_type=SyncDataPointType.RECORDS,
            data_store_provider=data_store_provider,
        )
        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self.rate_limiter = AsyncLimiter(GONG_REQUESTS_PER_SECOND, 1)

        # Gong user id -> email, populated per sync; used to resolve a call's
        # primary user when that user is absent from the call's parties array.
        self._user_email_by_id: dict[str, str] = {}

    # ========================================================================
    # Initialization
    # ========================================================================

    async def init(self) -> bool:
        try:
            self.external_client = await GongClient.build_from_services(
                logger=self.logger,
                config_service=self.config_service,
                connector_instance_id=self.connector_id,
            )
            self.data_source = GongDataSource(self.external_client)
            await self._load_filters()
            self.logger.info("✅ Gong connector initialised.")
            return True
        except Exception as exc:
            self.logger.error("❌ Gong connector init failed: %s", exc, exc_info=True)
            await self.notify(
                type=NotificationType.CONNECTOR_AUTH_ERROR,
                severity=NotificationSeverity.ERROR,
                title="Failed to initialize Gong connector",
                message=f"Could not build the Gong client: {exc}",
            )
            return False

    async def _load_filters(self) -> None:
        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, GONG_FILTER_KEY, self.connector_id, self.logger
        )

    async def _get_fresh_datasource(self) -> GongDataSource:
        """Return the data source with an up-to-date OAuth access token.

        The background token-refresh service writes new tokens into the config
        store, so the in-memory client can go stale mid-sync. Basic-auth
        instances have nothing to refresh and are returned as-is.
        """
        if self.data_source is None or self.external_client is None:
            raise RuntimeError("Gong connector is not initialised. Call init() first.")

        config = await self.config_service.get_config(
            f"/services/connectors/{self.connector_id}/config"
        )
        fresh_token = ((config or {}).get("credentials") or {}).get("access_token")
        if not fresh_token:
            return self.data_source

        internal = self.external_client.get_client()
        expected = f"Bearer {fresh_token}"
        # HTTPClient.execute merges self.headers into every request, so patching
        # the dict is enough — no need to rebuild the httpx client.
        if internal.headers.get("Authorization") != expected:
            self.logger.debug("🔄 Gong: applying refreshed access token")
            internal.headers["Authorization"] = expected

        return self.data_source

    # ========================================================================
    # Core sync
    # ========================================================================

    async def run_sync(self) -> None:
        """Sync every selected workspace's calls since its last checkpoint."""
        try:
            if not self.data_source and not await self.init():
                raise ConnectionError("Gong connector is not initialised.")

            await self._load_filters()

            async with self.data_store_provider.transaction() as tx_store:
                await tx_store.ensure_team_app_edge(
                    self.connector_id, self.data_entities_processor.org_id
                )

            users = await self._list_users()
            self.logger.info("👥 Gong: %d active user(s) found.", len(users))
            self._user_email_by_id = {
                u.id: u.emailAddress.strip().lower()
                for u in users
                if u.id and u.emailAddress
            }

            app_users = self._build_app_users(users)
            if app_users:
                await self.data_entities_processor.on_new_app_users(app_users)

            workspaces = await self._list_workspaces()
            self.logger.info("🗂️ Gong: syncing %d workspace(s).", len(workspaces))

            for workspace in workspaces:
                try:
                    await self._sync_workspace_calls(workspace)
                except Exception as exc:
                    self.logger.error(
                        "❌ Gong: workspace %s (%s) failed: %s",
                        workspace.name, workspace.id or "default", exc, exc_info=True,
                    )
                    continue

            self.logger.info("✅ Gong: sync complete.")

        except Exception as exc:
            self.logger.error("❌ Gong: sync failed: %s", exc, exc_info=True)
            await self.notify(
                type=NotificationType.CONNECTOR_SYNC_ERROR,
                severity=NotificationSeverity.ERROR,
                title="Gong sync failed",
                message=f"Gong sync failed: {exc}",
            )
            raise

    async def run_incremental_sync(self) -> None:
        """Incremental sync — run_sync already resumes from the checkpoint."""
        await self.run_sync()

    async def _sync_workspace_calls(self, workspace: GongWorkspace) -> None:
        """Page through a workspace's calls and persist each page as it arrives."""
        group, group_perms = self._build_record_group(workspace)
        await self.data_entities_processor.on_new_record_groups([(group, group_perms)])

        from_dt = await self._resolve_sync_start(workspace.id)
        to_dt = datetime.now(tz=timezone.utc)
        if from_dt >= to_dt:
            self.logger.info(
                "Gong: workspace %s already up to date.", workspace.name
            )
            return

        self.logger.info(
            "Gong: workspace %s — fetching calls %s → %s",
            workspace.name, self._iso(from_dt), self._iso(to_dt),
        )

        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        total = 0
        while True:
            calls, cursor = await self._list_calls_page(
                workspace_id=workspace.id,
                from_dt=from_dt,
                to_dt=to_dt,
                cursor=cursor,
            )
            if calls:
                batch = [
                    (
                        self._build_call_record(call, group.id),
                        self._build_call_permissions(call),
                    )
                    for call in calls
                    if call.metaData.id
                ]
                if batch:
                    # Flush per page: bounds memory and keeps partial progress
                    # if a later page fails.
                    await self.data_entities_processor.on_new_records(batch)
                    total += len(batch)
            if not cursor:
                break
            # A cursor Gong repeats would loop forever inside a background task
            # and burn the daily API quota doing it.
            if cursor in seen_cursors:
                self.logger.warning(
                    "Gong: workspace %s returned a repeated cursor; stopping pagination.",
                    workspace.name,
                )
                break
            seen_cursors.add(cursor)

        # ponytail: checkpoint advances only after the whole workspace finishes, so
        # an interrupted backfill repeats that workspace. Split the window into
        # date chunks and checkpoint per chunk if backfills grow long enough to matter.
        await self._update_sync_point(workspace.id, to_dt)
        self.logger.info(
            "Gong: workspace %s — synced %d call(s).", workspace.name, total
        )

    # ========================================================================
    # Checkpoint
    # ========================================================================

    def _sync_key(self, workspace_id: str) -> str:
        return f"{GONG_SYNC_KEY_PREFIX}/{workspace_id or 'default'}"

    async def _resolve_sync_start(self, workspace_id: str) -> datetime:
        """Where this workspace's window begins.

        Checkpoint (minus an overlap) when one exists, otherwise the user's
        "created" filter, otherwise a bounded default backfill.
        """
        data = await self.sync_point.read_sync_point(self._sync_key(workspace_id))
        raw = data.get("last_sync_at")
        if raw:
            try:
                return datetime.fromisoformat(str(raw)) - timedelta(
                    hours=GONG_SYNC_OVERLAP_HOURS
                )
            except ValueError:
                self.logger.warning(
                    "Gong: unreadable checkpoint %r for workspace %s; refetching window.",
                    raw, workspace_id or "default",
                )

        created_filter = self.sync_filters.get("created")
        if created_filter:
            start_epoch = created_filter.get_datetime_start()
            if start_epoch:
                return datetime.fromtimestamp(start_epoch / 1000, tz=timezone.utc)

        return datetime.now(tz=timezone.utc) - timedelta(
            days=GONG_DEFAULT_HISTORY_DAYS
        )

    async def _update_sync_point(self, workspace_id: str, when: datetime) -> None:
        await self.sync_point.update_sync_point(
            self._sync_key(workspace_id), {"last_sync_at": when.isoformat()}
        )

    # ========================================================================
    # API wrappers
    # ========================================================================

    @staticmethod
    def _iso(value: datetime) -> str:
        """Gong requires an offset on every datetime it is given."""
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _next_cursor(data: object) -> Optional[str]:
        """Pull ``records.cursor`` out of a Gong list response."""
        if not isinstance(data, dict):
            return None
        records = data.get("records")
        if not isinstance(records, dict):
            return None
        cursor = records.get("cursor")
        return str(cursor) if cursor else None

    async def _list_users(self) -> list[GongUser]:
        """GET /v2/users — cursor-paginated, active users only."""
        users: list[GongUser] = []
        cursor: Optional[str] = None
        while True:
            datasource = await self._get_fresh_datasource()
            async with self.rate_limiter:
                resp = await datasource.list_users(cursor=cursor)
            if not resp.success or not isinstance(resp.data, dict):
                self.logger.warning("Gong: list_users failed — %s", resp.message)
                break
            users.extend(
                GongUser.model_validate(u) for u in (resp.data.get("users") or [])
            )
            cursor = self._next_cursor(resp.data)
            if not cursor:
                break
        return [u for u in users if u.active and u.emailAddress]

    async def _list_workspaces(self) -> list[GongWorkspace]:
        """GET /v2/settings/workspaces, narrowed by the workspace filter.

        Falls back to a single unnamed workspace when the endpoint is
        unavailable, so calls still sync without the api:settings:read scope.
        """
        default = [
            GongWorkspace(
                id=GONG_DEFAULT_WORKSPACE_ID, name=GONG_DEFAULT_WORKSPACE_NAME
            )
        ]
        try:
            datasource = await self._get_fresh_datasource()
            async with self.rate_limiter:
                resp = await datasource.list_workspaces()
        except Exception as exc:
            self.logger.warning("Gong: list_workspaces errored (%s); syncing ungrouped.", exc)
            return default

        if not resp.success or not isinstance(resp.data, dict):
            self.logger.warning(
                "Gong: list_workspaces failed (%s); syncing ungrouped.", resp.message
            )
            return default

        workspaces = [
            GongWorkspace.model_validate(w)
            for w in (resp.data.get("workspaces") or [])
        ]
        workspaces = [w for w in workspaces if w.id]
        if not workspaces:
            return default

        selected = self.sync_filters.get("workspace_ids")
        wanted = set(selected.as_list()) if selected and not selected.is_empty() else set()
        if wanted:
            workspaces = [w for w in workspaces if w.id in wanted]

        return workspaces or default

    async def _list_calls_page(
        self,
        workspace_id: str,
        from_dt: datetime,
        to_dt: datetime,
        cursor: Optional[str],
    ) -> tuple[list[GongCall], Optional[str]]:
        """One page of POST /v2/calls/extensive.

        ``parties`` drives permissions and ``context`` carries the CRM objects,
        so both are requested here rather than re-fetched per record.
        """
        call_filter: dict[str, Any] = {
            "fromDateTime": self._iso(from_dt),
            "toDateTime": self._iso(to_dt),
        }
        if workspace_id:
            call_filter["workspaceId"] = workspace_id

        datasource = await self._get_fresh_datasource()
        async with self.rate_limiter:
            resp = await datasource.get_calls_extensive(
                content_selector={
                    "context": "Extended",
                    "exposedFields": {"parties": True},
                },
                filter=call_filter,
                cursor=cursor,
            )

        if not resp.success or not isinstance(resp.data, dict):
            # Raise rather than return empty: _sync_workspace_calls must not
            # advance its checkpoint past a window it never actually read.
            raise RuntimeError(
                f"Gong calls/extensive failed for workspace "
                f"{workspace_id or 'default'}: {resp.error or resp.message}"
            )

        calls = [
            GongCall.model_validate(c) for c in (resp.data.get("calls") or [])
        ]
        return calls, self._next_cursor(resp.data)

    async def _fetch_transcript_monologues(
        self, call_id: str
    ) -> list[GongTranscriptMonologue]:
        """POST /v2/calls/transcript for a single call."""
        try:
            datasource = await self._get_fresh_datasource()
            async with self.rate_limiter:
                resp = await datasource.get_call_transcripts(
                    filter={"callIds": [call_id]}
                )
        except Exception as exc:
            self.logger.warning("Gong: transcript fetch errored for %s: %s", call_id, exc)
            return []

        if not resp.success or not isinstance(resp.data, dict):
            self.logger.debug(
                "Gong: no transcript for call %s — %s", call_id, resp.message
            )
            return []

        for entry in resp.data.get("callTranscripts") or []:
            if str(entry.get("callId")) == call_id:
                return [
                    GongTranscriptMonologue.model_validate(m)
                    for m in (entry.get("transcript") or [])
                ]
        return []

    async def _fetch_call_detail(self, call_id: str) -> Optional[GongCall]:
        """Re-read one call so ``stream_record`` has current parties and CRM context."""
        try:
            datasource = await self._get_fresh_datasource()
            async with self.rate_limiter:
                resp = await datasource.get_calls_extensive(
                    content_selector={
                        "context": "Extended",
                        "exposedFields": {"parties": True},
                    },
                    filter={"callIds": [call_id]},
                )
        except Exception as exc:
            self.logger.warning("Gong: call detail errored for %s: %s", call_id, exc)
            return None

        if not resp.success or not isinstance(resp.data, dict):
            return None
        for raw in resp.data.get("calls") or []:
            call = GongCall.model_validate(raw)
            if call.metaData.id == call_id:
                return call
        return None

    # ========================================================================
    # Record / permission builders
    # ========================================================================

    @staticmethod
    def _to_epoch_ms(raw: Optional[str]) -> Optional[int]:
        if not raw or not str(raw).strip():
            return None
        try:
            return parse_timestamp(str(raw).strip())
        except Exception:
            return None

    def _build_app_users(self, users: list[GongUser]) -> list[AppUser]:
        org_id = self.data_entities_processor.org_id
        now_ms = get_epoch_timestamp_in_ms()
        app_users: list[AppUser] = []
        for user in users:
            email = user.emailAddress.strip().lower()
            if not user.id or not email:
                continue
            created_ms = self._to_epoch_ms(user.created) or now_ms
            app_users.append(
                AppUser(
                    app_name=Connectors.GONG,
                    connector_id=self.connector_id,
                    source_user_id=user.id,
                    org_id=org_id,
                    email=email,
                    full_name=user.full_name,
                    is_active=user.active,
                    title=user.title,
                    created_at=created_ms,
                    updated_at=now_ms,
                    source_created_at=created_ms,
                    source_updated_at=created_ms,
                )
            )
        return app_users

    def _build_record_group(
        self, workspace: GongWorkspace
    ) -> tuple[RecordGroup, list[Permission]]:
        now_ms = get_epoch_timestamp_in_ms()
        group = RecordGroup(
            id=str(uuid4()),
            org_id=self.data_entities_processor.org_id,
            name=workspace.name or GONG_DEFAULT_WORKSPACE_NAME,
            description=workspace.description,
            # A blank workspace id would collide with any other blank-id group on
            # this connector; the constant keeps the fallback group addressable.
            external_group_id=workspace.id or GONG_DEFAULT_WORKSPACE_NAME.lower(),
            connector_name=Connectors.GONG,
            connector_id=self.connector_id,
            group_type=RecordGroupType.GONG_WORKSPACE,
            inherit_permissions=False,
            created_at=now_ms,
            updated_at=now_ms,
        )
        # Access is decided per call from its participants, so the workspace
        # itself carries no permissions.
        return group, []

    def _primary_user_email(self, call: GongCall) -> str:
        """Email of the call's primary user, from its parties or the user map."""
        primary_id = call.metaData.primaryUserId
        if primary_id:
            for party in call.parties:
                if party.userId == primary_id and party.emailAddress:
                    return party.emailAddress.strip().lower()
            mapped = self._user_email_by_id.get(primary_id)
            if mapped:
                return mapped
        return ""

    def _build_call_record(
        self, call: GongCall, record_group_id: str
    ) -> MeetingRecord:
        meta = call.metaData
        started_ms = self._to_epoch_ms(meta.started) or self._to_epoch_ms(meta.scheduled)

        end_time: Optional[str] = None
        if started_ms and meta.duration:
            end_time = self._iso(
                datetime.fromtimestamp(started_ms / 1000, tz=timezone.utc)
                + timedelta(seconds=meta.duration)
            )

        duration_minutes = (
            round(meta.duration / 60) if meta.duration is not None else None
        )

        # Participants and CRM links change after a call ends (Gong keeps
        # attaching them), so they belong in the revision key alongside the
        # call's own fields — otherwise those updates never re-index.
        revision_id = "|".join([
            meta.started or "",
            str(meta.duration or ""),
            meta.title or "",
            str(len(call.parties)),
            str(sum(len(ctx.objects) for ctx in call.context)),
        ])

        now_ms = get_epoch_timestamp_in_ms()
        record = MeetingRecord(
            id=str(uuid4()),
            org_id=self.data_entities_processor.org_id,
            record_name=meta.title or "Gong Call",
            record_type=RecordType.MEETING,
            record_group_type=RecordGroupType.GONG_WORKSPACE,
            external_record_id=meta.id,
            external_revision_id=revision_id,
            external_record_group_id=meta.workspaceId or None,
            record_group_id=record_group_id,
            version=1,
            origin=OriginTypes.CONNECTOR,
            connector_name=Connectors.GONG,
            connector_id=self.connector_id,
            mime_type=MimeTypes.BLOCKS.value,
            weburl=meta.url or None,
            inherit_permissions=False,
            source_created_at=started_ms,
            source_updated_at=started_ms,
            created_at=now_ms,
            updated_at=now_ms,
            host_email=self._primary_user_email(call) or None,
            host_id=meta.primaryUserId or None,
            duration_minutes=duration_minutes,
            start_time=meta.started,
            end_time=end_time,
            recording_url=meta.url or None,
            preview_renderable=False,
            is_dependent_node=False,
            parent_node_id=None,
        )

        if not self.indexing_filters.is_enabled("calls"):
            record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

        return record

    def _build_call_permissions(self, call: GongCall) -> list[Permission]:
        """Primary user owns the call; internal participants get read access.

        External parties are deliberately skipped — they are customer contacts,
        not platform users, and granting on their email would leak the call to
        anyone who later signs up with that address.
        """
        permissions: list[Permission] = []
        seen: set[str] = set()

        owner_email = self._primary_user_email(call)
        if owner_email:
            permissions.append(Permission(
                email=owner_email,
                type=PermissionType.OWNER,
                entity_type=EntityType.USER,
            ))
            seen.add(owner_email)

        for party in call.parties:
            email = party.emailAddress.strip().lower()
            if not email or email in seen or not party.is_internal:
                continue
            permissions.append(Permission(
                email=email,
                type=PermissionType.READ,
                entity_type=EntityType.USER,
            ))
            seen.add(email)

        return permissions

    # ========================================================================
    # Content rendering
    # ========================================================================

    def _build_transcript_markdown(
        self,
        monologues: list[GongTranscriptMonologue],
        call: Optional[GongCall],
    ) -> str:
        """Render monologues as speaker-labelled markdown paragraphs."""
        if not monologues:
            return ""

        speaker_names: dict[str, str] = {}
        if call:
            for party in call.parties:
                if party.id:
                    speaker_names[party.id] = party.name or party.emailAddress

        lines: list[str] = []
        last_topic: Optional[str] = None
        for monologue in monologues:
            text = " ".join(
                sentence.text.strip()
                for sentence in monologue.sentences
                if sentence.text.strip()
            )
            if not text:
                continue
            if monologue.topic and monologue.topic != last_topic:
                lines.append(f"### {monologue.topic}")
                last_topic = monologue.topic
            speaker = speaker_names.get(monologue.speakerId or "", "Unknown speaker")
            lines.append(f"**{speaker}:** {text}")
        return "\n\n".join(lines)

    @staticmethod
    def _build_participants_markdown(call: Optional[GongCall]) -> str:
        """Render the call's parties as a markdown table."""
        if not call or not call.parties:
            return ""
        rows = [
            "| Name | Email | Title | Affiliation |",
            "| --- | --- | --- | --- |",
        ]
        rows.extend(
            f"| {party.name or '—'} | {party.emailAddress or '—'} "
            f"| {party.title or '—'} | {party.affiliation or 'Unknown'} |"
            for party in call.parties
        )
        return "\n".join(rows)

    @staticmethod
    def _build_crm_markdown(call: Optional[GongCall]) -> str:
        """Render the CRM accounts/opportunities Gong linked to the call."""
        if not call or not call.context:
            return ""
        lines: list[str] = []
        for context in call.context:
            if not context.objects:
                continue
            lines.append(f"### {context.system or 'CRM'}")
            for obj in context.objects:
                name = obj.field_value("name") or obj.objectId or "—"
                lines.append(f"- **{obj.objectType or 'Object'}**: {name}")
                for entry in obj.fields:
                    field_name = entry.get("name")
                    value = entry.get("value")
                    if field_name and field_name != "name" and value not in (None, ""):
                        lines.append(f"  - {field_name}: {value}")
        return "\n".join(lines)

    async def stream_record(
        self,
        record: Record,
        user_id: Optional[str] = None,
        convertTo: Optional[str] = None,
    ) -> StreamingResponse:
        """Fetch the call's transcript, participants, and CRM context on demand."""
        call_id = record.external_record_id
        transcript_md = ""
        participants_md = ""
        crm_md = ""

        if call_id and self.data_source:
            call = await self._fetch_call_detail(call_id)
            monologues = await self._fetch_transcript_monologues(call_id)
            transcript_md = self._build_transcript_markdown(monologues, call)
            participants_md = self._build_participants_markdown(call)
            if self.indexing_filters.is_enabled("crm_context"):
                crm_md = self._build_crm_markdown(call)

        sections = [
            ("Transcript", "Gong call transcript", transcript_md),
            ("Participants", "Call participants and their affiliation", participants_md),
            ("CRM Context", "CRM records Gong associated with this call", crm_md),
        ]

        block_groups = [
            BlockGroup(
                id=str(uuid4()),
                index=index,
                name=name,
                type=GroupType.TEXT_SECTION,
                sub_type=GroupSubType.CONTENT,
                description=description,
                source_group_id=call_id,
                format=DataFormat.MARKDOWN,
                weburl=record.weburl or "",
                requires_processing=True,
                data=data,
            )
            for index, (name, description, data) in enumerate(sections)
        ]

        container = BlocksContainer(block_groups=block_groups, blocks=[])
        payload = container.model_dump_json().encode("utf-8")
        return StreamingResponse(iter([payload]), media_type=MimeTypes.BLOCKS.value)

    # ========================================================================
    # Abstract method implementations
    # ========================================================================

    async def test_connection_and_access(self) -> bool:
        """Validate credentials with the cheapest authenticated call available."""
        try:
            if not self.data_source and not await self.init():
                return False
            datasource = await self._get_fresh_datasource()
            async with self.rate_limiter:
                resp = await datasource.list_users()
            if resp.success:
                self.logger.info("✅ Gong connection test successful")
                return True
            self.logger.error("❌ Gong connection test failed: %s", resp.message)
            return False
        except Exception as exc:
            self.logger.error("❌ Gong connection test failed: %s", exc, exc_info=True)
            return False

    def get_signed_url(self, record: Record) -> Optional[str]:
        return None

    def handle_webhook_notification(self, notification: dict[str, Any]) -> None:
        pass

    async def cleanup(self) -> None:
        try:
            if self.external_client:
                try:
                    internal = self.external_client.get_client()
                    if internal and hasattr(internal, "close"):
                        await internal.close()
                except Exception as exc:
                    self.logger.debug("Gong: error closing client: %s", exc)
                finally:
                    self.external_client = None
            self.data_source = None
            self.logger.info("Gong connector cleanup completed")
        except Exception as exc:
            self.logger.warning("Error during Gong cleanup: %s", exc)

    async def reindex_records(self, record_results: list[Record]) -> None:
        """Republish existing call records; content is re-fetched by stream_record."""
        if not record_results:
            return
        reindexable = [r for r in record_results if isinstance(r, MeetingRecord)]
        skipped = len(record_results) - len(reindexable)
        if skipped:
            self.logger.warning(
                "⚠️ Gong: skipped %d record(s) that are not MeetingRecord", skipped
            )
        if not reindexable:
            return
        try:
            await self.data_entities_processor.reindex_existing_records(reindexable)
            self.logger.info(
                "Published reindex events for %d Gong call record(s)", len(reindexable)
            )
        except Exception as exc:
            self.logger.error("❌ Gong: reindex failed: %s", exc, exc_info=True)
            raise

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> FilterOptionsResponse:
        if filter_key != "workspace_ids":
            raise ValueError(f"Unsupported filter key: {filter_key}")

        try:
            if not self.data_source and not await self.init():
                return FilterOptionsResponse(
                    success=False, options=[], page=page, limit=limit,
                    has_more=False, message="Gong connector is not initialized",
                )

            datasource = await self._get_fresh_datasource()
            async with self.rate_limiter:
                resp = await datasource.list_workspaces()
            if not resp.success or not isinstance(resp.data, dict):
                return FilterOptionsResponse(
                    success=False, options=[], page=page, limit=limit,
                    has_more=False,
                    message=f"Failed to list workspaces: {resp.error or resp.message}",
                )

            workspaces = [
                GongWorkspace.model_validate(w)
                for w in (resp.data.get("workspaces") or [])
            ]
            workspaces = [w for w in workspaces if w.id]
            if search:
                needle = search.lower()
                workspaces = [w for w in workspaces if needle in w.name.lower()]

            start = (page - 1) * limit
            end = start + limit
            return FilterOptionsResponse(
                success=True,
                options=[
                    FilterOption(id=w.id, label=w.name or w.id)
                    for w in workspaces[start:end]
                ],
                page=page,
                limit=limit,
                has_more=end < len(workspaces),
            )
        except Exception as exc:
            self.logger.error("Error getting Gong workspace options: %s", exc, exc_info=True)
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit,
                has_more=False, message=f"Error: {exc}",
            )

    # ========================================================================
    # Factory
    # ========================================================================

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str,
        created_by: str,
        **kwargs: object,
    ) -> "GongConnector":
        data_entities_processor = DataSourceEntitiesProcessor(
            logger, data_store_provider, config_service
        )
        await data_entities_processor.initialize()
        return cls(
            logger=logger,
            data_entities_processor=data_entities_processor,
            data_store_provider=data_store_provider,
            config_service=config_service,
            connector_id=connector_id,
            scope=scope,
            created_by=created_by,
        )
