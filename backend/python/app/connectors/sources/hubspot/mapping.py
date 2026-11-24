"""HubSpot CRM objects as PipesHub records: their text, their links, and who may read them.

Access copies HubSpot's read rules. Super Admins see every record. Everyone sees the
records they own and the ones shared with them or their teams. HubSpot's per-object
"All / Team / Owned" view setting is not in its API, so the admin names, per object type,
the roles or users whose setting is "All" or "Team"; everyone else counts as "Owned",
which errs towards hiding.

A record grants groups, never users: ``all_<type>``, its owner's ``owner_<user>_<type>``,
``super_admins``, and its shared users' and teams' groups. Memberships are rewritten every
sync, so a role, team or user change takes effect without touching the records.
"""

import hashlib
import mimetypes
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.config.constants.arangodb import (
    Connectors,
    MimeTypes,
    OriginTypes,
    ProgressStatus,
    RecordRelations,
)
from app.connectors.utils.record_ids import new_record_id
from app.models.entities import (
    DealRecord,
    FileRecord,
    MeetingRecord,
    Priority,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    RelatedExternalRecord,
    Status,
    TicketRecord,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.utils.html_markdown import html_to_markdown

SUPER_ADMINS_GROUP_ID = "super_admins"
# Every active user: lets them browse the folders. A folder grants no record, so each
# user still lists only the records their own grants reach.
ALL_USERS_GROUP_ID = "all_users"
USER_GROUP_PREFIX = "user_"
TEAM_GROUP_PREFIX = "team_"
ALL_GROUP_PREFIX = "all_"
OWNER_GROUP_PREFIX = "owner_"
SCOPE_ALL = "all"
SCOPE_TEAM = "team"
FILES_GROUP_ID = "files"
# ponytail: a record's text names its first 25 linked contacts and companies; more is noise.
MAX_LINKED = 25

ACCESS_PROPERTIES = (
    "hubspot_owner_id",
    "hubspot_team_id",
    "hs_all_team_ids",
    "hs_shared_user_ids",
    "hs_shared_team_ids",
    "hs_lastmodifieddate",
)
LINKED_PROPERTIES = {
    "contacts": ("firstname", "lastname", "email", "jobtitle", "phone"),
    "companies": ("name", "domain", "industry", "city"),
    "deals": ("dealname",),
    "tickets": ("subject",),
}


@dataclass(frozen=True)
class ObjectSpec:
    name: str
    type_id: str
    label: str
    group_name: str
    record_type: RecordType
    group_type: RecordGroupType
    title_property: str
    properties: tuple[str, ...]
    links_to: tuple[str, ...]
    # HubSpot refuses to page deleted meetings (HTTP 400 "not yet supported for object type
    # 0-47"); their deletions wait for a full sync's cleanup.
    lists_archived: bool = True

    @property
    def all_properties(self) -> list[str]:
        return list(dict.fromkeys(self.properties + ACCESS_PROPERTIES))


DEALS = ObjectSpec(
    "deals", "0-3", "Deal", "Deals", RecordType.DEAL, RecordGroupType.DEAL, "dealname",
    ("dealname", "amount", "dealstage", "pipeline", "closedate", "description", "dealtype",
     "hs_priority", "hs_is_closed", "hs_is_closed_won", "createdate"),
    ("companies", "contacts"),
)
TICKETS = ObjectSpec(
    "tickets", "0-5", "Ticket", "Tickets", RecordType.TICKET, RecordGroupType.CASE, "subject",
    ("subject", "content", "hs_pipeline", "hs_pipeline_stage", "hs_ticket_priority",
     "hs_ticket_category", "createdate", "closed_date"),
    ("companies", "contacts", "deals"),
)
TASKS = ObjectSpec(
    "tasks", "0-27", "Task", "Tasks", RecordType.TASK, RecordGroupType.TASK, "hs_task_subject",
    ("hs_task_subject", "hs_task_body", "hs_task_status", "hs_task_priority", "hs_task_type",
     "hs_timestamp", "hs_createdate"),
    ("companies", "contacts", "deals", "tickets"),
)
MEETINGS = ObjectSpec(
    "meetings", "0-47", "Meeting", "Meetings", RecordType.MEETING, RecordGroupType.TASK, "hs_meeting_title",
    ("hs_meeting_title", "hs_meeting_body", "hs_meeting_start_time", "hs_meeting_end_time",
     "hs_meeting_outcome", "hs_meeting_location", "hs_createdate"),
    ("companies", "contacts", "deals", "tickets"),
    lists_archived=False,
)
OBJECT_SPECS = (DEALS, TICKETS, TASKS, MEETINGS)
SPECS_BY_NAME = {spec.name: spec for spec in OBJECT_SPECS}
SPECS_BY_TYPE_ID = {spec.type_id: spec for spec in OBJECT_SPECS}
RECORD_TYPE_OF = {"deals": RecordType.DEAL, "tickets": RecordType.TICKET}

_TICKET_PRIORITY = {"LOW": Priority.LOW, "MEDIUM": Priority.MEDIUM, "HIGH": Priority.HIGH, "URGENT": Priority.CRITICAL}
_TASK_STATUS = {
    "NOT_STARTED": Status.NEW,
    "IN_PROGRESS": Status.IN_PROGRESS,
    "WAITING": Status.WAITING,
    "DEFERRED": Status.PENDING,
    "COMPLETED": Status.DONE,
}


@dataclass
class Directory:
    """What one sync read about the account: who exists, and the labels records refer to."""

    portal_id: str
    ui_domain: str = "app.hubspot.com"
    # Active users by HubSpot user id: {"email", "name", "super_admin"}.
    users: dict[str, dict[str, object]] = field(default_factory=dict)
    # Active owners by owner id: {"user_id", "email", "name"}.
    owners: dict[str, dict[str, str | None]] = field(default_factory=dict)
    # (object type, pipeline id) -> label, and (object type, stage id) -> {"label", "state"}.
    pipelines: dict[tuple[str, str], str] = field(default_factory=dict)
    stages: dict[tuple[str, str], dict[str, str | None]] = field(default_factory=dict)
    # User ids HubSpot no longer lists as active but that still own records.
    deactivated: set[str] = field(default_factory=set)

    def known_user(self, user_id: str) -> bool:
        return user_id in self.users or user_id in self.deactivated

    def record_url(self, type_id: str, object_id: str) -> str:
        return f"https://{self.ui_domain}/contacts/{self.portal_id}/record/{type_id}/{object_id}"

    def owner_label(self, owner_id: str | None) -> str | None:
        owner = self.owners.get(owner_id or "")
        if not owner:
            return None
        return f"{owner['name']} <{owner['email']}>" if owner.get("name") else owner.get("email")


def external_id(spec: ObjectSpec, object_id: str) -> str:
    # HubSpot ids are unique only within an object type.
    return f"{spec.type_id}:{object_id}"


def split_ids(value: object) -> list[str]:
    return [part for part in str(value or "").split(";") if part.strip()]


def to_ms(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        moment = value
    else:
        try:
            moment = datetime.fromisoformat(str(value))
        except ValueError:
            return int(value) if str(value).isdigit() else None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def _group(external_id: str) -> Permission:
    return Permission(external_id=external_id, type=PermissionType.READ, entity_type=EntityType.GROUP)


def all_group_id(object_type: str) -> str:
    return f"{ALL_GROUP_PREFIX}{object_type}"


def owner_group_id(user_id: str, object_type: str) -> str:
    return f"{OWNER_GROUP_PREFIX}{user_id}_{object_type}"


def record_permissions(object_type: str, props: Mapping[str, object], directory: Directory) -> list[Permission]:
    """Super Admins, the type's "All" viewers, the owner with their "Team" viewers, and shares."""
    grants = [SUPER_ADMINS_GROUP_ID, all_group_id(object_type)]
    owner = directory.owners.get(str(props.get("hubspot_owner_id") or ""))
    owner_user = owner.get("user_id") if owner else None
    if owner_user and directory.known_user(owner_user):
        grants.append(owner_group_id(owner_user, object_type))
    grants += [f"{USER_GROUP_PREFIX}{uid}" for uid in split_ids(props.get("hs_shared_user_ids")) if uid in directory.users]
    grants += [f"{TEAM_GROUP_PREFIX}{tid}" for tid in split_ids(props.get("hs_shared_team_ids"))]
    return [_group(gid) for gid in dict.fromkeys(grants)]


def _line(label: str, value: object) -> str | None:
    return f"**{label}:** {value}" if value not in (None, "") else None


def _linked_lines(object_type: str, linked: list[Mapping[str, object]]) -> list[str]:
    lines = []
    for item in linked[:MAX_LINKED]:
        props = item.get("properties") or {}
        if object_type == "contacts":
            name = " ".join(p for p in (props.get("firstname"), props.get("lastname")) if p) or "(no name)"
            details = ", ".join(str(p) for p in (props.get("email"), props.get("jobtitle"), props.get("phone")) if p)
        elif object_type == "companies":
            name = props.get("name") or props.get("domain") or "(no name)"
            details = ", ".join(str(p) for p in (props.get("domain"), props.get("industry"), props.get("city")) if p)
        else:
            name = props.get("dealname") or props.get("subject") or f"#{item.get('id')}"
            details = ""
        lines.append(f"- {name}" + (f" ({details})" if details else ""))
    return lines


def _body(value: object) -> str:
    text = str(value or "").strip()
    return html_to_markdown(text).strip() if "<" in text else text


def render_markdown(
    spec: ObjectSpec,
    obj: Mapping[str, object],
    linked: Mapping[str, list[Mapping[str, object]]],
    directory: Directory,
) -> str:
    """The text indexed for one object. Its hash is the record's revision, so keep it free of volatile fields."""
    props = obj.get("properties") or {}
    title = props.get(spec.title_property) or f"{spec.label} {obj.get('id')}"
    lines = [f"# {title}", "", f"**Type:** HubSpot {spec.label.lower()}"]
    pipeline = props.get("pipeline") or props.get("hs_pipeline")
    stage = props.get("dealstage") or props.get("hs_pipeline_stage")
    fields: list[str | None] = [
        _line("Pipeline", directory.pipelines.get((spec.name, str(pipeline)), pipeline) if pipeline else None),
        _line("Stage", (directory.stages.get((spec.name, str(stage))) or {}).get("label", stage) if stage else None),
        _line("Owner", directory.owner_label(str(props.get("hubspot_owner_id") or ""))),
    ]
    if spec is DEALS:
        fields += [_line("Amount", props.get("amount")), _line("Close date", props.get("closedate")),
                   _line("Deal type", props.get("dealtype")), _line("Priority", props.get("hs_priority"))]
    elif spec is TICKETS:
        fields += [_line("Priority", props.get("hs_ticket_priority")), _line("Category", props.get("hs_ticket_category")),
                   _line("Closed", props.get("closed_date"))]
    elif spec is TASKS:
        fields += [_line("Status", props.get("hs_task_status")), _line("Priority", props.get("hs_task_priority")),
                   _line("Task type", props.get("hs_task_type")), _line("Due", props.get("hs_timestamp"))]
    else:
        fields += [_line("Starts", props.get("hs_meeting_start_time")), _line("Ends", props.get("hs_meeting_end_time")),
                   _line("Location", props.get("hs_meeting_location")), _line("Outcome", props.get("hs_meeting_outcome"))]
    lines += [f for f in fields if f]
    body = _body(props.get("description") or props.get("content") or props.get("hs_task_body") or props.get("hs_meeting_body"))
    if body:
        lines += ["", "## Description", "", body]
    for object_type, heading in (("companies", "Companies"), ("contacts", "Contacts"), ("deals", "Deals"), ("tickets", "Tickets")):
        items = _linked_lines(object_type, list(linked.get(object_type) or []))
        if items:
            lines += ["", f"## {heading}", "", *items]
    return "\n".join(lines) + "\n"


def content_revision(markdown: str) -> str:
    return hashlib.sha256(markdown.encode("utf-8")).hexdigest()


def _float(value: object) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def build_record(
    spec: ObjectSpec,
    obj: Mapping[str, object],
    markdown: str,
    linked_ids: Mapping[str, list[str]],
    directory: Directory,
    connector_id: str,
    *,
    index: bool,
) -> Record:
    props = obj.get("properties") or {}
    object_id = str(obj["id"])
    record_external_id = external_id(spec, object_id)
    owner = directory.owners.get(str(props.get("hubspot_owner_id") or "")) or {}
    common: dict[str, object] = {
        "id": new_record_id(connector_id, record_external_id),
        "record_name": str(props.get(spec.title_property) or f"{spec.label} {object_id}"),
        "record_type": spec.record_type,
        "record_group_type": spec.group_type,
        "external_record_id": record_external_id,
        "external_revision_id": content_revision(markdown),
        "external_record_group_id": spec.name,
        "version": 0,
        "origin": OriginTypes.CONNECTOR,
        "connector_name": Connectors.HUBSPOT,
        "connector_id": connector_id,
        "mime_type": MimeTypes.BLOCKS.value,
        "weburl": directory.record_url(spec.type_id, object_id),
        "source_created_at": to_ms(obj.get("created_at")),
        "source_updated_at": to_ms(obj.get("updated_at")),
        # Each record carries its own grants; the Deals/Tickets folder does not open it.
        "inherit_permissions": False,
    }
    if not index:
        common["indexing_status"] = ProgressStatus.AUTO_INDEX_OFF.value

    if spec is DEALS:
        return DealRecord(
            **common,
            name=props.get("dealname"),
            amount=_float(props.get("amount")),
            expected_close_date=props.get("closedate"),
            type=props.get("dealtype"),
            owner_id=owner.get("user_id"),
            is_won=str(props.get("hs_is_closed_won")).lower() == "true" if props.get("hs_is_closed_won") else None,
            is_closed=str(props.get("hs_is_closed")).lower() == "true" if props.get("hs_is_closed") else None,
            created_date=props.get("createdate"),
        )
    if spec is MEETINGS:
        start, end = to_ms(props.get("hs_meeting_start_time")), to_ms(props.get("hs_meeting_end_time"))
        return MeetingRecord(
            **common,
            host_email=owner.get("email"),
            start_time=props.get("hs_meeting_start_time"),
            end_time=props.get("hs_meeting_end_time"),
            duration_minutes=(end - start) // 60_000 if start and end and end >= start else None,
        )
    if spec is TICKETS:
        stage = directory.stages.get((spec.name, str(props.get("hs_pipeline_stage")))) or {}
        state = str(stage.get("state") or "").upper()
        status = Status.CLOSED if state == "CLOSED" else Status.OPEN if state == "OPEN" else None
        priority = _TICKET_PRIORITY.get(str(props.get("hs_ticket_priority") or "").upper())
        source_status = stage.get("label")
    else:
        status = _TASK_STATUS.get(str(props.get("hs_task_status") or "").upper())
        priority = _TICKET_PRIORITY.get(str(props.get("hs_task_priority") or "").upper())
        source_status = props.get("hs_task_status")
    related = [
        RelatedExternalRecord(
            external_record_id=external_id(SPECS_BY_NAME[object_type], linked_id),
            record_type=RECORD_TYPE_OF[object_type],
            relation_type=RecordRelations.LINKED_TO,
        )
        for object_type in ("deals", "tickets")
        if object_type in RECORD_TYPE_OF and object_type != spec.name
        for linked_id in linked_ids.get(object_type) or []
    ]
    return TicketRecord(
        **common,
        status=status,
        source_status=source_status,
        priority=priority,
        assignee=owner.get("name") or owner.get("email"),
        assignee_email=owner.get("email"),
        related_external_records=related,
    )


def build_record_group(spec_name: str, group_name: str, group_type: RecordGroupType, connector_id: str, web_url: str) -> RecordGroup:
    return RecordGroup(
        name=group_name,
        external_group_id=spec_name,
        group_type=group_type,
        connector_name=Connectors.HUBSPOT,
        connector_id=connector_id,
        web_url=web_url,
    )


def build_file_record(file: Mapping[str, object], directory: Directory, connector_id: str, *, index: bool) -> FileRecord:
    file_id = str(file["id"])
    record_external_id = f"file:{file_id}"
    extension = str(file.get("extension") or "").lower() or None
    name = str(file.get("name") or file_id)
    filename = f"{name}.{extension}" if extension and not name.lower().endswith(f".{extension}") else name
    mime, _ = mimetypes.guess_type(filename)
    try:
        mime_type = MimeTypes(mime).value if mime else MimeTypes.BIN.value
    except ValueError:
        mime_type = MimeTypes.BIN.value
    updated = to_ms(file.get("updated_at"))
    record = FileRecord(
        id=new_record_id(connector_id, record_external_id),
        record_name=filename,
        record_type=RecordType.FILE,
        record_group_type=RecordGroupType.FILE_SHARE,
        external_record_id=record_external_id,
        # Replacing a file's content in HubSpot moves updatedAt; nothing finer is exposed.
        external_revision_id=str(updated or ""),
        external_record_group_id=FILES_GROUP_ID,
        version=0,
        origin=OriginTypes.CONNECTOR,
        connector_name=Connectors.HUBSPOT,
        connector_id=connector_id,
        mime_type=mime_type,
        weburl=f"https://{directory.ui_domain}/files/{directory.portal_id}/?showDetails={file_id}",
        source_created_at=to_ms(file.get("created_at")),
        source_updated_at=updated,
        size_in_bytes=file.get("size") if isinstance(file.get("size"), int) else None,
        is_file=True,
        extension=extension,
        path=file.get("path") if isinstance(file.get("path"), str) else None,
        inherit_permissions=False,
    )
    if not index:
        record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value
    return record


def file_permissions() -> list[Permission]:
    # HubSpot files have no owner and the API does not say who may open the file
    # manager, so only Super Admins, who can open everything, are granted.
    return [_group(SUPER_ADMINS_GROUP_ID)]
