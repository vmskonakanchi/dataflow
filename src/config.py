import os
from datetime import datetime
from typing import List, Dict, Optional, Literal, Union, Annotated, Any
from dataclasses import dataclass
from pydantic import BaseModel, Field, field_validator, model_validator, StringConstraints, TypeAdapter
from sqlmodel import SQLModel, Field as SQLField, JSON, Column, create_engine, Session, select
from sqlalchemy import event

# --- Database Setup ---
DB_PATH = os.environ.get("DATAFLOW_DB", "dataflow_config.db")
sqlite_url = f"sqlite:///{DB_PATH}?timeout=30"
engine = create_engine(sqlite_url, echo=False)


@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, _connection_record):
    """Enable WAL so the server and worker processes can access the DB
    concurrently without lock contention."""
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=30000")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.close()

def init_db():
    """Initialize the database and create tables."""
    SQLModel.metadata.create_all(engine)

def get_session():
    """Utility to get a basic session."""
    return Session(engine)


def seed_roles():
    """Ensure the built-in system role(s) exist. Idempotent; safe on every
    startup. Only `admin` is seeded. Any previously-seeded default roles that we
    no longer ship (viewer/editor) are removed — but only if no user still holds
    them, so we never orphan an existing account."""
    with Session(engine) as session:
        for name, perms in SYSTEM_ROLES.items():
            row = session.exec(select(Role).where(Role.name == name)).first()
            if row is None:
                session.add(Role(
                    name=name,
                    description=f"Built-in {name} role",
                    permissions=list(perms),
                    is_system=True,
                ))
            else:
                row.permissions = list(perms)
                row.is_system = True
                session.add(row)
        # Drop deprecated default roles we no longer seed, if nobody uses them.
        for legacy in _DEPRECATED_DEFAULT_ROLES:
            if legacy in SYSTEM_ROLES:
                continue
            row = session.exec(select(Role).where(Role.name == legacy)).first()
            if row and row.is_system:
                in_use = session.exec(select(User).where(User.role == legacy)).first()
                if not in_use:
                    session.delete(row)
        session.commit()


def prune_audit_logs(days: int) -> int:
    """Delete audit log rows older than `days`. Returns the number deleted.
    days <= 0 means keep forever (no-op)."""
    if not days or days <= 0:
        return 0
    from datetime import timedelta
    from sqlalchemy import delete as _delete
    cutoff = datetime.utcnow() - timedelta(days=days)
    with Session(engine) as session:
        result = session.execute(_delete(AuditLog).where(AuditLog.timestamp < cutoff))
        session.commit()
        return result.rowcount or 0

# --- Configuration Schemas (Pydantic) ---
NAME_REGEX = r"^[a-z][a-z0-9_]*$"
NameString = Annotated[str, StringConstraints(pattern=NAME_REGEX)]

# --- RBAC: permissions & roles ---
# Permissions are "<resource>.<action>" keys. A role is a stored set of these
# keys; a user references a role by name. Enforcement is per-permission.

# Built-in role names (seeded as system roles; admins may add custom roles).
ROLE_VIEWER = "viewer"
ROLE_EDITOR = "editor"
ROLE_ADMIN = "admin"

# Grouped for the matrix UI. Order here drives display order.
PERMISSION_GROUPS = {
    "Dashboard": ["dashboard.view"],
    "Pipelines": ["pipelines.view", "pipelines.create", "pipelines.edit", "pipelines.delete", "pipelines.run"],
    "Schedules": ["cronjobs.view", "cronjobs.create", "cronjobs.edit", "cronjobs.delete", "cronjobs.run"],
    "Query Tool": ["query.run"],
    "Flux AI": ["flux.ask"],
    "Administration": ["users.manage", "roles.manage", "settings.manage", "audit.view"],
}
ALL_PERMISSIONS = [p for perms in PERMISSION_GROUPS.values() for p in perms]

# Short, human-friendly column labels for the matrix UI (the raw action part is
# ambiguous — e.g. users/roles/settings all use ".manage").
PERMISSION_LABELS = {
    "dashboard.view": "View",
    "pipelines.view": "View", "pipelines.create": "Create", "pipelines.edit": "Edit",
    "pipelines.delete": "Delete", "pipelines.run": "Run",
    "cronjobs.view": "View", "cronjobs.create": "Create", "cronjobs.edit": "Edit",
    "cronjobs.delete": "Delete", "cronjobs.run": "Run",
    "query.run": "Run",
    "flux.ask": "Ask",
    "users.manage": "Users", "roles.manage": "Roles", "settings.manage": "Settings",
    "audit.view": "Audit",
}

# Wildcard grants every permission (used by the built-in admin role).
WILDCARD = "*"

# Every permission — including view — is explicitly grantable. Nothing is
# implicitly baseline or locked; a role has exactly the permissions it stores.
BASELINE_PERMISSIONS = set()
LOCKED_PERMISSIONS = set()

# The view permission that gates each page route (used for nav + landing).
PAGE_PERMISSIONS = [
    ("/", "dashboard.view"),
    ("/pipelines", "pipelines.view"),
    ("/cronjobs", "cronjobs.view"),
    ("/query", "query.run"),
    ("/audit", "audit.view"),
]

# Built-in roles seeded on startup. Only `admin` is seeded by default; other
# roles (viewer/editor or any custom role) are created by admins as needed.
SYSTEM_ROLES = {
    ROLE_ADMIN: [WILDCARD],
}

# Default roles we used to seed but no longer do — removed on startup if unused.
_DEPRECATED_DEFAULT_ROLES = (ROLE_VIEWER, ROLE_EDITOR)


def permissions_for(stored) -> set:
    """Resolve a stored permission list into the effective permission set.
    WILDCARD expands to every permission; otherwise the set is used verbatim
    (no implicit/baseline grants — view is a real, revocable permission)."""
    perms = set(stored or [])
    if WILDCARD in perms:
        return set(ALL_PERMISSIONS)
    return perms


# --- Data-access (bucket) scoping ---
import re as _re

_S3_URI_RE = _re.compile(r"s3://[^\s'\"();,]+", _re.IGNORECASE)


def extract_s3_paths(text: str) -> list:
    """Return all s3:// URIs referenced in a string (SQL, path, etc.)."""
    if not text:
        return []
    return _S3_URI_RE.findall(text)


def path_in_scope(bucket_allow, bucket_deny, path: str) -> bool:
    """Is `path` permitted by an allow/deny prefix list?
    - deny always wins (any matching deny prefix -> False)
    - empty allow -> unrestricted (opt-in scoping)
    - otherwise the path must start with an allowed prefix
    """
    for d in (bucket_deny or []):
        if d and path.startswith(d):
            return False
    if not bucket_allow:
        return True
    return any(path.startswith(a) for a in bucket_allow if a)


def role_disallowed_paths(permissions, bucket_allow, bucket_deny, paths) -> list:
    """Return the subset of `paths` this role may NOT access. The
    wildcard-permission (admin) role bypasses scoping. Only s3:// paths are
    scoped; local paths are never restricted here."""
    if WILDCARD in (permissions or []):
        return []
    bad = []
    for p in paths:
        if not p or not p.lower().startswith("s3://"):
            continue
        if not path_in_scope(bucket_allow, bucket_deny, p):
            bad.append(p)
    return bad


def role_disallowed_paths_by_name(role_name, paths) -> list:
    """Resolve a role by name from the DB and return which of `paths` it may not
    access. Empty/None role name -> unrestricted. Used at pipeline run time."""
    if not role_name:
        return []
    with Session(engine) as s:
        r = s.exec(select(Role).where(Role.name == role_name)).first()
    perms = list(r.permissions or []) if r else []
    allow = list(r.bucket_allow or []) if r else []
    deny = list(r.bucket_deny or []) if r else []
    return role_disallowed_paths(perms, allow, deny, paths)

class TransformSelect(BaseModel):
    type: Literal["select"]
    columns: List[str]

class TransformFilter(BaseModel):
    type: Literal["filter"]
    condition: str

class TransformAggregate(BaseModel):
    type: Literal["aggregate"]
    group_by: List[str]
    aggregates: List[str]

class TransformJoin(BaseModel):
    type: Literal["join"]
    right_path: str
    join_type: Literal["inner", "left", "right", "full"]
    on: str

class TransformPython(BaseModel):
    type: Literal["python"]
    function: NameString = Field(description="Name of a registered plugin in src/transforms/ (must expose transform(table, params))")
    params: Dict[str, Any] = Field(default_factory=dict, description="Keyword arguments passed to the plugin")
    chunk_rows: int = Field(
        default=0,
        ge=0,
        description=(
            "If > 0, run the plugin in memory-safe chunks of this many rows, each "
            "in a fresh subprocess so memory is fully reclaimed between chunks "
            "(bounds peak RAM regardless of dataset size). 0 = run in-process on "
            "the full table (fast, but the working set must fit in RAM)."
        ),
    )

TransformConfig = Annotated[
    Union[TransformSelect, TransformFilter, TransformAggregate, TransformJoin, TransformPython],
    Field(discriminator="type")
]

# --- Data Quality Check Schemas ---
class CheckNotNull(BaseModel):
    type: Literal["not_null"]
    columns: List[str]

class CheckUnique(BaseModel):
    type: Literal["unique"]
    columns: List[str]

class CheckRowCountMin(BaseModel):
    type: Literal["row_count_min"]
    value: int = Field(ge=1)

class CheckAcceptedValues(BaseModel):
    type: Literal["accepted_values"]
    column: str
    values: List[str]

class CheckCustomSQL(BaseModel):
    type: Literal["custom_sql"]
    query: str
    must_be: int = 0

CheckConfig = Annotated[
    Union[CheckNotNull, CheckUnique, CheckRowCountMin, CheckAcceptedValues, CheckCustomSQL],
    Field(discriminator="type")
]

class AlertConfig(BaseModel):
    on_failure: Literal["email", "none"]
    email: Optional[str] = None
    on_row_count_below: Optional[int] = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_email_required(self) -> 'AlertConfig':
        if self.on_failure == "email" and not self.email:
            raise ValueError("email is required when on_failure is email")
        return self

class PipelineConfig(BaseModel):
    name: NameString
    description: Optional[str] = None
    source_path: str
    sink_path: str
    sink_format: Literal["parquet", "delta"] = "parquet"
    partition_by: Optional[str] = Field(default=None, description="Column to partition output by (e.g. 'date')")
    checkpointing: bool = Field(default=False, description="Enable stage-level checkpointing for resume on failure")
    threads: Optional[int] = Field(default=None, ge=1, description="Number of threads for parallel execution")
    memory_limit: Optional[str] = Field(default=None, description="Memory footprint ceiling (e.g. '16GB')")
    run_as: Optional[str] = Field(default=None, description="Role whose data-access scope this pipeline runs under")
    target_file_size: Optional[str] = Field(default=None, description="Split a Parquet sink into multiple files of ~this size (e.g. '200MB'). Unset = single file.")
    row_group_size: Optional[int] = Field(default=None, ge=1, description="Parquet row group size in rows (advanced tuning)")
    transforms: List[TransformConfig] = []
    checks: List[CheckConfig] = []

    @field_validator("target_file_size")
    @classmethod
    def _validate_file_size(cls, v):
        if v is None or str(v).strip() == "":
            return None
        import re as _re
        if not _re.match(r"^\s*\d+(\.\d+)?\s*(B|KB|MB|GB|TB)\s*$", str(v), _re.IGNORECASE):
            raise ValueError("target_file_size must look like '200MB', '1GB', etc.")
        return v.strip()
    alerts: AlertConfig

class RetryConfig(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    delay_seconds: int = Field(default=60, ge=10, le=3600)

class CronJobConfig(BaseModel):
    name: NameString
    pipeline: str
    schedule: str
    timezone: str = "UTC"
    enabled: bool
    retry: RetryConfig

    @field_validator("schedule")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("schedule must be a valid 5-part cron expression")
        return v

# --- Database Tables (SQLModel) ---
class Pipeline(SQLModel, table=True):
    id: Optional[int] = SQLField(default=None, primary_key=True)
    name: str = SQLField(index=True, unique=True, regex=NAME_REGEX)
    description: Optional[str] = None
    source_path: str
    sink_path: str
    sink_format: str = "parquet"
    partition_by: Optional[str] = None
    checkpointing: bool = False
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    # The role whose data-access scope (and, later, AWS role) this pipeline runs
    # under. Defaults to the creator's role. None = unrestricted (legacy).
    run_as: Optional[str] = None
    target_file_size: Optional[str] = None
    row_group_size: Optional[int] = None
    transforms: List[Dict[str, Any]] = SQLField(default_factory=list, sa_column=Column(JSON))
    checks: List[Dict[str, Any]] = SQLField(default_factory=list, sa_column=Column(JSON))
    alerts: Dict[str, Any] = SQLField(default_factory=dict, sa_column=Column(JSON))

class CronJob(SQLModel, table=True):
    id: Optional[int] = SQLField(default=None, primary_key=True)
    name: str = SQLField(index=True, unique=True, regex=NAME_REGEX)
    pipeline: str
    schedule: str
    timezone: str = "UTC"
    enabled: bool = True
    retry: Dict[str, Any] = SQLField(default_factory=dict, sa_column=Column(JSON))

class User(SQLModel, table=True):
    id: Optional[int] = SQLField(default=None, primary_key=True)
    username: str = SQLField(index=True, unique=True)
    password_hash: str
    is_admin: bool = False
    # RBAC: the name of the Role this user has (see Role.permissions). is_admin
    # is kept in sync (is_admin == (role == "admin")) for backward compatibility.
    role: str = SQLField(default=ROLE_VIEWER)
    # How this account authenticates: "local" (username/password) or an SSO
    # provider key (e.g. "entra"). SSO users can't password-login.
    auth_provider: str = SQLField(default="local")
    created_at: datetime = SQLField(default_factory=datetime.utcnow)


class Role(SQLModel, table=True):
    """An RBAC role: a named set of permission keys. System roles (viewer,
    editor, admin) are seeded and locked; admins can create custom roles."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    name: str = SQLField(index=True, unique=True, regex=NAME_REGEX)
    description: Optional[str] = None
    permissions: List[str] = SQLField(default_factory=list, sa_column=Column(JSON))
    is_system: bool = False
    # Data-access scope: S3 URI prefixes this role may read/write. Empty allow =
    # unrestricted (opt-in scoping). deny always wins. The wildcard-permission
    # (admin) role bypasses scoping entirely. Interim app-level enforcement;
    # AWS AssumeRole scoping comes later.
    bucket_allow: List[str] = SQLField(default_factory=list, sa_column=Column(JSON))
    bucket_deny: List[str] = SQLField(default_factory=list, sa_column=Column(JSON))

class AppSetting(SQLModel, table=True):
    """Key-value settings stored in the database. Changeable at runtime."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    key: str = SQLField(index=True, unique=True)
    value: str = ""
    description: Optional[str] = None

class PipelineRun(SQLModel, table=True):
    """Execution history for pipeline runs."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    pipeline_name: str = SQLField(index=True)
    job_name: Optional[str] = None
    status: str = SQLField(index=True)  # started | success | failed
    started_at: datetime = SQLField(default_factory=datetime.utcnow, index=True)
    finished_at: Optional[datetime] = None
    rows_extracted: Optional[int] = None
    rows_written: Optional[int] = None
    error_message: Optional[str] = None

class Job(SQLModel, table=True):
    """Durable work queue entry. A worker claims queued jobs, runs them,
    and heartbeats while running so crashes can be detected and recovered."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    pipeline: str = SQLField(index=True)
    status: str = SQLField(default="queued", index=True)  # queued|running|success|failed|cancelled
    trigger: str = "manual"  # manual|schedule|retry
    attempts: int = 0
    max_attempts: int = 1
    run_after: datetime = SQLField(default_factory=datetime.utcnow, index=True)
    enqueued_at: datetime = SQLField(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    heartbeat_at: Optional[datetime] = SQLField(default=None, index=True)
    worker_pid: Optional[int] = None
    run_id: Optional[int] = None
    error_message: Optional[str] = None


class AuditLog(SQLModel, table=True):
    """An immutable record of a security-relevant action: who did what, to what,
    and when. Written at mutation points and viewable by roles with audit.view."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    timestamp: datetime = SQLField(default_factory=datetime.utcnow, index=True)
    username: Optional[str] = SQLField(default=None, index=True)
    role: Optional[str] = None
    action: str = SQLField(index=True)            # e.g. "pipeline.create", "auth.login"
    target_type: Optional[str] = SQLField(default=None, index=True)  # pipeline|cronjob|user|role|setting|auth|model
    target_name: Optional[str] = None
    detail: Optional[str] = None                  # freeform / JSON string
    ip: Optional[str] = None
    success: bool = True

# --- Config Loader ---
@dataclass
class ResolvedConfig:
    pipelines: Dict[str, PipelineConfig]
    cronjobs: Dict[str, CronJobConfig]

def load_configs(config_dir: str = None) -> ResolvedConfig:
    """Load configuration from the SQLite database."""
    init_db()
    
    with Session(engine) as session:
        # Load Pipelines
        pipelines_list = session.exec(select(Pipeline)).all()
        pipelines_dict: Dict[str, PipelineConfig] = {}
        pipeline_adapter = TypeAdapter(PipelineConfig)
        for p in pipelines_list:
            data = {
                "name": p.name,
                "description": p.description,
                "source_path": p.source_path,
                "sink_path": p.sink_path,
                "sink_format": p.sink_format,
                "partition_by": p.partition_by,
                "checkpointing": p.checkpointing,
                "threads": p.threads,
                "memory_limit": p.memory_limit,
                "run_as": p.run_as,
                "target_file_size": p.target_file_size,
                "row_group_size": p.row_group_size,
                "transforms": p.transforms,
                "checks": p.checks,
                "alerts": p.alerts
            }
            pipelines_dict[p.name] = pipeline_adapter.validate_python(data)

        # Load CronJobs
        cronjobs_list = session.exec(select(CronJob)).all()
        cronjobs_dict: Dict[str, CronJobConfig] = {}
        cronjob_adapter = TypeAdapter(CronJobConfig)
        for c in cronjobs_list:
            data = {
                "name": c.name,
                "pipeline": c.pipeline,
                "schedule": c.schedule,
                "timezone": c.timezone,
                "enabled": c.enabled,
                "retry": c.retry
            }
            cronjobs_dict[c.name] = cronjob_adapter.validate_python(data)

    return ResolvedConfig(
        pipelines=pipelines_dict,
        cronjobs=cronjobs_dict
    )
