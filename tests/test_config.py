"""Tests for config.py: Pydantic schemas, validation, S3 path scoping, and role seeding."""

import os
import sys
import tempfile
import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pydantic import ValidationError
from sqlmodel import Session, select

import config
from config import (
    engine, init_db, seed_roles,
    PipelineConfig, CronJobConfig, AlertConfig, TransformConfig, CheckConfig,
    extract_s3_paths, path_in_scope, role_disallowed_paths, role_disallowed_paths_by_name,
    Role, User, ROLE_ADMIN,
)


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


# --- Pydantic Schema Validation Tests ---

def test_alert_config_validation():
    # Valid alert config (no failure alert)
    cfg1 = AlertConfig(on_failure="none")
    assert cfg1.on_failure == "none"

    # Valid alert config (email failure alert)
    cfg2 = AlertConfig(on_failure="email", email="admin@example.com")
    assert cfg2.on_failure == "email"
    assert cfg2.email == "admin@example.com"

    # Invalid: email required when on_failure is "email"
    with pytest.raises(ValidationError) as excinfo:
        AlertConfig(on_failure="email")
    assert "email is required when on_failure is email" in str(excinfo.value)


def test_pipeline_config_validation():
    # Valid minimal pipeline config
    p1 = PipelineConfig(
        name="valid_pipeline",
        source_path="data/in.parquet",
        sink_path="data/out.parquet",
        alerts={"on_failure": "none"}
    )
    assert p1.name == "valid_pipeline"
    assert p1.sink_format == "parquet"

    # Invalid pipeline name (regex pattern requirement)
    with pytest.raises(ValidationError):
        PipelineConfig(
            name="Invalid-Name!",
            source_path="data/in.parquet",
            sink_path="data/out.parquet",
            alerts={"on_failure": "none"}
        )

    # Valid target_file_size formats
    for size in ["200MB", "1.5GB", " 10KB ", "100B"]:
        p = PipelineConfig(
            name="valid_pipeline",
            source_path="a",
            sink_path="b",
            target_file_size=size,
            alerts={"on_failure": "none"}
        )
        assert p.target_file_size == size.strip()

    # Invalid target_file_size formats
    for size in ["200", "200 gigabytes", "MB", "10X"]:
        with pytest.raises(ValidationError):
            PipelineConfig(
                name="valid_pipeline",
                source_path="a",
                sink_path="b",
                target_file_size=size,
                alerts={"on_failure": "none"}
            )


def test_cronjob_config_validation():
    # Valid cronjob config
    c1 = CronJobConfig(
        name="valid_job",
        pipeline="valid_pipeline",
        schedule="*/5 * * * *",
        enabled=True,
        retry={"max_attempts": 3, "delay_seconds": 60}
    )
    assert c1.name == "valid_job"
    assert c1.schedule == "*/5 * * * *"

    # Invalid cronjob schedule (not 5 parts)
    with pytest.raises(ValidationError) as excinfo:
        CronJobConfig(
            name="valid_job",
            pipeline="valid_pipeline",
            schedule="* * * * * *",  # 6 parts
            enabled=True,
            retry={"max_attempts": 3, "delay_seconds": 60}
        )
    assert "schedule must be a valid 5-part cron expression" in str(excinfo.value)

    with pytest.raises(ValidationError, match="valid IANA timezone"):
        CronJobConfig(
            name="valid_job",
            pipeline="valid_pipeline",
            schedule="0 0 * * *",
            timezone="Mars/Olympus",
            enabled=True,
            retry={"max_attempts": 3, "delay_seconds": 60},
        )

    with pytest.raises(ValidationError, match="named IANA timezone"):
        CronJobConfig(
            name="valid_job",
            pipeline="valid_pipeline",
            schedule="0 0 * * *",
            timezone="localtime",
            enabled=True,
            retry={"max_attempts": 3, "delay_seconds": 60},
        )


# --- S3 URI Path Scoping Tests ---

def test_extract_s3_paths():
    sql = "SELECT * FROM read_parquet('s3://my-bucket/path/to/file.parquet') JOIN read_parquet('S3://another-bucket/file2.parquet')"
    paths = extract_s3_paths(sql)
    assert len(paths) == 2
    assert "s3://my-bucket/path/to/file.parquet" in paths
    assert "S3://another-bucket/file2.parquet" in paths

    # Non-S3 string
    assert extract_s3_paths("SELECT * FROM read_parquet('local/file.parquet')") == []


def test_path_in_scope():
    # Empty allow list means unrestricted access
    assert path_in_scope(bucket_allow=[], bucket_deny=[], path="s3://any-bucket/file") is True

    # Allowed path prefix
    assert path_in_scope(bucket_allow=["s3://allowed-bucket/"], bucket_deny=[], path="s3://allowed-bucket/sub/file") is True
    assert path_in_scope(bucket_allow=["s3://allowed-bucket/"], bucket_deny=[], path="s3://denied-bucket/sub/file") is False

    # Denied path prefix (deny wins)
    assert path_in_scope(
        bucket_allow=["s3://allowed-bucket/"],
        bucket_deny=["s3://allowed-bucket/private/"],
        path="s3://allowed-bucket/private/file"
    ) is False


def test_role_disallowed_paths():
    # Admin wildcard bypasses scoping
    assert role_disallowed_paths(
        permissions=["*"],
        bucket_allow=["s3://restricted/"],
        bucket_deny=[],
        paths=["s3://other/file.parquet"]
    ) == []

    # Non-admin filtering
    disallowed = role_disallowed_paths(
        permissions=["pipelines.view"],
        bucket_allow=["s3://allowed-bucket/"],
        bucket_deny=["s3://allowed-bucket/private/"],
        paths=["s3://allowed-bucket/file.parquet", "s3://allowed-bucket/private/file.parquet", "s3://other/file.parquet", "/local/path/file.parquet"]
    )
    # "/local/path/file.parquet" is local, so never restricted.
    # "s3://allowed-bucket/file.parquet" is allowed.
    # "s3://allowed-bucket/private/file.parquet" is denied.
    # "s3://other/file.parquet" is not in allow-list, so denied.
    assert set(disallowed) == {"s3://allowed-bucket/private/file.parquet", "s3://other/file.parquet"}


def test_role_disallowed_paths_by_name():
    # Seed a test role
    with Session(engine) as db:
        role = db.exec(select(Role).where(Role.name == "scopetest")).first()
        if not role:
            db.add(Role(
                name="scopetest",
                permissions=["pipelines.view"],
                bucket_allow=["s3://scope-allow/"],
                bucket_deny=["s3://scope-allow/secret/"]
            ))
            db.commit()

    assert role_disallowed_paths_by_name("scopetest", ["s3://scope-allow/file", "s3://scope-allow/secret/file", "s3://scope-other/file"]) == [
        "s3://scope-allow/secret/file",
        "s3://scope-other/file"
    ]
    # Empty role name behaves as unrestricted
    assert role_disallowed_paths_by_name(None, ["s3://scope-other/file"]) == []


# --- Role Seeding & Deprecation Tests ---

def test_seed_roles():
    # Call seed_roles
    seed_roles()

    with Session(engine) as db:
        admin_role = db.exec(select(Role).where(Role.name == ROLE_ADMIN)).first()
        assert admin_role is not None
        assert admin_role.is_system is True
        assert "*" in admin_role.permissions


def test_seed_roles_deletes_deprecated_role_when_unused():
    # Add a deprecated system role to the database
    with Session(engine) as db:
        dep_role = db.exec(select(Role).where(Role.name == "viewer")).first()
        if not dep_role:
            db.add(Role(name="viewer", permissions=["dashboard.view"], is_system=True))
            db.commit()

    # Call seed_roles: "viewer" is deprecated and should be deleted since no users hold it
    seed_roles()

    with Session(engine) as db:
        dep_role = db.exec(select(Role).where(Role.name == "viewer")).first()
        assert dep_role is None


def test_seed_roles_keeps_deprecated_role_if_in_use():
    with Session(engine) as db:
        # Re-add deprecated system role
        dep_role = db.exec(select(Role).where(Role.name == "viewer")).first()
        if not dep_role:
            dep_role = Role(name="viewer", permissions=["dashboard.view"], is_system=True)
            db.add(dep_role)
            db.commit()

        # Add user holding the role
        user = db.exec(select(User).where(User.username == "viewer_user")).first()
        if not user:
            db.add(User(username="viewer_user", password_hash="dummy", role="viewer"))
            db.commit()

    # Call seed_roles
    seed_roles()

    # It should NOT be deleted
    with Session(engine) as db:
        dep_role = db.exec(select(Role).where(Role.name == "viewer")).first()
        assert dep_role is not None
        # Clean up
        user = db.exec(select(User).where(User.username == "viewer_user")).first()
        db.delete(user)
        db.delete(dep_role)
        db.commit()
