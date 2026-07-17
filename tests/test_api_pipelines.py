"""API integration tests for pipeline creation, validation, and conflict handling."""

import os
import sys
import pytest
from starlette.testclient import TestClient

# Ensure src/ is on PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sqlmodel import Session, select
import config
from config import engine, init_db, seed_roles, Pipeline, Role, User
import api
from api import app, hash_password


@pytest.fixture(scope="module", autouse=True)
def seed_admin_user():
    # Make sure we have admin role and user
    seed_roles()
    with Session(engine) as db:
        admin_user = db.exec(select(User).where(User.username == "api_admin")).first()
        if not admin_user:
            db.add(User(
                username="api_admin",
                password_hash=hash_password("adminpass"),
                role="admin",
                is_admin=True
            ))
            db.commit()
    yield
    # Clean up
    with Session(engine) as db:
        admin = db.exec(select(User).where(User.username == "api_admin")).first()
        if admin:
            db.delete(admin)
            db.commit()


def _get_client():
    client = TestClient(app, follow_redirects=False)
    r = client.post("/login", data={"username": "api_admin", "password": "adminpass"})
    assert r.status_code == 302, f"login failed: {r.status_code}"
    return client


# --- API Pipelines Tests ---

def test_api_create_pipeline_success():
    client = _get_client()
    
    # 1. Clean pipeline table
    with Session(engine) as db:
        db.execute(config.SQLModel.metadata.tables["pipeline"].delete())
        db.commit()

    # 2. POST valid pipeline data
    payload = {
        "original_name": "",
        "name": "api_valid_pipeline",
        "description": "A valid pipeline created via API",
        "source_path": "data/in.parquet",
        "sink_path": "data/out.parquet",
        "sink_format": "parquet",
        "timezone": "UTC",
        "threads": "4",
        "memory_limit": "2GB",
        "target_file_size": "200MB",
        "on_failure": "none",
        "transforms_json": "[]",
        "checks_json": "[]"
    }
    
    response = client.post("/api/pipelines", data=payload)
    assert response.status_code == 200
    
    # Check custom success header for Alpine modal to close
    assert response.headers.get("HX-Trigger") == "pipeline-saved"

    # Verify database entry exists
    with Session(engine) as db:
        pipeline = db.exec(select(Pipeline).where(Pipeline.name == "api_valid_pipeline")).first()
        assert pipeline is not None
        assert pipeline.target_file_size == "200MB"
        assert pipeline.threads == 4


def test_api_create_pipeline_duplicate_name():
    client = _get_client()
    
    # Ensure database has a pipeline
    with Session(engine) as db:
        db.execute(config.SQLModel.metadata.tables["pipeline"].delete())
        db.commit()
        db.add(Pipeline(
            name="duplicate_pipeline",
            source_path="data/in.parquet",
            sink_path="data/out.parquet",
            timezone="UTC",
            alerts={"on_failure": "none"}
        ))
        db.commit()

    # Try to create another with the same name
    payload = {
        "original_name": "",
        "name": "duplicate_pipeline",
        "source_path": "data/in2.parquet",
        "sink_path": "data/out2.parquet",
        "timezone": "UTC",
        "on_failure": "none",
        "transforms_json": "[]",
        "checks_json": "[]"
    }

    response = client.post("/api/pipelines", data=payload)
    assert response.status_code == 400
    assert "Pipeline name already exists" in response.text
    
    # Verify no second pipeline is created
    with Session(engine) as db:
        pipelines = db.exec(select(Pipeline).where(Pipeline.name == "duplicate_pipeline")).all()
        assert len(pipelines) == 1


def test_api_create_pipeline_validation_errors():
    client = _get_client()

    with Session(engine) as db:
        db.execute(config.SQLModel.metadata.tables["pipeline"].delete())
        db.commit()

    # 1. Invalid Timezone
    payload = {
        "original_name": "",
        "name": "bad_timezone_pipeline",
        "source_path": "data/in.parquet",
        "sink_path": "data/out.parquet",
        "timezone": "Mars/Olympus",  # Invalid IANA timezone!
        "on_failure": "none",
        "transforms_json": "[]",
        "checks_json": "[]"
    }
    response = client.post("/api/pipelines", data=payload)
    assert response.status_code == 400
    assert "timezone must be a valid IANA timezone" in response.text

    # 2. Invalid Threads (negative)
    payload["timezone"] = "UTC"
    payload["threads"] = "-3"  # Threads ge=1 is required!
    response = client.post("/api/pipelines", data=payload)
    assert response.status_code == 400
    assert "threads" in response.text.lower()
    
    # 3. Invalid target file size format
    payload["threads"] = "4"
    payload["target_file_size"] = "invalid_size"  # Must match size bounds regex (e.g. 200MB)
    response = client.post("/api/pipelines", data=payload)
    assert response.status_code == 400
    assert "target_file_size" in response.text.lower()

    # Verify nothing was saved
    with Session(engine) as db:
        count = db.exec(select(Pipeline)).all()
        assert len(count) == 0
