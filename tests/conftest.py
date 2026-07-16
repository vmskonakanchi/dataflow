"""Shared pytest configuration and fixtures for database cleanup and test isolation."""

import os
import sys
import tempfile

# 1. Create a single session-wide temp DB file and set the env var BEFORE importing config/settings
_SESSION_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_SESSION_DB.close()
os.environ["DATAFLOW_DB"] = _SESSION_DB.name

# Ensure src/ is on PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from sqlmodel import Session
import config
from config import engine, init_db, seed_roles
from settings import settings


@pytest.fixture(scope="session", autouse=True)
def session_db_lifecycle():
    """Ensure the session-wide database tables are created on startup and the file is deleted on shutdown."""
    init_db()
    yield
    if os.path.exists(_SESSION_DB.name):
        try:
            os.remove(_SESSION_DB.name)
        except Exception:
            pass


@pytest.fixture(scope="module", autouse=True)
def db_isolation():
    """Clear all database tables and seed default settings/roles at the start of each test module."""
    # Ensure tables are created (idempotent)
    init_db()
    
    # Delete all rows from tables to start fresh
    with Session(engine) as session:
        for table_name in [
            "auditlog", "job", "pipelinerun", "appsetting",
            "user", "role", "pipeline", "cronjob"
        ]:
            try:
                session.execute(config.SQLModel.metadata.tables[table_name].delete())
            except KeyError:
                pass
        session.commit()
    
    # Re-seed the fresh DB with settings and system roles
    settings.seed()
    seed_roles()
    
    yield
