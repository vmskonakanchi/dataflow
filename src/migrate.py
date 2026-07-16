"""Programmatic Alembic migration runner.

Called at application startup so the config database is always up to date
without the user ever running `alembic` by hand — keeping Dataflow zero-setup.

Handles three database states:
  1. Fresh DB (no tables)          -> upgrade head creates everything.
  2. Legacy DB (tables, no alembic
     version, created by create_all
     before Alembic existed)       -> stamp baseline, then upgrade head.
  3. Already-migrated DB           -> upgrade head applies any pending migrations.
"""

import os
import logging

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

logger = logging.getLogger("dataflow.migrate")

# The first (baseline) revision. Used to adopt pre-Alembic databases.
BASELINE_REVISION = "c4240d8b9eb6"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _alembic_config() -> Config:
    ini_path = os.path.join(PROJECT_ROOT, "alembic.ini")
    cfg = Config(ini_path)
    # Force absolute script location so it works regardless of CWD.
    cfg.set_main_option("script_location", os.path.join(PROJECT_ROOT, "src", "migrations"))
    return cfg


def run_migrations() -> None:
    """Bring the config database up to the latest schema."""
    from config import engine

    cfg = _alembic_config()

    try:
        insp = inspect(engine)
        tables = set(insp.get_table_names())

        # Legacy DB: app tables exist but Alembic was never initialized here.
        # Adopt the baseline so we don't try to re-create existing tables.
        if "alembic_version" not in tables and "pipeline" in tables:
            logger.info("Adopting pre-Alembic database at baseline revision")
            command.stamp(cfg, BASELINE_REVISION)

        # Apply any pending migrations (creates tables on a fresh DB).
        command.upgrade(cfg, "head")
        logger.info("Database migrations up to date")
    except Exception as e:
        # Don't hard-fail startup on migration errors; log loudly instead.
        logger.error("Migration run failed: %s", e)
        raise
