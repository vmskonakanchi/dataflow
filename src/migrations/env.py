"""Alembic migration environment for Dataflow's config database.

Wires Alembic to the SQLModel metadata and the engine defined in src/config.py,
so `alembic revision --autogenerate` sees Pipeline, CronJob, User, AppSetting, etc.
"""

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# --- Make the app's `src/` importable regardless of where alembic runs from ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# Import the app's engine + models. Importing config registers all SQLModel
# tables (Pipeline, CronJob, User, AppSetting) onto SQLModel.metadata.
import config  # noqa: E402
from sqlmodel import SQLModel  # noqa: E402

# SQLModel needs its table classes imported so metadata is populated.
from config import Pipeline, CronJob, User, AppSetting  # noqa: E402,F401

# Alembic Config object (reads alembic.ini)
alembic_config = context.config

# Set the DB URL from the app so there's a single source of truth.
# An env var override lets migrations target a different DB (e.g. a temp DB
# when generating the baseline, or a prod DB during deploy).
db_url = os.environ.get("DATAFLOW_DB_URL", config.sqlite_url)
alembic_config.set_main_option("sqlalchemy.url", db_url)

# Configure Python logging from alembic.ini if present.
if alembic_config.config_file_name is not None:
    fileConfig(alembic_config.config_file_name)

# Target metadata for autogenerate.
target_metadata = SQLModel.metadata


def run_migrations_offline() -> None:
    """Run migrations without a live DB connection (emits SQL)."""
    url = alembic_config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,  # required for SQLite ALTER support
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live DB connection."""
    connectable = engine_from_config(
        alembic_config.get_section(alembic_config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,  # SQLite: use batch mode for ALTER TABLE
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
