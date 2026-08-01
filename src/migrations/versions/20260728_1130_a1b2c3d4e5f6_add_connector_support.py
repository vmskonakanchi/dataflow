"""add connector support

Adds source_type and source_config columns to pipeline table, and creates
the saved_connector table for reusable connector configurations.

Revision ID: a1b2c3d4e5f6
Revises: b7c9d1e3f5a7
Create Date: 2026-07-28 11:30:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "b7c9d1e3f5a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add source_type and source_config to pipeline table
    op.add_column("pipeline", sa.Column("source_type", sa.String(), server_default="file", nullable=False))
    op.add_column("pipeline", sa.Column("source_config", sa.JSON(), server_default="{}", nullable=False))

    # Make source_path nullable (now optional when source_config is used)
    # SQLite doesn't support ALTER COLUMN, so we use batch mode
    with op.batch_alter_table("pipeline") as batch_op:
        batch_op.alter_column("source_path", existing_type=sa.String(), nullable=True)

    # Backfill existing rows: set source_config from source_path
    op.execute("""
        UPDATE pipeline
        SET source_config = json_object('path', source_path),
            source_type = 'file'
        WHERE source_path IS NOT NULL AND source_config = '{}'
    """)

    # Create saved_connector table
    op.create_table(
        "saved_connector",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("connector_type", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("config", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_saved_connector_name", "saved_connector", ["name"], unique=True)
    op.create_index("ix_saved_connector_connector_type", "saved_connector", ["connector_type"])


def downgrade() -> None:
    op.drop_index("ix_saved_connector_connector_type", table_name="saved_connector")
    op.drop_index("ix_saved_connector_name", table_name="saved_connector")
    op.drop_table("saved_connector")

    with op.batch_alter_table("pipeline") as batch_op:
        batch_op.alter_column("source_path", existing_type=sa.String(), nullable=False)

    op.drop_column("pipeline", "source_config")
    op.drop_column("pipeline", "source_type")
