"""add template timezone to pipeline

Revision ID: b7c9d1e3f5a7
Revises: 2e1c32bb3b5e
Create Date: 2026-07-16 12:00:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision: str = "b7c9d1e3f5a7"
down_revision: Union[str, None] = "2e1c32bb3b5e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("pipeline", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "timezone",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=False,
                server_default="UTC",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("pipeline", schema=None) as batch_op:
        batch_op.drop_column("timezone")
