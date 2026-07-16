"""add user role for RBAC

Revision ID: b7f3c9a1d2e4
Revises: f5ae1166f775
Create Date: 2026-07-11 16:30:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'b7f3c9a1d2e4'
down_revision: Union[str, None] = 'f5ae1166f775'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add the RBAC role column. server_default lets the non-null column be added
    # to a table that already has rows (required on SQLite).
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                'role',
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=False,
                server_default='viewer',
            )
        )
    # Backfill from the legacy is_admin flag so existing users keep working:
    #   is_admin=True  -> 'admin'
    #   is_admin=False -> 'editor'  (before RBAC, any authenticated user could
    #                                create/run/delete pipelines, so 'editor'
    #                                preserves that ability; only user/settings
    #                                management was admin-gated).
    op.execute(
        "UPDATE \"user\" SET role = CASE WHEN is_admin THEN 'admin' ELSE 'editor' END"
    )


def downgrade() -> None:
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.drop_column('role')
