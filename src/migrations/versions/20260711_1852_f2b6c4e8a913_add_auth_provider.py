"""add auth_provider to user (SSO)

Revision ID: f2b6c4e8a913
Revises: d9f4b2e7c1a8
Create Date: 2026-07-11 18:52:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'f2b6c4e8a913'
down_revision: Union[str, None] = 'd9f4b2e7c1a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('auth_provider', sqlmodel.sql.sqltypes.AutoString(),
                      nullable=False, server_default='local')
        )


def downgrade() -> None:
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.drop_column('auth_provider')
