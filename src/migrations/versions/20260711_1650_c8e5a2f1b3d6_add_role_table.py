"""add role table for custom roles / permission matrix

Revision ID: c8e5a2f1b3d6
Revises: b7f3c9a1d2e4
Create Date: 2026-07-11 16:50:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'c8e5a2f1b3d6'
down_revision: Union[str, None] = 'b7f3c9a1d2e4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'role',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('description', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('permissions', sa.JSON(), nullable=True),
        sa.Column('is_system', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('role', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_role_name'), ['name'], unique=True)
    # System roles (viewer/editor/admin) are populated at startup by
    # config.seed_roles(), so both fresh and existing databases converge.


def downgrade() -> None:
    with op.batch_alter_table('role', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_role_name'))
    op.drop_table('role')
