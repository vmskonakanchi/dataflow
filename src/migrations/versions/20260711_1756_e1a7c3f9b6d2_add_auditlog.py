"""add auditlog table

Revision ID: e1a7c3f9b6d2
Revises: d9f4b2e7c1a8
Create Date: 2026-07-11 17:56:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'e1a7c3f9b6d2'
down_revision: Union[str, None] = 'd9f4b2e7c1a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'auditlog',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('username', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('role', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('action', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('target_type', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('target_name', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('detail', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('ip', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('success', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('auditlog', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_auditlog_timestamp'), ['timestamp'], unique=False)
        batch_op.create_index(batch_op.f('ix_auditlog_username'), ['username'], unique=False)
        batch_op.create_index(batch_op.f('ix_auditlog_action'), ['action'], unique=False)
        batch_op.create_index(batch_op.f('ix_auditlog_target_type'), ['target_type'], unique=False)


def downgrade() -> None:
    with op.batch_alter_table('auditlog', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_auditlog_target_type'))
        batch_op.drop_index(batch_op.f('ix_auditlog_action'))
        batch_op.drop_index(batch_op.f('ix_auditlog_username'))
        batch_op.drop_index(batch_op.f('ix_auditlog_timestamp'))
    op.drop_table('auditlog')
