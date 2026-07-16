"""add bucket scope to role and run_as to pipeline

Revision ID: d9f4b2e7c1a8
Revises: c8e5a2f1b3d6
Create Date: 2026-07-11 17:40:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd9f4b2e7c1a8'
down_revision: Union[str, None] = 'c8e5a2f1b3d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('role', schema=None) as batch_op:
        batch_op.add_column(sa.Column('bucket_allow', sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column('bucket_deny', sa.JSON(), nullable=True))
    with op.batch_alter_table('pipeline', schema=None) as batch_op:
        batch_op.add_column(sa.Column('run_as', sa.String(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('pipeline', schema=None) as batch_op:
        batch_op.drop_column('run_as')
    with op.batch_alter_table('role', schema=None) as batch_op:
        batch_op.drop_column('bucket_deny')
        batch_op.drop_column('bucket_allow')
