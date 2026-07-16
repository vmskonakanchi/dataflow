"""add sink file sizing to pipeline

Revision ID: a3d8f01c25b4
Revises: f5ae1166f775
Create Date: 2026-07-11 18:26:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'a3d8f01c25b4'
down_revision: Union[str, None] = 'f5ae1166f775'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('pipeline', schema=None) as batch_op:
        batch_op.add_column(sa.Column('target_file_size', sqlmodel.sql.sqltypes.AutoString(), nullable=True))
        batch_op.add_column(sa.Column('row_group_size', sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('pipeline', schema=None) as batch_op:
        batch_op.drop_column('row_group_size')
        batch_op.drop_column('target_file_size')
