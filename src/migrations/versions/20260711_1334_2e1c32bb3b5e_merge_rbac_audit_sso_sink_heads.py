"""merge rbac_audit sso sink heads

Revision ID: 2e1c32bb3b5e
Revises: e1a7c3f9b6d2, a3d8f01c25b4, f2b6c4e8a913
Create Date: 2026-07-11 13:34:07.418779+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '2e1c32bb3b5e'
down_revision: Union[str, None] = ('e1a7c3f9b6d2', 'a3d8f01c25b4', 'f2b6c4e8a913')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
