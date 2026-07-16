"""OIDC / Microsoft Entra single sign-on.

Additive to local username/password auth: local accounts always keep working.
On a successful SSO login we map the IdP's group/role claim to a Dataflow RBAC
role and provision (or update) a local user record. SSO never replaces local
login — it sits alongside it.

The claim->user/role logic is factored into pure functions so it can be unit
tested without a live identity provider; only the HTTP redirect/token-exchange
pieces need Authlib.
"""

import secrets
from typing import Callable, List, Optional, Tuple

import bcrypt
from authlib.integrations.starlette_client import OAuth

from settings import settings

# Provider key stored in User.auth_provider for SSO-provisioned accounts.
SSO_PROVIDER = "entra"


class SSOError(Exception):
    """Raised when an SSO login can't be completed (config or claim issue)."""


def build_oauth() -> OAuth:
    """Construct an Authlib OAuth registry from current settings.

    Built fresh so runtime config changes take effect without a restart.
    Relies on OIDC discovery (``server_metadata_url``) for endpoints + JWKS,
    so Authlib validates the ID token signature for us.
    """
    if not settings.sso_configured:
        raise SSOError("SSO is not configured (enable it and set discovery URL, client id and secret)")
    oauth = OAuth()
    oauth.register(
        name=SSO_PROVIDER,
        client_id=settings.sso_client_id,
        client_secret=settings.sso_client_secret,
        server_metadata_url=settings.sso_discovery_url,
        client_kwargs={"scope": settings.sso_scopes},
    )
    return oauth


# --- Pure claim handling (unit-testable, no DB/network) ---

def extract_username(claims: dict) -> Optional[str]:
    """Pick a stable username from OIDC claims (case-normalized)."""
    for key in ("preferred_username", "upn", "email", "unique_name", "sub"):
        val = claims.get(key)
        if val:
            return str(val).strip().lower()
    return None


def extract_groups(claims: dict, group_claim: str) -> List[str]:
    """Read the configured group/role claim into a list of string IDs."""
    raw = claims.get(group_claim, [])
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, (list, tuple)):
        return []
    return [str(g) for g in raw]


def resolve_role(
    groups: List[str],
    role_map: dict,
    default_role: Optional[str],
    role_rank: Callable[[str], int],
) -> Optional[str]:
    """Pick the Dataflow role for a set of IdP groups.

    - Among groups present in ``role_map``, choose the HIGHEST-privilege role
      (by ``role_rank``) so a user in several groups gets the strongest grant.
    - Fall back to ``default_role`` when no group matches.
    - Return None to signal "deny" (no match and no default configured).
    """
    matched = [role_map[g] for g in groups if g in role_map]
    if matched:
        return max(matched, key=role_rank)
    return default_role or None


def resolve_sso_user(
    claims: dict,
    role_map: dict,
    default_role: Optional[str],
    role_rank: Callable[[str], int],
    group_claim: str = "groups",
) -> Tuple[str, str]:
    """Pure resolution: claims -> (username, role). Raises SSOError on failure."""
    username = extract_username(claims)
    if not username:
        raise SSOError("No username/email claim present in the SSO token")
    groups = extract_groups(claims, group_claim)
    role = resolve_role(groups, role_map, default_role, role_rank)
    if not role:
        raise SSOError(
            f"'{username}' is not a member of any mapped group and no default role is set"
        )
    return username, role


# --- DB-backed helpers ---

def db_role_rank(role_name: str) -> int:
    """Rank a role by its effective (wildcard-expanded) permission count.

    The built-in admin role stores just the wildcard, so a naive len() would
    under-rank it; expanding via permissions_for fixes that.
    """
    from config import engine, Role, permissions_for
    from sqlmodel import Session, select

    with Session(engine) as db:
        row = db.exec(select(Role).where(Role.name == role_name)).first()
        if not row:
            return -1
        return len(permissions_for(row.permissions))


def _unusable_password_hash() -> str:
    """A valid bcrypt hash of a random secret — password login can never match,
    and verify_password stays a clean False (never raises)."""
    return bcrypt.hashpw(secrets.token_hex(32).encode(), bcrypt.gensalt()).decode()


def provision_user(username: str, role: str) -> None:
    """Create or update the local user record for an SSO login.

    - New user: created with auth_provider='entra' and an unusable password
      (blocks password login), only if auto-provisioning is enabled.
    - Existing user (local or SSO): role is refreshed from the IdP mapping so
      Entra groups remain the source of truth. A pre-existing LOCAL account
      keeps its provider (password login still works for it).
    """
    from config import engine, User, Role, ROLE_ADMIN
    from sqlmodel import Session, select

    with Session(engine) as db:
        if not db.exec(select(Role).where(Role.name == role)).first():
            raise SSOError(f"Mapped role '{role}' does not exist")

        user = db.exec(select(User).where(User.username == username)).first()
        if user is None:
            if not settings.sso_auto_create_users:
                raise SSOError(
                    f"User '{username}' does not exist and auto-provisioning is disabled"
                )
            user = User(
                username=username,
                password_hash=_unusable_password_hash(),
                role=role,
                is_admin=(role == ROLE_ADMIN),
                auth_provider=SSO_PROVIDER,
            )
        else:
            user.role = role
            user.is_admin = (role == ROLE_ADMIN)
        db.add(user)
        db.commit()
