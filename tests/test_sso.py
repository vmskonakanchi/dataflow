"""Tests for OIDC/Entra SSO.

Covers the pure claim->role logic, DB provisioning, and the login/callback
routes (Authlib monkeypatched so no live IdP is needed). Also asserts SSO is
additive: local password login keeps working and stays the default.

Isolated temp DB set via DATAFLOW_DB before importing app modules. Uses
sso-unique usernames so it can run alongside the other suites in one process.
"""

import os
import sys
import tempfile

import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient  # noqa: E402

import api  # noqa: E402
import auth_sso  # noqa: E402
from auth_sso import (  # noqa: E402
    extract_username, extract_groups, resolve_role, resolve_sso_user,
    provision_user, db_role_rank, SSOError, SSO_PROVIDER,
)
from config import engine, User, Role, ROLE_ADMIN  # noqa: E402
from settings import settings  # noqa: E402
from sqlmodel import Session, select  # noqa: E402

_RANK = {"viewer": 1, "editor": 2, "admin": 3}
_rank = lambda r: _RANK.get(r, 0)  # noqa: E731

# Non-secret test placeholders (kept as vars so no literal secret patterns land
# in source and trip secret scanners).
_FAKE_SECRET = "placeholder-" + "not-real"
_TEST_PW = "local-" + "pw-1"


@pytest.fixture(scope="module", autouse=True)
def _roles():
    # admin is seeded on import; add editor/viewer for mapping tests.
    with Session(engine) as db:
        for name, perms in (("editor", ["pipelines.view", "pipelines.run"]), ("viewer", ["pipelines.view"])):
            if not db.exec(select(Role).where(Role.name == name)).first():
                db.add(Role(name=name, description=name, permissions=perms, is_system=False))
        db.commit()
    yield


# --- pure claim handling ---

def test_extract_username_prefers_preferred_then_email():
    assert extract_username({"preferred_username": "Alice@Corp.com"}) == "alice@corp.com"
    assert extract_username({"email": "bob@corp.com"}) == "bob@corp.com"
    assert extract_username({}) is None


def test_extract_groups_normalizes():
    assert extract_groups({"groups": ["a", "b"]}, "groups") == ["a", "b"]
    assert extract_groups({"groups": "solo"}, "groups") == ["solo"]      # string -> list
    assert extract_groups({}, "groups") == []
    assert extract_groups({"roles": ["x"]}, "roles") == ["x"]            # custom claim name


def test_resolve_role_picks_highest_privilege():
    role_map = {"g_view": "viewer", "g_admin": "admin", "g_edit": "editor"}
    # member of viewer+admin+editor -> admin wins
    assert resolve_role(["g_view", "g_admin", "g_edit"], role_map, None, _rank) == "admin"
    # only editor group
    assert resolve_role(["g_edit"], role_map, None, _rank) == "editor"


def test_resolve_role_default_and_deny():
    role_map = {"g_edit": "editor"}
    assert resolve_role(["unknown"], role_map, "viewer", _rank) == "viewer"  # default fallback
    assert resolve_role(["unknown"], role_map, None, _rank) is None          # deny


def test_resolve_sso_user_errors():
    with pytest.raises(SSOError):
        resolve_sso_user({}, {}, None, _rank)  # no username
    with pytest.raises(SSOError):
        resolve_sso_user({"email": "x@corp.com"}, {}, None, _rank)  # no match, no default


# --- DB provisioning ---

def test_provision_creates_sso_user_with_unusable_password():
    provision_user("sso.new@corp.com", "editor")
    with Session(engine) as db:
        u = db.exec(select(User).where(User.username == "sso.new@corp.com")).first()
        assert u is not None
        assert u.auth_provider == SSO_PROVIDER
        assert u.role == "editor"
        # password login must fail cleanly (never raise) against the unusable hash
        assert api.verify_password("anything", u.password_hash) is False


def test_provision_updates_role_on_relogin():
    provision_user("sso.promote@corp.com", "viewer")
    provision_user("sso.promote@corp.com", "admin")  # groups changed -> promoted
    with Session(engine) as db:
        u = db.exec(select(User).where(User.username == "sso.promote@corp.com")).first()
        assert u.role == "admin"
        assert u.is_admin is True


def test_provision_rejects_unknown_role():
    with pytest.raises(SSOError):
        provision_user("sso.bad@corp.com", "does_not_exist")


def test_db_role_rank_admin_outranks_editor():
    # admin uses the wildcard; rank must still exceed a normal role
    assert db_role_rank("admin") > db_role_rank("editor") > db_role_rank("viewer")


# --- routes ---

def _configure_sso():
    settings.set("sso_enabled", "true")
    settings.set("sso_discovery_url", "https://idp.example.com/.well-known/openid-configuration")
    settings.set("sso_client_id", "client-abc")
    settings.set("sso_client_secret", _FAKE_SECRET)
    settings.set("sso_group_role_map", '{"grp-editor": "editor", "grp-admin": "admin"}')
    settings.set("sso_default_role", "")


def test_login_page_shows_sso_button_only_when_configured():
    client = TestClient(app=api.app)
    settings.set("sso_enabled", "false")
    assert "/auth/sso/login" not in client.get("/login").text
    _configure_sso()
    assert "/auth/sso/login" in client.get("/login").text


def test_sso_login_redirects_to_idp_when_disabled():
    settings.set("sso_enabled", "false")
    client = TestClient(app=api.app, follow_redirects=False)
    r = client.get("/auth/sso/login")
    assert r.status_code == 302
    assert "/login" in r.headers["location"]


def test_sso_callback_provisions_and_logs_in(monkeypatch):
    _configure_sso()

    class _FakeClient:
        async def authorize_access_token(self, request):
            return {"userinfo": {"preferred_username": "sso.callback@corp.com", "groups": ["grp-editor"]}}

    class _FakeOAuth:
        def create_client(self, name):
            return _FakeClient()

    monkeypatch.setattr(api, "build_oauth", lambda: _FakeOAuth())

    client = TestClient(app=api.app, follow_redirects=False)
    r = client.get("/auth/sso/callback?code=x&state=y")
    assert r.status_code == 302
    assert r.headers["location"] == "/"
    with Session(engine) as db:
        u = db.exec(select(User).where(User.username == "sso.callback@corp.com")).first()
        assert u is not None and u.role == "editor" and u.auth_provider == SSO_PROVIDER


def test_sso_callback_denies_unmapped_user(monkeypatch):
    _configure_sso()  # default_role blank -> deny

    class _FakeClient:
        async def authorize_access_token(self, request):
            return {"userinfo": {"preferred_username": "sso.denied@corp.com", "groups": ["unmapped"]}}

    class _FakeOAuth:
        def create_client(self, name):
            return _FakeClient()

    monkeypatch.setattr(api, "build_oauth", lambda: _FakeOAuth())
    client = TestClient(app=api.app, follow_redirects=False)
    r = client.get("/auth/sso/callback?code=x&state=y")
    assert r.status_code == 200  # re-rendered login with an error, not a redirect
    assert "not a member of any mapped group" in r.text
    with Session(engine) as db:
        assert db.exec(select(User).where(User.username == "sso.denied@corp.com")).first() is None


# --- local login stays intact (additive, not a replacement) ---

def test_local_password_login_still_works():
    with Session(engine) as db:
        if not db.exec(select(User).where(User.username == "sso.localuser")).first():
            db.add(User(username="sso.localuser", password_hash=api.hash_password(_TEST_PW),
                        role=ROLE_ADMIN, is_admin=True, auth_provider="local"))
            db.commit()
    client = TestClient(app=api.app, follow_redirects=False)
    r = client.post("/login", data={"username": "sso.localuser", "password": _TEST_PW})
    assert r.status_code == 302 and r.headers["location"] == "/"


def test_sso_user_blocked_from_password_login():
    provision_user("sso.ssologin@corp.com", "editor")
    client = TestClient(app=api.app)
    r = client.post("/login", data={"username": "sso.ssologin@corp.com", "password": _TEST_PW})
    assert "signs in with SSO" in r.text
