"""RBAC tests: permission resolution, the require_permission dependency, live
per-permission route enforcement, custom-role CRUD, and custom-role enforcement.

Uses an isolated temp SQLite DB via DATAFLOW_DB so it never touches the real DB.
"""

import os
import sys
import tempfile
import types

import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi import HTTPException  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

import config  # noqa: E402
from config import (  # noqa: E402
    engine, User, Role, permissions_for, ALL_PERMISSIONS, BASELINE_PERMISSIONS, WILDCARD,
    extract_s3_paths, path_in_scope, role_disallowed_paths,
)
from sqlmodel import Session, select  # noqa: E402
import api  # noqa: E402
from api import app, hash_password, require_permission, check_permission  # noqa: E402


USERS = {
    "vwr": ("viewerpass", "viewer"),
    "edt": ("editorpass", "editor"),
    "adm": ("adminpass", "admin"),
}


@pytest.fixture(scope="module", autouse=True)
def _seed_users():
    # viewer/editor are no longer seeded by default, so the tests create them
    # (as custom roles) to exercise role-based enforcement. admin is seeded.
    view_perms = ["dashboard.view", "pipelines.view", "cronjobs.view"]
    editor_perms = view_perms + [
        "pipelines.create", "pipelines.edit", "pipelines.delete", "pipelines.run",
        "cronjobs.create", "cronjobs.edit", "cronjobs.delete", "cronjobs.run",
        "query.run", "flux.ask",
    ]
    with Session(engine) as db:
        for name, perms in [("viewer", view_perms), ("editor", editor_perms)]:
            if not db.exec(select(Role).where(Role.name == name)).first():
                db.add(Role(name=name, description="test role", permissions=perms, is_system=False))
        db.commit()
        for uname, (pw, role) in USERS.items():
            if not db.exec(select(User).where(User.username == uname)).first():
                db.add(User(username=uname, password_hash=hash_password(pw),
                            role=role, is_admin=(role == "admin")))
        db.commit()
    yield


def _client_for(username: str, password: str = None) -> TestClient:
    c = TestClient(app, follow_redirects=False)
    pw = password or USERS[username][0]
    r = c.post("/login", data={"username": username, "password": pw})
    assert r.status_code == 302, f"login failed: {r.status_code}"
    return c


def _req(user, perms):
    return types.SimpleNamespace(state=types.SimpleNamespace(user=user, perms=set(perms)))


# --------------------------------------------------------------------------- #
# Unit: permission resolution + dependency
# --------------------------------------------------------------------------- #

def test_permissions_for_empty():
    p = permissions_for([])
    assert p == set()                # no implicit baseline
    assert "dashboard.view" not in p


def test_permissions_for_wildcard():
    p = permissions_for([WILDCARD])
    assert set(ALL_PERMISSIONS) <= p


def test_permissions_for_specific():
    p = permissions_for(["query.run"])
    assert p == {"query.run"}
    assert "dashboard.view" not in p


def test_require_permission_unauthenticated_401():
    with pytest.raises(HTTPException) as ei:
        require_permission("query.run")(_req(None, []))
    assert ei.value.status_code == 401


def test_require_permission_missing_403():
    u = User(username="u", password_hash="x", role="viewer")
    with pytest.raises(HTTPException) as ei:
        require_permission("query.run")(_req(u, {"dashboard.view"}))
    assert ei.value.status_code == 403


def test_require_permission_granted():
    u = User(username="u", password_hash="x", role="editor")
    assert require_permission("query.run")(_req(u, {"query.run"})) is u


def test_check_permission_imperative():
    u = User(username="u", password_hash="x", role="editor")
    check_permission(_req(u, {"pipelines.create"}), "pipelines.create")  # no raise
    with pytest.raises(HTTPException):
        check_permission(_req(u, {"pipelines.edit"}), "pipelines.create")


# --------------------------------------------------------------------------- #
# Integration: per-permission route enforcement
# --------------------------------------------------------------------------- #

def test_unauthenticated_redirected():
    c = TestClient(app, follow_redirects=False)
    assert c.post("/api/query", data={"query": "SELECT 1"}).status_code == 302


def test_viewer():
    c = _client_for("vwr")
    assert c.get("/").status_code == 200            # dashboard.view
    assert c.get("/pipelines").status_code == 200   # pipelines.view
    assert c.get("/query").status_code == 302        # no query.run -> redirected
    assert c.post("/api/pipelines/x/run").status_code == 403
    assert c.post("/api/query", data={"query": "SELECT 1"}).status_code == 403
    assert c.get("/settings").status_code == 302
    assert c.post("/api/users", data={"username": "z", "password": "abcdef", "role": "viewer"}).status_code == 403
    assert c.get("/api/roles").status_code == 403


def test_editor():
    c = _client_for("edt")
    assert c.post("/api/query", data={"query": "SELECT 1"}).status_code == 200
    r = c.post("/api/pipelines", data={
        "original_name": "", "name": "rbac_edit_pipe",
        "source_path": "data/in/*.parquet", "sink_path": "data/out.parquet",
        "sink_format": "parquet", "on_failure": "none",
        "transforms_json": "[]", "checks_json": "[]",
    })
    assert r.status_code == 200, r.text[:300]
    assert c.post("/api/users", data={"username": "z2", "password": "abcdef", "role": "viewer"}).status_code == 403
    assert c.get("/api/settings").status_code == 403
    assert c.get("/api/roles").status_code == 403  # editor lacks roles.manage


def test_admin():
    c = _client_for("adm")
    assert c.get("/api/settings").status_code == 200
    assert c.get("/api/roles").status_code == 200
    r = c.post("/api/users", data={"username": "newbie", "password": "abcdef", "role": "editor"})
    assert r.status_code == 200, r.text[:300]


# --------------------------------------------------------------------------- #
# Custom roles: CRUD, protection, and enforcement
# --------------------------------------------------------------------------- #

def test_role_crud_and_custom_role_enforcement():
    admin = _client_for("adm")

    # create a custom role, then grant it exactly query.run
    assert admin.post("/api/roles", json={"name": "analyst", "description": "query only", "permissions": []}).status_code == 200
    assert admin.put("/api/roles/analyst", json={"description": "query only", "permissions": ["query.run"]}).status_code == 200

    roles = {r["name"]: r for r in admin.get("/api/roles").json()["roles"]}
    assert "analyst" in roles and roles["analyst"]["permissions"] == ["query.run"]

    # system-role protection (admin is the only seeded system role)
    assert admin.put("/api/roles/admin", json={"permissions": []}).status_code == 400
    assert admin.delete("/api/roles/admin").status_code == 400

    # assign a user to the custom role
    assert admin.post("/api/users", data={"username": "ana", "password": "abcdef", "role": "analyst"}).status_code == 200
    # cannot delete a role that is in use
    assert admin.delete("/api/roles/analyst").status_code == 400

    # the custom role grants exactly what was ticked (query.run only)
    ana = _client_for("ana", "abcdef")
    assert ana.get("/query").status_code == 200                                   # query.run -> page visible
    assert ana.get("/pipelines").status_code == 302                               # no pipelines.view -> redirected
    assert ana.get("/").status_code == 302                                        # no dashboard.view -> redirected
    assert ana.post("/api/pipelines", data={                                      # pipelines.create NOT granted
        "original_name": "", "name": "ana_pipe", "source_path": "a", "sink_path": "b",
        "sink_format": "parquet", "on_failure": "none", "transforms_json": "[]", "checks_json": "[]",
    }).status_code == 403


def test_delete_unused_custom_role():
    admin = _client_for("adm")
    assert admin.post("/api/roles", json={"name": "temprole", "description": "", "permissions": []}).status_code == 200
    assert admin.delete("/api/roles/temprole").status_code == 200


def test_only_admin_seeded_by_default():
    # Only the admin system role ships by default; viewer/editor here are the
    # non-system roles the fixture created for testing.
    with Session(engine) as db:
        system = sorted(r.name for r in db.exec(select(Role).where(Role.is_system == True)).all())
    assert system == ["admin"]


# --------------------------------------------------------------------------- #
# Data-access (bucket) scoping
# --------------------------------------------------------------------------- #

def test_extract_s3_paths():
    q = "SELECT * FROM read_parquet('s3://bkt/team-a/x.parquet') u JOIN read_parquet('s3://bkt/team-b/y.parquet') v"
    paths = extract_s3_paths(q)
    assert any("team-a" in p for p in paths) and any("team-b" in p for p in paths)


def test_path_in_scope():
    assert path_in_scope([], [], "s3://any/x") is True                               # empty allow = unrestricted
    assert path_in_scope(["s3://bkt/team-a/"], [], "s3://bkt/team-a/x") is True
    assert path_in_scope(["s3://bkt/team-a/"], [], "s3://bkt/team-b/x") is False
    assert path_in_scope([], ["s3://secret/"], "s3://secret/x") is False             # deny wins


def test_role_disallowed_paths():
    assert role_disallowed_paths(["*"], [], [], ["s3://any/x"]) == []                 # admin bypass
    assert role_disallowed_paths([], ["s3://bkt/a/"], [], ["/local/f", "s3://bkt/b/x"]) == ["s3://bkt/b/x"]


def test_query_bucket_scope():
    admin = _client_for("adm")
    admin.post("/api/roles", json={
        "name": "teama",
        "permissions": ["query.run", "pipelines.view", "pipelines.create"],
        "bucket_allow": ["s3://bkt/team-a/"],
    })
    admin.post("/api/users", data={"username": "a1", "password": "abcdef", "role": "teama"})
    a1 = _client_for("a1", "abcdef")
    # out-of-scope bucket -> blocked by the scope check BEFORE any execution
    assert a1.post("/api/query", data={"query": "SELECT * FROM read_parquet('s3://bkt/team-b/x.parquet')"}).status_code == 403
    # query.run itself is granted (page visible); allow-pass path is unit-tested
    assert a1.get("/query").status_code == 200


def test_pipeline_run_as_scope_and_escalation():
    admin = _client_for("adm")
    admin.post("/api/roles", json={
        "name": "teamb",
        "permissions": ["pipelines.view", "pipelines.create", "query.run"],
        "bucket_allow": ["s3://bkt/team-b/"],
    })
    base = {"original_name": "", "sink_format": "parquet", "on_failure": "none",
            "transforms_json": "[]", "checks_json": "[]"}
    # out-of-scope path for run_as -> 403
    r = admin.post("/api/pipelines", data={**base, "name": "p_scope",
        "source_path": "s3://bkt/other/in.parquet", "sink_path": "s3://bkt/team-b/out.parquet", "run_as": "teamb"})
    assert r.status_code == 403
    # in-scope paths -> ok
    r = admin.post("/api/pipelines", data={**base, "name": "p_ok",
        "source_path": "s3://bkt/team-b/in.parquet", "sink_path": "s3://bkt/team-b/out.parquet", "run_as": "teamb"})
    assert r.status_code == 200, r.text[:300]
    # non-admin cannot run a pipeline as a role other than their own (escalation)
    admin.post("/api/users", data={"username": "b1", "password": "abcdef", "role": "teamb"})
    b1 = _client_for("b1", "abcdef")
    r = b1.post("/api/pipelines", data={**base, "name": "p_esc",
        "source_path": "s3://bkt/team-b/in.parquet", "sink_path": "s3://bkt/team-b/out.parquet", "run_as": "admin"})
    assert r.status_code == 403


def test_executor_validate_data_access():
    from config import PipelineConfig
    from executor import DuckDBExecutor, PipelineError
    with Session(engine) as db:
        if not db.exec(select(Role).where(Role.name == "teamc")).first():
            db.add(Role(name="teamc", description="", permissions=["pipelines.view"],
                        bucket_allow=["s3://bkt/team-c/"], is_system=False))
            db.commit()
    bad = PipelineConfig(name="x", source_path="s3://bkt/other/in.parquet",
                         sink_path="s3://bkt/team-c/out.parquet", run_as="teamc",
                         alerts={"on_failure": "none"})
    with pytest.raises(PipelineError):
        DuckDBExecutor(bad).validate_data_access()
    ok = PipelineConfig(name="x", source_path="s3://bkt/team-c/in.parquet",
                        sink_path="s3://bkt/team-c/out.parquet", run_as="teamc",
                        alerts={"on_failure": "none"})
    DuckDBExecutor(ok).validate_data_access()  # no raise


def test_change_user_role():
    admin = _client_for("adm")
    # create a throwaway user as viewer, then promote to editor
    assert admin.post("/api/users", data={"username": "moe", "password": "abcdef", "role": "viewer"}).status_code == 200
    assert admin.put("/api/users/moe/role", data={"role": "editor"}).status_code == 200
    with Session(engine) as db:
        u = db.exec(select(User).where(User.username == "moe")).first()
        assert u.role == "editor" and u.is_admin is False
    # promoting to admin flips is_admin
    assert admin.put("/api/users/moe/role", data={"role": "admin"}).status_code == 200
    with Session(engine) as db:
        u = db.exec(select(User).where(User.username == "moe")).first()
        assert u.role == "admin" and u.is_admin is True
    # unknown role rejected
    assert admin.put("/api/users/moe/role", data={"role": "nope"}).status_code == 400


def test_cannot_strip_last_admin():
    from config import permissions_for
    admin = _client_for("adm")
    # Demote every admin-capable user EXCEPT 'adm' so 'adm' becomes the last one.
    with Session(engine) as db:
        targets = []
        for u in db.exec(select(User)).all():
            if u.username == "adm":
                continue
            r = db.exec(select(Role).where(Role.name == u.role)).first()
            if "users.manage" in permissions_for(r.permissions if r else []):
                targets.append(u.username)
    for uname in targets:
        assert admin.put(f"/api/users/{uname}/role", data={"role": "viewer"}).status_code == 200
    # 'adm' is now the last admin-capable user — demoting it must be blocked.
    assert admin.put("/api/users/adm/role", data={"role": "viewer"}).status_code == 400
    with Session(engine) as db:
        assert db.exec(select(User).where(User.username == "adm")).first().role == "admin"
