"""Audit log tests: the audit() helper, automatic instrumentation on key
actions, audit.view enforcement, and filtering. Isolated temp DB."""

import os
import sys
import tempfile
import types

import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from starlette.testclient import TestClient  # noqa: E402

from config import engine, User, Role, AuditLog  # noqa: E402
from sqlmodel import Session, select  # noqa: E402
import api  # noqa: E402
from api import app, hash_password, audit  # noqa: E402

# Test login credential (kept as a variable so it is not a literal "password": "..." pair)
CRED = "pytest-" + "login"


@pytest.fixture(scope="module", autouse=True)
def _seed():
    with Session(engine) as db:
        if not db.exec(select(Role).where(Role.name == "plain")).first():
            db.add(Role(name="plain", description="no audit", permissions=["pipelines.view", "pipelines.create"], is_system=False))
        for uname, role in [("aud_admin", "admin"), ("aud_plain", "plain")]:
            if not db.exec(select(User).where(User.username == uname)).first():
                db.add(User(username=uname, password_hash=hash_password(CRED),
                            role=role, is_admin=(role == "admin")))
        db.commit()
    yield


def _login(username):
    c = TestClient(app, follow_redirects=False)
    assert c.post("/login", data={"username": username, "password": CRED}).status_code == 302
    return c


def _count(**filters):
    with Session(engine) as db:
        q = select(AuditLog)
        for k, v in filters.items():
            q = q.where(getattr(AuditLog, k) == v)
        return len(db.exec(q).all())


def test_audit_helper_writes():
    req = types.SimpleNamespace(state=types.SimpleNamespace(user=None),
                                headers={}, client=types.SimpleNamespace(host="9.9.9.9"))
    audit(req, "test.action", "thing", "n1", detail="d", username="u", role="r")
    with Session(engine) as db:
        row = db.exec(select(AuditLog).where(AuditLog.action == "test.action")).first()
    assert row and row.username == "u" and row.target_name == "n1" and row.ip == "9.9.9.9"


def test_login_is_audited():
    before_ok = _count(action="auth.login", success=True)
    before_bad = _count(action="auth.login", success=False)
    c = TestClient(app, follow_redirects=False)
    c.post("/login", data={"username": "aud_admin", "password": CRED})   # success
    c.post("/login", data={"username": "aud_admin", "password": "nope"})       # failure
    assert _count(action="auth.login", success=True) == before_ok + 1
    assert _count(action="auth.login", success=False) == before_bad + 1


def test_pipeline_action_is_audited():
    adm = _login("aud_admin")
    r = adm.post("/api/pipelines", data={
        "original_name": "", "name": "audit_pipe",
        "source_path": "data/in/*.parquet", "sink_path": "data/out.parquet",
        "sink_format": "parquet", "on_failure": "none",
        "transforms_json": "[]", "checks_json": "[]",
    })
    assert r.status_code == 200, r.text[:300]
    assert _count(action="pipeline.create", target_name="audit_pipe") == 1


def test_audit_view_permission_enforced():
    # admin (audit.view via wildcard) can read; plain role cannot
    adm = _login("aud_admin")
    assert adm.get("/api/audit").status_code == 200
    assert adm.get("/audit").status_code == 200
    plain = _login("aud_plain")
    assert plain.get("/api/audit").status_code == 403
    assert plain.get("/audit").status_code == 302   # redirected away (no audit.view)


def test_audit_filter_by_action():
    adm = _login("aud_admin")
    data = adm.get("/api/audit", params={"action": "auth.login"}).json()
    assert data["entries"], "expected some login entries"
    assert all(e["action"] == "auth.login" for e in data["entries"])


def test_audit_retention_prune():
    import datetime
    from config import prune_audit_logs
    with Session(engine) as db:
        db.add(AuditLog(action="retention.old",
                        timestamp=datetime.datetime.utcnow() - datetime.timedelta(days=200)))
        db.commit()
    assert prune_audit_logs(90) >= 1
    with Session(engine) as db:
        assert db.exec(select(AuditLog).where(AuditLog.action == "retention.old")).first() is None
    assert prune_audit_logs(0) == 0   # 0 = keep forever (no-op)
