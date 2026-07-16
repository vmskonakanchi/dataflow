import os
import json
import logging
import sys
from typing import Optional
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, Request, Form, Response, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import bcrypt

from sqlmodel import Session, select
from pydantic import BaseModel

from config import (
    engine, init_db, Pipeline, CronJob, User, Role, AuditLog, load_configs, ResolvedConfig,
    seed_roles, permissions_for, ALL_PERMISSIONS, PERMISSION_GROUPS, PERMISSION_LABELS,
    BASELINE_PERMISSIONS, LOCKED_PERMISSIONS, WILDCARD, SYSTEM_ROLES, PAGE_PERMISSIONS,
    extract_s3_paths, role_disallowed_paths,
    ROLE_VIEWER, ROLE_EDITOR, ROLE_ADMIN,
)
from logger import get_run_history
from executor import run_pipeline
from settings import settings
from auth_sso import (
    build_oauth, resolve_sso_user, provision_user, db_role_rank,
    SSO_PROVIDER, SSOError,
)

# Bring the config database schema up to date via Alembic migrations.
# Falls back to create_all if migrations can't run, so startup is resilient.
try:
    from migrate import run_migrations
    run_migrations()
except Exception as _mig_err:
    print(f"Warning: migrations did not run ({_mig_err}); falling back to create_all")
    init_db()

# Seed default settings into the database
from settings import settings as app_settings
app_settings.seed()

# Seed the built-in RBAC roles (viewer/editor/admin).
try:
    seed_roles()
except Exception as _seed_err:
    print(f"Warning: role seeding failed ({_seed_err})")

# --- Auth Setup ---
SECRET_KEY = settings.secret_key

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())

# Pre-install essential DuckDB extensions once at server startup
try:
    import duckdb
    with duckdb.connect() as startup_conn:
        startup_conn.execute("INSTALL aws; INSTALL httpfs;")
except Exception as startup_err:
    print(f"Warning: Failed to pre-install DuckDB extensions: {startup_err}")

app = FastAPI(title="Dataflow API")

# Session middleware is registered AFTER auth_middleware below
# so Starlette's LIFO stack makes it outermost (runs first on every request)

# Setup templates and static directories
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# --- Embedded services: the server supervises a separate worker process ---
@app.on_event("startup")
def _start_embedded_services():
    # Recover any jobs orphaned by a previous crash so the dashboard is accurate.
    try:
        job_queue.reconcile_stale(settings.worker_stale_seconds)
    except Exception as e:
        print(f"Warning: job reconciliation failed at startup: {e}")

    if settings.embedded_worker:
        try:
            from worker import start_worker_process
            start_worker_process()
        except Exception as e:
            print(f"Warning: worker process failed to start: {e}")

    if settings.embedded_scheduler:
        try:
            from scheduler import start_scheduler
            start_scheduler(load_configs())
        except Exception as e:
            print(f"Warning: embedded scheduler failed to start: {e}")


@app.on_event("shutdown")
def _stop_embedded_services():
    try:
        from worker import stop_worker_process
        stop_worker_process()
    except Exception as e:
        print(f"Warning: failed to stop worker process cleanly: {e}")


# --- Auth Middleware ---

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    exempt = ("/login", "/setup", "/static", "/auth/sso")
    if any(request.url.path.startswith(p) for p in exempt):
        return await call_next(request)

    # Check if any users exist — if not, redirect to first-run setup
    with Session(engine) as db:
        users = db.exec(select(User)).all()

    if len(users) == 0:
        return RedirectResponse(url="/setup", status_code=302)

    username = request.session.get("user")
    if not username:
        return RedirectResponse(url="/login", status_code=302)

    # Attach the current user (and resolved permissions) to request state
    with Session(engine) as db:
        user = db.exec(select(User).where(User.username == username)).first()
        if not user:
            # User was deleted while session was active
            request.session.clear()
            return RedirectResponse(url="/login", status_code=302)
        request.state.user = user
        role_row = db.exec(select(Role).where(Role.name == user.role)).first()
        request.state.perms = permissions_for(role_row.permissions if role_row else [])

    return await call_next(request)

# SessionMiddleware registered last = outermost layer = runs first
# so request.session is always populated before auth_middleware reads it
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)


# Helper: Render HTML templates (auto-injects current_user + permissions)
def render(request: Request, name: str, context: Optional[dict] = None):
    user = getattr(request.state, "user", None)
    perms = getattr(request.state, "perms", set())
    ctx = {
        "request": request,
        "current_user": user,
        "perms": perms,
        # convenience: does this user have any admin-console permission?
        "can_admin": bool(perms & {"users.manage", "roles.manage", "settings.manage"}),
    }
    if context:
        ctx.update(context)
    return templates.TemplateResponse(request=request, name=name, context=ctx)


# --- RBAC enforcement (permission-based) ---

def _user_perms(request: Request) -> set:
    return getattr(request.state, "perms", set())


# Dedicated audit logger: structured JSON to stdout, independent of uvicorn's
# log config, so the platform (EKS/CloudWatch/SIEM) captures an immutable copy.
_audit_logger = logging.getLogger("dataflow.audit")
if not _audit_logger.handlers:
    _audit_handler = logging.StreamHandler(sys.stdout)
    _audit_handler.setFormatter(logging.Formatter("%(asctime)s dataflow.audit %(message)s"))
    _audit_logger.addHandler(_audit_handler)
    _audit_logger.setLevel(logging.INFO)
    _audit_logger.propagate = False


def _client_ip(request: Request) -> Optional[str]:
    if request is None:
        return None
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else None


def audit(request: Request, action: str, target_type: str = None, target_name: str = None,
          detail: str = None, success: bool = True, username: str = None, role: str = None) -> None:
    """Record a security-relevant action. Writes two copies: a structured JSON
    line to stdout (durable — shipped by the platform's log pipeline / SIEM) and
    a row in the DB (for the in-app viewer). Never raises."""
    user = getattr(request.state, "user", None) if request else None
    uname = username if username is not None else (user.username if user else None)
    urole = role if role is not None else (user.role if user else None)
    ip = _client_ip(request)
    ts = datetime.utcnow()
    # Durable stdout copy first (survives even if the DB write fails).
    try:
        _audit_logger.info(json.dumps({
            "ts": ts.isoformat(), "action": action, "username": uname, "role": urole,
            "target_type": target_type, "target_name": target_name,
            "detail": detail, "ip": ip, "success": success,
        }))
    except Exception:  # noqa: BLE001
        pass
    # DB copy for the in-app audit viewer.
    try:
        with Session(engine) as db:
            db.add(AuditLog(
                username=uname, role=urole, action=action,
                target_type=target_type, target_name=target_name,
                detail=detail, ip=ip, success=success, timestamp=ts,
            ))
            db.commit()
    except Exception as e:  # noqa: BLE001
        print(f"Warning: audit log DB write failed: {e}")


def require_permission(permission: str):
    """FastAPI dependency factory: require that the current user's role grants
    `permission`. Enforcement is server-side. Returns the User on success;
    raises 401 if unauthenticated, 403 if the permission is missing."""
    def _dependency(request: Request) -> User:
        user = getattr(request.state, "user", None)
        if user is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        if permission not in _user_perms(request):
            raise HTTPException(status_code=403, detail=f"Missing permission: {permission}")
        return user

    return _dependency


def check_permission(request: Request, permission: str) -> None:
    """Imperative permission check for handlers that need conditional logic
    (e.g. one endpoint covering both create and edit)."""
    if getattr(request.state, "user", None) is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if permission not in _user_perms(request):
        raise HTTPException(status_code=403, detail=f"Missing permission: {permission}")


def _role_exists(name: str) -> bool:
    with Session(engine) as db:
        return db.exec(select(Role).where(Role.name == name)).first() is not None


def _all_role_names() -> list:
    with Session(engine) as db:
        return sorted(r.name for r in db.exec(select(Role)).all())


def _role_scope(db, role_name: str):
    """Return (permissions, bucket_allow, bucket_deny) for a role name."""
    r = db.exec(select(Role).where(Role.name == role_name)).first()
    if not r:
        return [], [], []
    return list(r.permissions or []), list(r.bucket_allow or []), list(r.bucket_deny or [])


def _bucket_denied_for_role(role_name: str, paths) -> list:
    """Which of `paths` the given role may NOT access (empty = all allowed)."""
    with Session(engine) as db:
        perms, allow, deny = _role_scope(db, role_name)
    return role_disallowed_paths(perms, allow, deny, paths)


def _role_is_super(role_name: str) -> bool:
    """True if the role holds the wildcard permission (i.e. admin)."""
    with Session(engine) as db:
        perms, _, _ = _role_scope(db, role_name)
    return WILDCARD in perms


def _bucket_denied_for_user(user: User, text: str) -> list:
    """s3:// paths referenced in `text` that `user`'s role may not access."""
    return _bucket_denied_for_role(user.role, extract_s3_paths(text))


def _role_perms(db, role_name: str) -> set:
    r = db.exec(select(Role).where(Role.name == role_name)).first()
    return permissions_for(r.permissions if r else [])


def _admin_capable_count(db, *, exclude: str = None, override: tuple = None) -> int:
    """Count users who would still hold `users.manage` after a hypothetical
    change. `exclude` drops a user (simulating deletion); `override` is
    (username, new_role) (simulating a role change). Used to prevent locking
    everyone out of user administration."""
    n = 0
    for u in db.exec(select(User)).all():
        if exclude and u.username == exclude:
            continue
        role_name = override[1] if (override and u.username == override[0]) else u.role
        if "users.manage" in _role_perms(db, role_name):
            n += 1
    return n


def _landing_path(request: Request) -> Optional[str]:
    """The first page the user is allowed to view (used for post-login and for
    redirecting away from pages they can't view). None if they can view nothing."""
    perms = _user_perms(request)
    for path, perm in PAGE_PERMISSIONS:
        if perm in perms:
            return path
    if perms & {"users.manage", "roles.manage", "settings.manage"}:
        return "/settings"
    return None


def _view_guard(request: Request, permission: str):
    """Page-route guard: returns None if the user holds `permission`, otherwise a
    redirect to their first accessible page — or a friendly no-access page if
    their role can view nothing."""
    if permission in _user_perms(request):
        return None
    land = _landing_path(request)
    if land and land != request.url.path:
        return RedirectResponse(url=land, status_code=302)
    return render(request, "no_access.html", {})


# --- Setup (first-run) ---

@app.get("/setup", response_class=HTMLResponse)
def setup_page(request: Request):
    with Session(engine) as db:
        count = len(db.exec(select(User)).all())
    if count > 0:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request=request, name="setup.html", context={"request": request, "error": None})


@app.post("/setup", response_class=HTMLResponse)
def setup_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    with Session(engine) as db:
        count = len(db.exec(select(User)).all())
        if count > 0:
            return RedirectResponse(url="/", status_code=302)
        if len(password) < 6:
            return templates.TemplateResponse(request=request, name="setup.html", context={"request": request, "error": "Password must be at least 6 characters"})
        user = User(username=username, password_hash=hash_password(password), is_admin=True, role=ROLE_ADMIN)
        db.add(user)
        db.commit()
    return RedirectResponse(url="/login", status_code=302)


# --- Login / Logout ---

def _login_context(request: Request, error: Optional[str] = None) -> dict:
    """Login-page context: local form + optional SSO button."""
    return {
        "request": request,
        "error": error,
        "sso_enabled": settings.sso_configured,
        "sso_button_label": settings.sso_button_label,
    }


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.session.get("user"):
        return RedirectResponse(url="/", status_code=302)
    # Surface SSO redirect errors passed back as a query param.
    error = request.query_params.get("error")
    return templates.TemplateResponse(request=request, name="login.html",
                                      context=_login_context(request, error))


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    with Session(engine) as db:
        user = db.exec(select(User).where(User.username == username)).first()
    # Steer SSO-only accounts to the SSO button rather than a confusing failure.
    if user and user.auth_provider != "local":
        return templates.TemplateResponse(
            request=request, name="login.html",
            context=_login_context(request, "This account signs in with SSO. Use the button above."))
    if not user or not verify_password(password, user.password_hash):
        audit(request, "auth.login", "auth", username, detail="invalid credentials",
              success=False, username=username, role=None)
        return templates.TemplateResponse(
            request=request, name="login.html",
            context=_login_context(request, "Invalid username or password"))
    request.session["user"] = user.username
    audit(request, "auth.login", "auth", user.username, success=True,
          username=user.username, role=user.role)
    return RedirectResponse(url="/", status_code=302)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


# --- SSO (OIDC / Microsoft Entra) — additive to local login ---

@app.get("/auth/sso/login")
async def sso_login(request: Request):
    """Kick off the OIDC Authorization Code flow (redirect to the IdP)."""
    try:
        oauth = build_oauth()
    except SSOError:
        return RedirectResponse(url="/login?error=SSO+is+not+available", status_code=302)
    redirect_uri = settings.sso_redirect_uri or str(request.url_for("sso_callback"))
    client = oauth.create_client(SSO_PROVIDER)
    return await client.authorize_redirect(request, redirect_uri)


@app.get("/auth/sso/callback", name="sso_callback")
async def sso_callback(request: Request):
    """Handle the IdP redirect: exchange code, map claims->role, provision, login."""
    try:
        oauth = build_oauth()
        client = oauth.create_client(SSO_PROVIDER)
        token = await client.authorize_access_token(request)
    except Exception:
        return templates.TemplateResponse(
            request=request, name="login.html",
            context=_login_context(request, "SSO sign-in failed. Try again or use local login."))

    claims = token.get("userinfo") or {}
    if not claims and token.get("id_token"):
        try:
            claims = await client.parse_id_token(request, token)
        except Exception:
            claims = {}

    try:
        username, role = resolve_sso_user(
            claims,
            settings.sso_group_role_map(),
            settings.sso_default_role,
            db_role_rank,
            settings.sso_group_claim,
        )
        provision_user(username, role)
    except SSOError as e:
        audit(request, "auth.login", "auth", "sso", detail=f"sso denied: {e}", success=False)
        return templates.TemplateResponse(
            request=request, name="login.html",
            context=_login_context(request, str(e)))

    request.session["user"] = username
    audit(request, "auth.login", "auth", username, detail="via SSO",
          success=True, username=username, role=role)
    return RedirectResponse(url="/", status_code=302)


# --- Settings ---

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    current_user = getattr(request.state, "user", None)
    perms = getattr(request.state, "perms", set())
    if not current_user or not (perms & {"users.manage", "roles.manage", "settings.manage"}):
        return RedirectResponse(url="/", status_code=302)
    with Session(engine) as db:
        users = db.exec(select(User).order_by(User.created_at)).all()
    from settings import settings as app_settings
    all_settings = app_settings.get_all()
    return render(request, "settings.html", {
        "active_page": "settings",
        "users": users,
        "all_roles": _all_role_names(),
        "app_settings": all_settings,
    })


@app.post("/api/users", response_class=HTMLResponse)
def api_create_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form(...),
    _perm: User = Depends(require_permission("users.manage")),
):
    try:
        if len(password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
        role = (role or "").strip()
        if not _role_exists(role):
            raise HTTPException(status_code=400, detail=f"Unknown role '{role}'")
        with Session(engine) as db:
            if db.exec(select(User).where(User.username == username)).first():
                raise HTTPException(status_code=400, detail="Username already exists")
            db.add(User(
                username=username,
                password_hash=hash_password(password),
                role=role,
                is_admin=(role == ROLE_ADMIN),
            ))
            db.commit()
            users = db.exec(select(User).order_by(User.created_at)).all()
        audit(request, "user.create", "user", username, detail=f"role={role}")
        return render(request, "partials/users_list.html", {"users": users, "all_roles": _all_role_names()})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/users/{username}", response_class=HTMLResponse)
def api_delete_user(
    request: Request,
    username: str,
    current_user: User = Depends(require_permission("users.manage")),
):
    if username == current_user.username:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")
    try:
        with Session(engine) as db:
            user = db.exec(select(User).where(User.username == username)).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            # Never allow deleting the last user who can manage users.
            if _admin_capable_count(db, exclude=username) == 0:
                raise HTTPException(status_code=400, detail="Cannot delete the last administrator (users.manage)")
            db.delete(user)
            db.commit()
            users = db.exec(select(User).order_by(User.created_at)).all()
        audit(request, "user.delete", "user", username)
        return render(request, "partials/users_list.html", {"users": users, "all_roles": _all_role_names()})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/users/{username}/role", response_class=HTMLResponse)
def api_update_user_role(
    request: Request,
    username: str,
    role: str = Form(...),
    current_user: User = Depends(require_permission("users.manage")),
):
    """Change a user's role. Blocks any change that would leave nobody able to
    manage users (prevents locking everyone out)."""
    role = (role or "").strip()
    if not _role_exists(role):
        raise HTTPException(status_code=400, detail=f"Unknown role '{role}'")
    try:
        with Session(engine) as db:
            user = db.exec(select(User).where(User.username == username)).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            if _admin_capable_count(db, override=(username, role)) == 0:
                raise HTTPException(status_code=400, detail="This change would leave no user able to manage users")
            user.role = role
            user.is_admin = (role == ROLE_ADMIN)
            db.add(user)
            db.commit()
            users = db.exec(select(User).order_by(User.created_at)).all()
        audit(request, "user.role_change", "user", username, detail=f"role={role}")
        return render(request, "partials/users_list.html", {"users": users, "all_roles": _all_role_names()})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Settings API ---

@app.get("/api/settings")
def api_get_settings(request: Request, _perm: User = Depends(require_permission("settings.manage"))):
    """Get all application settings."""
    from settings import settings as app_settings
    return {"settings": app_settings.get_all()}


@app.put("/api/settings/{key}")
def api_update_setting(request: Request, key: str, value: str = Form(...), _perm: User = Depends(require_permission("settings.manage"))):
    """Update a single setting by key."""
    from settings import settings as app_settings
    app_settings.set(key, value)
    audit(request, "setting.update", "setting", key)  # value intentionally not logged
    return {"key": key, "value": value}


@app.post("/api/settings")
def api_update_settings_bulk(request: Request, settings_data: dict = None, _perm: User = Depends(require_permission("settings.manage"))):
    """Update multiple settings at once."""
    from settings import settings as app_settings
    # This will be called from the form with individual fields
    return {"settings": app_settings.get_all()}


# --- Role management API (custom roles + permission matrix) ---

class RoleUpsert(BaseModel):
    description: Optional[str] = None
    permissions: list[str] = []
    bucket_allow: list[str] = []
    bucket_deny: list[str] = []


class RoleCreate(RoleUpsert):
    name: str


def _clean_permissions(perms: list[str]) -> list[str]:
    """Keep only known, assignable permissions (drops unknowns, the wildcard,
    and baseline-locked view perms which are always granted anyway)."""
    allowed = set(ALL_PERMISSIONS) - set(LOCKED_PERMISSIONS)
    return sorted({p for p in (perms or []) if p in allowed})


def _clean_prefixes(prefixes: list[str]) -> list[str]:
    """Normalize a list of S3 URI prefixes: strip, drop blanks, dedupe."""
    return sorted({p.strip() for p in (prefixes or []) if p and p.strip()})


def _serialize_role(role: Role, user_count: int) -> dict:
    return {
        "name": role.name,
        "description": role.description,
        "permissions": role.permissions or [],
        "bucket_allow": role.bucket_allow or [],
        "bucket_deny": role.bucket_deny or [],
        "is_system": role.is_system,
        "user_count": user_count,
    }


@app.get("/api/roles")
def api_list_roles(request: Request, _perm: User = Depends(require_permission("roles.manage"))):
    """List all roles with their permissions and how many users hold each."""
    with Session(engine) as db:
        roles = db.exec(select(Role)).all()
        result = []
        for r in roles:
            count = len(db.exec(select(User).where(User.role == r.name)).all())
            result.append(_serialize_role(r, count))
    # Sort system roles first (viewer, editor, admin), then custom alphabetically.
    order = {ROLE_VIEWER: 0, ROLE_EDITOR: 1, ROLE_ADMIN: 2}
    result.sort(key=lambda r: (0 if r["is_system"] else 1, order.get(r["name"], 99), r["name"]))
    return {
        "roles": result,
        "permission_groups": PERMISSION_GROUPS,
        "permission_labels": PERMISSION_LABELS,
        "locked_permissions": sorted(LOCKED_PERMISSIONS),
    }


@app.post("/api/roles")
def api_create_role(request: Request, body: RoleCreate, _perm: User = Depends(require_permission("roles.manage"))):
    """Create a custom role."""
    import re as _re
    name = (body.name or "").strip().lower()
    if not _re.match(r"^[a-z][a-z0-9_]*$", name):
        raise HTTPException(status_code=400, detail="Role name must be lowercase letters, numbers, underscores (start with a letter)")
    with Session(engine) as db:
        if db.exec(select(Role).where(Role.name == name)).first():
            raise HTTPException(status_code=400, detail="A role with that name already exists")
        role = Role(
            name=name,
            description=(body.description or None),
            permissions=_clean_permissions(body.permissions),
            bucket_allow=_clean_prefixes(body.bucket_allow),
            bucket_deny=_clean_prefixes(body.bucket_deny),
            is_system=False,
        )
        db.add(role)
        db.commit()
    audit(request, "role.create", "role", name, detail=f"permissions={len(body.permissions or [])}")
    return {"status": "created", "name": name}


@app.put("/api/roles/{name}")
def api_update_role(request: Request, name: str, body: RoleUpsert, _perm: User = Depends(require_permission("roles.manage"))):
    """Update a custom role's description/permissions. System roles are locked."""
    with Session(engine) as db:
        role = db.exec(select(Role).where(Role.name == name)).first()
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        if role.is_system:
            raise HTTPException(status_code=400, detail="System roles cannot be modified")
        role.description = body.description if body.description is not None else role.description
        role.permissions = _clean_permissions(body.permissions)
        role.bucket_allow = _clean_prefixes(body.bucket_allow)
        role.bucket_deny = _clean_prefixes(body.bucket_deny)
        db.add(role)
        db.commit()
    audit(request, "role.update", "role", name)
    return {"status": "updated", "name": name}


@app.delete("/api/roles/{name}")
def api_delete_role(request: Request, name: str, _perm: User = Depends(require_permission("roles.manage"))):
    """Delete a custom role. Blocked for system roles or roles still in use."""
    with Session(engine) as db:
        role = db.exec(select(Role).where(Role.name == name)).first()
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        if role.is_system:
            raise HTTPException(status_code=400, detail="System roles cannot be deleted")
        in_use = len(db.exec(select(User).where(User.role == name)).all())
        if in_use:
            raise HTTPException(status_code=400, detail=f"Cannot delete: {in_use} user(s) still have this role")
        db.delete(role)
        db.commit()
    audit(request, "role.delete", "role", name)
    return {"status": "deleted", "name": name}


# Helper: run stats via SQLModel
def _get_stats():
    from config import engine, PipelineRun
    from sqlmodel import Session, select
    from sqlalchemy import func
    stats = {"total_runs": 0, "success": 0, "failed": 0, "started": 0}
    try:
        with Session(engine) as session:
            rows = session.exec(
                select(PipelineRun.status, func.count()).group_by(PipelineRun.status)
            ).all()
        for status, count in rows:
            stats[status] = count
            stats["total_runs"] += count
        return stats
    except Exception:
        return stats


# Helper: recent runs via SQLModel
def _get_recent_runs(limit: int = 15):
    from config import engine, PipelineRun
    from sqlmodel import Session, select
    try:
        with Session(engine) as session:
            runs_list = session.exec(
                select(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(limit)
            ).all()
    except Exception:
        return []

    runs = []
    for r in runs_list:
        duration = None
        if r.finished_at and r.started_at:
            duration = round((r.finished_at - r.started_at).total_seconds(), 1)
        started_display = None
        if r.started_at:
            started_display = r.started_at.isoformat().replace("T", " ").split(".")[0]
        runs.append({
            "id": r.id,
            "pipeline_name": r.pipeline_name,
            "job_name": r.job_name,
            "status": r.status,
            "started_at": started_display,
            "finished_at": r.finished_at,
            "rows_extracted": r.rows_extracted,
            "rows_written": r.rows_written,
            "error_message": r.error_message,
            "duration": duration,
        })
    return runs


# --- HTML Page Routes ---

@app.get("/", response_class=HTMLResponse)
def index_page(request: Request):
    guard = _view_guard(request, "dashboard.view")
    if guard:
        return guard
    stats = _get_stats()
    runs = _get_recent_runs()
    return render(request, "dashboard.html", {"active_page": "dashboard", "stats": stats, "runs": runs})


@app.get("/pipelines", response_class=HTMLResponse)
def pipelines_page(request: Request):
    guard = _view_guard(request, "pipelines.view")
    if guard:
        return guard
    try:
        resolved = load_configs()
    except Exception:
        resolved = ResolvedConfig(pipelines={}, cronjobs={})
        
    p_json = {p.name: p.model_dump_json() for p in resolved.pipelines.values()}
    # run_as options: admins (wildcard) may assign any role; others are pinned
    # to their own role (enforced server-side too).
    user = getattr(request.state, "user", None)
    assignable_roles = _all_role_names() if (user and _role_is_super(user.role)) else ([user.role] if user else [])
    return render(
        request,
        "pipelines.html",
        {
            "active_page": "pipelines",
            "pipelines": list(resolved.pipelines.values()),
            "p_json": p_json,
            "assignable_roles": assignable_roles,
        }
    )


@app.get("/cronjobs", response_class=HTMLResponse)
def cronjobs_page(request: Request):
    guard = _view_guard(request, "cronjobs.view")
    if guard:
        return guard
    try:
        resolved = load_configs()
    except Exception:
        resolved = ResolvedConfig(pipelines={}, cronjobs={})
        
    cronjobs_json = {c.name: c.model_dump_json() for c in resolved.cronjobs.values()}
    return render(
        request,
        "cronjobs.html",
        {
            "active_page": "cronjobs",
            "cronjobs": list(resolved.cronjobs.values()),
            "pipelines": list(resolved.pipelines.values()),
            "cronjobs_json": cronjobs_json
        }
    )


# --- HTMX / Partial Routes ---

@app.get("/api/stats", response_class=HTMLResponse)
def api_stats(request: Request, _perm: User = Depends(require_permission("dashboard.view"))):
    stats = _get_stats()
    return render(request, "partials/stats.html", {"stats": stats})


@app.get("/api/runs-table", response_class=HTMLResponse)
def api_runs_table(request: Request, _perm: User = Depends(require_permission("dashboard.view"))):
    runs = _get_recent_runs()
    return render(request, "partials/runs_table.html", {"runs": runs})


# Pipeline runs are dispatched through the durable job queue (see jobs.py).
import jobs as job_queue


def is_pipeline_running(name: str) -> bool:
    """A pipeline is 'active' if it has a queued or running job."""
    return job_queue.is_pipeline_active(name)


@app.get("/api/transforms/plugins")
def api_list_transform_plugins():
    """List available Python transform plugins (files in src/transforms/)."""
    try:
        from transforms import available_plugins
        return {"plugins": available_plugins()}
    except Exception as e:
        return {"plugins": [], "error": str(e)}


@app.post("/api/pipelines/{name}/run")
def api_run_pipeline(request: Request, name: str, _perm: User = Depends(require_permission("pipelines.run"))):
    try:
        resolved = load_configs()
        if name not in resolved.pipelines:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        job_id = job_queue.enqueue(name, trigger="manual")
        if job_id is None:
            raise HTTPException(status_code=409, detail="Pipeline is already queued or running")
        audit(request, "pipeline.run", "pipeline", name)
        return Response(status_code=204)  # queued; worker will pick it up
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Create or Update Pipeline
@app.post("/api/pipelines", response_class=HTMLResponse)
def api_save_pipeline(
    request: Request,
    original_name: str = Form(""),
    name: str = Form(...),
    description: Optional[str] = Form(None),
    source_path: str = Form(...),
    sink_path: str = Form(...),
    sink_format: str = Form("parquet"),
    partition_by: Optional[str] = Form(None),
    checkpointing: str = Form(""),
    threads: Optional[str] = Form(None),
    memory_limit: Optional[str] = Form(None),
    target_file_size: Optional[str] = Form(None),
    row_group_size: Optional[str] = Form(None),
    on_failure: str = Form("none"),
    email: Optional[str] = Form(None),
    on_row_count_below: Optional[str] = Form(None),
    run_as: Optional[str] = Form(None),
    transforms_json: str = Form("[]"),
    checks_json: str = Form("[]"),
):
    try:
        # A single endpoint covers both create and edit; require the matching
        # permission based on whether we're updating an existing pipeline.
        check_permission(request, "pipelines.edit" if original_name else "pipelines.create")
        transforms = json.loads(transforms_json)
        checks = json.loads(checks_json)

        # Parse optional integer fields — empty strings become None
        def _parse_int(v):
            if v is None or str(v).strip() == "":
                return None
            try:
                return int(v)
            except ValueError:
                return None

        threads_val = _parse_int(threads)
        row_count_below_val = _parse_int(on_row_count_below)
        row_group_size_val = _parse_int(row_group_size)
        target_file_size_val = (target_file_size or "").strip() or None

        # Prevent editing a pipeline while it is running
        if original_name and is_pipeline_running(original_name):
            raise HTTPException(status_code=409, detail="Cannot edit a pipeline while it is running")

        # Build alerts dictionary
        alerts = {
            "on_failure": on_failure,
            "email": email if on_failure == "email" else None,
            "on_row_count_below": row_count_below_val
        }

        # Resolve run_as (the role whose data-access scope this pipeline runs
        # under) and enforce the data boundary + anti-escalation rules.
        actor = getattr(request.state, "user", None)
        # "super" = the actor's role holds the wildcard permission (admin). Note
        # request.state.perms is the *expanded* set, so we check the raw role.
        is_super = bool(actor) and _role_is_super(actor.role)
        run_as_role = (run_as or "").strip() or (actor.role if actor else None)
        if run_as_role and not _role_exists(run_as_role):
            raise HTTPException(status_code=400, detail=f"Unknown run_as role '{run_as_role}'")
        # Non-admins may only run a pipeline as their own role (no escalation).
        if run_as_role and actor and run_as_role != actor.role and not is_super:
            raise HTTPException(status_code=403, detail="You may only run a pipeline as your own role")
        # Every source/sink/join path must be within the run_as role's bucket scope.
        if run_as_role:
            paths = [source_path, sink_path]
            for t in transforms:
                if isinstance(t, dict) and t.get("type") == "join" and t.get("right_path"):
                    paths.append(t["right_path"])
            denied = _bucket_denied_for_role(run_as_role, [p for p in paths if p])
            if denied:
                raise HTTPException(status_code=403, detail=f"Role '{run_as_role}' cannot access: " + ", ".join(sorted(set(denied))))

        with Session(engine) as session:
            if original_name:
                # Update existing
                db_p = session.exec(select(Pipeline).where(Pipeline.name == original_name)).first()
                if not db_p:
                    raise HTTPException(status_code=404, detail="Original pipeline not found")
                db_p.name = name
                db_p.description = description
                db_p.source_path = source_path
                db_p.sink_path = sink_path
                db_p.sink_format = sink_format
                db_p.partition_by = partition_by or None
                db_p.checkpointing = checkpointing == "true"
                db_p.threads = threads_val
                db_p.memory_limit = memory_limit
                db_p.run_as = run_as_role
                db_p.target_file_size = target_file_size_val
                db_p.row_group_size = row_group_size_val
                db_p.transforms = transforms
                db_p.checks = checks
                db_p.alerts = alerts
                session.add(db_p)
            else:
                # Create new
                # Check for unique name conflict
                conflict = session.exec(select(Pipeline).where(Pipeline.name == name)).first()
                if conflict:
                    raise HTTPException(status_code=400, detail="Pipeline name already exists")
                
                db_p = Pipeline(
                    name=name,
                    description=description,
                    source_path=source_path,
                    sink_path=sink_path,
                    sink_format=sink_format,
                    partition_by=partition_by or None,
                    checkpointing=checkpointing == "true",
                    threads=threads_val,
                    memory_limit=memory_limit,
                    run_as=run_as_role,
                    target_file_size=target_file_size_val,
                    row_group_size=row_group_size_val,
                    transforms=transforms,
                    checks=checks,
                    alerts=alerts
                )
                session.add(db_p)
                
            session.commit()
            
        # Return updated partial list
        audit(request, "pipeline.update" if original_name else "pipeline.create",
              "pipeline", name, detail=f"run_as={run_as_role}")
        resolved = load_configs()
        p_json = {p.name: p.model_dump_json() for p in resolved.pipelines.values()}
        return render(
            request,
            "partials/pipelines_list.html",
            {
                "pipelines": list(resolved.pipelines.values()),
                "p_json": p_json
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Delete Pipeline
@app.delete("/api/pipelines/{name}", response_class=HTMLResponse)
def api_delete_pipeline(request: Request, name: str, _perm: User = Depends(require_permission("pipelines.delete"))):
    try:
        with Session(engine) as session:
            db_p = session.exec(select(Pipeline).where(Pipeline.name == name)).first()
            if not db_p:
                raise HTTPException(status_code=404, detail="Pipeline not found")
            session.delete(db_p)
            
            # Delete associated cronjobs
            cronjobs = session.exec(select(CronJob).where(CronJob.pipeline == name)).all()
            for c in cronjobs:
                session.delete(c)
                
            session.commit()
            
        audit(request, "pipeline.delete", "pipeline", name)
        resolved = load_configs()
        p_json = {p.name: p.model_dump_json() for p in resolved.pipelines.values()}
        return render(
            request,
            "partials/pipelines_list.html",
            {
                "pipelines": list(resolved.pipelines.values()),
                "p_json": p_json
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Create or Update Schedule
@app.post("/api/cronjobs", response_class=HTMLResponse)
def api_save_cronjob(
    request: Request,
    original_name: str = Form(""),
    name: str = Form(...),
    pipeline: str = Form(...),
    schedule: str = Form(...),
    timezone: str = Form("UTC"),
    max_attempts: int = Form(3),
    delay_seconds: int = Form(60),
    enabled: bool = Form(False),
):
    try:
        check_permission(request, "cronjobs.edit" if original_name else "cronjobs.create")
        retry = {"max_attempts": max_attempts, "delay_seconds": delay_seconds}
        
        with Session(engine) as session:
            # Check pipeline validity
            db_p = session.exec(select(Pipeline).where(Pipeline.name == pipeline)).first()
            if not db_p:
                raise HTTPException(status_code=400, detail="Referenced pipeline does not exist")

            if original_name:
                # Update
                db_c = session.exec(select(CronJob).where(CronJob.name == original_name)).first()
                if not db_c:
                    raise HTTPException(status_code=404, detail="Original schedule not found")
                db_c.name = name
                db_c.pipeline = pipeline
                db_c.schedule = schedule
                db_c.timezone = timezone
                db_c.retry = retry
                db_c.enabled = enabled
                session.add(db_c)
            else:
                # Create
                conflict = session.exec(select(CronJob).where(CronJob.name == name)).first()
                if conflict:
                    raise HTTPException(status_code=400, detail="Schedule name already exists")
                    
                db_c = CronJob(
                    name=name,
                    pipeline=pipeline,
                    schedule=schedule,
                    timezone=timezone,
                    retry=retry,
                    enabled=enabled
                )
                session.add(db_c)
                
            session.commit()

        audit(request, "cronjob.update" if original_name else "cronjob.create", "cronjob", name)
        resolved = load_configs()
        cronjobs_json = {c.name: c.model_dump_json() for c in resolved.cronjobs.values()}
        return render(
            request,
            "partials/cronjobs_list.html",
            {
                "cronjobs": list(resolved.cronjobs.values()),
                "cronjobs_json": cronjobs_json
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Toggle Schedule Enabled/Disabled Status
@app.post("/api/cronjobs/{name}/toggle", response_class=HTMLResponse)
def api_toggle_cronjob(request: Request, name: str, _perm: User = Depends(require_permission("cronjobs.run"))):
    try:
        with Session(engine) as session:
            db_c = session.exec(select(CronJob).where(CronJob.name == name)).first()
            if not db_c:
                raise HTTPException(status_code=404, detail="Schedule not found")
            db_c.enabled = not db_c.enabled
            session.add(db_c)
            session.commit()
            audit(request, "cronjob.toggle", "cronjob", name, detail=f"enabled={db_c.enabled}")
            
        resolved = load_configs()
        cronjobs_json = {c.name: c.model_dump_json() for c in resolved.cronjobs.values()}
        return render(
            request,
            "partials/cronjobs_list.html",
            {
                "cronjobs": list(resolved.cronjobs.values()),
                "cronjobs_json": cronjobs_json
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Delete Schedule
@app.delete("/api/cronjobs/{name}", response_class=HTMLResponse)
def api_delete_cronjob(request: Request, name: str, _perm: User = Depends(require_permission("cronjobs.delete"))):
    try:
        with Session(engine) as session:
            db_c = session.exec(select(CronJob).where(CronJob.name == name)).first()
            if not db_c:
                raise HTTPException(status_code=404, detail="Schedule not found")
            session.delete(db_c)
            session.commit()
            audit(request, "cronjob.delete", "cronjob", name)
            
        resolved = load_configs()
        cronjobs_json = {c.name: c.model_dump_json() for c in resolved.cronjobs.values()}
        return render(
            request,
            "partials/cronjobs_list.html",
            {
                "cronjobs": list(resolved.cronjobs.values()),
                "cronjobs_json": cronjobs_json
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Audit Log ---

@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request):
    guard = _view_guard(request, "audit.view")
    if guard:
        return guard
    from settings import settings as app_settings
    return render(request, "audit.html", {
        "active_page": "audit",
        "audit_retention_days": app_settings.audit_retention_days,
    })


@app.get("/api/audit")
def api_audit(
    request: Request,
    username: Optional[str] = None,
    action: Optional[str] = None,
    target_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    _perm: User = Depends(require_permission("audit.view")),
):
    """Return audit entries, newest first, with optional filters."""
    limit = max(1, min(limit, 500))
    with Session(engine) as db:
        q = select(AuditLog).order_by(AuditLog.timestamp.desc())
        if username:
            q = q.where(AuditLog.username == username)
        if action:
            q = q.where(AuditLog.action == action)
        if target_type:
            q = q.where(AuditLog.target_type == target_type)
        rows = db.exec(q.offset(max(0, offset)).limit(limit)).all()
        actions = sorted({a for a in db.exec(select(AuditLog.action).distinct()).all()})
        target_types = sorted({t for t in db.exec(select(AuditLog.target_type).distinct()).all() if t})
    entries = [{
        "id": r.id,
        "timestamp": r.timestamp.isoformat().replace("T", " ").split(".")[0] if r.timestamp else None,
        "username": r.username,
        "role": r.role,
        "action": r.action,
        "target_type": r.target_type,
        "target_name": r.target_name,
        "detail": r.detail,
        "ip": r.ip,
        "success": r.success,
    } for r in rows]
    return {"entries": entries, "actions": actions, "target_types": target_types,
            "limit": limit, "offset": offset}


# --- Query Tool ---

@app.get("/query", response_class=HTMLResponse)
def query_page(request: Request):
    guard = _view_guard(request, "query.run")
    if guard:
        return guard
    from settings import settings as app_settings
    return render(request, "query.html", {"active_page": "query", "ai_chat_enabled": app_settings.ai_chat_enabled})


@app.post("/api/query")
def api_query(query: str = Form(...), page: int = Form(0), page_size: int = Form(50), count_total: bool = Form(False), user: User = Depends(require_permission("query.run"))):
    import duckdb
    import io
    import pyarrow as pa
    import re

    # Data-access scoping: refuse queries that touch buckets outside the user's role.
    denied = _bucket_denied_for_user(user, query)
    if denied:
        raise HTTPException(status_code=403, detail="Access denied to: " + ", ".join(sorted(set(denied))))

    conn = None
    try:
        conn = duckdb.connect()
        # If the query references S3, load the AWS extension and set up the default credential chain.
        if "s3://" in query.lower():
            conn.execute("LOAD aws;")
            conn.execute("CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, VALIDATION 'none');")

        # Strip trailing semicolons/whitespace
        clean_query = re.sub(r'(?:--[^\n]*|/\*.*?\*/|[\s;])+$', '', query, flags=re.DOTALL)

        # Only run COUNT if explicitly requested (e.g. first page load)
        total_rows = -1
        if count_total:
            count_query = f"SELECT COUNT(*) FROM ({clean_query}) AS _count_q"
            total_rows = conn.execute(count_query).fetchone()[0]

        # Fetch only the requested page (fetch page_size + 1 to know if there's more)
        offset = page * page_size
        paged_query = f"SELECT * FROM ({clean_query}) AS _paged_q LIMIT {page_size + 1} OFFSET {offset}"
        arrow_table = conn.execute(paged_query).arrow().read_all()

        has_more = arrow_table.num_rows > page_size
        # Trim to page_size if we got the extra row
        if has_more:
            arrow_table = arrow_table.slice(0, page_size)

        # Serialize to Arrow IPC Stream
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        data = sink.getvalue()
        return Response(
            content=data,
            media_type="application/vnd.apache.arrow.stream",
            headers={
                "X-Total-Rows": str(total_rows),
                "X-Page": str(page),
                "X-Page-Size": str(page_size),
                "X-Has-More": "true" if has_more else "false",
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.post("/api/query/export")
def api_query_export(query: str = Form(...), user: User = Depends(require_permission("query.run"))):
    """Execute a query and return the full result as a downloadable Parquet file."""
    import duckdb
    import io
    import re
    import pyarrow as pa
    import pyarrow.parquet as pq

    denied = _bucket_denied_for_user(user, query)
    if denied:
        raise HTTPException(status_code=403, detail="Access denied to: " + ", ".join(sorted(set(denied))))

    conn = None
    try:
        conn = duckdb.connect()
        if "s3://" in query.lower():
            conn.execute("LOAD aws;")
            conn.execute("CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, VALIDATION 'none');")

        clean_query = re.sub(r'(?:--[^\n]*|/\*.*?\*/|[\s;])+$', '', query, flags=re.DOTALL)
        arrow_table = conn.execute(clean_query).arrow()

        sink = io.BytesIO()
        pq.write_table(arrow_table, sink)
        data = sink.getvalue()

        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": "attachment; filename=query_results.parquet",
                "X-Row-Count": str(arrow_table.num_rows),
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if conn:
            conn.close()


# --- Local AI Chat ---

@app.post("/api/ai-chat")
async def api_ai_chat(request: Request, message: str = Form(...), _perm: User = Depends(require_permission("flux.ask"))):
    """
    AI chat endpoint powered by local LLM (llama-cpp-python).
    Falls back to a helpful message if no model is loaded.
    """
    from settings import settings as app_settings

    if not app_settings.ai_chat_enabled:
        return {
            "reply": "Flux is currently disabled. An admin can enable it in Settings.",
            "sql": None,
            "disabled": True,
        }

    try:
        from llm import is_model_loaded, chat_completion, get_all_schemas, get_active_model_id
    except ImportError:
        return {
            "reply": "The AI feature requires the 'ai' extra. Install with:\n\n`uv sync --extra ai`\n\nor:\n\n`pip install -e .[ai]`",
            "sql": None,
        }

    if not is_model_loaded():
        return {
            "reply": "No model is loaded. Open the model settings (gear icon) to download and activate a model.",
            "sql": None,
        }

    # Gather schema context from data/ directory
    schema_info = get_all_schemas()

    # Run inference
    try:
        result = chat_completion(
            messages=[{"role": "user", "content": message}],
            schema_info=schema_info,
        )
        return {
            "reply": result["reply"],
            "sql": result["sql"],
            "model": result["model"],
        }
    except Exception as e:
        return {
            "reply": f"Error during inference: {str(e)}",
            "sql": None,
        }


# --- Model Management API ---

@app.get("/api/ai-models")
async def api_list_models(request: Request):
    """List available models and their download/load status."""
    try:
        from llm import list_models, get_active_model_id
    except ImportError:
        raise HTTPException(status_code=501, detail="AI extra not installed. Run: uv sync --extra ai")

    models = list_models()
    active = get_active_model_id()
    return {"models": models, "active_model": active}


@app.post("/api/ai-models/{model_id}/download")
async def api_download_model(request: Request, model_id: str, _perm: User = Depends(require_permission("settings.manage"))):
    """Start downloading a model in the background."""
    try:
        from llm import download_model, MODEL_REGISTRY
    except ImportError:
        raise HTTPException(status_code=501, detail="AI extra not installed. Run: uv sync --extra ai")

    if model_id not in MODEL_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")

    download_model(model_id)
    return {"status": "downloading", "model_id": model_id}


@app.post("/api/ai-models/{model_id}/load")
async def api_load_model(request: Request, model_id: str, _perm: User = Depends(require_permission("settings.manage"))):
    """Load a downloaded model into memory for inference."""
    try:
        from llm import load_model, MODEL_REGISTRY, is_model_downloaded
    except ImportError:
        raise HTTPException(status_code=501, detail="AI extra not installed. Run: uv sync --extra ai")

    if model_id not in MODEL_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")

    if not is_model_downloaded(model_id):
        raise HTTPException(status_code=400, detail=f"Model '{model_id}' is not downloaded yet.")

    try:
        load_model(model_id)
        return {"status": "loaded", "model_id": model_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai-models/unload")
async def api_unload_model(request: Request, _perm: User = Depends(require_permission("settings.manage"))):
    """Unload the currently loaded model to free memory."""
    try:
        from llm import unload_model
    except ImportError:
        raise HTTPException(status_code=501, detail="AI extra not installed. Run: uv sync --extra ai")

    unload_model()
    return {"status": "unloaded"}


@app.get("/api/ai-models/{model_id}/status")
async def api_model_download_status(request: Request, model_id: str):
    """Check the download progress of a specific model."""
    try:
        from llm import get_download_status, MODEL_REGISTRY
    except ImportError:
        raise HTTPException(status_code=501, detail="AI extra not installed. Run: uv sync --extra ai")

    if model_id not in MODEL_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")

    return get_download_status(model_id)
