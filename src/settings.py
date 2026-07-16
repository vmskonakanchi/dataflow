"""
Application settings — all stored in the SQLite database, editable at runtime.

No environment variables or .env files. On first run the app seeds sensible
defaults and generates a unique SECRET_KEY, so it's secure with zero config.

Usage:
    from settings import settings

    settings.secret_key                 # auto-generated, persisted in DB
    value = settings.get("smtp_host")
    settings.set("smtp_host", "mail.example.com")
    if settings.get_bool("ai_chat_enabled"): ...
"""

import secrets
from typing import Optional, Dict, List


# --- Default settings (seeded into DB on first run) ---
DEFAULTS: Dict[str, Dict[str, str]] = {
    "smtp_host": {
        "value": "",
        "description": "SMTP server hostname for email alerts",
    },
    "smtp_port": {
        "value": "587",
        "description": "SMTP server port",
    },
    "smtp_username": {
        "value": "",
        "description": "SMTP login username",
    },
    "smtp_password": {
        "value": "",
        "description": "SMTP login password",
    },
    "smtp_from": {
        "value": "",
        "description": "Email sender address (From header)",
    },
    "webhook_url": {
        "value": "",
        "description": "Slack/Discord/generic webhook URL for failure & low-row-count alerts",
    },
    "models_dir": {
        "value": "models",
        "description": "Directory for downloaded AI model files",
    },
    "ai_chat_enabled": {
        "value": "true",
        "description": "Enable or disable the AI chat feature",
    },
    "embedded_worker": {
        "value": "true",
        "description": "Have the server spawn and supervise the job worker as a separate process",
    },
    "embedded_scheduler": {
        "value": "true",
        "description": "Run the cron scheduler inside the server process",
    },
    "worker_poll_seconds": {
        "value": "2",
        "description": "How often the worker polls the queue (seconds)",
    },
    "worker_stale_seconds": {
        "value": "60",
        "description": "Mark a running job as crashed if no heartbeat for this long (seconds)",
    },
    "audit_retention_days": {
        "value": "90",
        "description": "Delete audit log entries older than this many days (0 = keep forever)",
    },
    # --- Single Sign-On (OIDC / Microsoft Entra) ---
    # Additive to username/password login: local accounts always keep working.
    "sso_enabled": {
        "value": "false",
        "description": "Enable 'Sign in with Microsoft' (OIDC) on the login page",
    },
    "sso_discovery_url": {
        "value": "",
        "description": "OIDC discovery URL. Entra: https://login.microsoftonline.com/<tenant-id>/v2.0/.well-known/openid-configuration",
    },
    "sso_client_id": {
        "value": "",
        "description": "OIDC application (client) ID from the identity provider",
    },
    "sso_client_secret": {
        "value": "",
        "description": "OIDC client secret from the identity provider",
    },
    "sso_redirect_uri": {
        "value": "",
        "description": "Redirect/callback URI registered with the IdP. Blank = derive from the request as <base>/auth/sso/callback",
    },
    "sso_scopes": {
        "value": "openid email profile",
        "description": "Space-separated OIDC scopes requested at login",
    },
    "sso_group_claim": {
        "value": "groups",
        "description": "ID-token claim carrying the user's group/role identifiers (Entra: 'groups' or 'roles')",
    },
    "sso_group_role_map": {
        "value": "{}",
        "description": "JSON object mapping IdP group/role IDs to Dataflow roles, e.g. {\"<entra-group-id>\": \"editor\"}",
    },
    "sso_default_role": {
        "value": "",
        "description": "Role assigned when no group matches. Blank = deny login to unmapped users",
    },
    "sso_auto_create_users": {
        "value": "true",
        "description": "Create a local user record on first successful SSO login",
    },
    "sso_button_label": {
        "value": "Sign in with Microsoft",
        "description": "Label shown on the SSO button on the login page",
    },
}


# --- DB-backed settings manager ---
class AppSettings:
    """Settings manager that reads/writes from the SQLite database."""

    def _ensure_seeded(self):
        """Seed default settings into the DB if they don't exist."""
        from config import engine, AppSetting
        from sqlmodel import Session, select

        _INSECURE_DEFAULT = "dev-secret-key-change-in-production"

        with Session(engine) as session:
            rows = {s.key: s for s in session.exec(select(AppSetting)).all()}
            for key, meta in DEFAULTS.items():
                if key not in rows:
                    session.add(AppSetting(key=key, value=meta["value"], description=meta["description"]))

            # Generate a unique secret key on first run (secure by default).
            # Also replace the legacy insecure placeholder if present.
            sk = rows.get("secret_key")
            if sk is None:
                session.add(AppSetting(
                    key="secret_key",
                    value=secrets.token_hex(32),
                    description="Session cookie signing key (auto-generated)",
                ))
            elif not sk.value or sk.value == _INSECURE_DEFAULT:
                sk.value = secrets.token_hex(32)
                sk.description = "Session cookie signing key (auto-generated)"
                session.add(sk)

            session.commit()

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        from config import engine, AppSetting
        from sqlmodel import Session, select

        with Session(engine) as session:
            setting = session.exec(select(AppSetting).where(AppSetting.key == key)).first()
            if setting is not None:
                return setting.value
        if key in DEFAULTS:
            return DEFAULTS[key]["value"]
        return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        value = self.get(key)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes", "on")

    def get_int(self, key: str, default: int = 0) -> int:
        value = self.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def set(self, key: str, value: str) -> None:
        from config import engine, AppSetting
        from sqlmodel import Session, select

        with Session(engine) as session:
            setting = session.exec(select(AppSetting).where(AppSetting.key == key)).first()
            if setting:
                setting.value = value
            else:
                description = DEFAULTS.get(key, {}).get("description")
                setting = AppSetting(key=key, value=value, description=description)
                session.add(setting)
            session.commit()

    def get_all(self) -> List[Dict[str, str]]:
        from config import engine, AppSetting
        from sqlmodel import Session, select

        with Session(engine) as session:
            all_settings = session.exec(select(AppSetting)).all()
            return [
                {"key": s.key, "value": s.value, "description": s.description or ""}
                for s in all_settings
            ]

    def seed(self):
        """Seed defaults + generate the secret key. Called on app startup."""
        self._ensure_seeded()

    # --- Convenience properties ---
    @property
    def secret_key(self) -> str:
        key = self.get("secret_key")
        if not key:
            # Not seeded yet — generate, persist, and return.
            key = secrets.token_hex(32)
            self.set("secret_key", key)
        return key

    @property
    def smtp_host(self) -> Optional[str]:
        return self.get("smtp_host") or None

    @property
    def smtp_port(self) -> int:
        return self.get_int("smtp_port", 587)

    @property
    def smtp_username(self) -> Optional[str]:
        return self.get("smtp_username") or None

    @property
    def smtp_password(self) -> Optional[str]:
        return self.get("smtp_password") or None

    @property
    def smtp_from(self) -> Optional[str]:
        return self.get("smtp_from") or None

    @property
    def webhook_url(self) -> Optional[str]:
        return self.get("webhook_url") or None

    @property
    def webhook_configured(self) -> bool:
        return bool(self.webhook_url)

    @property
    def models_dir(self) -> str:
        return self.get("models_dir", "models")

    @property
    def ai_chat_enabled(self) -> bool:
        return self.get_bool("ai_chat_enabled", True)

    @property
    def embedded_worker(self) -> bool:
        return self.get_bool("embedded_worker", True)

    @property
    def embedded_scheduler(self) -> bool:
        return self.get_bool("embedded_scheduler", True)

    @property
    def worker_poll_seconds(self) -> int:
        return self.get_int("worker_poll_seconds", 2)

    @property
    def worker_stale_seconds(self) -> int:
        return self.get_int("worker_stale_seconds", 60)

    @property
    def audit_retention_days(self) -> int:
        return self.get_int("audit_retention_days", 90)

    @property
    def smtp_configured(self) -> bool:
        return all([self.smtp_host, self.smtp_username, self.smtp_password, self.smtp_from])

    # --- Single Sign-On (OIDC / Entra) ---
    @property
    def sso_enabled(self) -> bool:
        return self.get_bool("sso_enabled", False)

    @property
    def sso_discovery_url(self) -> Optional[str]:
        return self.get("sso_discovery_url") or None

    @property
    def sso_client_id(self) -> Optional[str]:
        return self.get("sso_client_id") or None

    @property
    def sso_client_secret(self) -> Optional[str]:
        return self.get("sso_client_secret") or None

    @property
    def sso_redirect_uri(self) -> Optional[str]:
        return self.get("sso_redirect_uri") or None

    @property
    def sso_scopes(self) -> str:
        return self.get("sso_scopes", "openid email profile") or "openid email profile"

    @property
    def sso_group_claim(self) -> str:
        return self.get("sso_group_claim", "groups") or "groups"

    @property
    def sso_default_role(self) -> Optional[str]:
        return self.get("sso_default_role") or None

    @property
    def sso_auto_create_users(self) -> bool:
        return self.get_bool("sso_auto_create_users", True)

    @property
    def sso_button_label(self) -> str:
        return self.get("sso_button_label", "Sign in with Microsoft") or "Sign in with Microsoft"

    @property
    def sso_configured(self) -> bool:
        """SSO is usable only when enabled AND the core OIDC params are present."""
        return bool(self.sso_enabled and self.sso_discovery_url
                    and self.sso_client_id and self.sso_client_secret)

    def sso_group_role_map(self) -> Dict[str, str]:
        """Parse the JSON group->role map. Returns {} on empty/invalid config."""
        import json
        raw = self.get("sso_group_role_map", "{}") or "{}"
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                # normalize to str->str
                return {str(k): str(v) for k, v in data.items()}
        except (ValueError, TypeError):
            pass
        return {}


# Singleton instance
settings = AppSettings()

