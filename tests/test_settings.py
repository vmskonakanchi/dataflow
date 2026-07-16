"""Tests for settings.py: AppSettings database operations, CRUD, casting, and helper properties."""

import os
import sys
import tempfile
import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sqlmodel import Session, select
import config
from config import AppSetting, init_db
from settings import settings


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    # Call seed once to initialize all defaults in the temp DB
    settings.seed()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


def test_seed_initializes_defaults():
    # Verify that seed generated a unique 64-char hex secret key
    sk = settings.secret_key
    assert len(sk) == 64
    assert all(c in "0123456789abcdef" for c in sk)

    # Verify standard defaults exist
    assert settings.get("ai_chat_enabled") == "true"
    assert settings.get("smtp_port") == "587"
    assert settings.get("embedded_worker") == "true"


def test_get_and_set():
    # Set a new value
    settings.set("custom_test_key", "custom_val")
    assert settings.get("custom_test_key") == "custom_val"

    # Overwrite value
    settings.set("custom_test_key", "new_val")
    assert settings.get("custom_test_key") == "new_val"

    # Default fallback for unknown keys
    assert settings.get("nonexistent_key", "fallback") == "fallback"


def test_get_bool():
    settings.set("test_bool_true", "true")
    settings.set("test_bool_yes", "yes")
    settings.set("test_bool_on", "on")
    settings.set("test_bool_1", "1")
    settings.set("test_bool_false", "false")
    settings.set("test_bool_no", "no")

    assert settings.get_bool("test_bool_true") is True
    assert settings.get_bool("test_bool_yes") is True
    assert settings.get_bool("test_bool_on") is True
    assert settings.get_bool("test_bool_1") is True
    assert settings.get_bool("test_bool_false") is False
    assert settings.get_bool("test_bool_no") is False

    # Default value fallback
    assert settings.get_bool("nonexistent_bool", True) is True
    assert settings.get_bool("nonexistent_bool", False) is False


def test_get_int():
    settings.set("test_int_val", "42")
    settings.set("test_int_bad", "not_an_int")

    assert settings.get_int("test_int_val") == 42
    assert settings.get_int("test_int_bad", 99) == 99  # Fallback on invalid format
    assert settings.get_int("nonexistent_int", 7) == 7   # Fallback on missing key


def test_get_all():
    all_settings = settings.get_all()
    # Check that it's a list of dictionaries with key/value/description
    assert isinstance(all_settings, list)
    keys = [s["key"] for s in all_settings]
    assert "secret_key" in keys
    assert "smtp_host" in keys
    assert "custom_test_key" in keys


def test_smtp_and_webhook_properties():
    # SMTP is initially unconfigured
    assert settings.smtp_configured is False

    # Set SMTP parameters
    settings.set("smtp_host", "smtp.gmail.com")
    settings.set("smtp_username", "user@gmail.com")
    settings.set("smtp_password", "pass")
    settings.set("smtp_from", "noreply@gmail.com")
    settings.set("smtp_port", "465")

    assert settings.smtp_host == "smtp.gmail.com"
    assert settings.smtp_username == "user@gmail.com"
    assert settings.smtp_password == "pass"
    assert settings.smtp_from == "noreply@gmail.com"
    assert settings.smtp_port == 465
    assert settings.smtp_configured is True

    # Webhook
    assert settings.webhook_configured is False
    settings.set("webhook_url", "https://discord.com/api/webhooks/xyz")
    assert settings.webhook_url == "https://discord.com/api/webhooks/xyz"
    assert settings.webhook_configured is True


def test_worker_and_scheduler_properties():
    settings.set("worker_poll_seconds", "5")
    settings.set("worker_stale_seconds", "120")
    settings.set("audit_retention_days", "30")

    assert settings.worker_poll_seconds == 5
    assert settings.worker_stale_seconds == 120
    assert settings.audit_retention_days == 30
    assert settings.embedded_worker is True
    assert settings.embedded_scheduler is True


def test_sso_properties_and_mapping():
    assert settings.sso_configured is False

    # Configure OIDC/SSO parameters
    settings.set("sso_enabled", "true")
    settings.set("sso_discovery_url", "https://login.microsoftonline.com/.well-known/openid-configuration")
    settings.set("sso_client_id", "my-client-id")
    settings.set("sso_client_secret", "my-client-secret")
    settings.set("sso_redirect_uri", "http://localhost:8000/callback")
    settings.set("sso_scopes", "openid email")
    settings.set("sso_group_claim", "roles")
    settings.set("sso_default_role", "viewer")
    settings.set("sso_auto_create_users", "false")
    settings.set("sso_button_label", "Log In with Entra")

    assert settings.sso_enabled is True
    assert settings.sso_discovery_url == "https://login.microsoftonline.com/.well-known/openid-configuration"
    assert settings.sso_client_id == "my-client-id"
    assert settings.sso_client_secret == "my-client-secret"
    assert settings.sso_redirect_uri == "http://localhost:8000/callback"
    assert settings.sso_scopes == "openid email"
    assert settings.sso_group_claim == "roles"
    assert settings.sso_default_role == "viewer"
    assert settings.sso_auto_create_users is False
    assert settings.sso_button_label == "Log In with Entra"
    assert settings.sso_configured is True

    # Test group role map parsing
    settings.set("sso_group_role_map", '{"group-editor-id": "editor", "group-admin-id": "admin"}')
    role_map = settings.sso_group_role_map()
    assert role_map == {"group-editor-id": "editor", "group-admin-id": "admin"}

    # Test group role map parsing with invalid JSON
    settings.set("sso_group_role_map", "invalid-json")
    assert settings.sso_group_role_map() == {}
