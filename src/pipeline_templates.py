"""Resolve the small, runtime-only template vocabulary used by pipelines."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from config import PipelineConfig


class TemplateResolutionError(ValueError):
    """A pipeline template is malformed or uses an unsupported variable."""


# Identity, authorization, and operational configuration must never become
# runtime-dependent. All other pipeline payload fields resolve recursively.
_EXCLUDED_TOP_LEVEL_FIELDS = {
    "name",
    "description",
    "timezone",
    "run_as",
    "alerts",
    "checkpointing",
    "threads",
    "memory_limit",
    "target_file_size",
    "row_group_size",
}
_DEFAULT_FORMATS = {
    "today": "%Y-%m-%d",
    "yesterday": "%Y-%m-%d",
    "now": "%Y-%m-%dT%H:%M:%S",
}


def resolve_pipeline_templates(
    pipeline: PipelineConfig, *, now: datetime | None = None
) -> PipelineConfig:
    """Return a per-run pipeline copy with supported templates resolved."""
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    local_now = instant.astimezone(ZoneInfo(pipeline.timezone))

    payload = pipeline.model_dump()
    for field, value in payload.items():
        if field not in _EXCLUDED_TOP_LEVEL_FIELDS:
            payload[field] = _resolve_value(value, local_now, field)
    return PipelineConfig.model_validate(payload)


def _resolve_value(value: Any, now: datetime, field_path: str) -> Any:
    if isinstance(value, str):
        return _resolve_string(value, now, field_path)
    if isinstance(value, list):
        return [
            _resolve_value(item, now, f"{field_path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        return {
            key: _resolve_value(item, now, f"{field_path}.{key}")
            for key, item in value.items()
        }
    return deepcopy(value)


def _resolve_string(value: str, now: datetime, field_path: str) -> str:
    parts: list[str] = []
    cursor = 0
    while cursor < len(value):
        opening = value.find("{{", cursor)
        closing = value.find("}}", cursor)
        if closing != -1 and (opening == -1 or closing < opening):
            raise TemplateResolutionError(
                f"{field_path}: unexpected closing delimiter in {value!r}"
            )
        if opening == -1:
            parts.append(value[cursor:])
            break

        parts.append(value[cursor:opening])
        end = value.find("}}", opening + 2)
        if end == -1:
            raise TemplateResolutionError(
                f"{field_path}: unclosed template in {value!r}"
            )
        token = value[opening + 2:end]
        parts.append(_resolve_token(token, now, field_path))
        cursor = end + 2
    return "".join(parts)


def _resolve_token(token: str, now: datetime, field_path: str) -> str:
    if not token or "{{" in token or "}}" in token:
        raise TemplateResolutionError(f"{field_path}: invalid template {{{{{token}}}}}")

    variable, separator, format_string = token.partition(":")
    if variable not in _DEFAULT_FORMATS:
        raise TemplateResolutionError(
            f"{field_path}: unsupported template variable {variable!r}"
        )
    if separator and not format_string:
        raise TemplateResolutionError(
            f"{field_path}: template {variable!r} requires a non-empty format"
        )

    value = now - timedelta(days=1) if variable == "yesterday" else now
    try:
        return value.strftime(format_string or _DEFAULT_FORMATS[variable])
    except (TypeError, ValueError) as exc:
        raise TemplateResolutionError(
            f"{field_path}: invalid format for template {variable!r}: {format_string!r}"
        ) from exc

