"""Tests for pipeline-level template resolution."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from config import PipelineConfig, ResolvedConfig
from executor import PipelineError, run_pipeline
from pipeline_templates import TemplateResolutionError, resolve_pipeline_templates


def _pipeline(**overrides):
    values = {
        "name": "templated_pipeline",
        "source_path": "sales_{{today:%Y_%m_%d}}.parquet",
        "sink_path": "output_{{yesterday:%Y-%m-%d}}.parquet",
        "alerts": {"on_failure": "none"},
    }
    values.update(overrides)
    return PipelineConfig(**values)


def test_resolves_today_yesterday_and_now_formats():
    pipeline = _pipeline(
        source_path="https://abc.com/sales_{{today:%Y_%m_%d}}.parquet",
        sink_path="{{now:%H:%M:%S}}",
        transforms=[
            {
                "type": "join",
                "right_path": "input_{{yesterday:%d_%m_%Y}}.parquet",
                "join_type": "inner",
                "on": "left.id = right.id",
            }
        ],
    )

    resolved = resolve_pipeline_templates(
        pipeline, now=datetime(2026, 7, 16, 13, 45, 6, tzinfo=timezone.utc)
    )

    assert resolved.source_path == "https://abc.com/sales_2026_07_16.parquet"
    assert resolved.sink_path == "13:45:06"
    assert resolved.transforms[0].right_path == "input_15_07_2026.parquet"


def test_uses_pipeline_timezone_for_today_yesterday_and_now():
    pipeline = _pipeline(
        timezone="Asia/Kolkata",
        source_path="{{today}}",
        sink_path="{{now:%Y-%m-%d_%H:%M}}",
        transforms=[],
    )

    resolved = resolve_pipeline_templates(
        pipeline, now=datetime(2026, 7, 16, 20, 15, tzinfo=timezone.utc)
    )

    assert resolved.source_path == "2026-07-17"
    assert resolved.sink_path == "2026-07-17_01:45"


@pytest.mark.parametrize(
    "source_path, error",
    [
        ("sales_{{today", "unclosed template"),
        ("sales_{{uuid}}", "unsupported template variable"),
        ("sales_{{today + 1}}", "unsupported template variable"),
        ("sales_{{today:}}", "requires a non-empty format"),
    ],
)
def test_invalid_templates_raise_meaningful_errors(source_path, error):
    with pytest.raises(TemplateResolutionError, match=error):
        resolve_pipeline_templates(_pipeline(source_path=source_path))


def test_plain_pipeline_is_unchanged_and_input_is_not_mutated():
    pipeline = _pipeline(source_path="input.parquet", sink_path="output.parquet")

    resolved = resolve_pipeline_templates(
        pipeline, now=datetime(2026, 7, 16, 13, 45, tzinfo=timezone.utc)
    )

    assert resolved.source_path == "input.parquet"
    assert resolved.sink_path == "output.parquet"
    assert resolved is not pipeline


def test_pipeline_timezone_must_be_an_iana_timezone():
    with pytest.raises(ValidationError, match="valid IANA timezone"):
        _pipeline(timezone="Mars/Olympus")
    with pytest.raises(ValidationError, match="named IANA timezone"):
        _pipeline(timezone="localtime")


def test_run_pipeline_resolves_before_constructing_executor(monkeypatch):
    pipeline = _pipeline(source_path="{{today:%Y-%m-%d}}.parquet")
    captured = {}

    class RecordingExecutor:
        def __init__(self, resolved_pipeline):
            captured["pipeline"] = resolved_pipeline

        def execute(self):
            return "executed"

    monkeypatch.setattr("executor.DuckDBExecutor", RecordingExecutor)
    monkeypatch.setattr(
        "executor.resolve_pipeline_templates",
        lambda value: resolve_pipeline_templates(
            value, now=datetime(2026, 7, 16, 13, 45, tzinfo=timezone.utc)
        ),
    )

    result = run_pipeline(
        "templated_pipeline",
        ResolvedConfig(pipelines={"templated_pipeline": pipeline}, cronjobs={}),
    )

    assert result == "executed"
    assert captured["pipeline"].source_path == "2026-07-16.parquet"


def test_run_pipeline_reports_template_errors_before_execution(monkeypatch):
    pipeline = _pipeline(source_path="{{unknown}}")

    monkeypatch.setattr(
        "executor.DuckDBExecutor",
        lambda _: pytest.fail("executor must not be constructed for invalid templates"),
    )

    with pytest.raises(PipelineError, match="template_resolution"):
        run_pipeline(
            "templated_pipeline",
            ResolvedConfig(pipelines={"templated_pipeline": pipeline}, cronjobs={}),
        )
