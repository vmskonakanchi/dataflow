"""Tests for executor.py: pipeline execution, transforms, quality checks, checkpointing, and alerting."""

import os
import sys
import shutil
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import config
from config import init_db, PipelineConfig, AlertConfig, TransformConfig, CheckConfig
from executor import DuckDBExecutor, PipelineError, RunResult, run_pipeline


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


@pytest.fixture
def clean_checkpoints():
    yield
    if os.path.exists(".checkpoints"):
        shutil.rmtree(".checkpoints", ignore_errors=True)


# --- Helper to create sample Parquet data ---
def _create_parquet(path, data_dict):
    table = pa.table(data_dict)
    pq.write_table(table, path)


# --- Source Validation Tests ---

def test_validate_source_local_exists(tmp_path):
    src = str(tmp_path / "valid.parquet")
    _create_parquet(src, {"id": [1]})
    
    cfg = PipelineConfig(
        name="test_val_source",
        source_path=src,
        sink_path=str(tmp_path / "out.parquet"),
        alerts={"on_failure": "none"}
    )
    # Should not raise error
    DuckDBExecutor(cfg).validate_source()


def test_validate_source_local_missing(tmp_path):
    src = str(tmp_path / "nonexistent.parquet")
    cfg = PipelineConfig(
        name="test_val_missing",
        source_path=src,
        sink_path=str(tmp_path / "out.parquet"),
        alerts={"on_failure": "none"}
    )
    with pytest.raises(PipelineError) as excinfo:
        DuckDBExecutor(cfg).validate_source()
    assert "Source path" in str(excinfo.value)
    assert "matched no files" in str(excinfo.value)


def test_validate_source_remote_skips(tmp_path):
    cfg = PipelineConfig(
        name="test_val_remote",
        source_path="s3://my-bucket/nonexistent.parquet",
        sink_path=str(tmp_path / "out.parquet"),
        alerts={"on_failure": "none"}
    )
    # Remote paths skip local validation, so this should not raise error
    DuckDBExecutor(cfg).validate_source()


# --- SQL Transformations Tests ---

def test_execute_transforms(tmp_path):
    src = str(tmp_path / "input.parquet")
    sink = str(tmp_path / "output.parquet")
    
    # 3 rows, id, category, value
    _create_parquet(src, {
        "id": [1, 2, 3],
        "category": ["A", "B", "A"],
        "value": [10, 20, 30]
    })
    
    # Let's write another table for joining
    join_src = str(tmp_path / "join_input.parquet")
    _create_parquet(join_src, {
        "category": ["A", "B"],
        "cat_name": ["Alpha", "Beta"]
    })

    # Configure a pipeline with select, filter, aggregate, and join
    cfg = PipelineConfig(
        name="test_transforms",
        source_path=src,
        sink_path=sink,
        transforms=[
            # 1. Filter: value > 10 (removes row id 1)
            {"type": "filter", "condition": "value > 10"},
            # 2. Join: join with join_src
            {"type": "join", "right_path": join_src, "join_type": "inner", "on": "left.category = right.category"},
            # 3. Select: keep id, category, value, cat_name
            {"type": "select", "columns": ["id", "category", "value", "cat_name"]},
            # 4. Aggregate: Group by category and cat_name, sum value
            {"type": "aggregate", "group_by": ["category", "cat_name"], "aggregates": ["sum(value) AS total_val"]}
        ],
        alerts={"on_failure": "none"}
    )
    
    result = DuckDBExecutor(cfg).execute()
    assert result.status == "success"
    assert result.rows_extracted == 3
    # Rows written should be 2 because we have category A (val 30) and B (val 20)
    assert result.rows_written == 2

    # Verify sink content
    import duckdb
    con = duckdb.connect()
    rows = con.execute(f"SELECT category, cat_name, total_val FROM read_parquet('{sink}') ORDER BY category").fetchall()
    con.close()
    
    assert rows == [("A", "Alpha", 30), ("B", "Beta", 20)]


# --- Data Quality Checks Tests ---

def test_data_quality_checks_success(tmp_path):
    src = str(tmp_path / "dq_success_in.parquet")
    sink = str(tmp_path / "dq_success_out.parquet")
    _create_parquet(src, {
        "id": [1, 2],
        "val": ["foo", "bar"]
    })

    cfg = PipelineConfig(
        name="dq_success",
        source_path=src,
        sink_path=sink,
        checks=[
            {"type": "not_null", "columns": ["id", "val"]},
            {"type": "unique", "columns": ["id"]},
            {"type": "row_count_min", "value": 2},
            {"type": "accepted_values", "column": "val", "values": ["foo", "bar"]},
            {"type": "custom_sql", "query": "SELECT COUNT(*) FROM output WHERE id > 0", "must_be": 2}
        ],
        alerts={"on_failure": "none"}
    )

    result = DuckDBExecutor(cfg).execute()
    assert result.status == "success"


@pytest.mark.parametrize("check_definition,expected_err", [
    ({"type": "not_null", "columns": ["val"]}, "val' has 1 null rows"),
    ({"type": "unique", "columns": ["id"]}, "id' has 1 duplicates"),
    ({"type": "row_count_min", "value": 5}, "got 3, expected >= 5"),
    ({"type": "accepted_values", "column": "val", "values": ["foo", "bar"]}, "val' has 1 rows with invalid values"),
    ({"type": "custom_sql", "query": "SELECT SUM(id) FROM output", "must_be": 10}, "got 5, expected 10")
])
def test_data_quality_checks_failures(tmp_path, check_definition, expected_err):
    src = str(tmp_path / f"dq_fail_in_{check_definition['type']}.parquet")
    sink = str(tmp_path / f"dq_fail_out_{check_definition['type']}.parquet")
    
    # Create data that will fail the parameterized checks
    _create_parquet(src, {
        "id": [1, 2, 2],           # Duplicate id
        "val": ["foo", None, "baz"] # Null val, baz is invalid value, sum(id) = 5 (wait, sum(id) = 1+2+2 = 5)
    })

    cfg = PipelineConfig(
        name=f"dq_fail_{check_definition['type']}",
        source_path=src,
        sink_path=sink,
        checks=[check_definition],
        alerts={"on_failure": "none"}
    )

    with pytest.raises(PipelineError) as excinfo:
        DuckDBExecutor(cfg).execute()
    
    assert "quality_check" in str(excinfo.value)
    assert expected_err in str(excinfo.value)


# --- Delta Lake Sink Test ---

def test_delta_lake_sink(tmp_path):
    src = str(tmp_path / "delta_in.parquet")
    sink_dir = str(tmp_path / "delta_out_lake")
    _create_parquet(src, {
        "id": [1, 2, 3],
        "category": ["X", "Y", "X"]
    })

    cfg = PipelineConfig(
        name="delta_sink",
        source_path=src,
        sink_path=sink_dir,
        sink_format="delta",
        partition_by="category",
        alerts={"on_failure": "none"}
    )

    result = DuckDBExecutor(cfg).execute()
    assert result.status == "success"
    assert result.rows_written == 3

    # Read from Delta Table using deltalake library
    from deltalake import DeltaTable
    dt = DeltaTable(sink_dir)
    arrow_tbl = dt.to_pyarrow_table()
    assert arrow_tbl.num_rows == 3
    # Check partition columns directories exist
    assert os.path.exists(os.path.join(sink_dir, "category=X"))
    assert os.path.exists(os.path.join(sink_dir, "category=Y"))


# --- Checkpointing / Resume Tests ---

def test_checkpointing_lifecycle(tmp_path, clean_checkpoints):
    src = str(tmp_path / "cp_in.parquet")
    sink = str(tmp_path / "cp_out.parquet")
    _create_parquet(src, {"n": [10, 20]})

    cfg = PipelineConfig(
        name="cp_pipeline",
        source_path=src,
        sink_path=sink,
        checkpointing=True,
        transforms=[
            {"type": "filter", "condition": "n > 10"},
            {"type": "select", "columns": ["n"]}
        ],
        alerts={"on_failure": "none"}
    )

    # First run succeeds
    executor = DuckDBExecutor(cfg)
    result = executor.execute()
    assert result.status == "success"
    # Success deletes checkpoints automatically
    assert not os.path.exists(executor._checkpoint_dir())


def test_checkpointing_resume_on_failure(tmp_path, clean_checkpoints, monkeypatch):
    src = str(tmp_path / "res_in.parquet")
    sink = str(tmp_path / "res_out.parquet")
    _create_parquet(src, {"n": [10, 20]})

    # Pipeline with three steps, second step will fail the first time
    cfg = PipelineConfig(
        name="res_pipeline",
        source_path=src,
        sink_path=sink,
        checkpointing=True,
        transforms=[
            {"type": "filter", "condition": "n >= 10"},
            {"type": "filter", "condition": "n > 15"},  # We'll fail this step hypothetically
            {"type": "select", "columns": ["n"]}
        ],
        alerts={"on_failure": "none"}
    )

    # We will simulate a failure at step 1 (second transform) by monkeypatching the sql command compiling
    original_execute = DuckDBExecutor.execute
    
    fail_once = True
    
    def mock_execute(self):
        nonlocal fail_once
        if fail_once:
            # Manually materialize step 0 (first transform) checkpoint so it behaves as if step 0 succeeded,
            # then raise an error during execution.
            os.makedirs(self._checkpoint_dir(), exist_ok=True)
            _create_parquet(self._checkpoint_path(0), {"n": [10, 20]})
            fail_once = False
            raise PipelineError(self.pipeline.name, "step_1", "Simulated step 1 failure")
        return original_execute(self)

    monkeypatch.setattr(DuckDBExecutor, "execute", mock_execute)

    # First run should fail
    with pytest.raises(PipelineError) as excinfo:
        run_pipeline("res_pipeline", config.ResolvedConfig(pipelines={"res_pipeline": cfg}, cronjobs={}))
    assert "Simulated step 1 failure" in str(excinfo.value)

    # Checkpoint for step 0 should exist
    executor = DuckDBExecutor(cfg)
    assert executor._find_last_checkpoint() == 0
    assert os.path.exists(executor._checkpoint_path(0))

    # Remove the mock so execution succeeds
    monkeypatch.undo()

    # Second run should resume from checkpoint 0 and complete
    result = DuckDBExecutor(cfg).execute()
    assert result.status == "success"
    
    # Checkpoints cleaned up
    assert not os.path.exists(executor._checkpoint_dir())
    
    # Read output to make sure it processed step 1 and 2
    import duckdb
    con = duckdb.connect()
    rows = con.execute(f"SELECT * FROM read_parquet('{sink}')").fetchall()
    con.close()
    assert rows == [(20,)]


# --- Graceful Shutdown Test ---

def test_graceful_shutdown(tmp_path, clean_checkpoints, monkeypatch):
    src = str(tmp_path / "shut_in.parquet")
    sink = str(tmp_path / "shut_out.parquet")
    _create_parquet(src, {"n": [1]})

    cfg = PipelineConfig(
        name="shut_pipeline",
        source_path=src,
        sink_path=sink,
        checkpointing=True,
        transforms=[
            {"type": "filter", "condition": "n > 0"},
            {"type": "select", "columns": ["n"]}
        ],
        alerts={"on_failure": "none"}
    )

    # Simulate shutdown request
    import executor
    monkeypatch.setattr(executor, "_shutdown_requested", True)

    with pytest.raises(PipelineError) as excinfo:
        DuckDBExecutor(cfg).execute()
    assert "shutdown" in str(excinfo.value)
    assert "Shutdown requested" in str(excinfo.value)


# --- Alerts Verification Tests ---

def test_alerts_on_failure_and_row_count(tmp_path, monkeypatch):
    src = str(tmp_path / "alert_in.parquet")
    sink = str(tmp_path / "alert_out.parquet")
    _create_parquet(src, {"n": [1, 2]})

    # Mock send_email and send_webhook inside alerts module
    email_calls = []
    webhook_calls = []

    import alerts
    monkeypatch.setattr(alerts, "send_email", lambda sub, body, rec: email_calls.append((sub, body, rec)))
    monkeypatch.setattr(alerts, "send_webhook", lambda msg: webhook_calls.append(msg))
    
    # Enable SMTP / Webhook settings
    from settings import settings as app_settings
    app_settings.set("smtp_host", "smtp.test.com")
    app_settings.set("smtp_username", "testuser")
    app_settings.set("smtp_password", "testpass")
    app_settings.set("smtp_from", "alerts@test.com")
    app_settings.set("webhook_url", "https://discord.com/webhook")

    # Case 1: Low row count alert
    cfg1 = PipelineConfig(
        name="low_row_pipeline",
        source_path=src,
        sink_path=sink,
        alerts={
            "on_failure": "none",
            "on_row_count_below": 10,  # Fills 2 rows, threshold is 10
            "email": "receiver@test.com"
        }
    )

    result = DuckDBExecutor(cfg1).execute()
    assert result.status == "success"
    
    # Verify row count email was sent
    assert len(email_calls) == 1
    assert "Low row count warning" in email_calls[0][0]
    assert "receiver@test.com" in email_calls[0][2]
    # Verify row count webhook was sent
    assert len(webhook_calls) == 1
    assert "low row count" in webhook_calls[0]

    # Case 2: Failure alert
    email_calls.clear()
    webhook_calls.clear()

    cfg2 = PipelineConfig(
        name="fail_alert_pipeline",
        source_path=src,
        sink_path=sink,
        checks=[
            {"type": "row_count_min", "value": 10}  # Fails quality check
        ],
        alerts={
            "on_failure": "email",
            "email": "fail_receiver@test.com"
        }
    )

    with pytest.raises(PipelineError):
        DuckDBExecutor(cfg2).execute()

    # Verify failure email was sent
    assert len(email_calls) == 1
    assert "Pipeline failed" in email_calls[0][0]
    assert "fail_receiver@test.com" in email_calls[0][2]
    # Verify failure webhook was sent
    assert len(webhook_calls) == 1
    assert "FAILED" in webhook_calls[0]
