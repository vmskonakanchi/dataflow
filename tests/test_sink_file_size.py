"""Tests for sink file sizing (target_file_size / row_group_size).

Validates the config and that a parquet sink with target_file_size writes
multiple files while preserving all rows; default writes a single file.
Isolated temp DB; runs the executor locally (no S3).
"""

import glob
import os
import sys
import tempfile

import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import config  # noqa: E402
from config import PipelineConfig  # noqa: E402
from executor import DuckDBExecutor  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _tables():
    config.init_db()  # create tables (incl. pipelinerun) in the temp DB
    yield


def _make_input(path, rows=10000):
    # a padded text column so the data is large enough to split at ~KB sizes
    pad = ["x" * 100] * rows
    pq.write_table(pa.table({"n": list(range(rows)), "pad": pad}), path)


# --- config validation ---

def test_config_accepts_target_file_size():
    c = PipelineConfig(name="p", source_path="a", sink_path="b",
                       target_file_size="200MB", alerts={"on_failure": "none"})
    assert c.target_file_size == "200MB"


def test_config_rejects_bad_target_file_size():
    with pytest.raises(Exception):
        PipelineConfig(name="p", source_path="a", sink_path="b",
                       target_file_size="200 gigabytes", alerts={"on_failure": "none"})


# --- executor behaviour ---

def test_sized_sink_writes_multiple_files(tmp_path):
    src = str(tmp_path / "in.parquet")
    out_dir = str(tmp_path / "out")
    _make_input(src, rows=10000)
    cfg = PipelineConfig(
        name="sized", source_path=src, sink_path=out_dir,
        target_file_size="30KB", row_group_size=1000,
        alerts={"on_failure": "none"},
    )
    result = DuckDBExecutor(cfg).execute()
    assert result.status == "success", result.error_message
    files = glob.glob(os.path.join(out_dir, "*.parquet"))
    assert len(files) > 1, f"expected multiple files, got {len(files)}"
    total = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{os.path.join(out_dir, '*.parquet')}')"
    ).fetchone()[0]
    assert total == 10000


def test_default_sink_is_single_file(tmp_path):
    src = str(tmp_path / "in2.parquet")
    out = str(tmp_path / "out2.parquet")
    _make_input(src, rows=2000)
    cfg = PipelineConfig(name="single", source_path=src, sink_path=out,
                         alerts={"on_failure": "none"})
    result = DuckDBExecutor(cfg).execute()
    assert result.status == "success", result.error_message
    assert os.path.isfile(out)   # single file, not a directory
    total = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    assert total == 2000
