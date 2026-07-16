"""Tests for transforms/ module: plugin loading, execution, chunk planning, and chunk worker subprocesses."""

import os
import sys
import tempfile
import shutil
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import config
from config import init_db
import transforms
from transforms import available_plugins, load_plugin, run_plugin, PluginError
from transforms.chunked import plan_chunks, run_plugin_chunked
import transforms._chunk_worker as chunk_worker


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


@pytest.fixture(scope="module", autouse=True)
def dummy_plugin():
    """Create a temporary dummy plugin module in src/transforms/ during the tests."""
    transforms_dir = os.path.dirname(transforms.__file__)
    dummy_path = os.path.join(transforms_dir, "dummy_test_plugin.py")
    
    code = """import pyarrow as pa
import pyarrow.compute as pc

def transform(table: pa.Table, params: dict) -> pa.Table:
    add_val = params.get("add", 0)
    new_col = pc.add(table["n"], add_val)
    return table.append_column("n_plus", new_col)
"""
    with open(dummy_path, "w") as f:
        f.write(code)
    
    yield "dummy_test_plugin"
    
    if os.path.exists(dummy_path):
        os.remove(dummy_path)
    # Clear import cache for this module
    sys.modules.pop("transforms.dummy_test_plugin", None)


# --- Loader Tests ---

def test_available_plugins(dummy_plugin):
    plugins = available_plugins()
    assert dummy_plugin in plugins
    # Reserved files should not show up
    assert "chunked" not in plugins
    assert "__init__" not in plugins
    assert "_chunk_worker" not in plugins


def test_load_plugin_success(dummy_plugin):
    fn = load_plugin(dummy_plugin)
    assert callable(fn)


def test_load_plugin_invalid_names():
    with pytest.raises(PluginError):
        load_plugin("nonexistent_plugin")
    with pytest.raises(PluginError):
        load_plugin("chunked")  # reserved
    with pytest.raises(PluginError):
        load_plugin("Invalid-Name")


def test_run_plugin_success(dummy_plugin):
    tbl = pa.table({"n": [1, 2, 3]})
    res = run_plugin(dummy_plugin, tbl, {"add": 10})
    assert isinstance(res, pa.Table)
    assert res.column("n_plus").to_pylist() == [11, 12, 13]


def test_run_plugin_returns_non_table(dummy_plugin, monkeypatch):
    # Mock the plugin to return a dict instead of a pyarrow.Table
    import transforms.dummy_test_plugin as dummy_mod
    monkeypatch.setattr(dummy_mod, "transform", lambda t, p: {"not": "a_table"})
    
    tbl = pa.table({"n": [1]})
    with pytest.raises(PluginError) as excinfo:
        run_plugin(dummy_plugin, tbl, {})
    assert "must return a pyarrow.Table" in str(excinfo.value)


# --- Chunk Planning Tests ---

def test_plan_chunks():
    assert plan_chunks(10, 3) == [(0, 3), (3, 6), (6, 9), (9, 10)]
    assert plan_chunks(5, 5) == [(0, 5)]
    assert plan_chunks(0, 3) == []
    
    with pytest.raises(ValueError):
        plan_chunks(10, 0)
    with pytest.raises(ValueError):
        plan_chunks(10, -5)


# --- Chunked Execution & Subprocess Tests ---

def test_run_plugin_chunked_e2e(dummy_plugin, tmp_path):
    in_parquet = str(tmp_path / "in.parquet")
    out_dir = str(tmp_path / "out_chunks")
    
    # Create input table with 8 rows
    tbl = pa.table({"n": list(range(8))})
    pq.write_table(tbl, in_parquet)
    
    # Run chunked execution with chunk_rows=3 -> should create 3 chunks (0-3, 3-6, 6-8)
    num_chunks = run_plugin_chunked(
        function=dummy_plugin,
        in_parquet=in_parquet,
        out_dir=out_dir,
        params={"add": 100},
        chunk_rows=3
    )
    assert num_chunks == 3
    
    # Verify outputs
    chunk_files = sorted(os.listdir(out_dir))
    assert chunk_files == ["chunk_00000.parquet", "chunk_00001.parquet", "chunk_00002.parquet"]
    
    # Read them back
    import duckdb
    con = duckdb.connect()
    rows = con.execute(f"SELECT n_plus FROM read_parquet('{os.path.join(out_dir, '*.parquet')}') ORDER BY n_plus").fetchall()
    con.close()
    
    assert [r[0] for r in rows] == [100, 101, 102, 103, 104, 105, 106, 107]


def test_chunk_worker_main_cli(dummy_plugin, tmp_path):
    in_p = str(tmp_path / "cli_in.parquet")
    out_p = str(tmp_path / "cli_out.parquet")
    
    tbl = pa.table({"n": [10, 20, 30]})
    pq.write_table(tbl, in_p)
    
    # Call worker's main function directly
    args = [
        "--in-parquet", in_p,
        "--rstart", "1",
        "--rend", "3",
        "--out-parquet", out_p,
        "--function", dummy_plugin,
        "--params-json", '{"add": 5}'
    ]
    
    rc = chunk_worker.main(args)
    assert rc == 0
    
    # Read output (should contain rows index 1 and 2: i.e., 20 and 30, plus 5 = 25 and 35)
    out_tbl = pq.read_table(out_p)
    assert out_tbl.column("n_plus").to_pylist() == [25, 35]
