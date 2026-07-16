"""Memory-safe chunked execution of a transform plugin.

``run_plugin_chunked`` splits the plugin's input parquet into bounded row-slices
and runs each slice in a fresh subprocess (``transforms._chunk_worker``). Peak
memory is bounded to a single chunk regardless of the total dataset size, which
is the only reliable way to run memory-heavy plugins (JSON parsing, embeddings,
UMAP, ...) on large partitions — see ``_chunk_worker`` for why a separate
process, not a loop, is required.

Chunks run sequentially (one resident at a time) to keep the memory guarantee.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Tuple

from transforms import PluginError


def _worker_env() -> dict:
    """Environment for a worker subprocess: put ``src/`` on PYTHONPATH so the
    ``transforms`` package imports as a top-level module (mirrors how the server
    spawns the job worker)."""
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../src
    env = dict(os.environ)
    env["PYTHONPATH"] = src_dir + os.pathsep + env.get("PYTHONPATH", "")
    return env


def plan_chunks(num_rows: int, chunk_rows: int) -> List[Tuple[int, int]]:
    """Return [(rstart, rend), ...] half-open row ranges covering [0, num_rows)."""
    if chunk_rows <= 0:
        raise ValueError("chunk_rows must be > 0")
    return [(s, min(s + chunk_rows, num_rows)) for s in range(0, num_rows, chunk_rows)]


def _num_rows(in_parquet: str) -> int:
    import duckdb

    con = duckdb.connect()
    try:
        # Row count from parquet footer metadata — no data scan.
        return con.execute(
            "SELECT sum(num_rows)::BIGINT FROM parquet_file_metadata(?)", [in_parquet]
        ).fetchone()[0] or 0
    finally:
        con.close()


def _spawn(cmd: List[str], env: dict) -> Tuple[bool, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    ok = proc.returncode == 0 and "CHUNK_OK" in (proc.stdout or "")
    return ok, (proc.stderr or "").strip()[-1000:]


def run_plugin_chunked(
    function: str,
    in_parquet: str,
    out_dir: str,
    params: Dict[str, Any],
    chunk_rows: int,
    retries: int = 1,
) -> int:
    """Run ``function`` over ``in_parquet`` in row-slices, writing one parquet
    file per chunk into ``out_dir``. Returns the number of chunks written.

    Raises ``PluginError`` if any chunk fails after ``retries`` re-attempts; the
    error carries the failing chunk's row range and the worker's stderr tail.
    """
    os.makedirs(out_dir, exist_ok=True)
    num_rows = _num_rows(in_parquet)
    chunks = plan_chunks(num_rows, chunk_rows)

    env = _worker_env()
    params_json = json.dumps(params or {})

    for k, (rstart, rend) in enumerate(chunks):
        out_parquet = os.path.join(out_dir, f"chunk_{k:05d}.parquet")
        cmd = [
            sys.executable, "-m", "transforms._chunk_worker",
            "--in-parquet", in_parquet,
            "--rstart", str(rstart), "--rend", str(rend),
            "--out-parquet", out_parquet,
            "--function", function,
            "--params-json", params_json,
        ]
        ok, detail = _spawn(cmd, env)
        attempt = 0
        while not ok and attempt < retries:
            attempt += 1
            ok, detail = _spawn(cmd, env)
        if not ok:
            raise PluginError(
                f"Plugin '{function}' failed on chunk {k} (rows {rstart}-{rend}) "
                f"after {retries} retry(ies): {detail}"
            )
    return len(chunks)
