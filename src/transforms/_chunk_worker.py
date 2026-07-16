"""Subprocess entrypoint: run a transform plugin on ONE bounded row-slice.

Spawned by ``run_plugin_chunked`` (see ``chunked.py``). Each invocation reads
only its ``[rstart, rend)`` slice of the input parquet, runs the plugin's
``transform`` on just those rows, writes the result to a slice parquet, and
exits.

Why a fresh process per slice (rather than a loop): parsing JSON / building
large embedding arrays allocates memory *outside* DuckDB's tracked buffers, so
``memory_limit`` cannot bound it and that memory is not reliably reclaimed
between statements in the same process. Exiting the process is the only
guaranteed way to return that memory to the OS, so peak RAM stays bounded to a
single chunk no matter how large the whole dataset is.

Invoked as::

    python -m transforms._chunk_worker \
        --in-parquet IN --rstart N --rend M --out-parquet OUT \
        --function NAME --params-json '{...}'

Prints ``CHUNK_OK`` to stdout on success; on failure it exits non-zero and the
traceback is on stderr (the parent surfaces it as a pipeline error).
"""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="transforms._chunk_worker")
    p.add_argument("--in-parquet", required=True)
    p.add_argument("--rstart", type=int, required=True)
    p.add_argument("--rend", type=int, required=True)
    p.add_argument("--out-parquet", required=True)
    p.add_argument("--function", required=True)
    p.add_argument("--params-json", default="{}")
    a = p.parse_args(argv)

    import duckdb
    import pyarrow.parquet as pq

    from transforms import run_plugin

    params = json.loads(a.params_json)

    # Read only this slice. `file_row_number` is assigned per file (0..n-1);
    # filtering on it lets DuckDB prune row groups so a late chunk does not
    # rescan the whole file. EXCLUDE drops the helper column so the plugin sees
    # exactly the pipeline's schema.
    con = duckdb.connect()
    try:
        slice_tbl = con.execute(
            "SELECT * EXCLUDE (file_row_number) "
            "FROM read_parquet(?, file_row_number := true) "
            "WHERE file_row_number >= ? AND file_row_number < ?",
            [a.in_parquet, a.rstart, a.rend],
        ).fetch_arrow_table()
    finally:
        con.close()

    result = run_plugin(a.function, slice_tbl, params)
    pq.write_table(result, a.out_parquet)
    print("CHUNK_OK", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
