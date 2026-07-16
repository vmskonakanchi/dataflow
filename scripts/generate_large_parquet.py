"""Generate synthetic user-event data as a Parquet file via DuckDB.

Defaults to the original large benchmark dataset (250M rows). Pass --rows for a
small dataset (e.g. a fast demo/recording) and --output to change the path.

Examples:
    # small dataset for a demo GIF (finishes in well under a second)
    python scripts/generate_large_parquet.py --rows 5000 --output data/raw/events.parquet

    # original large benchmark (~5-8 GB compressed)
    python scripts/generate_large_parquet.py
"""

import os
import time
import argparse

import duckdb

DEFAULT_ROWS = 250_000_000
DEFAULT_OUTPUT = "data/large/events_large.parquet"


def human_count(n: int) -> str:
    return f"{n:,}"


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic user-event Parquet data.")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS,
                        help=f"Number of rows to generate (default: {DEFAULT_ROWS:,})")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help=f"Output Parquet path (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    rows = args.rows
    output_path = args.output
    if rows < 1:
        parser.error("--rows must be >= 1")

    out_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(out_dir, exist_ok=True)

    print("Initializing DuckDB engine...")
    con = duckdb.connect()

    print(f"Generating {human_count(rows)} rows of user events -> {output_path}")
    start_time = time.time()

    # Keep the email cardinality sensible for small datasets: cap the modulo at
    # the row count so tiny runs still produce varied-but-not-absurd values.
    email_mod = min(1_000_000, max(1, rows))

    con.execute(f"""
    COPY (
      SELECT
        idx AS event_id,
        'user_' || (idx % {email_mod})::VARCHAR || '@example.com' AS email,
        round(10.0 + random() * 990.0, 2) AS revenue,
        CASE
          WHEN idx % 5 = 0 THEN 'purchase'
          WHEN idx % 5 = 1 THEN 'click'
          WHEN idx % 5 = 2 THEN 'view'
          WHEN idx % 5 = 3 THEN 'login'
          ELSE 'logout'
        END AS event_type,
        (TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (idx * 5) SECONDS) AS event_timestamp,
        'metadata_payload_hash_' || md5(idx::VARCHAR) AS payload_hash
      FROM range(1, {rows + 1}) as r(idx)
    ) TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
    """)

    duration = time.time() - start_time

    if os.path.exists(output_path):
        file_size_bytes = os.path.getsize(output_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        file_size_gb = file_size_bytes / (1024 * 1024 * 1024)
        rate = rows / duration if duration > 0 else 0
        print("\nGeneration complete!")
        print(f"File Path:         {output_path}")
        print(f"Rows Generated:    {human_count(rows)}")
        print(f"File Size on Disk: {file_size_mb:.2f} MB ({file_size_gb:.3f} GB)")
        print(f"Time Taken:        {duration:.2f} seconds")
        print(f"Throughput:        {rate:,.0f} rows/sec")
    else:
        print("Error: Output file was not created.")


if __name__ == "__main__":
    main()
