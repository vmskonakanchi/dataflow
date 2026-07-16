import os
import sqlite3

import duckdb
from sqlmodel import Session, select

from config import Pipeline, engine, init_db, load_configs
from executor import run_pipeline

# Ensure directories exist
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/clean", exist_ok=True)
os.makedirs("scratch", exist_ok=True)

raw_path = "data/raw/user_events.parquet"
clean_path = "data/clean/cleaned_events.parquet"

print("1. Generating mock messy Parquet data using DuckDB...")
con = duckdb.connect()

# Generate raw messy data directly via SQL
con.execute("""
CREATE OR REPLACE TABLE raw_events AS
SELECT
  CASE WHEN idx % 500 = 0 THEN 'evt_' || lpad((idx - 5)::TEXT, 6, '0')
       ELSE 'evt_' || lpad(idx::TEXT, 6, '0')
  END AS event_id,

  CASE WHEN idx % 10 = 0 THEN NULL
       ELSE '  User_' || idx::TEXT || '@Email.com  '
  END AS email,

  CASE WHEN idx % 7 = 0 THEN NULL
       ELSE '$' || round(10.0 + random() * 490.0, 2)::TEXT
  END AS revenue,

  CASE WHEN idx % 5 = 0 THEN 'SIGNUP'
       WHEN idx % 5 = 1 THEN 'purchase'
       WHEN idx % 5 = 2 THEN 'Click'
       WHEN idx % 5 = 3 THEN 'Login'
       ELSE 'logout'
  END AS event_type,

  (timestamp '2026-06-10 00:00:00' + interval (idx * 2) seconds)::TEXT AS event_timestamp
FROM range(1, 50001) as r(idx);
""")

con.execute(f"COPY raw_events TO '{raw_path}' (FORMAT 'PARQUET')")

# Check before stats
raw_rows = con.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
dup_ids = con.execute(
    "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM raw_events"
).fetchone()[0]
null_emails = con.execute(
    "SELECT COUNT(*) FROM raw_events WHERE email IS NULL"
).fetchone()[0]

print(f"Generated {raw_rows} raw rows at '{raw_path}'")
print(f"Duplicate Event IDs: {dup_ids}")
print(f"Null Emails: {null_emails}")

# Initialize Database and insert test pipeline
init_db()

print("\n2. Configuring DuckDB-native pipeline in SQLite database...")
pipeline_config = {
    "name": "clean_user_events",
    "description": "SQL-based cleaning of event data using DuckDB",
    "source_path": raw_path,
    "sink_path": clean_path,
    "threads": 4,
    "memory_limit": "2GB",
    "transforms": [
        {
            "type": "select",
            "columns": [
                "event_id",
                "LOWER(TRIM(email)) AS email",
                "CAST(REPLACE(revenue, '$', '') AS DOUBLE) AS revenue",
                "LOWER(event_type) AS event_type",
                "CAST(event_timestamp AS TIMESTAMP) AS event_timestamp",
            ],
        },
        {
            "type": "filter",
            "condition": "email IS NOT NULL AND revenue IS NOT NULL AND revenue > 20.0",
        },
    ],
    "alerts": {"on_failure": "none"},
}

with Session(engine) as session:
    # Remove existing test pipeline if it exists
    existing = session.exec(
        select(Pipeline).where(Pipeline.name == "clean_user_events")
    ).first()
    if existing:
        session.delete(existing)
        session.commit()

    db_p = Pipeline(
        name=pipeline_config["name"],
        description=pipeline_config["description"],
        source_path=pipeline_config["source_path"],
        sink_path=pipeline_config["sink_path"],
        threads=pipeline_config["threads"],
        memory_limit=pipeline_config["memory_limit"],
        transforms=pipeline_config["transforms"],
        alerts=pipeline_config["alerts"],
    )
    session.add(db_p)
    session.commit()

print("Pipeline configured successfully in SQLite.")

# 3. Load configurations and run pipeline
print("\n3. Executing pipeline using loader and runner...")
resolved = load_configs()
result = run_pipeline("clean_user_events", resolved)

print("\n4. Execution Results:")
print(f"Status:          {result.status}")
print(f"Rows Extracted:  {result.rows_extracted}")
print(f"Rows Written:    {result.rows_written}")
print(
    f"Duration:        {(result.finished_at - result.started_at).total_seconds():.3f}s"
)

# Validate output Parquet
print("\n5. Validating output Parquet dataset using DuckDB...")
con.execute(
    f"CREATE OR REPLACE VIEW clean_events AS SELECT * FROM read_parquet('{clean_path}')"
)
clean_rows = con.execute("SELECT COUNT(*) FROM clean_events").fetchone()[0]

print(f"Cleaned dataset rows: {clean_rows}")
print("Cleaned Data Sample:")
sample = con.execute("SELECT * FROM clean_events LIMIT 5").fetchall()
for row in sample:
    print(row)

# Check assertions
assert os.path.exists(clean_path), "Error: Output file not created"
assert clean_rows < raw_rows, "Error: Cleaning filter did not discard rows"

null_emails_clean = con.execute(
    "SELECT COUNT(*) FROM clean_events WHERE email IS NULL"
).fetchone()[0]
assert null_emails_clean == 0, "Error: Null emails still exist in cleaned dataset"

min_revenue = con.execute("SELECT MIN(revenue) FROM clean_events").fetchone()[0]
assert min_revenue > 20.0, "Error: Revenue filter failed"

whitespace_emails = con.execute(
    "SELECT COUNT(*) FROM clean_events WHERE email LIKE ' %' OR email LIKE '% '"
).fetchone()[0]
assert whitespace_emails == 0, "Error: Whitespace not trimmed from emails"

print(
    "\n🎉 ALL TESTS PASSED SUCCESSFULLY! The DuckDB-native pipeline executed correctly without Pandas or Numpy."
)
con.close()
