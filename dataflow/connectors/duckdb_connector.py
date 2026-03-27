import duckdb
import pandas as pd
from typing import Optional
from .base import BaseConnector
from ..config.models import DuckDBSourceConfig, DuckDBSinkConfig
from ..logger.run_log import get_last_successful_run


@BaseConnector.register("duckdb")
class DuckDBConnector(BaseConnector):
    def extract(self, source: DuckDBSourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        if not query:
            raise ValueError("DuckDB source requires a source_query (e.g. 'SELECT * FROM fact_orders')")

        last_run = get_last_successful_run(pipeline_name)
        last_run_str = last_run.isoformat() if last_run else "1970-01-01 00:00:00"
        query = query.replace("{{last_run}}", last_run_str)

        conn = duckdb.connect(source.file_path, read_only=True)
        try:
            return conn.execute(query).df()
        finally:
            conn.close()

    def load(self, df: pd.DataFrame, sink: DuckDBSinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        conn = duckdb.connect(sink.file_path)
        try:
            if mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            elif mode == "append":
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")
                conn.execute(f"INSERT INTO {table} SELECT * FROM df")
            elif mode == "upsert":
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")
                conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM df")

            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return result[0]
        finally:
            conn.close()
