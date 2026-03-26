import duckdb
import pandas as pd
from typing import Optional
from .base import BaseConnector
from ..config.models import SourceConfig, SinkConfig, DuckDBSinkConfig

@BaseConnector.register("duckdb")
class DuckDBConnector(BaseConnector):
    def extract(self, source: SourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        raise NotImplementedError("DuckDB is sink only in V1")

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
                # DuckDB INSERT OR REPLACE handles upsert well when primary keys are defined.
                # However, the prompt says "INSERT OR REPLACE INTO {table} SELECT * FROM df".
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")
                conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM df")
            
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return result[0]
        finally:
            conn.close()
