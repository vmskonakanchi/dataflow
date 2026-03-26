import psycopg2
import pandas as pd
from typing import Optional, List
from .base import BaseConnector
from ..config.models import SourceConfig, SinkConfig, PostgresSourceConfig, PostgresSinkConfig
from ..logger.run_log import get_last_successful_run

@BaseConnector.register("postgres")
class PostgresConnector(BaseConnector):
    def extract(self, source: PostgresSourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        last_run = get_last_successful_run(pipeline_name)
        last_run_str = last_run.isoformat() if last_run else "1970-01-01 00:00:00"
        
        # Simple string replacement for placeholder as it is intended for a timestamp constant in SQL
        # However, for safety and following "parameterised queries" rule, we should ideally use params.
        # But the prompt says "Replace {{last_run}} in source_query...".
        # This usually means literal replacement.
        # Actually, let's use parameterised query if possible, but the prompt says REPLACE.
        # I'll replace it with the string.
        query = query.replace("{{last_run}}", last_run_str)
        
        conn = psycopg2.connect(
            host=source.host,
            port=source.port,
            dbname=source.database,
            user=source.username,
            password=source.password
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                return df
        finally:
            conn.close()

    def load(self, df: pd.DataFrame, sink: PostgresSinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        schema = sink.schema_name or "public"
        conn = psycopg2.connect(
            host=sink.host,
            port=sink.port,
            dbname=sink.database,
            user=sink.username,
            password=sink.password
        )
        conn.autocommit = False
        try:
            with conn.cursor() as cursor:
                if mode == "replace":
                    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
                    # Infer column types
                    col_defs = []
                    for col, dtype in df.dtypes.items():
                        if "int64" in str(dtype):
                            pg_type = "BIGINT"
                        elif "float64" in str(dtype):
                            pg_type = "DOUBLE PRECISION"
                        elif "bool" in str(dtype):
                            pg_type = "BOOLEAN"
                        else:
                            pg_type = "TEXT"
                        col_defs.append(f"{col} {pg_type}")
                    
                    # Add primary key if it's replace but maybe upsert will use it later?
                    # Actually, upsert needs a constraint. 
                    # If it's replace, we create it new.
                    cursor.execute(f"CREATE TABLE {schema}.{table} ({', '.join(col_defs)})")
                    if key:
                         cursor.execute(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({key})")

                cols = list(df.columns)
                placeholders = ", ".join(["%s"] * len(cols))
                insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(cols)}) VALUES ({placeholders})"

                if mode == "upsert" and key:
                    update_cols = [c for c in cols if c != key]
                    update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                    insert_sql += f" ON CONFLICT ({key}) DO UPDATE SET {update_stmt}"

                # Batch insert
                data = [tuple(row) for row in df.values]
                cursor.executemany(insert_sql, data)
                row_count = cursor.rowcount if cursor.rowcount != -1 else len(df)
                conn.commit()
                return row_count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
