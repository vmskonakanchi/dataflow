import pymysql
import pandas as pd
from typing import Optional
from .base import BaseConnector
from ..config.models import MySQLSourceConfig, MySQLSinkConfig
from ..logger.run_log import get_last_successful_run


@BaseConnector.register("mysql")
class MySQLConnector(BaseConnector):
    def extract(self, source: MySQLSourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        last_run = get_last_successful_run(pipeline_name)
        last_run_str = last_run.isoformat() if last_run else "1970-01-01 00:00:00"
        query = query.replace("{{last_run}}", last_run_str)

        conn = pymysql.connect(
            host=source.host,
            port=source.port,
            db=source.database,
            user=source.username,
            password=source.password,
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8mb4'
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return pd.DataFrame(rows)
        finally:
            conn.close()

    def load(self, df: pd.DataFrame, sink: MySQLSinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        conn = pymysql.connect(
            host=sink.host,
            port=sink.port,
            db=sink.database,
            user=sink.username,
            password=sink.password,
            charset='utf8mb4'
        )
        try:
            with conn.cursor() as cursor:
                if mode == "replace":
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                    conn.commit()

                # Build CREATE TABLE IF NOT EXISTS from df dtypes
                col_defs = []
                for col, dtype in zip(df.columns, df.dtypes):
                    if "int" in str(dtype):
                        sql_type = "BIGINT"
                    elif "float" in str(dtype):
                        sql_type = "DOUBLE"
                    elif "datetime" in str(dtype):
                        sql_type = "DATETIME"
                    else:
                        sql_type = "TEXT"
                    col_defs.append(f"`{col}` {sql_type}")

                create_sql = f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(col_defs)})"
                cursor.execute(create_sql)
                conn.commit()

                if mode == "upsert" and key:
                    sql = f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))}) ON DUPLICATE KEY UPDATE {', '.join(f'`{c}`=VALUES(`{c}`)' for c in df.columns if c != key)}"
                else:
                    sql = f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"

                cursor.executemany(sql, df.where(pd.notnull(df), None).values.tolist())
                conn.commit()
                return len(df)
        finally:
            conn.close()
