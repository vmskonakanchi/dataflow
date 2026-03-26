import pymysql
import pandas as pd
from typing import Optional
from .base import BaseConnector
from ..config.models import SourceConfig, SinkConfig, MySQLSourceConfig
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

    def load(self, df: pd.DataFrame, sink: SinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        raise NotImplementedError("MySQL is source only in V1")
