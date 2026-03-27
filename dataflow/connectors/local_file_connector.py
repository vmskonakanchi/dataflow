import os
import pandas as pd
from datetime import datetime
from typing import Optional
from .base import BaseConnector
from ..config.models import LocalFileSourceConfig, LocalFileSinkConfig


@BaseConnector.register("local_file")
class LocalFileConnector(BaseConnector):
    def extract(self, source: LocalFileSourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        if not os.path.exists(source.file_path):
            raise FileNotFoundError(f"Local file not found: {source.file_path}")

        if source.file_format == "parquet":
            return pd.read_parquet(source.file_path)
        elif source.file_format == "csv":
            return pd.read_csv(
                source.file_path,
                delimiter=source.delimiter,
                header=0 if source.has_header else None
            )
        elif source.file_format == "json":
            return pd.read_json(source.file_path, orient="records")
        else:
            raise ValueError(f"Unsupported file format: {source.file_format}")

    def load(self, df: pd.DataFrame, sink: LocalFileSinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        os.makedirs(sink.directory, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        filename = f"{table}_{timestamp}.{sink.file_format}"
        file_path = os.path.join(sink.directory, filename)

        if sink.file_format == "parquet":
            df.to_parquet(file_path, index=False)
        elif sink.file_format == "csv":
            df.to_csv(file_path, index=False)
        elif sink.file_format == "json":
            df.to_json(file_path, orient="records", indent=2)
        else:
            raise ValueError(f"Unsupported file format: {sink.file_format}")

        return len(df)
