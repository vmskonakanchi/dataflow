import pandas as pd
import os
from typing import Optional
from .base import BaseConnector
from ..config.models import CSVSourceConfig, CSVSinkConfig


@BaseConnector.register("csv")
class CSVConnector(BaseConnector):
    def extract(self, source: CSVSourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        if not os.path.exists(source.file_path):
            raise FileNotFoundError(f"CSV source file not found: {source.file_path}")
        return pd.read_csv(
            source.file_path,
            delimiter=source.delimiter or ',',
            header=0 if source.has_header else None
        )

    def load(self, df: pd.DataFrame, sink: CSVSinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        path = sink.file_path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

        write_header = True
        write_mode = "w"

        if mode == "append" and os.path.exists(path):
            write_header = False  # don't duplicate headers
            write_mode = "a"

        df.to_csv(path, sep=sink.delimiter, index=False, header=write_header, mode=write_mode)
        return len(df)
