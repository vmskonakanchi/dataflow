import pandas as pd
import os
from typing import Optional
from .base import BaseConnector
from ..config.models import SourceConfig, SinkConfig, CSVSourceConfig

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

    def load(self, df: pd.DataFrame, sink: SinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        raise NotImplementedError("CSV is source only in V1")
