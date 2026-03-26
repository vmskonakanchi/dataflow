import pandas as pd
from .base import BaseTransform

@BaseTransform.register("rename")
class RenameTransform(BaseTransform):
    def __init__(self, from_col: str, to_col: str):
        self.from_col = from_col
        self.to_col = to_col

    def apply(self, df: pd.DataFrame, resolved_config: 'ResolvedConfig', pipeline_name: str) -> pd.DataFrame:
        if self.from_col not in df.columns:
            raise ValueError(f"Rename transform failed: Column '{self.from_col}' not found in DataFrame")
        return df.rename(columns={self.from_col: self.to_col})
