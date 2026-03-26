import pandas as pd
from .base import BaseTransform
from typing import List, Dict

@BaseTransform.register("aggregate")
class AggregateTransform(BaseTransform):
    def __init__(self, group_by: List[str], agg: Dict[str, str]):
        self.group_by = group_by
        self.agg = agg

    def apply(self, df: pd.DataFrame, resolved_config: 'ResolvedConfig', pipeline_name: str) -> pd.DataFrame:
        for col in self.group_by:
            if col not in df.columns:
                raise ValueError(f"Aggregate transform failed: Group by column '{col}' not found")
        
        for col in self.agg.keys():
            if col not in df.columns:
                raise ValueError(f"Aggregate transform failed: Aggregation column '{col}' not found")

        # Map agg function names
        mapping = {
            "SUM": "sum",
            "COUNT": "count",
            "AVG": "mean",
            "MIN": "min",
            "MAX": "max"
        }
        mapped_agg = {col: mapping[func] for col, func in self.agg.items()}
        
        return df.groupby(self.group_by).agg(mapped_agg).reset_index()
