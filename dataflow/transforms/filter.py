import pandas as pd
from .base import BaseTransform

@BaseTransform.register("filter")
class FilterTransform(BaseTransform):
    def __init__(self, condition: str):
        self.condition = condition

    def apply(self, df: pd.DataFrame, resolved_config: 'ResolvedConfig', pipeline_name: str) -> pd.DataFrame:
        try:
            return df.query(self.condition)
        except Exception as e:
            # Check if column exists error
            raise ValueError(f"Filter transform failed: {str(e)}")
