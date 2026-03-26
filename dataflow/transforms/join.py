import pandas as pd
import re
from .base import BaseTransform
from ..connectors.base import BaseConnector
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config.loader import ResolvedConfig

@BaseTransform.register("join")
class JoinTransform(BaseTransform):
    def __init__(self, right_source: str, right_query: str, join_type: str, on: str):
        self.right_source = right_source
        self.right_query = right_query
        self.join_type = join_type
        self.on_str = on

    def apply(self, df: pd.DataFrame, resolved_config: 'ResolvedConfig', pipeline_name: str) -> pd.DataFrame:
        # Parse 'on' string: "left.col = right.col"
        match = re.match(r"left\.(.+) = right\.(.+)", self.on_str)
        if not match:
            raise ValueError(f"Join transform failed: Invalid 'on' condition format '{self.on_str}'. Expected 'left.col = right.col'")
        
        left_key, right_key = match.groups()

        if left_key not in df.columns:
            raise ValueError(f"Join transform failed: Left key '{left_key}' not found in DataFrame")

        # Fetch right source
        source_config = resolved_config.sources.get(self.right_source)
        if not source_config:
            raise ValueError(f"Join transform failed: Right source '{self.right_source}' not found in config")

        connector = BaseConnector.get(source_config.type)
        right_df = connector.extract(source_config, self.right_query, pipeline_name)

        if right_key not in right_df.columns:
            raise ValueError(f"Join transform failed: Right key '{right_key}' not found in source '{self.right_source}'")

        return pd.merge(
            left=df,
            right=right_df,
            left_on=left_key,
            right_on=right_key,
            how=self.join_type
        )
