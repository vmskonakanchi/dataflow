from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Type

class BaseTransform(ABC):
    _registry: Dict[str, Type['BaseTransform']] = {}

    @abstractmethod
    def apply(self, df: pd.DataFrame, resolved_config: 'ResolvedConfig', pipeline_name: str) -> pd.DataFrame:
        pass

    @classmethod
    def register(cls, type_name: str):
        def decorator(subclass: Type['BaseTransform']):
            cls._registry[type_name] = subclass
            return subclass
        return decorator

    @classmethod
    def get(cls, type_name: str) -> 'BaseTransform':
        if type_name not in cls._registry:
            raise ValueError(f"No transform registered for type: {type_name}")
        return cls._registry[type_name]
