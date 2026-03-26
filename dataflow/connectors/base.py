from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Type, Optional
from ..config.models import SourceConfig, SinkConfig

class BaseConnector(ABC):
    _registry: Dict[str, Type['BaseConnector']] = {}

    @abstractmethod
    def extract(self, source: SourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def load(self, df: pd.DataFrame, sink: SinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        pass

    @classmethod
    def register(cls, type_name: str):
        def decorator(subclass: Type['BaseConnector']):
            cls._registry[type_name] = subclass
            return subclass
        return decorator

    @classmethod
    def get(cls, type_name: str) -> 'BaseConnector':
        if type_name not in cls._registry:
            raise ValueError(f"No connector registered for type: {type_name}")
        return cls._registry[type_name]()
