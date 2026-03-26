import requests
import pandas as pd
from typing import Optional, Dict, Any
from .base import BaseConnector
from ..config.models import SourceConfig, SinkConfig, RestApiSourceConfig
from requests.auth import HTTPBasicAuth

@BaseConnector.register("rest_api")
class RestAPIConnector(BaseConnector):
    def extract(self, source: RestApiSourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        headers = source.headers or {}
        auth = None

        if source.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {source.auth_token}"
        elif source.auth_type == "basic":
            # Assuming auth_token is "username:password" for basic auth if not explicitly separated
            # However, the model doesn't have username/password for basic.
            # I'll assume token is "user:pass"
            if source.auth_token and ":" in source.auth_token:
                user, password = source.auth_token.split(":", 1)
                auth = HTTPBasicAuth(user, password)

        method = source.method.upper()
        response = requests.request(method, source.url, headers=headers, auth=auth)

        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

        data = response.json()

        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.DataFrame(value)
            return pd.DataFrame([data])
        else:
            raise ValueError(f"Unexpected JSON response format: {type(data)}")

    def load(self, df: pd.DataFrame, sink: SinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        raise NotImplementedError("REST API is source only in V1")
