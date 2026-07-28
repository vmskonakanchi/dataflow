"""
Connector SDK — pluggable source/sink interface for Dataflow.

Connectors abstract data access behind a uniform interface. Each connector:
- Declares its type and metadata (for UI rendering and discovery)
- Returns a DuckDB FROM-able expression from `read()` (memory-safe, streaming)
- Optionally implements `write()` for sink-capable connectors
- Optionally implements `test_connection()` for the "Test" button in the UI

Auto-discovery: any Connector subclass in this package is registered automatically
on import. To add a connector, create a file in src/connectors/ and subclass Connector.
"""

import importlib
import pkgutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ConnectionStatus:
    """Result of a test_connection() call."""
    connected: bool
    message: str = ""
    latency_ms: Optional[float] = None
    details: dict = field(default_factory=dict)


class Connector(ABC):
    """Base interface for all Dataflow connectors."""

    @classmethod
    @abstractmethod
    def connector_type(cls) -> str:
        """Unique registry key, e.g. 'file', 'http', 'postgres'."""
        ...

    @classmethod
    @abstractmethod
    def metadata(cls) -> dict:
        """Connector metadata for UI and documentation.

        Expected keys:
            name (str): Human-readable display name
            description (str): Short description for UI tooltips
            icon (str): Icon identifier for the UI
            version (str): Connector version
            author (str): Author name
            capabilities (set): {"source"} | {"sink"} | {"source", "sink"}
            config_schema (dict): JSON-schema-like dict that drives dynamic form generation
        """
        ...

    @abstractmethod
    def read(self, conn, params: dict) -> str:
        """Set up the source and return a DuckDB FROM-able expression.

        Args:
            conn: Active DuckDB connection (for extensions, secrets, ATTACH)
            params: Source configuration from the pipeline's source_config

        Returns:
            A SQL expression string usable in SELECT * FROM {result},
            e.g. "read_parquet('s3://...')" or "read_json('https://...')"
        """
        ...

    def write(self, conn, params: dict, source_expr: str) -> int:
        """Write data from source_expr to the destination.

        Args:
            conn: Active DuckDB connection
            params: Sink configuration
            source_expr: DuckDB expression for the data to write

        Returns:
            Number of rows written

        Override this for sink-capable connectors.
        """
        raise NotImplementedError(
            f"Connector '{self.connector_type()}' does not support sink operations."
        )

    def test_connection(self, params: dict) -> ConnectionStatus:
        """Test whether the connector can reach the target with given params.

        Override this to provide a "Test Connection" button in the UI.
        Default implementation returns a generic success.
        """
        return ConnectionStatus(connected=True, message="No test implemented for this connector.")


# ---------------------------------------------------------------------------
# Registry — auto-discovery of all Connector subclasses in this package
# ---------------------------------------------------------------------------

_registry: dict[str, type[Connector]] = {}


def register(cls: type[Connector]) -> type[Connector]:
    """Register a connector class. Used as a decorator."""
    key = cls.connector_type()
    if key in _registry:
        raise ValueError(f"Duplicate connector type: '{key}' (already registered by {_registry[key].__name__})")
    _registry[key] = cls
    return cls


def get_connector(connector_type: str) -> Connector:
    """Instantiate a connector by type key."""
    cls = _registry.get(connector_type)
    if cls is None:
        available = ", ".join(sorted(_registry.keys()))
        raise KeyError(f"Unknown connector type: '{connector_type}'. Available: {available}")
    return cls()


def get_connector_class(connector_type: str) -> type[Connector]:
    """Get the connector class (not instantiated) by type key."""
    cls = _registry.get(connector_type)
    if cls is None:
        available = ", ".join(sorted(_registry.keys()))
        raise KeyError(f"Unknown connector type: '{connector_type}'. Available: {available}")
    return cls


def available_connectors() -> dict[str, dict]:
    """Return metadata for all registered connectors. Used by the UI/API."""
    return {key: cls.metadata() for key, cls in _registry.items()}


def available_source_connectors() -> dict[str, dict]:
    """Return only connectors that support reading (source capability)."""
    return {
        key: cls.metadata()
        for key, cls in _registry.items()
        if "source" in cls.metadata().get("capabilities", set())
    }


def available_sink_connectors() -> dict[str, dict]:
    """Return only connectors that support writing (sink capability)."""
    return {
        key: cls.metadata()
        for key, cls in _registry.items()
        if "sink" in cls.metadata().get("capabilities", set())
    }


# Auto-discover all modules in this package to trigger @register decorators
def _auto_discover():
    """Import all submodules of this package to register connectors."""
    package_path = __path__
    for importer, module_name, is_pkg in pkgutil.iter_modules(package_path):
        if module_name.startswith("_"):
            continue
        importlib.import_module(f".{module_name}", __name__)


_auto_discover()
