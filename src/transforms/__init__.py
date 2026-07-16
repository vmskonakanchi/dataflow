"""Python transform plugin system.

Each module in this package that exposes a module-level ``transform(table, params)``
function is a plugin. Plugins are discovered by name at runtime — a pipeline
config references one via ``{"type": "python", "function": "<name>", ...}`` and the
executor loads and calls it.

The contract for a plugin:

    def transform(table: pyarrow.Table, params: dict) -> pyarrow.Table:
        ...

- ``table`` is the current pipeline data (fully materialized in memory).
- ``params`` is the ``params`` dict from the pipeline config.
- The return value MUST be a pyarrow.Table; it replaces the pipeline data for
  subsequent steps.

Only files that live in this package can be loaded — the pipeline config stores
a *name*, never code, so there is no arbitrary-code-execution path through the
database.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import TYPE_CHECKING, Any, Callable, Dict, Protocol

if TYPE_CHECKING:
    import pyarrow as pa


# Module name pattern is validated upstream by NameString (^[a-z][a-z0-9_]*$),
# but we re-validate here so the loader is safe even if called directly.
import re

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Names that are part of the package machinery, not plugins.
_RESERVED = {"__init__", "chunked"}


class PluginError(Exception):
    """Raised when a plugin cannot be loaded or does not satisfy the contract."""


class TransformPlugin(Protocol):
    def transform(self, table: "pa.Table", params: Dict[str, Any]) -> "pa.Table": ...


def available_plugins() -> list[str]:
    """Return the names of all discoverable transform plugins."""
    names: list[str] = []
    for _importer, name, ispkg in pkgutil.iter_modules(__path__):
        # Skip subpackages, reserved machinery, and private modules (e.g. the
        # chunk worker) — only user-facing plugin files are discoverable.
        if ispkg or name in _RESERVED or name.startswith("_"):
            continue
        names.append(name)
    return sorted(names)


def load_plugin(name: str) -> Callable[["pa.Table", Dict[str, Any]], "pa.Table"]:
    """Load a plugin by name and return its ``transform`` callable.

    Raises PluginError if the name is invalid, the module is missing, or the
    module does not expose a callable ``transform``.
    """
    if not _NAME_RE.match(name) or name in _RESERVED:
        raise PluginError(f"Invalid plugin name '{name}'")

    # Only allow names that actually correspond to modules in this package.
    if name not in available_plugins():
        raise PluginError(
            f"Unknown transform plugin '{name}'. Available: {', '.join(available_plugins()) or '(none)'}"
        )

    try:
        mod = importlib.import_module(f"{__name__}.{name}")
    except Exception as e:  # noqa: BLE001 - surface any import error clearly
        raise PluginError(f"Failed to import plugin '{name}': {e}") from e

    fn = getattr(mod, "transform", None)
    if fn is None or not callable(fn):
        raise PluginError(
            f"Plugin '{name}' does not expose a callable 'transform(table, params)'"
        )
    return fn


def run_plugin(name: str, table: "pa.Table", params: Dict[str, Any]) -> "pa.Table":
    """Load and execute a plugin, validating that it returns a pyarrow.Table."""
    import pyarrow as pa

    fn = load_plugin(name)
    result = fn(table, params or {})
    if not isinstance(result, pa.Table):
        raise PluginError(
            f"Plugin '{name}' must return a pyarrow.Table, got {type(result).__name__}"
        )
    return result
