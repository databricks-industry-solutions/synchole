"""Setup and provisioning helpers, imported on first use (see synchole/__init__.py)."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "IngestionJobProvisioner": "synchole.utils.create_ingestion_job",
    "LakebaseTestDatabaseSetup": "synchole.utils.lakebase_test_database_setup",
    "UnityCatalogSetup": "synchole.utils.uc_setup",
    "ZerobusLogHandler": "synchole.utils.zerobus_logger",
    "setup_synchole_logging": "synchole.utils.zerobus_logger",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module 'synchole.utils' has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
