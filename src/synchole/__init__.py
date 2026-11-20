"""SyncHole: metadata-driven JDBC ingestion into Delta.

Names are imported on first use, so the parts that only talk to the Databricks API (such as the
``synchole-create-job`` CLI) work outside Databricks, without PySpark or Delta Lake installed.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "CdcScd2PipelineBuilder": "synchole.cdc_pipeline",
    "build_pipeline_from_spark_conf": "synchole.cdc_pipeline",
    "IngestionConfigPlanner": "synchole.config_planner",
    "PlannerConfigRepository": ("synchole.config_planner", "JdbcIngestionConfigRepository"),
    "ChangeProcedureIngestionWorker": "synchole.ingestion",
    "ChangeTableIngestionWorker": "synchole.ingestion",
    "JdbcIngestionOrchestrator": "synchole.ingestion",
    "JdbcIngestionWorker": "synchole.ingestion",
    "TimestampWatermarkIngestionWorker": "synchole.ingestion",
    "IngestionConfig": "synchole.models",
    "JdbcRuntimeSettings": "synchole.models",
    "JobProvisioningSettings": "synchole.models",
    "ReadResult": "synchole.models",
    "IngestionJobProvisioner": "synchole.utils.create_ingestion_job",
    "LakebaseTestDatabaseSetup": "synchole.utils.lakebase_test_database_setup",
    "UnityCatalogSetup": "synchole.utils.uc_setup",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'synchole' has no attribute {name!r}")
    module, attribute = target if isinstance(target, tuple) else (target, name)
    value = getattr(import_module(module), attribute)
    globals()[name] = value
    return value
