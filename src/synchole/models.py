from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

_LOGGING_CONFIGURED = False


def _configure_default_logging() -> None:
    """Configure console logging for the synchole namespace (singleton)."""
    global _LOGGING_CONFIGURED  # noqa: PLW0603
    if _LOGGING_CONFIGURED:
        return
    _LOGGING_CONFIGURED = True

    sh_logger = logging.getLogger("synchole")
    sh_logger.setLevel(logging.INFO)
    if not any(isinstance(h, logging.StreamHandler) for h in sh_logger.handlers):
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)-8s %(name)s \u2014 %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        sh_logger.addHandler(console)
    sh_logger.propagate = False


@dataclass(frozen=True)
class JdbcRuntimeSettings:
    configuration_fqn: str
    watermark_fqn: str
    ingestion_group: str = ""
    source_table_name: str = ""
    full_refresh: bool = False

    def __post_init__(self) -> None:
        _configure_default_logging()

    @property
    def config_table(self) -> str:
        return self.configuration_fqn

    @property
    def state_table(self) -> str:
        return self.watermark_fqn

    @property
    def profiles_table(self) -> str:
        """change_tracking_profiles, next to the config table."""
        catalog, schema, _ = self.configuration_fqn.split(".", 2)
        return f"{catalog}.{schema}.change_tracking_profiles"


CHANGE_TRACKING_METHODS = ("watermark", "change_table", "change_procedure")


@dataclass(frozen=True)
class ChangeTrackingProfile:
    """How a source system records changes: a row of change_tracking_profiles, shared by many config rows.

    change_table: a change table per source table (``change_table_name`` template), whose
    ``sequence_column`` only grows, with a delete flag and a change time.
    change_procedure: a stored procedure (``procedure_call`` template) that lists the keys changed in a
    time window, with their change time in ``change_time_column``.
    """

    profile_name: str
    method: str
    change_table_name: str = ""
    sequence_column: str = ""
    delete_flag_column: str = ""
    change_time_column: str = ""
    procedure_call: str = ""
    results_table: str = ""


@dataclass(frozen=True)
class IngestionConfig:
    ingestion_group: str
    source_table_name: str
    staging_table_fqn: str
    target_table_fqn: str
    ingestion_type: str
    key_columns: str | None
    watermark_column: str
    partition_column: str
    predicate_column: str
    change_tracking: str = "watermark"
    change_profile: str = ""
    change_columns: str = ""
    skip_change_time: bool = False
    auto_cdc_from_snapshot: bool = False
    jdbc_url: str = ""
    jdbc_user: str = ""
    jdbc_secret_scope: str = ""
    jdbc_secret_key: str = ""
    uc_secret_name: str = ""
    connection_name: str = ""
    connection_database: str = ""
    watermark_threshold_minutes: int = 5
    fetch_size: int = 10000
    num_partitions: int = 8
    profile: ChangeTrackingProfile | None = None

    @property
    def uses_change_source(self) -> bool:
        """True when changes come from a change table or a change procedure instead of a watermark column."""
        return self.change_tracking in ("change_table", "change_procedure")

    @property
    def key_column_list(self) -> list[str]:
        return [key.strip() for key in (self.key_columns or "").split(",") if key.strip()]

    @property
    def jdbc_properties(self) -> dict[str, str]:
        properties = {"fetchsize": str(self.fetch_size)}
        if self.jdbc_user:
            properties["user"] = self.jdbc_user
        if self.uc_secret_name:
            from databricks.sdk.runtime import dbutils

            parts = self.uc_secret_name.split(".", 2)
            if len(parts) != 3:
                raise ValueError(
                    f"UC secret name '{self.uc_secret_name}' must be in 'catalog.schema.secret_name' format"
                )
            properties["password"] = dbutils.secrets.get(
                catalog=parts[0], schema=parts[1], key=parts[2]
            )
        elif self.jdbc_secret_scope and self.jdbc_secret_key:
            from databricks.sdk.runtime import dbutils

            properties["password"] = dbutils.secrets.get(
                scope=self.jdbc_secret_scope, key=self.jdbc_secret_key
            )
        return properties


@dataclass
class ReadResult:
    df: Any | None
    persisted_watermark: str | None = None
    skip: bool = False


@dataclass(frozen=True)
class JobProvisioningSettings:
    ingestion_group: str
    configuration_fqn: str
    watermark_fqn: str
    planner_notebook_path: str
    worker_notebook_path: str
    wheel_uri: str
    foreach_concurrency: int = 4
    cdc_pipeline_id: str = ""
    cdc_pipeline_file_path: str = ""
    use_serverless: bool = True
    # Notebooks from a Git repository at a branch, tag or commit; an empty git_url means workspace notebook paths.
    git_url: str = ""
    git_branch: str = "main"
    git_tag: str = ""
    git_commit: str = ""
    # Detected from git_url when empty; needed for self-hosted servers (e.g. gitHubEnterprise).
    git_provider: str = ""
