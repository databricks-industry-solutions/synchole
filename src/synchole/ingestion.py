from __future__ import annotations

import logging
from typing import Any

from synchole.change_tracking import validate_change_tracking
from synchole.common import (
    normalize_ingestion_type,
    normalize_text,
    quote_sql_string,
    resolve_staging_table_fqn,
    row_to_dict,
    validate_table_fqn,
)
from synchole.models import ChangeTrackingProfile, IngestionConfig, JdbcRuntimeSettings
from synchole.workers import (
    ChangeProcedureIngestionWorker,
    ChangeTableIngestionWorker,
    JdbcIngestionWorker,
    TimestampWatermarkIngestionWorker,
)

logger = logging.getLogger(__name__)

PROFILE_COLUMNS = (
    "change_table_name",
    "sequence_column",
    "delete_flag_column",
    "change_time_column",
    "procedure_call",
    "results_table",
)


def profile_from_row(row_dict: dict[str, Any]) -> ChangeTrackingProfile:
    return ChangeTrackingProfile(
        profile_name=normalize_text(row_dict.get("profile_name")),
        method=normalize_text(row_dict.get("method")).lower(),
        # procedure_call keeps its line breaks; only the ends are trimmed.
        **{column: normalize_text(row_dict.get(column)) for column in PROFILE_COLUMNS},
    )


class JdbcIngestionConfigRepository:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime

    def load_profiles(self) -> dict[str, ChangeTrackingProfile]:
        """change_tracking_profiles by name; none when the table hasn't been created (watermark rows only)."""
        if not self.spark.catalog.tableExists(self.runtime.profiles_table):
            return {}
        rows = self.spark.sql(f"SELECT * FROM {self.runtime.profiles_table}").collect()
        profiles = (profile_from_row(row_to_dict(row)) for row in rows)
        return {profile.profile_name: profile for profile in profiles}

    def load_selected_configs(
        self,
        ingestion_group: str = "",
        source_table_name: str = "",
    ) -> list[IngestionConfig]:
        filters = ["enabled = true"]
        if ingestion_group:
            filters.append(f"ingestion_group = '{quote_sql_string(ingestion_group)}'")
        if source_table_name:
            filters.append(f"source_table_name = '{quote_sql_string(source_table_name)}'")
        profiles = self.load_profiles()

        query = f"""
            SELECT
                ingestion_group,
                source_table_name,
                staging_table_fqn,
                target_table_fqn,
                lower(coalesce(ingestion_type, 'incremental')) AS ingestion_type,
                key_columns,
                watermark_column,
                partition_column,
                predicate_column,
                change_tracking, change_profile, change_columns, skip_change_time,
                auto_cdc_from_snapshot,
                jdbc_url, jdbc_user, jdbc_secret_scope, jdbc_secret_key, uc_secret_name,
                connection_name, connection_database,
                watermark_threshold_minutes, fetch_size, num_partitions
            FROM {self.runtime.config_table}
            WHERE {' AND '.join(filters)}
            ORDER BY ingestion_group, source_table_name
        """

        configs: list[IngestionConfig] = []
        for row in self.spark.sql(query).collect():
            row_dict = row_to_dict(row)
            config_catalog, config_schema, _ = self.runtime.config_table.split(".", 2)
            change_profile = normalize_text(row_dict.get("change_profile"))
            config = IngestionConfig(
                ingestion_group=normalize_text(row_dict.get("ingestion_group")),
                source_table_name=normalize_text(row_dict.get("source_table_name")),
                staging_table_fqn=resolve_staging_table_fqn(
                    row_dict.get("staging_table_fqn"),
                    normalize_text(row_dict.get("source_table_name")),
                    config_catalog,
                    config_schema,
                ),
                target_table_fqn=validate_table_fqn(row_dict.get("target_table_fqn") or ""),
                ingestion_type=normalize_ingestion_type(row_dict.get("ingestion_type")),
                key_columns=row_dict.get("key_columns"),
                watermark_column=normalize_text(row_dict.get("watermark_column")),
                partition_column=normalize_text(row_dict.get("partition_column")),
                predicate_column=normalize_text(row_dict.get("predicate_column")),
                change_tracking=normalize_text(row_dict.get("change_tracking")).lower() or "watermark",
                change_profile=change_profile,
                change_columns=normalize_text(row_dict.get("change_columns")),
                skip_change_time=bool(row_dict.get("skip_change_time")),
                auto_cdc_from_snapshot=bool(row_dict.get("auto_cdc_from_snapshot")),
                jdbc_url=normalize_text(row_dict.get("jdbc_url")),
                jdbc_user=normalize_text(row_dict.get("jdbc_user")),
                jdbc_secret_scope=normalize_text(row_dict.get("jdbc_secret_scope")),
                jdbc_secret_key=normalize_text(row_dict.get("jdbc_secret_key")),
                uc_secret_name=normalize_text(row_dict.get("uc_secret_name")),
                connection_name=normalize_text(row_dict.get("connection_name")),
                connection_database=normalize_text(row_dict.get("connection_database")),
                watermark_threshold_minutes=int(
                    5 if row_dict.get("watermark_threshold_minutes") is None else row_dict["watermark_threshold_minutes"]
                ),
                fetch_size=int(row_dict.get("fetch_size") or 10000),
                num_partitions=int(row_dict.get("num_partitions") or 8),
                profile=profiles.get(change_profile),
            )
            if not config.ingestion_group:
                raise ValueError(
                    f"Config row for {config.source_table_name} is missing ingestion_group"
                )
            if not config.jdbc_url and not config.connection_name:
                raise ValueError(
                    f"Config row for {config.source_table_name} requires jdbc_url or connection_name"
                )
            has_scope_secret = bool(config.jdbc_secret_scope) and bool(config.jdbc_secret_key)
            has_uc_secret = bool(config.uc_secret_name)
            if bool(config.jdbc_secret_scope) != bool(config.jdbc_secret_key):
                raise ValueError(
                    f"Config row for {config.source_table_name} must set both jdbc_secret_scope and jdbc_secret_key"
                )
            if not has_scope_secret and not has_uc_secret and not config.connection_name and config.jdbc_url:
                raise ValueError(
                    f"Config row for {config.source_table_name} requires a secret (jdbc_secret_scope/key or uc_secret_name) for JDBC authentication"
                )
            if (
                config.ingestion_type == "incremental"
                and not config.watermark_column
                and not config.uses_change_source
            ):
                raise ValueError(
                    f"Incremental config row for {config.source_table_name} requires watermark_column"
                )
            validate_change_tracking(config)
            configs.append(config)
        logger.info(
            "[CONFIG] Loaded %d config(s)  group=%s  table_filter=%s",
            len(configs),
            ingestion_group or "(all)",
            source_table_name or "(all)",
        )
        return configs


class JdbcIngestionOrchestrator:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime
        self.repository = JdbcIngestionConfigRepository(spark=spark, runtime=runtime)

    def build_worker(self, config: IngestionConfig) -> JdbcIngestionWorker:
        if config.change_tracking == "change_procedure":
            return ChangeProcedureIngestionWorker(self.spark, self.runtime, config)
        if config.change_tracking == "change_table":
            return ChangeTableIngestionWorker(self.spark, self.runtime, config)
        return TimestampWatermarkIngestionWorker(self.spark, self.runtime, config)

    def run_selected_ingestion(self) -> list[dict[str, Any]]:
        configs = self.repository.load_selected_configs(
            ingestion_group=self.runtime.ingestion_group,
            source_table_name=self.runtime.source_table_name,
        )
        logger.info(
            "[ORCH]   Starting ingestion run  group=%s  tables=%d",
            self.runtime.ingestion_group or "(all)",
            len(configs),
        )
        results: list[dict[str, Any]] = []
        for config in configs:
            worker = self.build_worker(config)
            try:
                result = worker.process()
                results.append(result)
                logger.info(
                    "[ORCH]   %s — %s",
                    config.source_table_name,
                    result["status"],
                    extra={
                        "ingestion_group": config.ingestion_group,
                        "source_table": config.source_table_name,
                    },
                )
            except Exception as exc:
                worker.update_watermark_state(None, "FAILED", str(exc)[:4000])
                results.append(worker._result("FAILED", error=str(exc)))
                logger.error(
                    "[ORCH]   %s — FAILED: %s",
                    config.source_table_name,
                    str(exc)[:500],
                    exc_info=True,
                    extra={
                        "ingestion_group": config.ingestion_group,
                        "source_table": config.source_table_name,
                    },
                )

        failed = [result for result in results if result["status"] == "FAILED"]
        logger.info(
            "[ORCH]   Run complete  total=%d  success=%d  skipped=%d  failed=%d",
            len(results),
            sum(1 for r in results if r["status"] == "SUCCESS"),
            sum(1 for r in results if r["status"] == "SKIPPED"),
            len(failed),
        )
        if failed:
            raise RuntimeError(f"{len(failed)} table(s) failed during ingestion")
        return results


__all__ = [
    "ChangeProcedureIngestionWorker",
    "ChangeTableIngestionWorker",
    "JdbcIngestionConfigRepository",
    "JdbcIngestionOrchestrator",
    "JdbcIngestionWorker",
    "TimestampWatermarkIngestionWorker",
]
