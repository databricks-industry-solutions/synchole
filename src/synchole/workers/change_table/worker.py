from __future__ import annotations

import logging

from pyspark.sql import functions as F

from synchole.change_tracking import render_name
from synchole.common import quote_sql_string
from synchole.models import ChangeTrackingProfile, ReadResult
from synchole.sql_dialect import sql_bigint_cast
from synchole.workers.base import JdbcIngestionWorker

logger = logging.getLogger(__name__)


class ChangeTableIngestionWorker(JdbcIngestionWorker):
    """Reads the keys a change table lists after the last sequence value, joined to the source table.

    The change table, its sequence, delete flag and change time columns come from the config row's
    change-tracking profile.
    """

    @property
    def profile(self) -> ChangeTrackingProfile:
        if self.config.profile is None:
            raise ValueError(f"Config row for {self.config.source_table_name} has no change-tracking profile")
        return self.config.profile

    def derive_change_table_name(self) -> str:
        return render_name(self.profile.change_table_name, self.config.source_table_name, "change_table_name")

    def get_last_watermark(self) -> str:
        row = self.spark.sql(
            f"""
            SELECT last_watermark
            FROM {self.runtime.state_table}
            WHERE ingestion_group = '{quote_sql_string(self.config.ingestion_group)}'
              AND source_table_name = '{quote_sql_string(self.config.source_table_name)}'
              AND staging_table_fqn = '{quote_sql_string(self.config.staging_table_fqn)}'
              AND ingestion_type = '{quote_sql_string(self.config.ingestion_type)}'
              AND last_watermark IS NOT NULL
            ORDER BY last_run_timestamp DESC
            LIMIT 1
            """
        ).first()
        if row and row["last_watermark"] is not None:
            return str(row["last_watermark"])
        return "-1"

    def get_change_max_watermark(self) -> int | None:
        query = (
            f"(SELECT MAX({sql_bigint_cast(self.profile.sequence_column, self.sql_dialect)}) AS max_change_wm "
            f"FROM {self.derive_change_table_name()}) change_max"
        )
        row = self.build_jdbc_reader(query).load().first()
        if row and row["max_change_wm"] is not None:
            return int(row["max_change_wm"])
        return None

    def _get_change_partition_bounds(
        self,
        join_condition: str,
        wm_filter: str,
    ) -> tuple[int | None, int | None]:
        """MIN/MAX of partition_column over the change-table join + watermark window."""
        if not self.config.partition_column:
            return None, None
        bounds_query = (
            f"(SELECT MIN(main.{self.config.partition_column}) AS min_val, "
            f"MAX(main.{self.config.partition_column}) AS max_val "
            f"FROM {self.derive_change_table_name()} chg "
            f"LEFT JOIN {self.config.source_table_name} main ON {join_condition} "
            f"WHERE {wm_filter}) bounds"
        )
        row = self.build_jdbc_reader(bounds_query).load().first()
        if row is None or row["min_val"] is None or row["max_val"] is None:
            return None, None
        return int(row["min_val"]), int(row["max_val"])

    def _get_change_predicate_boundaries(
        self,
        join_condition: str,
        wm_filter: str,
    ) -> list[str]:
        """NTILE boundaries of predicate_column over the change-table join + watermark window."""
        if not self.config.predicate_column:
            return []
        bounds_query = (
            f"(SELECT MIN({self.config.predicate_column}) AS boundary_val FROM ("
            f"SELECT main.{self.config.predicate_column}, "
            f"NTILE({self.config.num_partitions}) OVER "
            f"(ORDER BY main.{self.config.predicate_column}) AS bucket "
            f"FROM {self.derive_change_table_name()} chg "
            f"LEFT JOIN {self.config.source_table_name} main ON {join_condition} "
            f"WHERE {wm_filter}"
            f") sub WHERE bucket > 1 GROUP BY bucket) bounds"
        )
        return sorted(
            row["boundary_val"]
            for row in self.build_jdbc_reader(bounds_query).load().collect()
            if row["boundary_val"] is not None
        )

    def read_full_source_jdbc(self):
        source_query = f"(SELECT * FROM {self.config.source_table_name}) source_data"

        if self.config.predicate_column:
            boundaries = self.build_predicate_boundaries()
            predicates = self.build_string_predicates(
                self.config.predicate_column,
                boundaries,
            )
            df = self.build_jdbc_reader_with_predicates(source_query, predicates)
        elif self.config.partition_column:
            lower_bound, upper_bound = self.get_partition_bounds()
            df = self.build_jdbc_reader(
                source_query,
                self.config.partition_column,
                lower_bound,
                upper_bound,
            ).load()
        else:
            df = self.build_jdbc_reader(source_query).load()

        if "_sync_deleted" not in df.columns:
            df = df.withColumn("_sync_deleted", F.lit(False))
        if "_sync_change_ts" not in df.columns:
            df = df.withColumn(
                "_sync_change_ts",
                F.lit("1970-01-01 00:00:00").cast("timestamp"),
            )
        return df

    def read_source_jdbc_via_change_table(
        self,
        last_watermark: int,
        new_watermark: int,
    ):
        join_keys = self.config.key_column_list
        if not join_keys:
            raise ValueError(
                f"Change-table config row for {self.config.source_table_name} requires key_columns"
            )

        profile = self.profile
        join_condition = " AND ".join(f"chg.{key} = main.{key}" for key in join_keys)
        sequence_as_bigint = sql_bigint_cast(f"chg.{profile.sequence_column}", self.sql_dialect)
        wm_filter = (
            f"{sequence_as_bigint} > {last_watermark} "
            f"AND {sequence_as_bigint} <= {new_watermark}"
        )
        change_key_aliases = ", ".join(
            f"chg.{key} AS _chg_key_{key}" for key in join_keys
        )
        source_query = (
            f"(SELECT chg.{profile.delete_flag_column} AS _sync_deleted, "
            f"chg.{profile.change_time_column} AS _sync_change_ts, {change_key_aliases}, main.* "
            f"FROM {self.derive_change_table_name()} chg "
            f"LEFT JOIN {self.config.source_table_name} main ON {join_condition} "
            f"WHERE {wm_filter}) chg_source"
        )

        if self.config.predicate_column:
            boundaries = self._get_change_predicate_boundaries(join_condition, wm_filter)
            predicates = self.build_string_predicates(
                self.config.predicate_column,
                boundaries,
            )
            df = self.build_jdbc_reader_with_predicates(source_query, predicates)
        elif self.config.partition_column:
            lower_bound, upper_bound = self._get_change_partition_bounds(
                join_condition, wm_filter
            )
            df = self.build_jdbc_reader(
                source_query,
                self.config.partition_column,
                lower_bound,
                upper_bound,
            ).load()
        else:
            df = self.build_jdbc_reader(source_query).load()

        for key in join_keys:
            df = df.withColumn(key, F.coalesce(F.col(key), F.col(f"_chg_key_{key}")))
        # The flag can be a bit or an integer; the CDC pipeline applies deletes where it is true.
        df = df.withColumn("_sync_deleted", F.col("_sync_deleted").cast("boolean"))
        return df.drop(*[f"_chg_key_{key}" for key in join_keys])

    def read_source(self) -> ReadResult:
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }

        # ── Full-refresh override ────────────────────────────────────
        # When the user triggers a full refresh (e.g. after the change
        # table's retention window has passed), bypass the sequence watermark and
        # pull every row from the source table.  The staging overwrite
        # in write_to_staging ensures a clean snapshot.
        # Triggered by the global runtime flag OR a per-table
        # FULL_REFRESH status in the watermark table (set from the UI).
        if self.effective_full_refresh:
            logger.info(
                "[READ-CHG] %s — full_refresh requested, performing full source read",
                self.config.source_table_name,
                extra=_ctx,
            )
            source_df = self.read_full_source_jdbc()
            new_watermark = self.get_change_max_watermark()
            return ReadResult(
                df=source_df,
                persisted_watermark=str(new_watermark) if new_watermark is not None else "-1",
            )

        last_watermark = int(self.get_last_watermark())
        new_watermark = self.get_change_max_watermark()

        if new_watermark is None:
            logger.info(
                "[READ-CHG] %s — change table is empty, skipping",
                self.config.source_table_name,
                extra=_ctx,
            )
            return ReadResult(df=None, skip=True)

        if last_watermark == -1:
            logger.info(
                "[READ-CHG] %s — first run, performing full load  change_wm=%d",
                self.config.source_table_name,
                new_watermark,
                extra=_ctx,
            )
            source_df = self.read_full_source_jdbc()
        else:
            if new_watermark <= last_watermark:
                logger.info(
                    "[READ-CHG] %s — no new changes (last=%d, current=%d), skipping",
                    self.config.source_table_name,
                    last_watermark,
                    new_watermark,
                    extra=_ctx,
                )
                return ReadResult(df=None, skip=True)
            logger.info(
                "[READ-CHG] %s — incremental via change table  window=%d→%d",
                self.config.source_table_name,
                last_watermark,
                new_watermark,
                extra=_ctx,
            )
            source_df = self.read_source_jdbc_via_change_table(
                last_watermark,
                new_watermark,
            )

        return ReadResult(df=source_df, persisted_watermark=str(new_watermark))
