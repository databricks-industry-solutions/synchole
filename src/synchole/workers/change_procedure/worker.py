from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

from synchole.change_tracking import (
    fill_template,
    is_valid_column,
    is_valid_column_names,
    is_valid_key_columns,
    is_valid_name,
    name_values,
    render_name,
)
from synchole.common import quote_sql_string
from synchole.models import ReadResult
from synchole.sql_dialect import SqlDialect, sql_timestamp_literal
from synchole.workers.change_table.worker import ChangeTableIngestionWorker

logger = logging.getLogger(__name__)

_WATERMARK_FORMAT = "%Y-%m-%d %H:%M:%S"
# A parallel range read is used only when it reads at most this many source rows per changed key,
# assuming roughly dense keys; sparser changes are joined on the source over one connection.
_MAX_SCAN_RATIO = 10


def format_watermark(value: datetime) -> str:
    """Whole seconds, because the state table keeps only the first 19 characters of a watermark.

    Rounding the window end down never skips changes; they fall into the next window.
    """
    return value.strftime(_WATERMARK_FORMAT)


def parse_watermark(value: str | None) -> datetime | None:
    """None for a missing watermark or one written by another method (e.g. a change-table sequence value)."""
    for pattern in (_WATERMARK_FORMAT, f"{_WATERMARK_FORMAT}.%f"):
        try:
            return datetime.strptime(value or "", pattern)
        except ValueError:
            continue
    return None


def _require_supported_dialect(dialect: SqlDialect) -> None:
    if dialect not in ("sqlserver", "postgres"):
        raise ValueError(f"Change-procedure ingestion does not support the '{dialect}' dialect")


def _require_valid_names(table: str, key_columns: str) -> None:
    if not is_valid_name(table):
        raise ValueError(f"Invalid source table name for the change procedure: {table!r}")
    if not is_valid_key_columns(key_columns):
        raise ValueError(f"Invalid key_columns for the change procedure: {key_columns!r}")


def _require_valid_column(column: str) -> None:
    if not is_valid_column(column):
        raise ValueError(f"Invalid change_time_column: {column!r}")


def _sql_server_datetime(value: str) -> str:
    # The 'T' form is read the same way whatever the session's language or DATEFORMAT.
    return quote_sql_string(value).replace(" ", "T", 1)


def is_integral_type(data_type: T.DataType) -> bool:
    """True for an integer key column (INT, BIGINT, NUMERIC(18, 0)), which Spark can split into ranges."""
    if isinstance(data_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return True
    return isinstance(data_type, T.DecimalType) and data_type.scale == 0


def is_integral_key(value) -> bool:
    """True for an integer key value (INT, BIGINT, NUMERIC(18, 0)), which Spark can split into ranges."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, Decimal) and value == value.to_integral_value()


def parallel_read_bounds(changed: int, low, high, num_partitions: int) -> tuple[int, int] | None:
    """[low, high] of the range read, or None when one connection with a join on the source is better."""
    if num_partitions <= 1 or changed <= 0 or low is None or high is None:
        return None
    low, high = int(low), int(high)
    if high - low + 1 > _MAX_SCAN_RATIO * changed:
        return None
    return low, high


def source_now_query(dialect: SqlDialect) -> str:
    """Source server time as text, so no Spark or Python timezone conversion shifts the window."""
    _require_supported_dialect(dialect)
    if dialect == "sqlserver":
        return "(SELECT CONVERT(VARCHAR(23), SYSDATETIME(), 121) AS now_ts) source_now"
    return "(SELECT to_char(LOCALTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') AS now_ts) source_now"


def render_procedure_call(
    template: str,
    source_table: str,
    start_literal: str,
    end_literal: str,
    temp_table: str = "",
    multi_key: bool = False,
    skip_change_time: bool = False,
    columns: str = "",
) -> str:
    """The profile's procedure_call for one window, without a trailing semicolon.

    start_literal and end_literal are SQL expressions (a quoted timestamp or a variable). The names are
    validated and columns is quoted, so the template is the only free SQL.
    """
    if not is_valid_column_names(columns):
        raise ValueError(f"Invalid change_columns: {columns!r}")
    if temp_table and not is_valid_name(temp_table.lstrip("#")):
        raise ValueError(f"Invalid temp table name: {temp_table!r}")
    values = {
        **name_values(source_table),
        "start": start_literal,
        "end": end_literal,
        "temp_table": temp_table,
        "multi_key": "1" if multi_key else "0",
        "skip_change_time": "1" if skip_change_time else "0",
        "columns": f"'{quote_sql_string(columns)}'" if columns else "NULL",
    }
    return fill_template(template, values).strip().rstrip(";").strip()


# --- SQL Server: the procedure fills a temp table named by the caller ------------------------------------
#
# The procedure runs in sessionInitStatement, which Spark runs only on the connection that reads the data.
# prepareQuery, which Spark also runs when it first resolves the result's columns, only creates an empty
# temp table with the right columns there; on the reading connection the procedure has already filled it.


def changed_rows_temp_table(run_id: str) -> str:
    # A global temp table: the procedure fills it inside its own scope, where a #local table would be
    # dropped on return. It goes away when the connection closes.
    return f"##sh_chg_{run_id}"


def build_procedure_call(
    template: str,
    source_table: str,
    start: str,
    end: str,
    temp_table: str,
    multi_key: bool,
    skip_change_time: bool = False,
    columns: str = "",
    end_is_variable: bool = False,
) -> str:
    """The profile's procedure_call on SQL Server, filling temp_table with the changes in [start, end)."""
    end_argument = end if end_is_variable else f"'{_sql_server_datetime(end)}'"
    call = render_procedure_call(
        template, source_table, f"'{_sql_server_datetime(start)}'", end_argument, temp_table,
        multi_key, skip_change_time, columns,
    )
    # NOCOUNT keeps row-count messages from coming back ahead of the SELECT's result set.
    return f"SET NOCOUNT ON; {call};"


def build_procedure_call_until_now(
    template: str,
    source_table: str,
    start: str,
    threshold_minutes: int,
    temp_table: str,
    multi_key: bool,
    first_key: str,
    skip_change_time: bool = False,
    columns: str = "",
) -> str:
    """sessionInitStatement: the procedure for a window ending at the server's clock minus threshold_minutes.

    @sh_window_end is cut to whole seconds (style 120 truncates) and never before start. After the call,
    one scan of the temp table gives the count and the first key's range (NULL when the key isn't a
    number). All four are kept in the session context, because sessionInitStatement and the query are
    separate batches; build_changed_keys_query returns them, so no clock query or Spark aggregation is needed.
    """
    start_literal = _sql_server_datetime(start)
    call = build_procedure_call(
        template, source_table, start, "@sh_window_end", temp_table, multi_key,
        skip_change_time, columns, end_is_variable=True,
    )
    return (
        "SET NOCOUNT ON; "
        "DECLARE @sh_window_end DATETIME2(0) = CONVERT(DATETIME2(0), CONVERT(VARCHAR(19), "
        f"DATEADD(MINUTE, -{int(threshold_minutes)}, SYSDATETIME()), 120)); "
        f"IF @sh_window_end < '{start_literal}' SET @sh_window_end = '{start_literal}'; "
        f"{call.removeprefix('SET NOCOUNT ON; ')} "
        "DECLARE @sh_changed BIGINT, @sh_low BIGINT, @sh_high BIGINT; "
        f"SELECT @sh_changed = COUNT_BIG(*), @sh_low = TRY_CAST(MIN({first_key}) AS BIGINT), "
        f"@sh_high = TRY_CAST(MAX({first_key}) AS BIGINT) FROM {temp_table}; "
        "EXEC sp_set_session_context @key = N'sh_window_end', @value = @sh_window_end; "
        "EXEC sp_set_session_context @key = N'sh_changed', @value = @sh_changed; "
        "EXEC sp_set_session_context @key = N'sh_low', @value = @sh_low; "
        "EXEC sp_set_session_context @key = N'sh_high', @value = @sh_high;"
    )


def build_temp_table_shape(
    temp_table: str, source_table: str, keys: list[str], change_time_column: str, skip_change_time: bool
) -> str:
    """prepareQuery: an empty temp table with the procedure's columns, so Spark can resolve the result.

    Does nothing where the procedure (in sessionInitStatement) has already filled the table.
    """
    _require_valid_column(change_time_column)
    change_column = "" if skip_change_time else f", CAST(NULL AS DATETIME) AS {change_time_column}"
    return (
        f"SET NOCOUNT ON; IF OBJECT_ID('tempdb..{temp_table}') IS NULL "
        f"SELECT TOP 0 {', '.join(keys)}{change_column} INTO {temp_table} FROM {source_table};"
    )


def build_changed_keys_query(
    temp_table: str, keys: list[str], change_time_column: str, skip_change_time: bool
) -> str:
    """SQL Server: changed keys and their change time, plus one row with NULL keys carrying the summary.

    The summary (window end, count, key range) comes from the session context set by
    build_procedure_call_until_now. LEFT JOIN ON 1 = 0 gives that row the temp table's column types,
    so the UNION ALL keeps them, and it comes back even when nothing changed.
    """
    _require_valid_column(change_time_column)
    key_columns = ", ".join(f"chg.{key}" for key in keys)
    change_column = "" if skip_change_time else f", chg.{change_time_column} AS _sync_change_ts"
    no_summary = (
        "CAST(NULL AS VARCHAR(19)) AS _sh_window_end, CAST(NULL AS BIGINT) AS _sh_changed, "
        "CAST(NULL AS BIGINT) AS _sh_low, CAST(NULL AS BIGINT) AS _sh_high"
    )
    summary = (
        "CONVERT(VARCHAR(19), CAST(SESSION_CONTEXT(N'sh_window_end') AS DATETIME2(0)), 120), "
        "CAST(SESSION_CONTEXT(N'sh_changed') AS BIGINT), CAST(SESSION_CONTEXT(N'sh_low') AS BIGINT), "
        "CAST(SESSION_CONTEXT(N'sh_high') AS BIGINT)"
    )
    return (
        f"(SELECT {key_columns}{change_column}, {no_summary} FROM {temp_table} chg "
        f"UNION ALL SELECT {key_columns}{change_column}, {summary} "
        f"FROM (SELECT 1 AS one) o LEFT JOIN {temp_table} chg ON 1 = 0) chg_keys"
    )


def build_temp_table_rows_query(
    temp_table: str,
    source_table: str,
    keys: list[str],
    change_time_column: str,
    window_end_if_skipped: str | None = None,
) -> str:
    """SQL Server: the temp table's changed keys joined to the source; a missing source row means a delete.

    Without a change time (skip_change_time), the window end is the change time.
    """
    _require_valid_column(change_time_column)
    join_condition = " AND ".join(f"chg.{key} = main.{key}" for key in keys)
    key_aliases = ", ".join(f"chg.{key} AS _chg_key_{key}" for key in keys)
    change_time = (
        f"CAST('{_sql_server_datetime(window_end_if_skipped)}' AS DATETIME2(0))"
        if window_end_if_skipped
        else f"chg.{change_time_column}"
    )
    return (
        f"(SELECT {change_time} AS _sync_change_ts, {key_aliases}, main.* "
        f"FROM {temp_table} chg LEFT JOIN {source_table} main ON {join_condition}) chg_source"
    )


def build_key_range_query(source_table: str, key: str, low: int, high: int) -> str:
    """Source rows whose key is in [low, high]; Spark splits it into num_partitions ranges."""
    return f"(SELECT * FROM {source_table} WHERE {key} >= {int(low)} AND {key} <= {int(high)}) src_range"


# --- Postgres: the procedure writes its results to a table (the profile's results_table) ------------------


def build_postgres_procedure_run(
    template: str,
    source_table: str,
    start: str,
    end: str,
    multi_key: bool,
    skip_change_time: bool = False,
    columns: str = "",
) -> str:
    """A SELECT that runs the profile's procedure_call, a function call that a SELECT can run.

    The Postgres JDBC driver returns no rows for "CALL ...; SELECT ...", so the procedure is called
    through a function whose single value is ignored.
    """
    call = render_procedure_call(
        template, source_table, sql_timestamp_literal(start, "postgres"), sql_timestamp_literal(end, "postgres"),
        "", multi_key, skip_change_time, columns,
    )
    return f"(SELECT {call} AS procedure_result) procedure_run"


def build_results_stats_query(
    results_table: str, first_key: str, change_time_column: str, start: str, end: str, dialect: SqlDialect
) -> str:
    """Count and key range of the changed rows in [start, end)."""
    _require_valid_column(change_time_column)
    return (
        f"(SELECT COUNT(*) AS changed, MIN({first_key}) AS low, MAX({first_key}) AS high "
        f"FROM {results_table} WHERE {_results_window(start, end, dialect, change_time_column)}) results_stats"
    )


def build_results_rows_query(
    results_table: str,
    source_table: str,
    keys: list[str],
    change_time_column: str,
    start: str,
    end: str,
    dialect: SqlDialect,
) -> str:
    """The results table's changed keys joined to the source; a key with no source row is a delete.

    The window filter keeps the read correct even if the results table holds rows from other calls.
    """
    _require_valid_column(change_time_column)
    join_condition = " AND ".join(f"res.{key} = main.{key}" for key in keys)
    key_aliases = ", ".join(f"res.{key} AS _chg_key_{key}" for key in keys)
    return (
        f"(SELECT res.{change_time_column} AS _sync_change_ts, {key_aliases}, main.* "
        f"FROM {results_table} res LEFT JOIN {source_table} main ON {join_condition} "
        f"WHERE {_results_window(start, end, dialect, f'res.{change_time_column}')}) chg_source"
    )


def _results_window(start: str, end: str, dialect: SqlDialect, column: str) -> str:
    return (
        f"{column} >= {sql_timestamp_literal(start, dialect)} "
        f"AND {column} < {sql_timestamp_literal(end, dialect)}"
    )


class ChangeProcedureIngestionWorker(ChangeTableIngestionWorker):
    """Reads changes through the stored procedure in the config row's change-tracking profile.

    On SQL Server the procedure fills a temp table named by the caller; the changed keys come back
    over that one connection and the source rows are read in num_partitions key ranges. On Postgres
    it writes them to the profile's results_table. The watermark is the end of the last [start, end) window.
    """

    @property
    def procedure_call(self) -> str:
        return self.profile.procedure_call

    @property
    def change_time_column(self) -> str:
        return self.profile.change_time_column

    @property
    def multi_key(self) -> bool:
        """Tables keyed on more than one column; the procedure may need to be told to return all of them."""
        return len(self.config.key_column_list) > 1

    def get_last_watermark(self) -> str | None:
        last = parse_watermark(super().get_last_watermark())
        return format_watermark(last) if last else None

    def get_window_end(self) -> str:
        """Source server time minus watermark_threshold_minutes, so late commits are not skipped."""
        row = self.build_jdbc_reader(source_now_query(self.sql_dialect)).load().first()
        now = parse_watermark(row["now_ts"])
        if now is None:
            raise ValueError(f"Unexpected source timestamp format: {row['now_ts']!r}")
        return format_watermark(now - timedelta(minutes=self.config.watermark_threshold_minutes))

    def read_changes(self, start: str) -> tuple[object | None, str]:
        """(rows, window_end) for [start, window_end); rows is None when the window is empty."""
        _require_valid_names(self.config.source_table_name, self.config.key_columns)
        if self.sql_dialect == "sqlserver":
            if self.config.num_partitions > 1:
                return self.read_changed_rows_in_parallel(start)
            window_end = self.get_window_end()
            if window_end <= start:
                return None, window_end
            return self.read_changed_rows_on_source(start, window_end), window_end
        _require_supported_dialect(self.sql_dialect)
        return self.read_results_table_changes(start)

    # --- SQL Server ------------------------------------------------------------------------------------

    def _sqlserver_reader(self, dbtable: str, *partitioning, prepare_query: str, session_init: str):
        """A SQL Server read that runs the procedure once (session_init) and resolves its columns (prepare_query)."""
        reader = self.build_jdbc_reader(dbtable, *partitioning, prepare_query=prepare_query)
        return reader.option("sessionInitStatement", session_init)

    def read_changed_keys(self, start: str):
        """Changed keys, their change time, and the window end, count and key range, from one procedure run."""
        keys = self.config.key_column_list
        skip = self.config.skip_change_time
        temp_table = changed_rows_temp_table(uuid.uuid4().hex[:12])
        source_table = self.config.source_table_name
        return (
            self._sqlserver_reader(
                build_changed_keys_query(temp_table, keys, self.change_time_column, skip),
                prepare_query=build_temp_table_shape(temp_table, source_table, keys, self.change_time_column, skip),
                session_init=build_procedure_call_until_now(
                    self.procedure_call, source_table, start,
                    self.config.watermark_threshold_minutes, temp_table, self.multi_key,
                    keys[0], skip, self.config.change_columns,
                ),
            ).load()
        )

    def read_changed_rows_in_parallel(self, start: str) -> tuple[object | None, str]:
        """Changed keys over one connection, then the source rows in num_partitions key ranges, joined in Spark.

        When the first key column isn't an integer or the changed keys are too sparse, the rows are
        joined on the source over one connection instead.
        """
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }
        keys = self.config.key_column_list
        first_key = keys[0]
        fixed = self.fix_changed_keys(self.read_changed_keys(start))
        # One row with NULL keys carries the window end, count and key range computed on SQL Server.
        summary = fixed.where(F.col("_sh_window_end").isNotNull()).select(
            "_sh_window_end", "_sh_changed", "_sh_low", "_sh_high"
        ).first()
        window_end, changed = summary["_sh_window_end"], summary["_sh_changed"]
        low, high = summary["_sh_low"], summary["_sh_high"]
        if window_end <= start:
            return None, window_end
        changes = fixed.where(F.col(first_key).isNotNull()).drop("_sh_window_end", "_sh_changed", "_sh_low", "_sh_high")
        if self.config.skip_change_time:
            # Without a change time, the window end orders this window's changes after the previous one's.
            changes = changes.withColumn("_sync_change_ts", F.to_timestamp(F.lit(window_end)))

        if changed == 0:
            source = self.build_jdbc_reader(
                f"(SELECT * FROM {self.config.source_table_name} WHERE 1 = 0) src_range"
            ).load()
            return self._join_changes(changes, source, keys), window_end

        if not is_integral_type(changes.schema[first_key].dataType):
            logger.info(
                "[READ-PROC] %s — key %s is not an integer, joining on the source",
                self.config.source_table_name,
                first_key,
                extra=_ctx,
            )
            return self.read_changed_rows_on_source(start, window_end), window_end

        bounds = parallel_read_bounds(changed, low, high, self.config.num_partitions)
        if bounds is None:
            logger.info(
                "[READ-PROC] %s — %d changed keys spread over %s..%s, joining on the source",
                self.config.source_table_name,
                changed,
                low,
                high,
                extra=_ctx,
            )
            return self.read_changed_rows_on_source(start, window_end), window_end

        low, high = bounds
        logger.info(
            "[READ-PROC] %s — %d changed keys, reading %s in %s..%s with %d partitions",
            self.config.source_table_name,
            changed,
            first_key,
            low,
            high,
            self.config.num_partitions,
            extra=_ctx,
        )
        source = self.build_jdbc_reader(
            build_key_range_query(self.config.source_table_name, first_key, low, high),
            first_key,
            low,
            high,
        ).load()
        return self._join_changes(changes, source, keys), window_end

    def fix_changed_keys(self, changed_keys):
        """A fixed copy of the changed keys, used for both the read range and the join.

        Re-running the procedure for the join could return keys outside the read range,
        which would then be staged as deletes; it would also stream the keys a second time.
        localCheckpoint keeps them on the executors for this run, without a table; if those
        blocks are lost the run fails rather than reading the procedure again. The keys arrive in
        one partition (one connection), so they are spread first; one task storing them all is slow.
        """
        return changed_keys.repartition(max(self.config.num_partitions, 1)).localCheckpoint(eager=True)

    @staticmethod
    def _join_changes(changes, source, keys: list[str]):
        """Same columns as read_changed_rows_on_source; a key with no source row is a delete."""
        key_names = {key.lower() for key in keys}
        joined = changes.alias("chg").join(
            source.alias("main"),
            on=[F.col(f"chg.{key}") == F.col(f"main.{key}") for key in keys],
            how="left",
        )
        columns = [F.col("chg._sync_change_ts").alias("_sync_change_ts")]
        for column in source.columns:
            if column.lower() in key_names:
                columns.append(F.coalesce(F.col(f"main.{column}"), F.col(f"chg.{column}")).alias(column))
            else:
                columns.append(F.col(f"main.{column}").alias(column))
        columns.append(F.col(f"main.{keys[0]}").isNull().alias("_sync_deleted"))
        return joined.select(*columns)

    def read_changed_rows_on_source(self, start: str, end: str):
        """One connection runs the procedure once and joins its temp table to the source."""
        keys = self.config.key_column_list
        skip = self.config.skip_change_time
        temp_table = changed_rows_temp_table(uuid.uuid4().hex[:12])
        source_table = self.config.source_table_name
        # Single partition: each partition would run the procedure again on its own connection.
        df = self._sqlserver_reader(
            build_temp_table_rows_query(temp_table, source_table, keys, self.change_time_column, end if skip else None),
            prepare_query=build_temp_table_shape(temp_table, source_table, keys, self.change_time_column, skip),
            session_init=build_procedure_call(
                self.procedure_call, source_table, start, end, temp_table,
                self.multi_key, skip, self.config.change_columns,
            ),
        ).load()
        return self._restore_keys(df, keys)

    # --- Postgres -----------------------------------------------------------------------------------------

    @property
    def results_table(self) -> str:
        if not self.profile.results_table:
            raise ValueError(
                f"Profile '{self.profile.profile_name}' needs results_table for the '{self.sql_dialect}' dialect"
            )
        return render_name(self.profile.results_table, self.config.source_table_name, "results_table")

    def read_results_table_changes(self, start: str) -> tuple[object | None, str]:
        """Run the procedure once, then read its results table joined to the source in num_partitions key ranges."""
        keys = self.config.key_column_list
        source_table = self.config.source_table_name
        window_end = self.get_window_end()
        if window_end <= start:
            return None, window_end
        self.build_jdbc_reader(
            build_postgres_procedure_run(
                self.procedure_call, source_table, start, window_end, self.multi_key,
                self.config.skip_change_time, self.config.change_columns,
            )
        ).load().collect()
        stats = self.build_jdbc_reader(
            build_results_stats_query(
                self.results_table, keys[0], self.change_time_column, start, window_end, self.sql_dialect
            )
        ).load().first()
        query = build_results_rows_query(
            self.results_table, source_table, keys, self.change_time_column, start, window_end, self.sql_dialect
        )
        low, high = stats["low"], stats["high"]
        if self.config.num_partitions > 1 and stats["changed"] > 0 and is_integral_key(low) and low != high:
            reader = self.build_jdbc_reader(query, f"_chg_key_{keys[0]}", int(low), int(high))
        else:
            reader = self.build_jdbc_reader(query)
        return self._restore_keys(reader.load(), keys), window_end

    @staticmethod
    def _restore_keys(df, keys: list[str]):
        """A key with no source row is a delete; its key columns come from the procedure's output."""
        df = df.withColumn("_sync_deleted", F.col(keys[0]).isNull())
        for key in keys:
            df = df.withColumn(key, F.coalesce(F.col(key), F.col(f"_chg_key_{key}")))
        return df.drop(*[f"_chg_key_{key}" for key in keys])

    def read_source(self) -> ReadResult:
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }
        last_watermark = self.get_last_watermark()

        if self.effective_full_refresh or last_watermark is None:
            # Capture the window end before the read, so changes made during the full
            # load are picked up again by the next incremental window.
            window_end = self.get_window_end()
            logger.info(
                "[READ-PROC] %s — %s, performing full source read  window_end=%s",
                self.config.source_table_name,
                "full_refresh requested" if self.effective_full_refresh else "first run",
                window_end,
                extra=_ctx,
            )
            return ReadResult(df=self.read_full_source_jdbc(), persisted_watermark=window_end)

        df, window_end = self.read_changes(last_watermark)
        if df is None:
            logger.info(
                "[READ-PROC] %s — window is empty (last=%s, end=%s), skipping",
                self.config.source_table_name,
                last_watermark,
                window_end,
                extra=_ctx,
            )
            return ReadResult(df=None, skip=True)

        logger.info(
            "[READ-PROC] %s — incremental via profile %s  window=[%s, %s)",
            self.config.source_table_name,
            self.profile.profile_name,
            last_watermark,
            window_end,
            extra=_ctx,
        )
        return ReadResult(df=df, persisted_watermark=window_end)
