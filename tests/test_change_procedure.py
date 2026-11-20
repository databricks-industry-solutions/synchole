from __future__ import annotations

import sys
import types
import unittest
from dataclasses import replace
from decimal import Decimal

delta_tables = types.ModuleType("delta.tables")
delta_tables.DeltaTable = object
sys.modules.setdefault("delta.tables", delta_tables)

from synchole.change_tracking import (
    is_valid_column_names,
    is_valid_name,
    render_name,
    validate_change_tracking,
    validate_profile,
)
from synchole.ingestion import JdbcIngestionOrchestrator, profile_from_row
from synchole.models import ChangeTrackingProfile, IngestionConfig, JdbcRuntimeSettings
from synchole.workers import (
    ChangeProcedureIngestionWorker,
    ChangeTableIngestionWorker,
    TimestampWatermarkIngestionWorker,
)
from synchole.workers.change_procedure.worker import (
    build_changed_keys_query,
    build_key_range_query,
    build_postgres_procedure_run,
    build_procedure_call,
    build_procedure_call_until_now,
    build_results_rows_query,
    build_results_stats_query,
    build_temp_table_rows_query,
    build_temp_table_shape,
    changed_rows_temp_table,
    format_watermark,
    is_integral_key,
    is_integral_type,
    parallel_read_bounds,
    parse_watermark,
    render_procedure_call,
    source_now_query,
)

RUNTIME = JdbcRuntimeSettings(
    configuration_fqn="main.synchole.jdbc_ingestion_config",
    watermark_fqn="main.synchole.jdbc_ingestion_watermark",
)
# A made-up SQL Server procedure: every system names its parameters differently.
PROCEDURE_CALL = (
    "EXEC sync.list_changed_rows @table = N'{table}', @from = {start}, @to = {end}, "
    "@into = N'{temp_table}', @all_keys = {multi_key}, @no_time = {skip_change_time}, @only = {columns}"
)
PROCEDURE_PROFILE = ChangeTrackingProfile(
    profile_name="sqlserver-proc",
    method="change_procedure",
    change_time_column="CHANGED_AT",
    procedure_call=PROCEDURE_CALL,
)
TABLE_PROFILE = ChangeTrackingProfile(
    profile_name="change-tables",
    method="change_table",
    change_table_name="audit.chg_{table_lower}",
    sequence_column="CHANGE_SEQ",
    delete_flag_column="IS_DELETED",
    change_time_column="CHANGED_AT",
)
CONFIG = IngestionConfig(
    ingestion_group="sales",
    source_table_name="dbo.SALES_ORDER",
    staging_table_fqn="main.synchole.stg_sales_order",
    target_table_fqn="main.sales.sales_order",
    ingestion_type="incremental",
    key_columns="SALES_ORDER_ID,LINE",
    watermark_column="",
    partition_column="",
    predicate_column="",
    change_tracking="change_procedure",
    change_profile="sqlserver-proc",
    jdbc_url="jdbc:sqlserver://host:1433;databaseName=sourcedb",
    profile=PROCEDURE_PROFILE,
)
TABLE_CONFIG = replace(CONFIG, change_tracking="change_table", change_profile="change-tables", profile=TABLE_PROFILE)


class ProcedureSqlTest(unittest.TestCase):
    def test_sqlserver_call_fills_the_profile_template(self) -> None:
        temp = changed_rows_temp_table("abc")
        self.assertEqual(temp, "##sh_chg_abc")
        self.assertEqual(
            build_procedure_call(PROCEDURE_CALL, "dbo.SALES_ORDER", "2026-11-01 00:00:00", "2026-11-02 00:00:00", temp, False),
            "SET NOCOUNT ON; EXEC sync.list_changed_rows @table = N'SALES_ORDER', @from = '2026-11-01T00:00:00', "
            "@to = '2026-11-02T00:00:00', @into = N'##sh_chg_abc', @all_keys = 0, @no_time = 0, @only = NULL;",
        )

    def test_multi_key_tables_skip_change_time_and_columns(self) -> None:
        call = build_procedure_call(
            PROCEDURE_CALL, "dbo.SALES_ORDER", "a", "b", "##t", True,
            skip_change_time=True, columns="ACCOUNT_ID,ADMISSION_ID",
        )
        self.assertTrue(call.endswith("@all_keys = 1, @no_time = 1, @only = 'ACCOUNT_ID,ADMISSION_ID';"))

    def test_template_placeholders_for_the_schema_and_lower_case_name(self) -> None:
        self.assertEqual(
            render_procedure_call("CALL x('{schema}', '{table}', '{table_lower}', {start}, {end})", "sourcedb.dbo.ORDERS", "S", "E"),
            "CALL x('dbo', 'ORDERS', 'orders', S, E)",
        )
        # A trailing semicolon in the template is dropped: the worker adds its own.
        self.assertEqual(render_procedure_call("EXEC p {start}, {end};", "dbo.T", "S", "E"), "EXEC p S, E")

    def test_procedure_call_computes_window_end_and_summary_on_the_server(self) -> None:
        self.assertEqual(
            build_procedure_call_until_now(
                PROCEDURE_CALL, "dbo.SALES_ORDER", "2026-11-03 14:56:44", 5, "##sh_chg_abc", False, "SALES_ORDER_ID",
            ),
            "SET NOCOUNT ON; DECLARE @sh_window_end DATETIME2(0) = CONVERT(DATETIME2(0), CONVERT(VARCHAR(19), "
            "DATEADD(MINUTE, -5, SYSDATETIME()), 120)); "
            "IF @sh_window_end < '2026-11-03T14:56:44' SET @sh_window_end = '2026-11-03T14:56:44'; "
            "EXEC sync.list_changed_rows @table = N'SALES_ORDER', @from = '2026-11-03T14:56:44', "
            "@to = @sh_window_end, @into = N'##sh_chg_abc', @all_keys = 0, @no_time = 0, @only = NULL; "
            "DECLARE @sh_changed BIGINT, @sh_low BIGINT, @sh_high BIGINT; "
            "SELECT @sh_changed = COUNT_BIG(*), @sh_low = TRY_CAST(MIN(SALES_ORDER_ID) AS BIGINT), "
            "@sh_high = TRY_CAST(MAX(SALES_ORDER_ID) AS BIGINT) FROM ##sh_chg_abc; "
            "EXEC sp_set_session_context @key = N'sh_window_end', @value = @sh_window_end; "
            "EXEC sp_set_session_context @key = N'sh_changed', @value = @sh_changed; "
            "EXEC sp_set_session_context @key = N'sh_low', @value = @sh_low; "
            "EXEC sp_set_session_context @key = N'sh_high', @value = @sh_high;",
        )

    def test_changed_keys_query_adds_one_typed_summary_row(self) -> None:
        self.assertEqual(
            build_changed_keys_query("##sh_chg_abc", ["SALES_ORDER_ID", "LINE"], "CHANGED_AT", False),
            "(SELECT chg.SALES_ORDER_ID, chg.LINE, chg.CHANGED_AT AS _sync_change_ts, "
            "CAST(NULL AS VARCHAR(19)) AS _sh_window_end, CAST(NULL AS BIGINT) AS _sh_changed, "
            "CAST(NULL AS BIGINT) AS _sh_low, CAST(NULL AS BIGINT) AS _sh_high FROM ##sh_chg_abc chg "
            "UNION ALL SELECT chg.SALES_ORDER_ID, chg.LINE, chg.CHANGED_AT AS _sync_change_ts, "
            "CONVERT(VARCHAR(19), CAST(SESSION_CONTEXT(N'sh_window_end') AS DATETIME2(0)), 120), "
            "CAST(SESSION_CONTEXT(N'sh_changed') AS BIGINT), CAST(SESSION_CONTEXT(N'sh_low') AS BIGINT), "
            "CAST(SESSION_CONTEXT(N'sh_high') AS BIGINT) "
            "FROM (SELECT 1 AS one) o LEFT JOIN ##sh_chg_abc chg ON 1 = 0) chg_keys",
        )
        skipped = build_changed_keys_query("##sh_chg_abc", ["SALES_ORDER_ID"], "CHANGED_AT", True)
        self.assertNotIn("CHANGED_AT", skipped)

    def test_temp_table_shape_only_creates_an_empty_table(self) -> None:
        self.assertEqual(
            build_temp_table_shape("##sh_chg_abc", "dbo.SALES_ORDER", ["SALES_ORDER_ID", "LINE"], "CHANGED_AT", False),
            "SET NOCOUNT ON; IF OBJECT_ID('tempdb..##sh_chg_abc') IS NULL "
            "SELECT TOP 0 SALES_ORDER_ID, LINE, CAST(NULL AS DATETIME) AS CHANGED_AT INTO ##sh_chg_abc FROM dbo.SALES_ORDER;",
        )
        self.assertNotIn(
            "CHANGED_AT", build_temp_table_shape("##t", "dbo.SALES_ORDER", ["SALES_ORDER_ID"], "CHANGED_AT", True)
        )

    def test_temp_table_rows_query_joins_composite_key(self) -> None:
        self.assertEqual(
            build_temp_table_rows_query("##sh_chg_abc", "dbo.SALES_ORDER", ["SALES_ORDER_ID", "LINE"], "CHANGED_AT"),
            "(SELECT chg.CHANGED_AT AS _sync_change_ts, "
            "chg.SALES_ORDER_ID AS _chg_key_SALES_ORDER_ID, chg.LINE AS _chg_key_LINE, main.* "
            "FROM ##sh_chg_abc chg "
            "LEFT JOIN dbo.SALES_ORDER main ON chg.SALES_ORDER_ID = main.SALES_ORDER_ID "
            "AND chg.LINE = main.LINE) chg_source",
        )
        self.assertIn(
            "CAST('2026-11-03T15:00:00' AS DATETIME2(0)) AS _sync_change_ts",
            build_temp_table_rows_query("##t", "dbo.SALES_ORDER", ["SALES_ORDER_ID"], "CHANGED_AT", "2026-11-03 15:00:00"),
        )

    def test_key_range_query_and_bounds(self) -> None:
        self.assertEqual(
            build_key_range_query("dbo.SALES_ORDER", "SALES_ORDER_ID", 10, 99),
            "(SELECT * FROM dbo.SALES_ORDER WHERE SALES_ORDER_ID >= 10 AND SALES_ORDER_ID <= 99) src_range",
        )
        self.assertEqual(parallel_read_bounds(5_000_000, 1, 5_000_000, 8), (1, 5_000_000))
        # 1,000 changed keys over a 1M-wide range: too sparse for a range read.
        self.assertIsNone(parallel_read_bounds(1_000, 1, 1_000_000, 8))
        self.assertIsNone(parallel_read_bounds(5_000_000, 1, 5_000_000, 1))
        self.assertIsNone(parallel_read_bounds(0, None, None, 8))

    def test_postgres_runs_the_template_and_reads_the_results_table(self) -> None:
        template = "sync_test.list_changes_run('{schema}.{table}', {start}, {end}, {multi_key})"
        self.assertEqual(
            build_postgres_procedure_run(template, "dbo.orders", "2026-11-03 14:56:44", "2026-11-03 15:00:00", True),
            "(SELECT sync_test.list_changes_run('dbo.orders', CAST('2026-11-03 14:56:44' AS TIMESTAMP), "
            "CAST('2026-11-03 15:00:00' AS TIMESTAMP), 1) AS procedure_result) procedure_run",
        )
        self.assertEqual(
            render_name("sync_test.changed_{table_lower}", "dbo.ORDERS", "results_table"), "sync_test.changed_orders"
        )
        self.assertEqual(
            build_results_stats_query(
                "sync_test.changed_orders", "order_id", "changed_at", "2026-11-03 14:56:44", "2026-11-03 15:00:00",
                "postgres",
            ),
            "(SELECT COUNT(*) AS changed, MIN(order_id) AS low, MAX(order_id) AS high FROM sync_test.changed_orders "
            "WHERE changed_at >= CAST('2026-11-03 14:56:44' AS TIMESTAMP) "
            "AND changed_at < CAST('2026-11-03 15:00:00' AS TIMESTAMP)) results_stats",
        )
        self.assertIn(
            "FROM sync_test.changed_orders res LEFT JOIN dbo.orders main ON res.order_id = main.order_id "
            "AND res.line_id = main.line_id WHERE res.changed_at >= CAST('a' AS TIMESTAMP)",
            build_results_rows_query(
                "sync_test.changed_orders", "dbo.orders", ["order_id", "line_id"], "changed_at", "a", "b", "postgres"
            ),
        )

    def test_values_put_into_the_template_are_quoted_or_validated(self) -> None:
        self.assertIn("@table = N'SALES_ORDER'", build_procedure_call(PROCEDURE_CALL, "dbo.SALES_ORDER", "a", "b", "##t", False))
        self.assertIn("@from = 'o''clock'", build_procedure_call(PROCEDURE_CALL, "dbo.T", "o'clock", "b", "##t", False))
        self.assertTrue(is_valid_column_names(""))
        self.assertTrue(is_valid_column_names("ACCOUNT_ID, ADMISSION_ID"))
        self.assertFalse(is_valid_column_names("A'; DROP TABLE x --"))
        with self.assertRaises(ValueError):
            build_procedure_call(PROCEDURE_CALL, "dbo.T", "a", "b", "##t", False, columns="A'--")
        with self.assertRaises(ValueError):
            build_procedure_call(PROCEDURE_CALL, "dbo.T", "a", "b", "##t'; DROP TABLE x --", False)
        with self.assertRaises(ValueError):
            build_temp_table_shape("##t", "dbo.T", ["K"], "X; DROP TABLE y", False)
        with self.assertRaises(ValueError):
            source_now_query("oracle")

    def test_integral_key_types(self) -> None:
        from pyspark.sql import types as T

        self.assertTrue(is_integral_type(T.IntegerType()))
        self.assertTrue(is_integral_type(T.DecimalType(18, 0)))
        self.assertFalse(is_integral_type(T.DecimalType(18, 2)))
        self.assertFalse(is_integral_type(T.StringType()))

    def test_integral_keys(self) -> None:
        self.assertTrue(is_integral_key(42))
        self.assertTrue(is_integral_key(Decimal("42")))
        self.assertFalse(is_integral_key(Decimal("4.2")))
        self.assertFalse(is_integral_key("Z1234"))
        self.assertFalse(is_integral_key(True))

    def test_names(self) -> None:
        for name in ("LIST_CHANGES", "dbo.LIST_CHANGES", "sourcedb.dbo.LIST_CHANGES"):
            self.assertTrue(is_valid_name(name), name)
        for name in ("", "dbo.", "a.b.c.d", "dbo.x; DROP TABLE y", "x'--"):
            self.assertFalse(is_valid_name(name), name)

    def test_change_table_sequence_is_not_a_watermark(self) -> None:
        self.assertIsNone(parse_watermark("1730000000000000"))
        self.assertIsNone(parse_watermark(None))
        self.assertIsNotNone(parse_watermark("2026-11-02 10:00:00.123"))

    def test_watermark_survives_state_table_truncation(self) -> None:
        # update_watermark_state stores str(watermark)[:19]
        stored = format_watermark(parse_watermark("2026-11-02 10:00:00.987"))[:19]
        self.assertEqual(stored, "2026-11-02 10:00:00")
        self.assertEqual(parse_watermark(stored), parse_watermark("2026-11-02 10:00:00"))


class ProfileValidationTest(unittest.TestCase):
    def test_valid_profiles(self) -> None:
        validate_profile(PROCEDURE_PROFILE)
        validate_profile(TABLE_PROFILE)
        validate_profile(
            replace(PROCEDURE_PROFILE, procedure_call="x.run({start}, {end})", results_table="x.out_{table}")
        )

    def test_invalid_profiles(self) -> None:
        for profile in (
            replace(PROCEDURE_PROFILE, method="cdc"),
            replace(PROCEDURE_PROFILE, change_time_column=""),
            replace(PROCEDURE_PROFILE, change_time_column="A; DROP TABLE x"),
            replace(PROCEDURE_PROFILE, procedure_call=""),
            replace(PROCEDURE_PROFILE, procedure_call="EXEC p {start}, N'{temp_table}'"),
            replace(PROCEDURE_PROFILE, procedure_call="EXEC p {start}, {end}, {password}, N'{temp_table}'"),
            # Neither a temp table to fill nor a results table to read.
            replace(PROCEDURE_PROFILE, procedure_call="EXEC p {start}, {end}"),
            replace(TABLE_PROFILE, change_table_name=""),
            replace(TABLE_PROFILE, change_table_name="audit.all_changes"),
            replace(TABLE_PROFILE, change_table_name="audit.chg_{name}"),
            replace(TABLE_PROFILE, sequence_column=""),
            replace(TABLE_PROFILE, delete_flag_column="x y"),
        ):
            with self.assertRaises(ValueError, msg=profile):
                validate_profile(profile)

    def test_change_table_name_template(self) -> None:
        self.assertEqual(render_name("audit.chg_{table_lower}", "dbo.SALES_ORDER", "f"), "audit.chg_sales_order")
        self.assertEqual(render_name("{schema}.{table}_CT", "sourcedb.dbo.SALES_ORDER", "f"), "dbo.SALES_ORDER_CT")
        with self.assertRaises(ValueError):
            render_name("audit.chg_{table}", "dbo.[ENC OUNTER]", "f")

    def test_profile_from_row_reads_every_column(self) -> None:
        row = {
            "profile_name": " p ", "method": "CHANGE_TABLE", "change_table_name": "a.c_{table}",
            "sequence_column": "S", "delete_flag_column": "D", "change_time_column": "T", "description": "x",
        }
        self.assertEqual(
            profile_from_row(row),
            ChangeTrackingProfile("p", "change_table", "a.c_{table}", "S", "D", "T", "", ""),
        )


class ConfigValidationTest(unittest.TestCase):
    def test_valid_configs(self) -> None:
        validate_change_tracking(CONFIG)
        validate_change_tracking(TABLE_CONFIG)
        validate_change_tracking(replace(CONFIG, change_tracking="watermark", change_profile="", profile=None))
        validate_change_tracking(replace(CONFIG, change_columns="ACCOUNT_ID,ADMISSION_ID"))

    def test_invalid_configs(self) -> None:
        for config in (
            replace(CONFIG, change_tracking="cdc"),
            replace(CONFIG, ingestion_type="full"),
            replace(CONFIG, key_columns=""),
            replace(CONFIG, change_profile=""),
            # The profile row doesn't exist.
            replace(CONFIG, profile=None),
            # The profile is for the other method.
            replace(CONFIG, profile=TABLE_PROFILE),
            replace(CONFIG, change_columns="A; DROP TABLE x"),
        ):
            with self.assertRaises(ValueError, msg=config):
                validate_change_tracking(config)

    def test_table_and_keys_must_be_plain_names_with_a_procedure(self) -> None:
        # The procedure's batch can run several statements, so these never reach it.
        for config in (
            replace(CONFIG, source_table_name="dbo.SALES_ORDER; DROP TABLE x"),
            replace(CONFIG, source_table_name="[dbo].[ENC OUNTER]"),
            replace(CONFIG, key_columns="SALES_ORDER_ID) AND 1=1; EXEC x; SELECT (SALES_ORDER_ID"),
        ):
            with self.assertRaises(ValueError, msg=config):
                validate_change_tracking(config)
            with self.assertRaises(ValueError, msg=config):
                ChangeProcedureIngestionWorker(None, RUNTIME, config).read_changes("2026-01-01 00:00:00")
        validate_change_tracking(replace(CONFIG, source_table_name="sourcedb.dbo.SALES_ORDER", key_columns="SALES_ORDER_ID, LINE"))

    def test_build_worker_dispatch(self) -> None:
        orchestrator = JdbcIngestionOrchestrator(spark=None, runtime=RUNTIME)
        self.assertIsInstance(orchestrator.build_worker(CONFIG), ChangeProcedureIngestionWorker)
        self.assertIs(type(orchestrator.build_worker(TABLE_CONFIG)), ChangeTableIngestionWorker)
        self.assertIsInstance(
            orchestrator.build_worker(replace(CONFIG, change_tracking="watermark", profile=None)),
            TimestampWatermarkIngestionWorker,
        )

    def test_profiles_table_is_next_to_the_config_table(self) -> None:
        self.assertEqual(RUNTIME.profiles_table, "main.synchole.change_tracking_profiles")


class _QueryCapturingChangeTableWorker(ChangeTableIngestionWorker):
    sql_dialect = "sqlserver"

    def __init__(self) -> None:
        super().__init__(None, RUNTIME, replace(TABLE_CONFIG, key_columns="SALES_ORDER_ID"))
        self.queries: list[str] = []

    def build_jdbc_reader(self, query, *args, **kwargs):
        self.queries.append(query)
        raise StopIteration


class ChangeTableSqlTest(unittest.TestCase):
    def test_change_table_read_uses_the_profile_columns(self) -> None:
        worker = _QueryCapturingChangeTableWorker()
        self.assertEqual(worker.derive_change_table_name(), "audit.chg_sales_order")
        with self.assertRaises(StopIteration):
            worker.read_source_jdbc_via_change_table(10, 20)
        self.assertEqual(
            worker.queries[0],
            "(SELECT chg.IS_DELETED AS _sync_deleted, chg.CHANGED_AT AS _sync_change_ts, "
            "chg.SALES_ORDER_ID AS _chg_key_SALES_ORDER_ID, main.* FROM audit.chg_sales_order chg "
            "LEFT JOIN dbo.SALES_ORDER main ON chg.SALES_ORDER_ID = main.SALES_ORDER_ID "
            "WHERE CAST(chg.CHANGE_SEQ AS BIGINT) > 10 AND CAST(chg.CHANGE_SEQ AS BIGINT) <= 20) chg_source",
        )

    def test_max_watermark_reads_the_sequence_column(self) -> None:
        worker = _QueryCapturingChangeTableWorker()
        with self.assertRaises(StopIteration):
            worker.get_change_max_watermark()
        self.assertEqual(
            worker.queries[0],
            "(SELECT MAX(CAST(CHANGE_SEQ AS BIGINT)) AS max_change_wm FROM audit.chg_sales_order) change_max",
        )

    def test_a_config_without_a_profile_fails_clearly(self) -> None:
        worker = ChangeTableIngestionWorker(None, RUNTIME, replace(TABLE_CONFIG, profile=None))
        with self.assertRaisesRegex(ValueError, "no change-tracking profile"):
            worker.derive_change_table_name()


class StubWorker(ChangeProcedureIngestionWorker):
    def __init__(self, last_watermark: str | None, window_end: str, full_refresh: bool = False) -> None:
        super().__init__(None, RUNTIME, CONFIG)
        self.__dict__["effective_full_refresh"] = full_refresh
        self._last_watermark = last_watermark
        self._window_end = window_end
        self.calls: list[tuple] = []

    def get_last_watermark(self) -> str | None:
        return self._last_watermark

    def get_window_end(self) -> str:
        self.calls.append(("clock",))
        return self._window_end

    def read_full_source_jdbc(self):
        self.calls.append(("full",))
        return "full_df"

    def read_changes(self, start: str):
        # The real read gets the window end from the procedure's statement.
        self.calls.append(("changes", start))
        return ("changed_df" if self._window_end > start else None), self._window_end


class ReadSourceTest(unittest.TestCase):
    def test_first_run_is_full_load_ending_at_window_end(self) -> None:
        worker = StubWorker(None, "2026-11-02 10:00:00")
        result = worker.read_source()
        self.assertEqual(worker.calls, [("clock",), ("full",)])
        self.assertEqual(result.persisted_watermark, "2026-11-02 10:00:00")

    def test_full_refresh_overrides_watermark(self) -> None:
        worker = StubWorker("2026-11-02 09:00:00", "2026-11-02 10:00:00", full_refresh=True)
        worker.read_source()
        self.assertEqual(worker.calls, [("clock",), ("full",)])

    def test_incremental_reads_changes_without_a_clock_query(self) -> None:
        worker = StubWorker("2026-11-02 09:00:00", "2026-11-02 10:00:00")
        result = worker.read_source()
        self.assertEqual(worker.calls, [("changes", "2026-11-02 09:00:00")])
        self.assertEqual(result.df, "changed_df")
        self.assertEqual(result.persisted_watermark, "2026-11-02 10:00:00")

    def test_empty_window_is_skipped(self) -> None:
        worker = StubWorker("2026-11-02 10:00:00", "2026-11-02 10:00:00")
        self.assertTrue(worker.read_source().skip)
        self.assertEqual(worker.calls, [("changes", "2026-11-02 10:00:00")])


class _ClockCountingWorker(ChangeProcedureIngestionWorker):
    """SQL Server with one partition: a clock query, then the procedure joined on the source."""

    sql_dialect = "sqlserver"

    def __init__(self, window_end: str) -> None:
        super().__init__(None, RUNTIME, replace(CONFIG, num_partitions=1))
        self._window_end = window_end
        self.calls: list[tuple] = []

    def get_window_end(self) -> str:
        self.calls.append(("clock",))
        return self._window_end

    def read_changed_rows_on_source(self, start: str, end: str):
        self.calls.append(("on_source", start, end))
        return "changed_df"


class _PostgresWorker(ChangeProcedureIngestionWorker):
    """Postgres: the clock, then the procedure, then the results table."""

    sql_dialect = "postgres"

    def __init__(self, window_end: str, results_table: str = "sync_test.changed_{table_lower}") -> None:
        profile = replace(
            PROCEDURE_PROFILE,
            procedure_call="sync_test.list_changes_run('{schema}.{table}', {start}, {end}, {multi_key})",
            results_table=results_table,
        )
        super().__init__(None, RUNTIME, replace(CONFIG, profile=profile))
        self._window_end = window_end
        self.queries: list[str] = []

    def get_window_end(self) -> str:
        return self._window_end

    def build_jdbc_reader(self, query, *args, **kwargs):
        self.queries.append(query)
        raise StopIteration


class ReadChangesTest(unittest.TestCase):
    def test_single_partition_sqlserver_uses_clock_then_joins_on_source(self) -> None:
        worker = _ClockCountingWorker("2026-11-02 10:00:00")
        self.assertEqual(worker.read_changes("2026-11-02 09:00:00"), ("changed_df", "2026-11-02 10:00:00"))
        self.assertEqual(worker.calls, [("clock",), ("on_source", "2026-11-02 09:00:00", "2026-11-02 10:00:00")])

    def test_single_partition_empty_window(self) -> None:
        worker = _ClockCountingWorker("2026-11-02 09:00:00")
        self.assertEqual(worker.read_changes("2026-11-02 09:00:00"), (None, "2026-11-02 09:00:00"))
        self.assertEqual(worker.calls, [("clock",)])

    def test_postgres_runs_the_procedure_for_the_window(self) -> None:
        worker = _PostgresWorker("2026-11-02 10:00:00")
        with self.assertRaises(StopIteration):
            worker.read_changes("2026-11-02 09:00:00")
        self.assertIn("sync_test.list_changes_run('dbo.SALES_ORDER', CAST('2026-11-02 09:00:00'", worker.queries[0])

    def test_postgres_empty_window_does_not_run_the_procedure(self) -> None:
        worker = _PostgresWorker("2026-11-02 09:00:00")
        self.assertEqual(worker.read_changes("2026-11-02 09:00:00"), (None, "2026-11-02 09:00:00"))
        self.assertEqual(worker.queries, [])

    def test_postgres_needs_a_results_table(self) -> None:
        with self.assertRaisesRegex(ValueError, "needs results_table"):
            _PostgresWorker("2026-11-02 10:00:00", results_table="").results_table


if __name__ == "__main__":
    unittest.main()
