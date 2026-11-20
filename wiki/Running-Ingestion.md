# Running Ingestion

### From a notebook

```python
from synchole.ingestion import JdbcIngestionOrchestrator
from synchole.models import JdbcRuntimeSettings

runtime = JdbcRuntimeSettings(
    configuration_fqn="main.synchole.jdbc_ingestion_config",
    watermark_fqn="main.synchole.jdbc_ingestion_watermark",
    ingestion_group="sales",
    source_table_name="",   # empty = all enabled tables in the group
    full_refresh=False,
)

orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
results = orchestrator.run_selected_ingestion()
```

Connection settings come from each config row, not from `JdbcRuntimeSettings`. The orchestrator raises after processing every table if any of them failed.

`notebooks/SyncHole Notebook Runner` wraps the same calls behind widgets (`plan_configs`, `run_ingestion`, `create_job`, `setup_uc`, `setup_lakebase`).

### Structured logs with ZeroBus

Every `synchole.*` logger can also stream its records to a Delta table through ZeroBus Ingest, so a run can be inspected with SQL after it ends. This is opt-in: the generated jobs and notebooks do not turn it on. Install the extra (`%pip install "synchole[zerobus]"`) and wrap the run:

```python
from synchole.utils import setup_synchole_logging

handler = setup_synchole_logging(
    server_endpoint="https://<id>.zerobus.<region>.cloud.databricks.com",
    workspace_url="https://<workspace>.cloud.databricks.com",
    table_name="main.synchole.synchole_logs",  # created by UnityCatalogSetup.create_all()
    client_id=dbutils.secrets.get(scope="synchole", key="zb_client_id"),
    client_secret=dbutils.secrets.get(scope="synchole", key="zb_client_secret"),
)
try:
    JdbcIngestionOrchestrator(spark=spark, runtime=runtime).run_selected_ingestion()
finally:
    handler.close()  # flushes and closes the stream
```

- **Authentication**, first match wins:
  - `client_id` and `client_secret` of a service principal (OAuth M2M), which works on serverless and classic compute and is recommended;
  - `token_provider`, a function returning a bearer token on each call, which works on classic interactive clusters;
  - neither, which falls back to the runtime's OAuth credentials, for jobs that run as a service principal.
- **Sending is fire-and-forget,** so logging never blocks ingestion. `console=True` (the default) also prints records to the notebook. Records are filtered by `level` (default `INFO`), and `run_id` is generated when not given (read it from `handler.run_id`).
- **Table layout:** each record is one row of `run_id STRING`, `log_timestamp STRING` (UTC, ISO 8601) and `payload VARIANT`. `payload` holds `level`, `logger_name`, `message` (`text`, `module`, `func`, `lineno`, `exc`), and `ingestion_group` and `source_table` when the record has them. New fields need no schema change.

```sql
-- Errors of one run
SELECT log_timestamp,
       payload:source_table::STRING AS source_table,
       payload:message:text::STRING AS message
FROM main.synchole.synchole_logs
WHERE run_id = '<run_id>' AND payload:level::STRING = 'ERROR'
ORDER BY log_timestamp;
```

### Via the CLI

```bash
synchole-run-ingestion \
  --configuration-fqn main.synchole.jdbc_ingestion_config \
  --watermark-fqn main.synchole.jdbc_ingestion_watermark \
  --ingestion-group sales \
  --source-table-name dbo.SALES_ORDER   # optional
```

### Fan-out planning

The planner returns one for-each input per enabled table:

```python
from synchole.config_planner import IngestionConfigPlanner

planner = IngestionConfigPlanner(spark=spark, runtime=runtime)
inputs = planner.build_for_each_inputs(ingestion_group="sales")
# [{"ingestion_group": "sales", "source_table_name": "dbo.SHIPMENT_EVENT"}, ...]
```

Or with task-value publishing inside a Lakeflow Job task:

```bash
synchole-plan-configs \
  --configuration-fqn main.synchole.jdbc_ingestion_config \
  --watermark-fqn main.synchole.jdbc_ingestion_watermark \
  --ingestion-group sales \
  --publish-task-value true
```

## Worker Types

### `TimestampWatermarkIngestionWorker`

Default worker, selected when `change_tracking = 'watermark'` (or empty).

- **Incremental** — runs an existence check, then reads rows where `watermark_column > last_watermark` and `<= now() - watermark_threshold_minutes`, appends them to staging, and stores the max watermark in staging
- **Full** — reads the whole table and overwrites `target_table_fqn` (or staging when `auto_cdc_from_snapshot` is set). No watermark is tracked, so `watermark_column` is not used
- Supports a numeric `partition_column` or string `predicate_column` for parallel reads

### `ChangeTableIngestionWorker`

Selected when `change_tracking = 'change_table'`. Many source systems list the rows that changed in a change table of their own instead of keeping a reliable timestamp on each row. The config row's [change-tracking profile](Change-Tracking-Profiles) says how that change table is named and which of its columns to read, so no system's names are built into SyncHole. The config row's connection must be able to read the change tables.

- The change table is the profile's `change_table_name` template filled in for `source_table_name` (e.g. `audit.{table_lower}_changes` and `dbo.SALES_ORDER` → `audit.sales_order_changes`)
- The watermark is the profile's `sequence_column` (cast to BIGINT), which must only grow
- **First run** (no stored watermark) — full read of the source table
- **Later runs** — read the sequence's `MAX`; skip if it has not advanced, otherwise read the change rows in `(last, max]` `LEFT JOIN`ed to the source table on `key_columns`, and append them to staging with `_sync_deleted` (from `delete_flag_column`) and `_sync_change_ts` (from `change_time_column`)
- Deleted rows keep their keys from the change table, so the CDC pipeline can apply them as deletes
- `key_columns` is required; `watermark_column` is ignored

### `ChangeProcedureIngestionWorker`

Selected when `change_tracking = 'change_procedure'`. Some source systems instead offer a stored procedure that lists the keys changed in a time window. The profile's `procedure_call` template runs it for each window `[last, now - watermark_threshold_minutes)`, and the worker reads the changed rows from the source table; a key with no source row is staged as a delete (`_sync_deleted`), so the same CDC pipeline applies the changes.

- **SQL Server** — the procedure fills a global temp table that SyncHole names (`{temp_table}`). Its keys and change times (`change_time_column`) come back over one connection, and the source rows are read in `num_partitions` key ranges when the first key is an integer and the changed keys are dense enough; otherwise they are joined on the source over one connection
- **Postgres** — the procedure writes its results to the profile's `results_table`, which is then read joined to the source
- The watermark is the end of the last window, in whole seconds
- `key_columns` is required and must be plain column names, as must `source_table_name`
