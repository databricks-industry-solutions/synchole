# Change-Tracking Profiles

Some source systems don't keep a reliable timestamp on every row. Instead they record
changes themselves: in a **change table** per source table, or through a **change procedure** that lists the keys
changed in a time window. A change-tracking profile describes how one source system does it, so SyncHole can read
those changes without any system's names built into the code.

Profiles live in `change_tracking_profiles`, next to `jdbc_ingestion_config`. A config row uses one with
`change_tracking = 'change_table'` or `'change_procedure'` and `change_profile = '<profile_name>'`; every table of
the same source system shares the same profile. Manage profiles on the **Configuration** page of the Control Plane
app, in an [import file](Import-and-Export) (`profiles`), or with SQL.

The names to put in a profile come from the source system's own documentation: which change tables exist and what
their columns are called, or which procedure lists the changes and what its parameters are. Check a new profile on
a test copy of the source system before using it in production.

## Columns

| Column               | Method             | Description                                                                                   |
| -------------------- | ------------------ | --------------------------------------------------------------------------------------------- |
| `profile_name`       | both               | The name config rows use in `change_profile`                                                  |
| `description`        | both               | What source system and setup the profile is for                                               |
| `method`             | both               | `change_table` or `change_procedure`                                                          |
| `change_table_name`  | `change_table`     | The change table of each source table, as a [name template](#name-templates)                  |
| `sequence_column`    | `change_table`     | Column of the change table that only grows; SyncHole keeps its last value as the watermark    |
| `delete_flag_column` | `change_table`     | Column of the change table that is true (or non-zero) when the row was deleted                |
| `change_time_column` | both               | When the row changed: a column of the change table, or of the procedure's results             |
| `procedure_call`     | `change_procedure` | The SQL that runs the procedure for one window, as a [call template](#call-templates)         |
| `results_table`      | `change_procedure` | Where the procedure writes its results, when it doesn't fill `{temp_table}` (a name template) |

Column names must be plain names. A change table must list the source table's `key_columns` under the same names.

## Name templates

`change_table_name` and `results_table` name a table for each source table. They can use:

| Placeholder     | Value for `sourcedb.dbo.SalesOrder` |
| --------------- | ----------------------------------- |
| `{schema}`      | `dbo`                               |
| `{table}`       | `SalesOrder`                        |
| `{table_lower}` | `salesorder`                        |

For example `audit.{table_lower}_changes` gives `audit.salesorder_changes`. A change table template must contain
`{table}` or `{table_lower}`, so each source table has its own change table.

## Call templates

`procedure_call` runs the procedure for the window `[start, end)` and can use the name placeholders above plus:

| Placeholder          | Value                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------ |
| `{start}`, `{end}`   | The window, as timestamp literals (`'2026-11-01T00:00:00'` on SQL Server); required        |
| `{temp_table}`       | SQL Server: the global temp table (`##sh_chg_…`) the procedure must fill with the changed keys and `change_time_column` |

Every value is a validated name, a number or a quoted literal; the template itself is the only SQL you write, so
treat `change_tracking_profiles` like the JDBC URLs in the config table and let only admins change it. A trailing
`;` is dropped.

A SQL Server procedure that fills a temp table, with made-up names:

```sql
EXEC audit.list_changed_rows
  @table = N'{table}', @from = {start}, @to = {end}, @into = N'{temp_table}'
```

On Postgres the JDBC driver can't return rows after a `CALL`, so the call is a function that a `SELECT` can run,
and the procedure writes its results to `results_table`, which SyncHole then reads for the window. See the test
database's profile in [Lakebase Test Setup](Lakebase-Test-Setup).

## Adding a profile with SQL

```sql
INSERT INTO main.synchole.change_tracking_profiles
  (profile_name, description, method, change_table_name, sequence_column, delete_flag_column,
   change_time_column, update_dttm)
VALUES
  ('source-change-tables', 'Change tables: one audit table per source table', 'change_table',
   'audit.{table_lower}_changes', 'CHANGE_SEQ', 'IS_DELETED', 'CHANGED_AT', current_timestamp());
```

The ingestion job checks each config row's profile when it loads the row: a missing profile, a profile for the
other method, or a template with an unknown placeholder fails the row with a message that says what to fix. The
app refuses to delete a profile that config rows still use, or to change its method while they do; renaming it
renames it in those rows too.

How each method reads the changes is described in [Running Ingestion](Running-Ingestion#changetableingestionworker).
