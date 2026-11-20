# Exporting and Importing a SyncHole Configuration

A SyncHole setup lives in one Unity Catalog location (`<catalog>.<schema>`): the source tables in
`jdbc_ingestion_config`, plus one Lakeflow Job (and, for incremental groups, one CDC SCD2 pipeline) per ingestion
group. Export writes all of it to a single JSON file, and import recreates it in any location, in the same or
another workspace.

Typical uses:

- **Promote dev → prod**: export from dev, review and commit the file, import it into prod from a deployment
  pipeline.
- **Configuration as code**: keep the file in Git and treat the import as the deployment step.
- **Back up or clone** a location before a large change.

There are two ways to run it, with identical behavior:

|                       | Where                                                | Best for                                          |
| --------------------- | ---------------------------------------------------- | ------------------------------------------------- |
| **Control Plane app** | **Export** / **Import** in the header, on every page | Ad hoc exports, reviewing a file before importing |
| **Python script**     | `scripts/synchole_config.py`                        | Deployment pipelines and automation               |

The script calls the deployed app, so validation, the config write, and job creation all run the same code.

To import without the app at all, `synchole-import` reads the same file and writes the configs and jobs directly,
through a SQL warehouse and the Databricks API. See [Bundle Deployment](Bundle-Deployment).

---

## What the file contains

| Included                                                                                                                                                                                            | Not included                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Every row of `jdbc_ingestion_config` in the location                                                                                                                                                | Watermarks (`jdbc_ingestion_watermark`): runtime state                    |
| Each group's job settings: Git source, volume wheel, parallelism, schedule, job compute, SDP compute, run as                                                                                        | Secret **values**: only secret scope/key and UC secret names are exported |
| The [row check jobs](Row-Checks#row-check-jobs) of the location and of its groups (`rowCheckJobs`): check, group, range start, Git source, volume wheel, parallelism, schedule, job compute, run as |                                                                           |
|                                                                                                                                                                                                     | Job run history, job IDs and permissions                                  |

Job settings are **read back from the Databricks API** at export time, from the job named
`[<group>] Ingestion Pipeline` and its CDC pipeline. That means:

- Edits made in the Databricks UI (for example a changed schedule or cluster size) are included.
- Only jobs whose `configuration_fqn` parameter points at the exported location count. A job for a group with the
  same name in another location is ignored.
- Groups without such a job are exported with their tables only.
- If the app service principal cannot see a job, that job is not exported.

---

## Exporting

### From the app

1. Open the app and select the source catalog and schema with the location button in the header.
2. Click **Export** next to it.
3. The browser downloads `synchole-<catalog>.<schema>-<date>.json`.

### From the command line

```bash
python scripts/synchole_config.py --profile dev \
  export --catalog dev --schema synchole --output config/synchole.json
```

Without `--output`, the file is written to stdout.

---

## Importing

### What an import does

1. **Validates the whole file** against the same rules as the app's editors (see [Validation](#validation)).
   If anything is wrong, nothing is written and every problem is reported with its position, for example
   `file.configs.3.watermarkColumn: Incremental ingestion requires a watermark column, unless it uses a change table or change procedure`.
2. **Retargets** every entry at the location you import into. The `source` block and any `catalog`/`schema`
   in the entries are ignored.
3. **Upserts change-tracking profiles** by `profileName`, then **source tables** by `(ingestionGroup, sourceTableName)`: existing rows are updated, new ones are
   added, and rows of other groups are left alone.
4. Optionally (**Replace imported groups** / `--replace-groups`), **deletes** the rows of the imported groups
   that are not in the file, so each group matches the file exactly. This runs only after the upsert succeeded.
5. Optionally (**Create or update jobs** / `--deploy-jobs`), **creates or updates the job** of every group with
   job settings, exactly as the **Jobs** page does:
   - The job is matched by name, `[<group>] Ingestion Pipeline`, and updated in place if it exists.
   - A group with enabled incremental (or snapshot CDC) tables gets a CDC pipeline. A pipeline SyncHole
     provisioned in the source is provisioned again in the target; a pipeline that was supplied by ID keeps that
     ID (see [Limitations](#limitations)).
   - Groups are deployed one by one. A failing group is reported and does not stop the others.
   - Then every row check job in the file is created or updated, matched by its name, as the **Row checks** page
     does. A failing one is reported and does not stop the others.

The location's metadata tables are created first if they do not exist, as when you select a location in the app.
After the import, the first run of each table reads it in full because there is no watermark yet.

### From the app

1. Select the **target** catalog and schema with the location button in the header.
2. Click **Import** next to it.
3. Choose the export file. The app validates it immediately and shows, per group, the number of source tables
   and whether job settings are included. Nothing has been written at this point.
4. Choose the options: **Replace imported groups** and **Create or update the Databricks job**.
5. Click **Import**. The dialog then shows the result per group, including the job ID or the error, and the
   open page reloads its data.

### From the command line

```bash
# Validate only
python scripts/synchole_config.py import --catalog prod --schema synchole \
  --file config/synchole.json --dry-run

# Import, make each group match the file, and create or update the jobs
python scripts/synchole_config.py import --catalog prod --schema synchole \
  --file config/synchole.json --replace-groups --deploy-jobs
```

Example output:

```
Imported 42 source tables into prod.synchole: finance (12), sales (30)
Removed 3 source tables no longer in the file
Job for finance: updated job 812345678, CDC pipeline 0f3c...
Job for sales: created job 812345990
```

---

## Script reference

```
python scripts/synchole_config.py [--profile PROFILE] [--app-name NAME | --app-url URL] COMMAND ...
```

Global options must come **before** the command.

| Option       | Description                                                                                                     |
| ------------ | --------------------------------------------------------------------------------------------------------------- |
| `--profile`  | Databricks CLI profile. Without it, unified auth reads the environment (see [Authentication](#authentication)). |
| `--app-name` | Name of the SyncHole app, used to look up its URL. Default: `synchole-control-plane`.                         |
| `--app-url`  | URL of the app, instead of looking it up by name.                                                               |

`export`:

| Option                  | Description                     |
| ----------------------- | ------------------------------- |
| `--catalog`, `--schema` | Location to export (required).  |
| `--output`              | File to write. Default: stdout. |

`import`:

| Option                  | Description                                                                  |
| ----------------------- | ---------------------------------------------------------------------------- |
| `--catalog`, `--schema` | Location to import into (required).                                          |
| `--file`                | Export file to import (required).                                            |
| `--dry-run`             | Validate and summarize only; write nothing.                                  |
| `--replace-groups`      | Delete source tables of the imported groups that are not in the file.        |
| `--deploy-jobs`         | Create or update the job (and CDC pipeline) of each group with job settings. |

Exit code `0` means everything succeeded. `1` means the request failed (validation, permissions, connectivity)
or at least one job could not be deployed. The reason is printed on stderr, together with the app's request ID
when there is one, so you can find the failure in the app logs.

The script needs Python 3.10+ and `databricks-sdk`. It does not need the `synchole` package or pyspark.

### Authentication

The script authenticates with
[Databricks unified authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth) and sends the
token to the app.

- **Databricks Apps accept OAuth tokens only.** Personal access tokens are rejected, and the script stops early
  if it detects one.
- Locally: `databricks auth login --profile <name>`, then pass `--profile <name>`.
- In CI: use a service principal with OAuth machine-to-machine credentials:
  `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`.
- The caller needs the **CAN USE** permission on the app (app page → **Permissions**).

The app performs the import **as its own service principal**, not as the caller. That service principal needs the
same rights as when the UI is used: write access to the target schema, permission to create jobs and pipelines,
and access to the referenced connections and secret scopes.

---

## Using it in a deployment pipeline

### GitHub Actions

```yaml
name: Deploy SyncHole configuration

on:
  push:
    branches: [main]
    paths: [config/synchole.json]

jobs:
  import:
    runs-on: ubuntu-latest
    environment: prod
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - run: pip install "databricks-sdk>=0.120.0,<1"
      - name: Validate
        run: >
          python scripts/synchole_config.py import
          --catalog prod --schema synchole --file config/synchole.json --dry-run
      - name: Import and deploy jobs
        run: >
          python scripts/synchole_config.py import
          --catalog prod --schema synchole --file config/synchole.json
          --replace-groups --deploy-jobs
```

Pin the actions to commit SHAs if your organization requires it, as this repository's own workflows do.

### Any other CI system

The same three steps apply: install `databricks-sdk`, export the three `DATABRICKS_*` variables, and run the
script. A non-zero exit code fails the step.

### Recommended flow

1. Make changes in dev through the app.
2. Export dev (`export --output config/synchole.json`) and open a pull request. The file is stable JSON, so the
   diff shows exactly which tables and job settings changed.
3. If prod uses different names (for example target catalogs), adjust them in the pull request.
4. On merge, the pipeline runs `--dry-run` and then the import.

---

## File format

```json
{
  "format": "synchole-config",
  "version": 1,
  "exportedAt": "2026-10-17T12:00:00.000Z",
  "source": { "catalog": "dev", "schema": "synchole" },
  "profiles": [
    {
      "profileName": "source-change-tables",
      "description": "Change tables: one audit table per source table",
      "method": "change_table",
      "changeTableName": "audit.{table_lower}_changes",
      "sequenceColumn": "CHANGE_SEQ",
      "deleteFlagColumn": "IS_DELETED",
      "changeTimeColumn": "CHANGED_AT",
      "procedureCall": null,
      "resultsTable": null
    }
  ],
  "configs": [
    {
      "ingestionGroup": "sales",
      "sourceTableName": "public.orders",
      "stagingTableFqn": "dev_bronze.sales.orders_staging",
      "targetTableFqn": "dev_bronze.sales.orders",
      "ingestionType": "incremental",
      "keyColumns": "order_id",
      "watermarkColumn": "updated_at",
      "partitionColumn": null,
      "predicateColumn": null,
      "changeTracking": "watermark",
      "changeProfile": null,
      "autoCdcFromSnapshot": false,
      "jdbcUrl": null,
      "jdbcUser": null,
      "jdbcSecretScope": null,
      "jdbcSecretKey": null,
      "ucSecretName": null,
      "connectionName": "sales_postgres",
      "connectionDatabase": null,
      "watermarkThresholdMinutes": 5,
      "fetchSize": 10000,
      "numPartitions": 8,
      "enabled": true
    }
  ],
  "jobs": [
    {
      "ingestionGroup": "sales",
      "notebooks": {
        "source": "GIT",
        "gitUrl": "https://github.com/databricks-industry-solutions/synchole",
        "gitProvider": null,
        "referenceType": "BRANCH",
        "reference": "main"
      },
      "library": { "source": "GIT" },
      "foreachConcurrency": 4,
      "cdcPipelineId": null,
      "schedule": {
        "enabled": true,
        "quartzCronExpression": "0 0 8 * * ?",
        "timezoneId": "America/New_York",
        "pauseStatus": "UNPAUSED"
      },
      "compute": {
        "mode": "SERVERLESS",
        "performanceTarget": "PERFORMANCE_OPTIMIZED",
        "sparkVersion": "",
        "driverNodeTypeId": "",
        "workerNodeTypeId": "",
        "minWorkers": 1,
        "maxWorkers": 4
      },
      "pipelineCompute": {
        "mode": "SERVERLESS",
        "nodeTypeId": "",
        "driverNodeTypeId": "",
        "minWorkers": 1,
        "maxWorkers": 4,
        "photon": false
      },
      "runAs": {
        "job": { "mode": "DEFAULT", "principal": "" },
        "pipeline": { "mode": "SAME_AS_JOB", "principal": "" }
      }
    }
  ]
}
```

`format` and `version` are required. `exportedAt` and `source` are informational. `profiles`, `configs` and
`jobs` may be empty or omitted, so a file can carry only tables or only job settings. A file holds at most 500
profiles, 5,000 configs and 500 jobs.

### Sample file

[`examples/synchole-config.json`](https://github.com/databricks-industry-solutions/synchole/blob/main/examples/synchole-config.json)
is a complete file to start from, with every field of every entry. It has two change-tracking profiles and four ingestion
groups, each set up a different way:

| Group               | Tables                                            | Change detection                                       | Connection                | Job                                                      | Notebooks and package                                        |
| ------------------- | ------------------------------------------------- | ------------------------------------------------------ | ------------------------- | -------------------------------------------------------- | ------------------------------------------------------------ |
| `source_change_tables`    | `SALES_ORDER`, `CUSTOMER`, `SALES_ORDER_LINE`     | Change tables (profile `source-change-tables`)            | UC connection             | Serverless, hourly                                       | GitHub `main`, package from the same branch                  |
| `source_change_procedure` | `ORDERS`, `ACCOUNT`, `SHIPMENT_EVENT`                  | Change procedure (profile `source-change-procedure`)      | JDBC URL and UC secret    | Serverless, every 15 minutes, run as a service principal | Azure DevOps `main`, wheel in a volume                       |
| `source_reference`        | `DEPARTMENT`, `EMPLOYEE`                          | Full loads: one overwrite, one with SCD Type 2 history | JDBC URL and secret scope | Job cluster and classic CDC pipeline, nightly            | A bundle's workspace folder, the wheel the bundle deployed   |
| `finance`              | PostgreSQL `invoices`, `invoice_lines` (disabled) | Timestamp watermark                                    | UC connection             | Serverless, manual runs, run as a user                   | GitHub tag `v0.1.0`, `synchole==0.1.0` from a package index |

It also has three row check jobs: a daily basic check of the whole location, and weekly detailed checks of two
groups. Before importing it, replace the catalog and schema names, connections, secrets, JDBC URLs, node types,
the service principal and user, the wheel path, and the profiles' table, column and procedure names with your own, and delete the groups you don't need. Then
validate it with `--dry-run`.

### `profiles[]`

Each entry is one row of `change_tracking_profiles`. See [Change-Tracking Profiles](Change-Tracking-Profiles) for
the meaning of each field and the placeholders of the templates.

| Field              | Column               | Type                                  | Notes                                                            |
| ------------------ | -------------------- | ------------------------------------- | ---------------------------------------------------------------- |
| `profileName`      | `profile_name`       | string                                | Required; the key. Config rows name it in `changeProfile`.       |
| `description`      | `description`        | string \| null                        |                                                                  |
| `method`           | `method`             | `change_table` \| `change_procedure`  | Required.                                                        |
| `changeTableName`  | `change_table_name`  | string \| null                        | `change_table`: a template with `{table}` or `{table_lower}`.    |
| `sequenceColumn`   | `sequence_column`    | string \| null                        | `change_table`: a column name.                                   |
| `deleteFlagColumn` | `delete_flag_column` | string \| null                        | `change_table`: a column name.                                   |
| `changeTimeColumn` | `change_time_column` | string                                | Required, a column name.                                         |
| `procedureCall`    | `procedure_call`     | string \| null                        | `change_procedure`: a template with `{start}` and `{end}`.       |
| `resultsTable`     | `results_table`      | string \| null                        | `change_procedure`: required when the call has no `{temp_table}`. |

### `configs[]`

Each entry is one row of `jdbc_ingestion_config`. See
[Configuration Table](Configuration-Table) for the meaning of each column.

| Field                       | Column                         | Type                              | Notes                                                             |
| --------------------------- | ------------------------------ | --------------------------------- | ----------------------------------------------------------------- |
| `ingestionGroup`            | `ingestion_group`              | string                            | Required. With `sourceTableName`, the key of the row.             |
| `sourceTableName`           | `source_table_name`            | string                            | Required.                                                         |
| `stagingTableFqn`           | `staging_table_fqn`            | string \| null                    | Required for snapshot CDC.                                        |
| `targetTableFqn`            | `target_table_fqn`             | string                            | Required, `catalog.schema.table`.                                 |
| `ingestionType`             | `ingestion_type`               | `incremental` \| `full`           |                                                                   |
| `keyColumns`                | `key_columns`                  | string \| null                    | Comma-separated. Required for incremental and snapshot CDC.       |
| `watermarkColumn`           | `watermark_column`             | string \| null                    | Required for incremental with `watermark`. Dropped otherwise.     |
| `partitionColumn`           | `partition_column`             | string \| null                    |                                                                   |
| `predicateColumn`           | `predicate_column`             | string \| null                    |                                                                   |
| `changeTracking`            | `change_tracking`              | `watermark` \| `change_table` \| `change_procedure` | Default `watermark`. The others need incremental. |
| `changeProfile`             | `change_profile`               | string \| null                    | Required with `change_table` / `change_procedure`; must be in `profiles`. |
| `autoCdcFromSnapshot`       | `auto_cdc_from_snapshot`       | boolean                           | Full only.                                                        |
| `jdbcUrl`                   | `jdbc_url`                     | string \| null                    | Direct JDBC.                                                      |
| `jdbcUser`                  | `jdbc_user`                    | string \| null                    | Direct JDBC.                                                      |
| `jdbcSecretScope`           | `jdbc_secret_scope`            | string \| null                    | Secret scope **name**.                                            |
| `jdbcSecretKey`             | `jdbc_secret_key`              | string \| null                    | Secret key **name**.                                              |
| `ucSecretName`              | `uc_secret_name`               | string \| null                    | UC secret name.                                                   |
| `connectionName`            | `connection_name`              | string \| null                    | UC connection.                                                    |
| `connectionDatabase`        | `connection_database`          | string \| null                    | Kept only together with `connectionName`.                         |
| `watermarkThresholdMinutes` | `watermark_threshold_minutes`  | integer ≥ 0                       |                                                                   |
| `fetchSize`                 | `fetch_size`                   | integer > 0                       |                                                                   |
| `numPartitions`             | `num_partitions`               | integer > 0                       |                                                                   |
| `enabled`                   | `enabled`                      | boolean                           |                                                                   |

### `jobs[]`

Each entry holds the settings of one group's job, matching the fields of the **Jobs** page's dialog.

| Field                                                          | Type                                                        | Notes                                                                                                                                                                                                |
| -------------------------------------------------------------- | ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ingestionGroup`                                               | string                                                      | Required; one entry per group.                                                                                                                                                                       |
| `notebooks.source`                                             | `GIT` \| `WORKSPACE`                                        | Where the notebooks come from. Files with the older flat `gitUrl` and `gitBranch` fields still import, as a branch.                                                                                  |
| `notebooks.gitUrl`                                             | URL                                                         | `GIT`. An `http(s)` URL without whitespace, quotes, backslashes or `$`. A user name in the URL is dropped.                                                                                           |
| `notebooks.gitProvider`                                        | string \| null                                              | `GIT`. `null` detects it for github.com, dev.azure.com, gitlab.com, bitbucket.org and AWS CodeCommit; a self-hosted server needs `gitHubEnterprise`, `gitLabEnterpriseEdition` or `bitbucketServer`. |
| `notebooks.referenceType`, `reference`                         | `BRANCH` \| `TAG` \| `COMMIT`, string                       | `GIT`. No whitespace, quotes or backslashes; a commit is its hash.                                                                                                                                   |
| `notebooks.basePath`                                           | string                                                      | `WORKSPACE`. The folder that holds `notebooks/` and `src/`.                                                                                                                                          |
| `library.source`                                               | `GIT` \| `WHEEL` \| `PIP`                                   | Where tasks install SyncHole from. `GIT` needs `GIT` notebooks and a public repository. Older files' `libraryPath` imports as `WHEEL`.                                                              |
| `library.path`                                                 | string                                                      | `WHEEL`. A `.whl` file under `/Volumes/<catalog>/<schema>/<volume>/` or `/Workspace/`.                                                                                                               |
| `library.requirement`                                          | string                                                      | `PIP`. One pip requirement, such as `synchole==0.1.0` or a wheel URL: no spaces, quotes, backslashes or `$`, and no pip options (nothing starting with `-`).                                        |
| `foreachConcurrency`                                           | integer 1–100                                               | Tables ingested in parallel.                                                                                                                                                                         |
| `cdcPipelineId`                                                | string \| null                                              | `null` lets SyncHole provision the group's CDC pipeline; an ID reuses that pipeline.                                                                                                                |
| `schedule.enabled`                                             | boolean                                                     | `false` means the job runs only when triggered.                                                                                                                                                      |
| `schedule.quartzCronExpression`                                | string                                                      | Quartz cron, at least 6 fields.                                                                                                                                                                      |
| `schedule.timezoneId`                                          | string                                                      | For example `UTC` or `Europe/Rome`.                                                                                                                                                                  |
| `schedule.pauseStatus`                                         | `PAUSED` \| `UNPAUSED`                                      |                                                                                                                                                                                                      |
| `compute.mode`                                                 | `SERVERLESS` \| `JOB_CLUSTER`                               |                                                                                                                                                                                                      |
| `compute.performanceTarget`                                    | `STANDARD` \| `PERFORMANCE_OPTIMIZED`                       | Serverless performance mode, shared by the job's serverless tasks. Defaults to `PERFORMANCE_OPTIMIZED`; `STANDARD` costs less but each task can wait minutes for compute.                            |
| `compute.sparkVersion`, `driverNodeTypeId`, `workerNodeTypeId` | string                                                      | Required for `JOB_CLUSTER`. Node types are cloud-specific.                                                                                                                                           |
| `compute.minWorkers`, `maxWorkers`                             | integer                                                     | Autoscaling range; `maxWorkers` ≥ `minWorkers`.                                                                                                                                                      |
| `pipelineCompute`                                              | object                                                      | Optional; defaults to serverless.                                                                                                                                                                    |
| `pipelineCompute.mode`                                         | `SERVERLESS` \| `CLASSIC`                                   |                                                                                                                                                                                                      |
| `pipelineCompute.nodeTypeId`, `driverNodeTypeId`               | string                                                      | Required for `CLASSIC`.                                                                                                                                                                              |
| `pipelineCompute.minWorkers`, `maxWorkers`, `photon`           | integer, integer, boolean                                   | Classic only.                                                                                                                                                                                        |
| `runAs`                                                        | object                                                      | Optional; defaults to the owner for both.                                                                                                                                                            |
| `runAs.job.mode`                                               | `DEFAULT` \| `USER` \| `SERVICE_PRINCIPAL`                  | `DEFAULT` runs as the owner, the app service principal.                                                                                                                                              |
| `runAs.pipeline.mode`                                          | `SAME_AS_JOB` \| `DEFAULT` \| `USER` \| `SERVICE_PRINCIPAL` | Ignored when `cdcPipelineId` is set.                                                                                                                                                                 |
| `runAs.*.principal`                                            | string                                                      | User email for `USER`, application ID for `SERVICE_PRINCIPAL`.                                                                                                                                       |

### Validation

An import is rejected, with nothing written, when:

- `format` is not `synchole-config` or `version` is not `1`.
- A config breaks an ingestion rule: incremental without key columns, or without a watermark column (unless it
  uses a change table or change procedure); a change table or change procedure on a full load, without a profile,
  or with a profile that is not in the file's `profiles` or is for the other method; snapshot CDC on an
  incremental load, or without a staging table or key columns.
- A profile could not run: a template with an unknown placeholder, a change table template without `{table}` or
  `{table_lower}`, a procedure call without `{start}` and `{end}` (or without `{temp_table}` and a results table),
  or a column name that is not a plain name.
- A config's `targetTableFqn` is not three-part.
- The same `(ingestionGroup, sourceTableName)` appears twice, or two job entries share a group.
- A job setting is out of range or missing (see the table above).

---

## Limitations

- **Names are imported as-is.** Target and staging table names, UC connections, secret scopes, UC secrets,
  run-as principals, and node types must exist (and be valid) in the target workspace. Edit the file when they
  differ between environments.
- **Pipelines supplied by ID** keep their ID, which usually refers to a pipeline in the source workspace. Set
  `cdcPipelineId` to `null` to let SyncHole provision one, or to the ID of the target's pipeline.
- **Job names are global within a workspace.** Importing a group into a second location of the same workspace
  updates the existing `[<group>] Ingestion Pipeline` job to point at the new location.
- **Jobs SyncHole did not generate**, or generated jobs that were renamed or restructured by hand, are skipped or
  only partly read on export.
- **Removing a group** from the file does not delete it from the target; `--replace-groups` trims only the groups
  the file contains. Delete groups and their jobs from the app.
- **Watermarks are not transferred**, so the first run after an import is a full read of every table.

---

## Troubleshooting

| Message                                                               | Cause and fix                                                                                                                                                                                                                                          |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Not a SyncHole configuration export` / `Unsupported export version` | The file is not an export from this feature, or comes from a newer version. Re-export it.                                                                                                                                                              |
| `file.configs.<n>.<field>: …`                                         | Entry `n` (0-based) of `configs` breaks the rule shown. Fix the entry and retry.                                                                                                                                                                       |
| `Duplicate source table <group> / <table>`                            | The same table is listed twice in the file.                                                                                                                                                                                                            |
| `Databricks Apps do not accept personal access tokens`                | The script found a PAT. Use `databricks auth login` or service principal OAuth credentials.                                                                                                                                                            |
| `The app refused the request (HTTP 302/401/403)`                      | The caller lacks **CAN USE** on the app, or the token is not valid for it.                                                                                                                                                                             |
| `App <name> has no URL yet`                                           | The app is not deployed or not running. Start it, or pass `--app-url`.                                                                                                                                                                                 |
| `Job for <group> failed: …`                                           | The app service principal could not create the job or pipeline (permissions, invalid node type, missing principal). The import of tables has already succeeded; fix the cause and rerun. Reruns are safe because tables and jobs are updated in place. |
| Request ID in an error                                                | Search the app logs (`databricks apps logs <app-name>`) for it to see the full error.                                                                                                                                                                  |

## API

The app and the script use these endpoints:

| Endpoint           | Body or query                                                     | Returns                                                               |
| ------------------ | ----------------------------------------------------------------- | --------------------------------------------------------------------- |
| `GET /api/export`  | `?catalog=&schema=`                                               | The export file.                                                      |
| `POST /api/import` | `{ catalog, schema, file, replaceGroups?, deployJobs?, dryRun? }` | `{ dryRun, configCount, jobCount, groups[], removedCount?, jobs[]? }` |
| `POST /api/jobs`   | One `jobs[]` entry plus `catalog` and `schema`                    | `{ jobId, pipelineId?, action }`                                      |

Validation errors return HTTP 400 with `{ error, requestId }`.
