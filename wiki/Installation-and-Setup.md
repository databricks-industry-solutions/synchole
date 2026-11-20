# Installation and Setup

## Installation

Install in editable mode from a notebook:

```python
%pip install -e /Workspace/Users/<user>/synchole
```

Or build a wheel for production jobs:

```bash
pip install build
python -m build --wheel
# upload dist/synchole-0.4.0-py3-none-any.whl to a UC volume
```

Optional extras: `synchole[lakebase]` (Lakebase test setup) and `synchole[zerobus]` (ZeroBus log handler, see [Structured logs with ZeroBus](Running-Ingestion#structured-logs-with-zerobus)).

## One-Time Setup

Create the Unity Catalog schema and the metadata tables:

```python
from synchole.utils import UnityCatalogSetup

setup = UnityCatalogSetup(spark, catalog="main", schema="synchole")
setup.create_all()
# Tables created:
#   main.synchole.jdbc_ingestion_config
#   main.synchole.jdbc_ingestion_watermark
#   main.synchole.synchole_logs
#   main.synchole._synchole_row_checks
```

Or via the CLI:

```bash
synchole-setup-uc --catalog main --schema synchole
```

Setup is idempotent: existing tables and watermark state are kept, and missing config columns are added. Pass `--truncate-existing` to truncate every table in the schema except `jdbc_ingestion_config`. That resets watermark state and also clears the logs and the [row check](Row-Checks) history.
