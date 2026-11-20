# Lakebase Test Setup

Provision a Lakebase Postgres project seeded with `customers`, `products`, and `orders` tables, plus matching change tables (`sync_test.chg_*`) kept up to date by triggers and a change procedure, to test every worker without a real source system:

```python
from synchole.utils import LakebaseTestDatabaseSetup

setup = LakebaseTestDatabaseSetup(
    project_id="synchole-jdbc-test",
    project_display_name="SyncHole JDBC Test DB",
)
setup.ensure_project()
setup.create_standard_tables()
setup.seed_standard_data(customer_count=200, product_count=100, order_count=500)

jdbc_settings = setup.jdbc_settings()      # JDBC URL and credentials for config rows
profiles = setup.change_profiles()        # change_tracking_profiles rows for the test database
change_rows = setup.change_config_rows()  # config rows that read the change tables
```

`create_standard_tables()` also creates a change procedure, `sync_test.list_changes`, that writes the keys changed in a window to `sync_test.changed_<table>`, to test the [change-procedure approach](Running-Ingestion#changeprocedureingestionworker). Use `setup.change_config_rows(change_tracking="change_procedure")` for config rows that use it.

Insert the two profiles of `setup.change_profiles()` (`test-change-table` and `test-change-procedure`) into `change_tracking_profiles`, or add them in the app, before running the change config rows. See [Change-Tracking Profiles](Change-Tracking-Profiles).

Simulate ongoing changes:

```python
setup.simulate_updates()
setup.simulate_changes(delete_rows=2)
```

Or via the CLI:

```bash
synchole-setup-lakebase \
  --project-id synchole-jdbc-test \
  --customer-count 200 \
  --order-count 500 \
  --simulate-updates
```
