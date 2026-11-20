from __future__ import annotations

import itertools
import random
from datetime import datetime, timedelta

try:
    import psycopg2
except (ImportError, OSError):
    psycopg2 = None  # Not available in pipeline serverless context

try:
    from databricks.sdk import WorkspaceClient
except (ImportError, OSError):
    WorkspaceClient = None

try:
    from databricks.sdk.service.postgres import Project, ProjectSpec
except (ImportError, ModuleNotFoundError):
    Project = None
    ProjectSpec = None


def _require_psycopg2():
    if psycopg2 is None:
        raise ImportError(
            "psycopg2 is required for LakebaseTestDatabaseSetup but is not installed. "
            "Install it with: pip install psycopg2-binary"
        )


class LakebaseTestDatabaseSetup:
    # Source tables that get a change table (sync_test.chg_<table>) filled by triggers.
    CHANGE_KEY_COLUMNS: dict[str, dict[str, str]] = {
        "customers": {"customer_id": "INTEGER"},
        "products": {"product_id": "INTEGER"},
        "orders": {"order_id": "INTEGER", "line_id": "INTEGER"},
    }
    CHANGE_MODIFIED_COLUMNS: dict[str, str] = {
        "customers": "updated_at",
        "products": "last_changed",
        "orders": "modified_at",
    }

    def __init__(
        self,
        project_id: str = "synchole-jdbc-test",
        project_display_name: str = "SyncHole JDBC Test DB",
        database_name: str = "databricks_postgres",
        change_schema: str = "sync_test",
        workspace_client: "WorkspaceClient | None" = None,
    ):
        if WorkspaceClient is None:
            raise ImportError("databricks-sdk is required for LakebaseTestDatabaseSetup")
        self.w = workspace_client or WorkspaceClient()
        self.project_id = project_id
        self.project_display_name = project_display_name
        self.database_name = database_name
        self.change_schema = change_schema
        self.username = self.w.current_user.me().user_name

    def ensure_project(self):
        for project in itertools.islice(self.w.postgres.list_projects(page_size=50), 100):
            if project.name == f"projects/{self.project_id}":
                return project
        operation = self.w.postgres.create_project(
            project=Project(spec=ProjectSpec(display_name=self.project_display_name, pg_version=17)),
            project_id=self.project_id,
        )
        return operation.wait()

    def resolve_endpoint(self):
        branch = next(iter(self.w.postgres.list_branches(parent=f"projects/{self.project_id}")))
        endpoint = next(iter(self.w.postgres.list_endpoints(parent=branch.name)))
        return branch, endpoint

    def generate_token(self, endpoint_name: str) -> str:
        return self.w.postgres.generate_database_credential(endpoint=endpoint_name).token

    def connection(self):
        _require_psycopg2()
        _, endpoint = self.resolve_endpoint()
        token = self.generate_token(endpoint.name)
        return psycopg2.connect(
            host=endpoint.status.hosts.host,
            port=5432,
            dbname=self.database_name,
            user=self.username,
            password=token,
            sslmode="require",
        )

    def create_standard_tables(self, include_change_tracking: bool = True) -> None:
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dbo")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.customers (
                        customer_id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        email VARCHAR(255),
                        city VARCHAR(100),
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.products (
                        product_id SERIAL PRIMARY KEY,
                        sku VARCHAR(50),
                        product_name VARCHAR(255),
                        category VARCHAR(100),
                        price NUMERIC(12, 2),
                        last_changed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.orders (
                        order_id INTEGER NOT NULL,
                        line_id INTEGER NOT NULL,
                        customer_id INTEGER,
                        product_id INTEGER,
                        quantity INTEGER,
                        order_status VARCHAR(50),
                        modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (order_id, line_id)
                    )
                """)

        if include_change_tracking:
            self.create_change_tables()

    def seed_standard_data(self, customer_count: int = 200, product_count: int = 100, order_count: int = 500, include_change_tracking: bool = True) -> None:
        first_names = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Davis", "Miller"]
        cities = ["New York", "London", "Berlin", "Paris", "Milan", "Toronto", "Sydney"]
        categories = ["Hardware", "Software", "Services", "Support"]
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"]

        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dbo.customers")
                if cur.fetchone()[0] == 0:
                    for _ in range(customer_count):
                        first_name = random.choice(first_names)
                        last_name = random.choice(last_names)
                        cur.execute(
                            "INSERT INTO dbo.customers(first_name, last_name, email, city, updated_at) VALUES (%s, %s, %s, %s, %s)",
                            (first_name, last_name, f"{first_name.lower()}.{last_name.lower()}@example.com", random.choice(cities), datetime.now() - timedelta(days=random.randint(1, 60))),
                        )
                cur.execute("SELECT COUNT(*) FROM dbo.products")
                if cur.fetchone()[0] == 0:
                    for idx in range(product_count):
                        cur.execute(
                            "INSERT INTO dbo.products(sku, product_name, category, price, last_changed) VALUES (%s, %s, %s, %s, %s)",
                            (f"SKU-{idx:04d}", f"Product {idx:04d}", random.choice(categories), round(random.uniform(10, 500), 2), datetime.now() - timedelta(days=random.randint(1, 60))),
                        )
                cur.execute("SELECT COUNT(*) FROM dbo.orders")
                if cur.fetchone()[0] == 0:
                    for order_id in range(1, order_count + 1):
                        line_total = random.randint(1, 4)
                        for line_id in range(1, line_total + 1):
                            cur.execute(
                                "INSERT INTO dbo.orders(order_id, line_id, customer_id, product_id, quantity, order_status, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (order_id, line_id, random.randint(1, customer_count), random.randint(1, product_count), random.randint(1, 5), random.choice(statuses), datetime.now() - timedelta(days=random.randint(1, 60))),
                            )

        if include_change_tracking:
            self.seed_change_data()

    def simulate_updates(self, customer_rows: int = 3, product_rows: int = 2, order_rows: int = 5, delete_rows: int = 0) -> None:
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "RETURNED"]
        cities = ["New York", "London", "Berlin", "Paris", "Milan", "Toronto", "Sydney"]
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for _ in range(customer_rows):
                    cur.execute(
                        "UPDATE dbo.customers SET city = %s, updated_at = %s WHERE customer_id = (SELECT customer_id FROM dbo.customers ORDER BY random() LIMIT 1)",
                        (random.choice(cities), datetime.now() - timedelta(seconds=random.randint(1, 30))),
                    )
                for _ in range(product_rows):
                    cur.execute(
                        "UPDATE dbo.products SET price = price + 1, last_changed = %s WHERE product_id = (SELECT product_id FROM dbo.products ORDER BY random() LIMIT 1)",
                        (datetime.now() - timedelta(seconds=random.randint(1, 30)),),
                    )
                for _ in range(order_rows):
                    cur.execute(
                        "UPDATE dbo.orders SET order_status = %s, modified_at = %s WHERE (order_id, line_id) = (SELECT order_id, line_id FROM dbo.orders ORDER BY random() LIMIT 1)",
                        (random.choice(statuses), datetime.now() - timedelta(seconds=random.randint(1, 30))),
                    )
                for _ in range(delete_rows):
                    cur.execute(
                        "DELETE FROM dbo.orders WHERE (order_id, line_id) = (SELECT order_id, line_id FROM dbo.orders ORDER BY random() LIMIT 1)"
                    )

    # ------------------------------------------------------------------
    # Change tracking: change tables (sync_test.chg_<table>) and a change procedure
    # ------------------------------------------------------------------
    CHANGE_TABLE_PROFILE = "test-change-table"
    CHANGE_PROCEDURE_PROFILE = "test-change-procedure"

    def change_table_name(self, base_table: str) -> str:
        """The change table of a test table; matches change_table_name of the test-change-table profile."""
        return f"{self.change_schema}.chg_{base_table.split('.')[-1].lower()}"

    def _change_trigger_function_sql(self) -> str:
        """PL/pgSQL trigger that writes one change row per affected row."""
        return f"""
            CREATE OR REPLACE FUNCTION {self.change_schema}.record_change() RETURNS trigger
            LANGUAGE plpgsql AS $chg$
            DECLARE
                change_table text := TG_ARGV[0];
                key_columns  text[] := string_to_array(TG_ARGV[1], ',');
                payload      jsonb;
                key_column   text;
                value_list   text := '';
                change_ts    timestamp := clock_timestamp();
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    payload := to_jsonb(OLD);
                ELSE
                    payload := to_jsonb(NEW);
                END IF;

                FOREACH key_column IN ARRAY key_columns LOOP
                    IF value_list <> '' THEN
                        value_list := value_list || ', ';
                    END IF;
                    value_list := value_list || quote_nullable(payload ->> key_column);
                END LOOP;

                EXECUTE format(
                    'INSERT INTO %s (%s, is_deleted, changed_at, change_seq) VALUES (%s, %L, %L, %s)',
                    change_table,
                    array_to_string(key_columns, ', '),
                    value_list,
                    (TG_OP = 'DELETE'),
                    change_ts,
                    (EXTRACT(EPOCH FROM change_ts) * 1000000)::bigint
                );

                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END;
            $chg$
        """

    def _list_changes_procedure_sql(self) -> str:
        """A change procedure: the latest change of each key changed in [p_start, p_end).

        It leaves the keys and changed_at in sync_test.changed_<table>, replacing what the table held,
        reading sync_test.chg_<table>. p_multi_key is accepted but ignored: every key column is returned.
        """
        return f"""
            CREATE OR REPLACE PROCEDURE {self.change_schema}.list_changes(
                p_table text,
                p_start timestamp,
                p_end timestamp,
                p_multi_key int DEFAULT 0
            )
            LANGUAGE plpgsql AS $proc$
            DECLARE
                base          text := lower(split_part(p_table, '.', -1));
                change_table  text := 'chg_' || base;
                results_table text := 'changed_' || base;
                key_list      text;
                key_defs      text;
            BEGIN
                SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position),
                       string_agg(quote_ident(column_name) || ' ' || data_type, ', ' ORDER BY ordinal_position)
                INTO key_list, key_defs
                FROM information_schema.columns
                WHERE table_schema = '{self.change_schema}'
                  AND table_name = change_table
                  AND column_name NOT IN ('change_event_id', 'is_deleted', 'changed_at', 'change_seq');
                IF key_list IS NULL THEN
                    RAISE EXCEPTION 'No change table {self.change_schema}.% for %', change_table, p_table;
                END IF;

                EXECUTE format('CREATE TABLE IF NOT EXISTS {self.change_schema}.%I (%s, changed_at timestamp)',
                               results_table, key_defs);
                EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON {self.change_schema}.%I (%s)',
                               results_table || '_keys', results_table, key_list);
                EXECUTE format('DELETE FROM {self.change_schema}.%I', results_table);
                EXECUTE format(
                    'INSERT INTO {self.change_schema}.%I (%s, changed_at) SELECT DISTINCT ON (%s) %s, changed_at '
                    'FROM {self.change_schema}.%I WHERE changed_at >= %L AND changed_at < %L '
                    'ORDER BY %s, changed_at DESC, change_seq DESC',
                    results_table, key_list, key_list, key_list, change_table, p_start, p_end, key_list
                );
            END;
            $proc$
        """

    def _list_changes_run_function_sql(self) -> str:
        """Calls list_changes and returns how many keys it found.

        The Postgres JDBC driver can't return a SELECT that follows a CALL in one statement, so the
        test-change-procedure profile's procedure_call runs the procedure through this function.
        """
        return f"""
            CREATE OR REPLACE FUNCTION {self.change_schema}.list_changes_run(
                p_table text,
                p_start timestamp,
                p_end timestamp,
                p_multi_key int
            ) RETURNS bigint
            LANGUAGE plpgsql AS $proc$
            DECLARE
                changed bigint;
            BEGIN
                CALL {self.change_schema}.list_changes(p_table, p_start, p_end, p_multi_key);
                EXECUTE format('SELECT COUNT(*) FROM {self.change_schema}.%I', 'changed_' || lower(split_part(p_table, '.', -1)))
                INTO changed;
                RETURN changed;
            END;
            $proc$
        """

    def create_change_tables(self) -> None:
        """Create sync_test.chg_<table> change tables, the triggers that fill them, and the change procedure."""
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.change_schema}")
                cur.execute(self._change_trigger_function_sql())
                cur.execute(self._list_changes_procedure_sql())
                cur.execute(self._list_changes_run_function_sql())
                for base_table, key_columns in self.CHANGE_KEY_COLUMNS.items():
                    change_table = self.change_table_name(base_table)
                    key_ddl = ", ".join(f"{name} {dtype} NOT NULL" for name, dtype in key_columns.items())
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {change_table} (
                            change_event_id BIGSERIAL PRIMARY KEY,
                            {key_ddl},
                            is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                            changed_at TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
                            change_seq BIGINT NOT NULL
                        )
                    """)
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS chg_{base_table}_seq_idx "
                        f"ON {change_table} (change_seq)"
                    )
                    trigger_name = f"chg_{base_table}_trg"
                    cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON dbo.{base_table}")
                    cur.execute(f"""
                        CREATE TRIGGER {trigger_name}
                        AFTER INSERT OR UPDATE OR DELETE ON dbo.{base_table}
                        FOR EACH ROW EXECUTE FUNCTION {self.change_schema}.record_change(
                            '{change_table}', '{",".join(key_columns)}'
                        )
                    """)

    def seed_change_data(self) -> None:
        """Backfill one change per existing source row (skipped when the change table already has rows)."""
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for base_table, key_columns in self.CHANGE_KEY_COLUMNS.items():
                    change_table = self.change_table_name(base_table)
                    modified_column = self.CHANGE_MODIFIED_COLUMNS[base_table]
                    cur.execute(f"SELECT COUNT(*) FROM {change_table}")
                    if cur.fetchone()[0] > 0:
                        continue
                    key_list = ", ".join(key_columns)
                    cur.execute(f"""
                        INSERT INTO {change_table} ({key_list}, is_deleted, changed_at, change_seq)
                        SELECT {key_list}, FALSE, {modified_column},
                               (EXTRACT(EPOCH FROM {modified_column}) * 1000000)::BIGINT
                        FROM dbo.{base_table}
                    """)

    def simulate_changes(self, customer_rows: int = 3, product_rows: int = 2, order_rows: int = 5, delete_rows: int = 2) -> None:
        """Generate updates and deletes on the source tables; triggers record them in the change tables."""
        self.simulate_updates(
            customer_rows=customer_rows,
            product_rows=product_rows,
            order_rows=order_rows,
            delete_rows=delete_rows,
        )

    def change_status(self) -> list[dict[str, str]]:
        """Per change table: change count, delete count and current change_seq watermark."""
        status: list[dict[str, str]] = []
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for base_table in self.CHANGE_KEY_COLUMNS:
                    change_table = self.change_table_name(base_table)
                    cur.execute(
                        f"SELECT COUNT(*), COALESCE(MAX(change_seq), -1), "
                        f"COUNT(*) FILTER (WHERE is_deleted) FROM {change_table}"
                    )
                    change_count, max_seq, delete_count = cur.fetchone()
                    status.append(
                        {
                            "change_table": change_table,
                            "change_count": str(change_count),
                            "delete_count": str(delete_count),
                            "max_change_seq": str(max_seq),
                        }
                    )
        return status

    def change_profiles(self) -> list[dict[str, str]]:
        """change_tracking_profiles rows for the test change tables and the test change procedure."""
        return [
            {
                "profile_name": self.CHANGE_TABLE_PROFILE,
                "description": "SyncHole test database: trigger-filled change tables",
                "method": "change_table",
                "change_table_name": f"{self.change_schema}.chg_{{table_lower}}",
                "sequence_column": "change_seq",
                "delete_flag_column": "is_deleted",
                "change_time_column": "changed_at",
            },
            {
                "profile_name": self.CHANGE_PROCEDURE_PROFILE,
                "description": "SyncHole test database: change procedure writing a results table",
                "method": "change_procedure",
                "change_time_column": "changed_at",
                "procedure_call": (
                    f"{self.change_schema}.list_changes_run('{{schema}}.{{table}}', {{start}}, {{end}}, {{multi_key}})"
                ),
                "results_table": f"{self.change_schema}.changed_{{table_lower}}",
            },
        ]

    def change_config_rows(self, change_tracking: str = "change_table") -> list[dict[str, str]]:
        """Ingestion-config rows for incremental ingestion of the test tables through a change-tracking profile.

        change_tracking="change_procedure" reads through the test change procedure instead of the change tables.
        """
        settings = self.jdbc_settings()
        profile = self.CHANGE_PROCEDURE_PROFILE if change_tracking == "change_procedure" else self.CHANGE_TABLE_PROFILE
        return [
            {
                "source_table_name": f"dbo.{base_table}",
                "change_table_name": self.change_table_name(base_table),
                "ingestion_type": "incremental",
                "change_tracking": change_tracking,
                "change_profile": profile,
                "key_columns": ",".join(key_columns),
                "watermark_column": "",
                "partition_column": next(iter(key_columns)),
                "jdbc_url": settings["jdbc_url"],
                "jdbc_user": settings["jdbc_user"],
            }
            for base_table, key_columns in self.CHANGE_KEY_COLUMNS.items()
        ]

    def jdbc_settings(self) -> dict[str, str]:
        _, endpoint = self.resolve_endpoint()
        host = endpoint.status.hosts.host
        return {
            "jdbc_url": f"jdbc:postgresql://{host}:5432/{self.database_name}?sslmode=require",
            "jdbc_user": self.username,
            "jdbc_password_hint": f"w.postgres.generate_database_credential(endpoint=\'{endpoint.name}\').token",
            "jdbc_driver": "org.postgresql.Driver",
            "fetch_size": "10000",
            "num_partitions": "8",
        }
