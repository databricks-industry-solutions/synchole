"""Change-tracking profiles: validating them and filling in their templates.

A profile describes how a source system records changes, so nothing about a particular system is built
into the code. Templates use ``{placeholder}`` names; every value put into them is a validated name, a
number or a quoted literal, so the template, which an admin writes like a JDBC URL, is the only free SQL.
"""

from __future__ import annotations

import re

from synchole.models import CHANGE_TRACKING_METHODS, ChangeTrackingProfile, IngestionConfig

# Up to database.schema.object; names are interpolated into SQL, so nothing else is allowed.
_NAME = re.compile(r"^[A-Za-z_][\w$#]*(\.[A-Za-z_][\w$#]*){0,2}$")
_COLUMN = re.compile(r"^[A-Za-z_][\w$#]*$")
_COLUMN_NAMES = re.compile(r"^[A-Za-z_][\w$#]*(\s*,\s*[A-Za-z_][\w$#]*)*$")
_PLACEHOLDER = re.compile(r"\{([A-Za-z_]+)\}")

PROFILE_METHODS = ("change_table", "change_procedure")
# Placeholders of change_table_name and results_table: the parts of the source table's name.
NAME_PLACEHOLDERS = ("schema", "table", "table_lower")
# Placeholders of procedure_call. start and end are timestamp literals (or a variable for end), multi_key
# and skip_change_time are 1 or 0, columns is a quoted list or NULL, temp_table is the table to fill.
PROCEDURE_PLACEHOLDERS = (
    *NAME_PLACEHOLDERS,
    "start",
    "end",
    "temp_table",
    "multi_key",
    "skip_change_time",
    "columns",
)


def is_valid_name(name: str) -> bool:
    """A table or procedure name, up to database.schema.object."""
    return bool(_NAME.match(name or ""))


def is_valid_column(name: str) -> bool:
    return bool(_COLUMN.match(name or ""))


def is_valid_key_columns(value: str) -> bool:
    """Comma-separated plain column names."""
    return bool(_COLUMN_NAMES.match((value or "").strip()))


def is_valid_column_names(value: str) -> bool:
    """change_columns: empty, or comma-separated column names."""
    return not value or bool(_COLUMN_NAMES.match(value))


def placeholders(template: str) -> set[str]:
    return set(_PLACEHOLDER.findall(template or ""))


def _check_placeholders(template: str, allowed: tuple[str, ...], field: str) -> None:
    unknown = sorted(placeholders(template) - set(allowed))
    if unknown:
        raise ValueError(
            f"{field} has unknown placeholder(s) {', '.join('{' + name + '}' for name in unknown)}; "
            f"use {', '.join('{' + name + '}' for name in allowed)}"
        )


def fill_template(template: str, values: dict[str, str]) -> str:
    """The template with each {placeholder} replaced; values must already be safe to put into SQL."""
    return _PLACEHOLDER.sub(lambda match: values[match.group(1)], template)


def name_values(source_table_name: str) -> dict[str, str]:
    parts = source_table_name.split(".")
    table = parts[-1]
    return {"schema": parts[-2] if len(parts) >= 2 else "", "table": table, "table_lower": table.lower()}


def render_name(template: str, source_table_name: str, field: str) -> str:
    """change_table_name or results_table for a source table: ``{schema}.chg_{table_lower}`` -> ``dbo.chg_orders``."""
    _check_placeholders(template, NAME_PLACEHOLDERS, field)
    name = fill_template(template, name_values(source_table_name))
    if not is_valid_name(name):
        raise ValueError(f"{field} '{template}' gives '{name}' for {source_table_name}, which is not a table name")
    return name


def validate_profile(profile: ChangeTrackingProfile) -> None:
    """Raises ValueError when the profile can't be used by its method."""
    name = profile.profile_name
    if profile.method not in PROFILE_METHODS:
        raise ValueError(
            f"Change-tracking profile '{name}' has unsupported method '{profile.method}' "
            f"(expected one of {', '.join(PROFILE_METHODS)})"
        )
    if not is_valid_column(profile.change_time_column):
        raise ValueError(f"Change-tracking profile '{name}' needs change_time_column, a column name")
    if profile.method == "change_table":
        if not profile.change_table_name:
            raise ValueError(f"Change-tracking profile '{name}' needs change_table_name")
        _check_placeholders(profile.change_table_name, NAME_PLACEHOLDERS, "change_table_name")
        if not placeholders(profile.change_table_name) & {"table", "table_lower"}:
            raise ValueError(
                f"change_table_name of profile '{name}' must contain {{table}} or {{table_lower}}, "
                "so each source table has its own change table"
            )
        for field in ("sequence_column", "delete_flag_column"):
            if not is_valid_column(getattr(profile, field)):
                raise ValueError(f"Change-tracking profile '{name}' needs {field}, a column name")
        return
    if not profile.procedure_call.strip():
        raise ValueError(f"Change-tracking profile '{name}' needs procedure_call")
    _check_placeholders(profile.procedure_call, PROCEDURE_PLACEHOLDERS, "procedure_call")
    missing = [p for p in ("start", "end") if p not in placeholders(profile.procedure_call)]
    if missing:
        raise ValueError(
            f"procedure_call of profile '{name}' must contain {' and '.join('{' + p + '}' for p in missing)}"
        )
    if "temp_table" not in placeholders(profile.procedure_call) and not profile.results_table:
        raise ValueError(
            f"procedure_call of profile '{name}' must contain {{temp_table}}, or the profile must set "
            "results_table, the table the procedure writes its results to"
        )
    if profile.results_table:
        _check_placeholders(profile.results_table, NAME_PLACEHOLDERS, "results_table")


def validate_change_tracking(config: IngestionConfig) -> None:
    """Raises ValueError when a config row's change tracking can't run."""
    table = config.source_table_name
    if config.change_tracking not in CHANGE_TRACKING_METHODS:
        raise ValueError(
            f"Config row for {table} has unsupported change_tracking '{config.change_tracking}' "
            f"(expected one of {', '.join(CHANGE_TRACKING_METHODS)})"
        )
    if not config.uses_change_source:
        return
    if config.ingestion_type != "incremental":
        raise ValueError(f"change_tracking '{config.change_tracking}' for {table} requires ingestion_type = 'incremental'")
    if not config.key_column_list:
        raise ValueError(f"change_tracking '{config.change_tracking}' for {table} requires key_columns")
    if not config.change_profile:
        raise ValueError(f"change_tracking '{config.change_tracking}' for {table} requires change_profile")
    profile = config.profile
    if profile is None:
        raise ValueError(f"Config row for {table} uses change_profile '{config.change_profile}', which does not exist")
    if profile.method != config.change_tracking:
        raise ValueError(
            f"Config row for {table} uses change_tracking '{config.change_tracking}', but profile "
            f"'{profile.profile_name}' is for '{profile.method}'"
        )
    validate_profile(profile)
    if config.change_tracking == "change_table":
        render_name(profile.change_table_name, table, "change_table_name")
        return
    # The procedure's batch can run several statements: the table and keys must be plain names.
    if not is_valid_name(table):
        raise ValueError(f"change_tracking 'change_procedure' needs a plain table name (schema.table), got '{table}'")
    if not is_valid_key_columns(config.key_columns or ""):
        raise ValueError(
            f"key_columns for {table} must be comma-separated column names with a change procedure, "
            f"got '{config.key_columns}'"
        )
    if not is_valid_column_names(config.change_columns):
        raise ValueError(f"change_columns for {table} must be comma-separated column names, got '{config.change_columns}'")
    if profile.results_table:
        render_name(profile.results_table, table, "results_table")
