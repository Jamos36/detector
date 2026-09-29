"""DuckDB connection factory. Every connection in the pipeline comes from here
so that timezone, memory limit and spill directory are never forgotten."""

from __future__ import annotations

from pathlib import Path

import duckdb

from netanomaly.config import DuckDBSettings


def sql_literal(value: str | Path) -> str:
    """Quote a string or path as a SQL literal (paths may contain apostrophes)."""
    text = value.as_posix() if isinstance(value, Path) else value
    return "'" + text.replace("'", "''") + "'"


def connect(settings: DuckDBSettings | None = None) -> duckdb.DuckDBPyConnection:
    s = settings or DuckDBSettings()
    s.temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # UTC everywhere: without this DuckDB renders TIMESTAMPTZ in the OS zone,
    # which silently shifts date partitions and hour-of-day features.
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET memory_limit = {sql_literal(s.memory_limit)}")
    con.execute(f"SET threads = {int(s.threads)}")
    con.execute(f"SET temp_directory = {sql_literal(s.temp_directory)}")
    return con
