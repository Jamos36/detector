"""Timestamp parsing policy for TIMESTAMPTZ contract columns (V1-4, ADR-015).

- A value with an explicit zone (`Z`, `UTC` or a numeric offset) is converted to UTC.
- A valid value without an offset (text, or a Parquet TIMESTAMP without isAdjustedToUTC)
  is ASSUMED to be UTC. This is done explicitly with `timezone('UTC', ...)`, so it never
  depends on the DuckDB session zone, and every such value is counted so the data-quality
  report can warn about it.
- Anything else is invalid (a `cast_failed` reject). Text must match TEXT_PATTERN: DuckDB's
  own parser is more lenient (it accepts `infinity`, `24:00:00`, offsets of +25:00, date-only
  values and zone abbreviations such as `EST`, which it silently ignores), so it is not
  used to decide validity.

Parsing happens in two projections so the regex runs once per value (inline, DuckDB
evaluates each repetition): `match` fills a helper column, `parsed` builds
STRUCT(value TIMESTAMPTZ, status VARCHAR) from it, and later SQL reads only that struct.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from netanomaly.db import sql_literal

TIMESTAMPTZ = "TIMESTAMPTZ"
# (date 'T'|' ' hh:mm[:ss[.fraction]]) then an optional zone (Z | UTC | +hh | +hhmm | +hh:mm, or -)
TEXT_PATTERN = (
    r"^(\d{4}-\d{2}-\d{2}[T ](?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,9})?)?)"
    r"\s*(Z|UTC|[+-](\d{2})(?::?([0-5]\d))?)?$"
)
MATCH_FIELDS = ("datetime", "zone", "offset_hours", "offset_minutes")  # the pattern's capture groups, in order
MAX_OFFSET_MINUTES = 14 * 60  # UTC+14:00 (Line Islands) is the largest offset in use
NAIVE_TYPES = frozenset({"TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"})
AWARE_TYPE = "TIMESTAMP WITH TIME ZONE"
TEXT_FORMAT_DETAIL = (
    "not a timestamp: expected YYYY-MM-DD[T ]hh:mm[:ss[.f]] with optional Z, UTC or ±hh[:mm] "
    "(offset at most 14:00)"
)


class SourceKind(StrEnum):
    """How a source column holds a timestamp."""

    TEXT = "text"  # CSV (always staged as text) or a Parquet VARCHAR column
    NAIVE = "naive"  # Parquet TIMESTAMP without isAdjustedToUTC: no offset by definition
    AWARE = "aware"  # Parquet TIMESTAMP with isAdjustedToUTC: an instant in UTC
    UNSUPPORTED = "unsupported"  # e.g. integers or DATE: never guessed


class ParseStatus(StrEnum):
    OK = "ok"  # had an offset (or is a UTC instant)
    NO_OFFSET = "no_offset"  # valid, no offset: assumed UTC
    INVALID = "invalid"  # a cast_failed reject


def source_kind(duck_type: str) -> SourceKind:
    if duck_type == "VARCHAR":
        return SourceKind.TEXT
    if duck_type in NAIVE_TYPES:
        return SourceKind.NAIVE
    if duck_type == AWARE_TYPE:
        return SourceKind.AWARE
    return SourceKind.UNSUPPORTED


@dataclass(frozen=True)
class TimestampSql:
    """SQL for one raw column. Project `match AS match_column`, then `parsed AS column`, then use the properties."""

    column: str  # quoted name of the parsed STRUCT(value, status) column
    match_column: str  # quoted name of the helper column
    match: str
    parsed: str
    detail: str  # reject detail for invalid values

    @property
    def value(self) -> str:
        """TIMESTAMPTZ; meaningful only where the status is not invalid."""
        return f"struct_extract({self.column}, 'value')"

    def status_is(self, status: ParseStatus) -> str:
        return f"coalesce(struct_extract({self.column}, 'status') = '{status}', false)"


def _parsed(value: str, status: str) -> str:
    return f"struct_pack(value := {value}, status := {status})"


def _status(raw: str, invalid: str, no_offset: str) -> str:
    return (f"CASE WHEN {raw} IS NULL THEN NULL WHEN {invalid} THEN '{ParseStatus.INVALID}' "
            f"WHEN {no_offset} THEN '{ParseStatus.NO_OFFSET}' ELSE '{ParseStatus.OK}' END")


def timestamp_sql(raw: str, duck_type: str, name: str) -> TimestampSql:
    """SQL for raw column `raw` (already quoted) of source type `duck_type`; `name` names the new columns."""
    column, match_column = f'"{name}"', f'"{name}_match"'
    kind = source_kind(duck_type)
    if kind is SourceKind.AWARE:
        parsed = _parsed(f"CAST({raw} AS {TIMESTAMPTZ})", _status(raw, "false", "false"))
        return TimestampSql(column, match_column, "NULL", parsed, "")
    if kind is SourceKind.NAIVE:
        # CAST to TIMESTAMP first: truncates TIMESTAMP_NS to microseconds, like text input
        parsed = _parsed(f"timezone('UTC', CAST({raw} AS TIMESTAMP))", _status(raw, "false", "true"))
        return TimestampSql(column, match_column, "NULL", parsed, "")
    if kind is SourceKind.UNSUPPORTED:
        parsed = _parsed(f"CAST(NULL AS {TIMESTAMPTZ})", _status(raw, "true", "false"))
        return TimestampSql(column, match_column, "NULL", parsed, f"source type {duck_type} is not a timestamp or text")

    text = f"trim(CAST({raw} AS VARCHAR))"
    fields = "[" + ", ".join(f"'{f}'" for f in MATCH_FIELDS) + "]"
    match = f"regexp_extract({text}, {sql_literal(TEXT_PATTERN)}, {fields})"  # all fields '' when no match

    def field(f: str) -> str:
        return f"struct_extract({match_column}, '{f}')"

    no_zone = f"({field('zone')} = '')"
    value = (f"CASE WHEN {no_zone} THEN timezone('UTC', TRY_CAST({field('datetime')} AS TIMESTAMP)) "
             f"ELSE TRY_CAST({text} AS {TIMESTAMPTZ}) END")
    offset_minutes = (f"coalesce(TRY_CAST(nullif({field('offset_hours')}, '') AS INTEGER), 0) * 60 + "
                      f"coalesce(TRY_CAST(nullif({field('offset_minutes')}, '') AS INTEGER), 0)")
    # TRY_CAST still rejects impossible dates of the accepted shape, such as 2026-02-30.
    invalid = f"({field('datetime')} = '' OR {offset_minutes} > {MAX_OFFSET_MINUTES} OR ({value}) IS NULL)"
    return TimestampSql(column, match_column, match, _parsed(value, _status(raw, invalid, no_zone)),
                        TEXT_FORMAT_DETAIL)
