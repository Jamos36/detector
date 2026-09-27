"""V1-4: timestamps with an offset, without an offset (assumed UTC + DQ warning), and invalid values."""

from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from netanomaly.cli import main
from netanomaly.config import DQSettings
from netanomaly.ingest import LedgerEntry, Status, ingest_file, read_ledger
from netanomaly.quality import build_report, render_markdown
from netanomaly.timestamps import TEXT_FORMAT_DETAIL, ParseStatus, SourceKind, source_kind, timestamp_sql

TS_COLUMNS = ("flow_start_time", "flow_end_time", "time_stamp")
NO_OFFSET_FORMAT = "%Y-%m-%d %H:%M:%S.%n"  # DuckDB strftime: nanoseconds, no zone


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


def _parse(con, value: str | None, duck_type: str = "VARCHAR"):
    """(invalid, without_offset, value) of one raw value, evaluated with the SQL ingest uses."""
    ts = timestamp_sql('"x"', duck_type, "_ts")
    invalid, no_offset = ts.status_is(ParseStatus.INVALID), ts.status_is(ParseStatus.NO_OFFSET)
    return con.execute(
        f"SELECT {invalid}, {no_offset}, CASE WHEN NOT {invalid} THEN {ts.value} END "
        f"FROM (SELECT *, {ts.parsed} AS {ts.column} FROM (SELECT *, {ts.match} AS {ts.match_column} "
        f'FROM (SELECT CAST(? AS {duck_type}) AS "x")))', [value]
    ).fetchone()


def _edit_csv(sample_csv: Path, out: Path, edits: dict[int, dict[str, str]]) -> Path:
    """Copy sample_csv with field overrides for 1-based data rows."""
    with sample_csv.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r, changes in edits.items():
        rows[r - 1] = {**rows[r - 1], **changes}
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return out


def _text_parquet(con, csv_path: Path, out: Path) -> pa.Table:
    """Every column as text, like a CSV converted to Parquet without types."""
    con.execute(f"COPY (SELECT * FROM read_csv('{csv_path.as_posix()}', all_varchar = true)) "
                f"TO '{out.as_posix()}' (FORMAT parquet)")
    return pq.read_table(out)


def _replace(table: pa.Table, column: str, values: pa.Array) -> pa.Table:
    return table.set_column(table.schema.get_field_index(column), column, values)


def _lake(con, lake: Path, cols: str) -> list[tuple]:
    glob = (lake / "flows" / "**" / "*.parquet").as_posix()
    return con.sql(
        f"SELECT {cols} FROM read_parquet('{glob}', hive_partitioning = true) ORDER BY source_row_number"
    ).fetchall()


def _rejects(con, lake: Path) -> list[tuple]:
    glob = (lake / "rejects" / "*.parquet").as_posix()
    return con.sql(
        f"SELECT source_row_number, reason, column_name, raw_value, detail FROM read_parquet('{glob}') ORDER BY 1"
    ).fetchall()


@pytest.fixture
def ny_con(con):
    """A session zone other than UTC: the parsing policy must not depend on it."""
    con.execute("SET TimeZone = 'America/New_York'")
    return con


# --- the policy, value by value -----------------------------------------------------

@pytest.mark.parametrize(("text", "expected"), [
    ("2026-09-01 10:00:00+00:00", _utc(2026, 9, 1, 10)),
    ("2026-09-01 10:00:00+02:00", _utc(2026, 9, 1, 8)),
    ("2026-09-01T10:00:00Z", _utc(2026, 9, 1, 10)),
    ("2026-09-01 10:00:00 UTC", _utc(2026, 9, 1, 10)),
    ("2026-09-01 10:00:00-0530", _utc(2026, 9, 1, 15, 30)),
    ("2026-09-01 10:00:00-05", _utc(2026, 9, 1, 15)),
    ("2026-09-01 10:00:00+14:00", _utc(2026, 8, 31, 20)),
    ("2026-09-01 10:00:00.123456789+01:00", _utc(2026, 9, 1, 9, 0, 0, 123456)),  # ns truncated to us
])
def test_values_with_an_offset_are_converted_to_utc_without_warning(ny_con, text, expected):
    assert _parse(ny_con, text) == (False, False, expected)


@pytest.mark.parametrize(("text", "expected"), [
    ("2026-09-01 10:00:00", _utc(2026, 9, 1, 10)),
    ("2026-09-01T10:00:00", _utc(2026, 9, 1, 10)),
    ("2026-09-01 10:00", _utc(2026, 9, 1, 10)),
    ("  2026-09-01 23:59:59.999999999 ", _utc(2026, 9, 1, 23, 59, 59, 999999)),
])
def test_values_without_an_offset_are_assumed_utc_and_flagged(ny_con, text, expected):
    # the session zone is New York: a session-zone parse would be 4 hours off
    assert _parse(ny_con, text) == (False, True, expected)


@pytest.mark.parametrize("text", [
    "", "garbage", "1788300000", "2026-09-01", "20260901 10:00:00", "2026/09/01 10:00:00",
    "2026-02-30 10:00:00", "2026-09-01 24:00:00", "2026-09-01 10:60:00", "infinity",
    "2026-09-01 10:00:00+25:00", "2026-09-01 10:00:00+14:01", "2026-09-01 10:00:00+2",
    "2026-09-01 10:00:00 EST", "2026-09-01 10:00:00 Europe/Berlin", "2026-09-01t10:00:00",
])
def test_invalid_values_are_invalid_not_assumed_utc(ny_con, text):
    invalid, without_offset, _ = _parse(ny_con, text)
    assert (invalid, without_offset) == (True, False)


def test_null_is_neither_invalid_nor_flagged(ny_con):
    assert _parse(ny_con, None) == (False, False, None)


@pytest.mark.parametrize(("duck_type", "kind"), [
    ("VARCHAR", SourceKind.TEXT), ("TIMESTAMP", SourceKind.NAIVE), ("TIMESTAMP_NS", SourceKind.NAIVE),
    ("TIMESTAMP WITH TIME ZONE", SourceKind.AWARE), ("BIGINT", SourceKind.UNSUPPORTED),
    ("DATE", SourceKind.UNSUPPORTED),
])
def test_source_kind_by_parquet_type(duck_type, kind):
    assert source_kind(duck_type) is kind


def test_naive_aware_and_unsupported_source_types(ny_con):
    assert _parse(ny_con, "2026-09-01 10:00:00", "TIMESTAMP") == (False, True, _utc(2026, 9, 1, 10))
    assert _parse(ny_con, "2026-09-01 10:00:00.123456789", "TIMESTAMP_NS") == (
        False, True, _utc(2026, 9, 1, 10, 0, 0, 123456))
    aware = "TIMESTAMP WITH TIME ZONE"  # as DESCRIBE spells it
    assert _parse(ny_con, "2026-09-01 10:00:00+02:00", aware) == (False, False, _utc(2026, 9, 1, 8))
    assert _parse(ny_con, "1788300000", "BIGINT")[:2] == (True, False)


# --- ingestion ----------------------------------------------------------------------

def test_csv_offsets_missing_offsets_and_invalid_values(con, contract, sample_csv, tmp_path):
    src = _edit_csv(sample_csv, tmp_path / "ts.csv", {
        1: {"time_stamp": "2026-09-01 22:00:00+02:00"},  # offset
        2: {"time_stamp": "2026-09-01 20:00:00"},  # no offset, same instant
        3: {"flow_start_time": "2026-09-01 00:30:00", "flow_end_time": "2026-09-01 00:31:00"},  # no offset
        4: {"time_stamp": "2026-09-01 20:00:00 EST"},  # invalid
        5: {"flow_start_time": "not a time"},  # invalid in a required column
    })
    lake = tmp_path / "lake"
    entry = ingest_file(con, src, contract, lake, tmp_path / "stage", "b", max_reject_fraction=0.1)

    assert (entry.status, entry.rows, entry.rejected_rows) == (Status.INGESTED, 48, 2)
    assert entry.timestamps_without_offset == {"flow_start_time": 1, "flow_end_time": 1, "time_stamp": 1}
    assert read_ledger(lake)[entry.file_hash].timestamps_without_offset == entry.timestamps_without_offset
    rows = {r[0]: r[1:] for r in _lake(con, lake, "source_row_number, export_time, flow_start, flow_date")}
    assert rows[1][0] == rows[2][0] == _utc(2026, 9, 1, 20)
    assert rows[3][1:] == (_utc(2026, 9, 1, 0, 30), date(2026, 9, 1))  # partitioned by the UTC date
    assert _rejects(con, lake) == [
        (4, "cast_failed", "time_stamp", "2026-09-01 20:00:00 EST", TEXT_FORMAT_DETAIL),
        (5, "cast_failed", "flow_start_time", "not a time", TEXT_FORMAT_DETAIL),
    ]


def test_rejected_rows_are_not_counted_as_assumed_utc(con, contract, sample_csv, tmp_path):
    src = _edit_csv(sample_csv, tmp_path / "ts.csv", {
        7: {"time_stamp": "2026-09-01 20:00:00", "num_bytes": "abc"},  # rejected for num_bytes
        8: {"time_stamp": "2026-09-01 20:00:00"},
    })
    entry = ingest_file(con, src, contract, tmp_path / "lake", tmp_path / "stage", "b", max_reject_fraction=0.1)
    assert entry.rejected_rows == 1
    assert entry.timestamps_without_offset == {"flow_start_time": 0, "flow_end_time": 0, "time_stamp": 1}


def test_ingested_values_do_not_depend_on_the_session_zone(ny_con, contract, sample_csv, tmp_path):
    src = _edit_csv(sample_csv, tmp_path / "ts.csv", {2: {"time_stamp": "2026-09-01 20:00:00"}})
    ingest_file(ny_con, src, contract, tmp_path / "lake", tmp_path / "stage", "b")
    assert _lake(ny_con, tmp_path / "lake", "export_time")[1] == (_utc(2026, 9, 1, 20),)


def test_naive_parquet_and_offset_free_csv_give_the_same_lake(con, contract, sample_csv, tmp_path):
    """Same wall-clock values without an offset: CSV text, naive Parquet and the +00:00 original agree."""
    naive = ", ".join(
        f"CAST(CAST({c} AS TIMESTAMPTZ) AS TIMESTAMP{'_NS' if c == 'time_stamp' else ''}) AS {c}" for c in TS_COLUMNS)
    pq_path, csv_path = tmp_path / "naive.parquet", tmp_path / "naive.csv"
    con.execute(f"COPY (SELECT * REPLACE ({naive}) FROM read_csv('{sample_csv.as_posix()}')) "
                f"TO '{pq_path.as_posix()}' (FORMAT parquet)")
    as_text = ", ".join(f"strftime({c}, '{NO_OFFSET_FORMAT}') AS {c}" for c in TS_COLUMNS)
    con.execute(f"COPY (SELECT * REPLACE ({as_text}) FROM read_parquet('{pq_path.as_posix()}')) "
                f"TO '{csv_path.as_posix()}' (HEADER)")
    types = dict(con.execute(f"SELECT column_name, column_type FROM (DESCRIBE FROM '{pq_path.as_posix()}')").fetchall())
    assert [types[c] for c in TS_COLUMNS] == ["TIMESTAMP", "TIMESTAMP", "TIMESTAMP_NS"]

    e_pq = ingest_file(con, pq_path, contract, tmp_path / "lake_pq", tmp_path / "stage", "b")
    e_csv = ingest_file(con, csv_path, contract, tmp_path / "lake_csv", tmp_path / "stage", "b")
    e_ref = ingest_file(con, sample_csv, contract, tmp_path / "lake_ref", tmp_path / "stage", "b")

    assert e_pq.timestamps_without_offset == e_csv.timestamps_without_offset == dict.fromkeys(TS_COLUMNS, 50)
    assert e_ref.timestamps_without_offset == dict.fromkeys(TS_COLUMNS, 0)  # sample.csv carries +00:00
    cols = "source_row_number, flow_start, flow_end, export_time, flow_date"
    ref = _lake(con, tmp_path / "lake_ref", cols)
    assert _lake(con, tmp_path / "lake_pq", cols) == ref
    assert _lake(con, tmp_path / "lake_csv", cols) == ref


def test_parquet_text_timestamps_follow_the_csv_rules(con, contract, sample_csv, tmp_path):
    table = _text_parquet(con, sample_csv, tmp_path / "text.parquet")
    values = table.column("time_stamp").to_pylist()
    values[0], values[1] = "2026-09-01 20:00:00", "garbage"
    path = tmp_path / "mixed.parquet"
    pq.write_table(_replace(table, "time_stamp", pa.array(values)), path)

    entry = ingest_file(con, path, contract, tmp_path / "lake", tmp_path / "stage", "b", max_reject_fraction=0.1)

    assert (entry.status, entry.rows, entry.rejected_rows) == (Status.INGESTED, 49, 1)
    assert entry.timestamps_without_offset == {"flow_start_time": 0, "flow_end_time": 0, "time_stamp": 1}
    assert _rejects(con, tmp_path / "lake") == [(2, "cast_failed", "time_stamp", "garbage", TEXT_FORMAT_DETAIL)]


def test_unsupported_parquet_type_is_rejected_not_guessed(con, contract, sample_csv, tmp_path):
    table = _text_parquet(con, sample_csv, tmp_path / "text.parquet")
    epoch = pa.array([1_788_300_000] * table.num_rows, pa.int64())  # epoch seconds? milliseconds? never guessed
    path = tmp_path / "epoch.parquet"
    pq.write_table(_replace(table, "time_stamp", epoch), path)

    entry = ingest_file(con, path, contract, tmp_path / "lake", tmp_path / "stage", "b")

    assert (entry.status, entry.rows, entry.rejected_rows) == (Status.QUARANTINED, 0, 50)
    assert "cast_failed=50" in entry.reason


# --- ledger and data-quality report --------------------------------------------------

def test_ledger_lines_written_before_v1_4_still_load(tmp_path):
    old = {"source_file": "a.csv", "file_hash": "ab" * 32, "fmt": "csv", "status": "ingested", "rows": 5,
           "reason": "", "ingest_batch_id": "b", "recorded_at": "2026-09-01T00:00:00+00:00", "rejected_rows": 0}
    (tmp_path / "_ingest_ledger.jsonl").write_text(json.dumps(old) + "\n", encoding="utf-8")
    assert read_ledger(tmp_path)["ab" * 32] == LedgerEntry(**old)
    assert read_ledger(tmp_path)["ab" * 32].timestamps_without_offset == {}


def test_dq_report_warns_about_timestamps_without_offset(con, contract, sample_csv, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    _edit_csv(sample_csv, raw / "ts.csv", {r: {"time_stamp": "2026-09-01 20:00:00"} for r in (1, 2, 3)})
    lake = tmp_path / "lake"
    ingest_file(con, raw / "ts.csv", contract, lake, tmp_path / "stage", "b")

    report = build_report(con, contract, lake, raw, "b", DQSettings())

    assert [(w.source_file, w.column, w.values, w.rate) for w in report.timestamps_without_offset] == [
        ("ts.csv", "time_stamp", 3, 3 / 50)]
    md = render_markdown(report)
    assert "**Warning:** 3 timestamp values had no UTC offset" in md
    assert "| ts.csv | time_stamp | 3 | 6.00% |" in md


def test_dq_report_without_offset_free_timestamps_says_none(con, contract, sample_csv, tmp_path):
    lake = tmp_path / "lake"
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b")
    md = render_markdown(build_report(con, contract, lake, sample_csv.parent, "b", DQSettings()))
    assert "**Warning:**" not in md
    assert "None: every ingested timestamp carried an offset." in md


def test_dq_command_json_lists_the_warning(sample_csv, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    _edit_csv(sample_csv, raw / "ts.csv", {1: {"flow_start_time": "2026-09-01 20:10:21"}})
    main(["--root", str(tmp_path), "ingest"])
    main(["--root", str(tmp_path), "dq"])
    (json_path,) = (tmp_path / "outputs" / "dq").glob("dq_*.json")
    assert json.loads(json_path.read_text(encoding="utf-8"))["timestamps_without_offset"] == [
        {"source_file": "ts.csv", "column": "flow_start_time", "values": 1, "rate": 1 / 50}]
