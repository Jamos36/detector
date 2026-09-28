from __future__ import annotations

import csv
import json
import shutil
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

import duckdb
import pytest

from netanomaly.cli import main
from netanomaly.ingest import (
    FileFormat,
    Status,
    detect_format,
    file_sha256,
    ingest_directory,
    ingest_file,
    read_ledger,
    select_sources,
)
from netanomaly.schema import Confidence, ModelUse
from tests.conftest import FIXTURES


def _flows(con, lake: Path, where: str = "true"):
    glob = (lake / "flows" / "**" / "*.parquet").as_posix()
    return con.sql(f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true) WHERE {where}")


def _to_parquet(con, csv_path: Path, out: Path) -> Path:
    con.execute(f"COPY (SELECT * FROM read_csv('{csv_path.as_posix()}')) TO '{out.as_posix()}' (FORMAT parquet)")
    return out


# --- schema contract ---------------------------------------------------------

def test_contract_covers_every_raw_column(contract, sample_csv):
    header = next(csv.reader(sample_csv.open(encoding="utf-8")))
    assert contract.check_columns(header) == ([], [])


def test_schema_md_is_generated_from_contract(contract):
    schema_md = Path(__file__).parent.parent / "SCHEMA.md"
    assert schema_md.read_text(encoding="utf-8") == contract.to_markdown(), "run: uv run netanomaly schema-doc"


def test_no_field_is_marked_validated_without_collector_docs(contract):
    # Flip a field to validated only when its meaning is confirmed from the exporter's
    # documentation, then update this test with the confirmed fields.
    assert [c.raw for c in contract.columns if c.validated] == []


def test_low_confidence_columns_are_never_model_features(contract):
    for col in contract.columns:
        if col.confidence in (Confidence.LOW, Confidence.UNKNOWN):
            assert col.model_use is not ModelUse.FEATURE, col.raw


# --- format detection --------------------------------------------------------

def test_detects_csv_by_content(sample_csv):
    assert detect_format(sample_csv) is FileFormat.CSV


def test_detects_parquet_even_with_csv_extension(con, sample_csv, tmp_path):
    disguised = _to_parquet(con, sample_csv, tmp_path / "actually_parquet.csv")
    assert detect_format(disguised) is FileFormat.PARQUET


@pytest.mark.parametrize("content", [b"", b"\x00\x01\x02binary", b"PAR1 truncated parquet"])
def test_rejects_empty_binary_and_truncated_files(tmp_path, content):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(content)
    assert detect_format(bad) is FileFormat.UNKNOWN


def test_parquet_wins_over_same_named_csv(tmp_path):
    paths = [tmp_path / "a.csv", tmp_path / "a.parquet", tmp_path / "b.csv"]
    chosen, skipped = select_sources(paths)
    assert sorted(p.name for p in chosen) == ["a.parquet", "b.csv"]
    assert [p.name for p in skipped] == ["a.csv"]


# --- ingestion and provenance --------------------------------------------------

def test_csv_ingest_preserves_row_provenance(con, contract, sample_csv, tmp_path):
    entry = ingest_file(con, sample_csv, contract, tmp_path / "lake", tmp_path / "stage", "b1")
    assert entry.status == Status.INGESTED and entry.rows == 50

    rel = _flows(con, tmp_path / "lake")
    cols = rel.columns
    raw_lines = list(csv.DictReader(sample_csv.open(encoding="utf-8")))
    for row in rel.order("source_row_number").fetchall():
        rec = dict(zip(cols, row, strict=True))
        original = raw_lines[rec["source_row_number"] - 1]
        assert rec["src_ip"] == original["src_id_addr"]
        assert rec["src_port"] == int(original["src_port"])
        assert rec["bytes"] == int(original["num_bytes"])


def test_csv_and_parquet_sources_normalize_identically(con, contract, sample_csv, tmp_path):
    pq_path = _to_parquet(con, sample_csv, tmp_path / "sample.parquet")
    ingest_file(con, sample_csv, contract, tmp_path / "lake_csv", tmp_path / "stage", "b")
    ingest_file(con, pq_path, contract, tmp_path / "lake_pq", tmp_path / "stage", "b")
    cols = "source_row_number, src_ip, dst_ip, dst_port, bytes, packets, flow_start, flow_end, tcp_flags"
    a = _flows(con, tmp_path / "lake_csv").select(cols).order("source_row_number").fetchall()
    b = _flows(con, tmp_path / "lake_pq").select(cols).order("source_row_number").fetchall()
    assert a == b


def test_flow_id_is_deterministic_across_reingest(con, contract, sample_csv, tmp_path):
    lake = tmp_path / "lake"
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b1")
    first = _flows(con, lake).select("flow_id").order("flow_id").fetchall()
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b2")
    second = _flows(con, lake).select("flow_id").order("flow_id").fetchall()
    assert first == second and len(second) == 50  # re-ingest replaces, never duplicates


def test_timestamps_are_utc_and_partitioned_by_utc_date(con, contract, sample_csv, tmp_path):
    ingest_file(con, sample_csv, contract, tmp_path / "lake", tmp_path / "stage", "b")
    bad = _flows(con, tmp_path / "lake", "flow_date <> CAST(flow_start AS DATE)").fetchall()
    assert bad == []
    assert con.sql("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"


def test_tcp_flag_bits_parse_hyphenated_labels(con, contract, sample_csv, tmp_path):
    ingest_file(con, sample_csv, contract, tmp_path / "lake", tmp_path / "stage", "b")
    wrong = _flows(
        con, tmp_path / "lake",
        "(tcp_flags = 'SYN-ACK' AND NOT (tcp_syn AND tcp_ack)) OR (tcp_flags IS NULL AND (tcp_syn OR tcp_ack))",
    ).fetchall()
    assert wrong == []


def test_directory_ingest_resumes_and_quarantines(con, contract, sample_csv, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    shutil.copy(sample_csv, raw / "good.csv")
    (raw / "corrupt.parquet").write_bytes(b"PAR1 not really")
    (raw / "wrong_schema.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    lake = tmp_path / "lake"

    first = {e.source_file: e.status for e in ingest_directory(con, raw, contract, lake, tmp_path / "stage")}
    assert first == {"good.csv": "ingested", "corrupt.parquet": "quarantined", "wrong_schema.csv": "quarantined"}

    second = ingest_directory(con, raw, contract, lake, tmp_path / "stage")
    assert {e.source_file for e in second} == {"corrupt.parquet", "wrong_schema.csv"}  # good.csv skipped
    assert any(e.status == Status.INGESTED for e in read_ledger(lake).values())


# --- regressions from code review ---------------------------------------------

def test_truncated_ledger_line_is_skipped_not_fatal(con, contract, sample_csv, tmp_path):
    raw, lake = tmp_path / "raw", tmp_path / "lake"
    raw.mkdir()
    shutil.copy(sample_csv, raw / "good.csv")
    lake.mkdir()
    (lake / "_ingest_ledger.jsonl").write_text('{"source_file": "x.csv", "file_ha', encoding="utf-8")  # crash mid-append
    results = ingest_directory(con, raw, contract, lake, tmp_path / "stage")
    assert [e.status for e in results] == ["ingested"]


def test_in_progress_file_is_retried(con, contract, sample_csv, tmp_path):
    raw, lake = tmp_path / "raw", tmp_path / "lake"
    raw.mkdir()
    shutil.copy(sample_csv, raw / "good.csv")
    ingest_directory(con, raw, contract, lake, tmp_path / "stage")
    h = next(iter(read_ledger(lake)))
    with (lake / "_ingest_ledger.jsonl").open("a", encoding="utf-8") as fh:  # simulate a crash after IN_PROGRESS
        fh.write(json.dumps({**asdict(read_ledger(lake)[h]), "status": "in_progress"}) + "\n")
    again = ingest_directory(con, raw, contract, lake, tmp_path / "stage")
    assert [e.status for e in again] == ["ingested"]
    assert _flows(con, lake).count("*").fetchone()[0] == 50  # replaced, not duplicated


def test_failed_reingest_keeps_previous_output(con, contract, sample_csv, tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b1")

    def boom(*_args, **_kwargs):
        raise duckdb.IOException("disk full")

    monkeypatch.setattr("netanomaly.ingest._normalize_select", boom)
    entry = ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b2")
    assert entry.status == Status.QUARANTINED
    assert _flows(con, lake).count("*").fetchone()[0] == 50


def test_ledger_lines_written_before_rejects_existed_still_load(tmp_path):
    lake = tmp_path / "lake"
    lake.mkdir()
    old = {"source_file": "a.csv", "file_hash": "h", "fmt": "csv", "status": "ingested", "rows": 5,
           "reason": "", "ingest_batch_id": "b", "recorded_at": "2026-09-27T00:00:00+00:00"}
    (lake / "_ingest_ledger.jsonl").write_text(json.dumps(old) + "\n", encoding="utf-8")
    assert read_ledger(lake)["h"].rejected_rows == 0


# --- row-level rejects (V1-1) --------------------------------------------------

def _corrupt_csv(sample_csv: Path, out: Path, edits: dict[int, Callable[[dict[str, str]], str]]) -> Path:
    """Copy sample_csv, replacing 1-based data row r with edits[r](fields)."""
    header, *rows = sample_csv.read_text(encoding="utf-8").splitlines()
    cols = header.split(",")
    for r, edit in edits.items():
        rows[r - 1] = edit(dict(zip(cols, next(csv.reader([rows[r - 1]])), strict=True)))
    out.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return out


def _with(**changes: str) -> Callable[[dict[str, str]], str]:
    def edit(fields: dict[str, str]) -> str:
        return ",".join(changes.get(k, v) for k, v in fields.items())
    return edit


def _extra_column(fields: dict[str, str]) -> str:
    return ",".join(fields.values()) + ",EXTRA"


def _rejects(con, lake: Path):
    glob = (lake / "rejects" / "*.parquet").as_posix()
    return con.sql(f"SELECT * FROM read_parquet('{glob}')")


def _assert_provenance(con, lake: Path, source_csv: Path) -> list[int]:
    """Every lake row matches its source record; returns the kept source_row_numbers."""
    records = list(csv.DictReader(source_csv.open(encoding="utf-8", newline="")))
    rows = _flows(con, lake).select("source_row_number, src_ip, bytes").order("source_row_number").fetchall()
    for row_number, src_ip, n_bytes in rows:
        original = records[row_number - 1]
        assert (src_ip, n_bytes) == (original["src_id_addr"], int(original["num_bytes"])), row_number
    return [r[0] for r in rows]


def test_bad_csv_rows_are_rejected_with_reasons_not_file_quarantine(con, contract, sample_csv, tmp_path):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {
        3: _extra_column,
        10: _with(num_bytes="abc"),
        20: _with(flow_start_time=""),
    })
    lake = tmp_path / "lake"
    entry = ingest_file(con, bad, contract, lake, tmp_path / "stage", "b1", max_reject_fraction=0.1)

    assert (entry.status, entry.rows, entry.rejected_rows) == (Status.INGESTED, 47, 3)
    rejects = _rejects(con, lake).select("source_row_number, reason, column_name, raw_value").order("1").fetchall()
    assert rejects == [
        (3, "too_many_columns", None, bad.read_text(encoding="utf-8").splitlines()[3]),
        (10, "cast_failed", "num_bytes", "abc"),
        (20, "missing_required", "flow_start_time", None),
    ]
    assert _assert_provenance(con, lake, bad) == [r for r in range(1, 51) if r not in (3, 10, 20)]


def test_truncated_record_is_one_reject_naming_the_first_missing_column(con, contract, sample_csv, tmp_path):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {6: lambda f: ",".join(list(f.values())[:10])})
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b")
    assert _rejects(con, lake).select("source_row_number, reason, column_name, detail").fetchall() == [
        (6, "missing_columns", contract.columns[10].raw, "Expected Number of Columns: 42 Found: 10")
    ]


def test_blank_required_ip_is_rejected(con, contract, sample_csv, tmp_path):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {9: _with(dist_id_addr="  ")})
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b")
    assert _rejects(con, lake).select("source_row_number, reason, column_name").fetchall() == [
        (9, "missing_required", "dist_id_addr")
    ]


def test_invalid_encoding_rejects_the_record_and_keeps_numbering(con, contract, sample_csv, tmp_path):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {})
    lines = bad.read_bytes().split(b"\n")
    lines[12] = lines[12].replace(b"IPv4", b"IPv\xff4", 1)  # data row 12
    bad.write_bytes(b"\n".join(lines))
    lake = tmp_path / "lake"
    entry = ingest_file(con, bad, contract, lake, tmp_path / "stage", "b")
    assert _rejects(con, lake).select("source_row_number, reason").fetchall() == [(12, "invalid_encoding")]
    assert entry.rows == 49
    assert _assert_provenance(con, lake, sample_csv) == [r for r in range(1, 51) if r != 12]


def test_reject_rows_keep_traceable_ids(con, contract, sample_csv, tmp_path):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {7: _with(ttl_min="not-a-number")})
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b1")
    rec = _rejects(con, lake).select("flow_id, source_file, source_file_hash, ingest_batch_id").fetchall()
    assert len(rec) == 1 and rec[0][1:] == ("bad.csv", file_sha256(bad), "b1")
    assert len(rec[0][0]) == 32 and rec[0][0] not in {r[0] for r in _flows(con, lake).select("flow_id").fetchall()}


@pytest.mark.parametrize("bad_rows", [(1,), (50,), (2, 3, 4), (1, 2, 49, 50), (5, 6, 20, 21, 22, 40)])
def test_rows_after_structural_rejects_keep_their_original_row_number(con, contract, sample_csv, tmp_path, bad_rows):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", dict.fromkeys(bad_rows, _extra_column))
    lake = tmp_path / "lake"
    entry = ingest_file(con, bad, contract, lake, tmp_path / "stage", "b", max_reject_fraction=0.5)
    assert entry.rejected_rows == len(bad_rows)
    assert _assert_provenance(con, lake, bad) == [r for r in range(1, 51) if r not in bad_rows]


def test_quoted_newline_does_not_shift_row_numbers(con, contract, sample_csv, tmp_path):
    # time_code is VARCHAR, so a quoted multi-line value is valid; row numbers count records, not lines.
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {2: _with(time_code='"multi\nline"'), 5: _extra_column})
    lake = tmp_path / "lake"
    entry = ingest_file(con, bad, contract, lake, tmp_path / "stage", "b", max_reject_fraction=0.5)
    assert (entry.rows, entry.rejected_rows) == (49, 1)
    assert _rejects(con, lake).select("source_row_number").fetchall() == [(5,)]
    assert _assert_provenance(con, lake, bad) == [r for r in range(1, 51) if r != 5]


def test_parquet_rows_with_uncastable_values_are_rejected(con, contract, sample_csv, tmp_path):
    bad_csv = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {4: _with(dist_port="https")})
    pq_path = _to_parquet(con, bad_csv, tmp_path / "bad.parquet")  # dist_port is VARCHAR in this file
    lake = tmp_path / "lake"
    entry = ingest_file(con, pq_path, contract, lake, tmp_path / "stage", "b")
    assert (entry.status, entry.rows, entry.rejected_rows) == (Status.INGESTED, 49, 1)
    assert _rejects(con, lake).select("source_row_number, reason, column_name, raw_value").fetchall() == [
        (4, "cast_failed", "dist_port", "https")
    ]


def test_too_many_rejects_quarantine_the_file_without_writing_it(con, contract, sample_csv, tmp_path):
    lake = tmp_path / "lake"
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b1")
    # same name as an already-ingested file, new content: the earlier output stays, nothing new is written
    bad = _corrupt_csv(sample_csv, tmp_path / "sample.csv", {r: _with(num_packets="x") for r in range(1, 11)})
    entry = ingest_file(con, bad, contract, lake, tmp_path / "stage", "b2", max_reject_fraction=0.1)
    assert entry.status == Status.QUARANTINED and (entry.rows, entry.rejected_rows) == (0, 10)
    assert "10 of 50 rows rejected" in entry.reason and "cast_failed=10" in entry.reason
    assert _flows(con, lake).count("*").fetchone()[0] == 50
    assert not list(lake.glob("rejects/*.parquet"))


def test_reingest_replaces_rejects_instead_of_appending(con, contract, sample_csv, tmp_path):
    bad = _corrupt_csv(sample_csv, tmp_path / "bad.csv", {8: _extra_column})
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b1")
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b2")
    assert _rejects(con, lake).select("ingest_batch_id").fetchall() == [("b2",)]
    assert _flows(con, lake).count("*").fetchone()[0] == 49


def test_clean_file_writes_no_rejects(con, contract, sample_csv, tmp_path):
    entry = ingest_file(con, sample_csv, contract, tmp_path / "lake", tmp_path / "stage", "b")
    assert entry.rejected_rows == 0
    assert not list((tmp_path / "lake").glob("rejects/*.parquet"))


def test_paths_with_apostrophes_work_end_to_end(tmp_path):
    root = tmp_path / "o'brien data"
    raw = root / "raw"
    raw.mkdir(parents=True)
    shutil.copy(FIXTURES / "sample.csv", raw / "sample.csv")
    main(["--root", str(root), "ingest"])  # a single day: ingest and features, but no time split to train on
    main(["--root", str(root), "features"])
    # two synthetic CSV days, so `run` can train on the first and score the second (V3 time split)
    main(["--root", str(root), "generate", "--days", "2", "--clean-days", "2", "--hosts", "20", "--format", "csv"])
    (raw / "sample.csv").unlink()
    shutil.rmtree(root / "lake")
    main(["--root", str(root), "run"])
    assert (root / "outputs" / "top_alerts.csv").exists()
