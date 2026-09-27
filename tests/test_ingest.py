from __future__ import annotations

import csv
import json
import shutil
from dataclasses import asdict
from pathlib import Path

import duckdb
import pytest

from netanomaly.cli import main
from netanomaly.ingest import (
    FileFormat,
    Status,
    detect_format,
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


def test_paths_with_apostrophes_work_end_to_end(tmp_path):
    root = tmp_path / "o'brien data"
    raw = root / "raw"
    raw.mkdir(parents=True)
    shutil.copy(FIXTURES / "sample.csv", raw / "sample.csv")
    main(["--root", str(root), "run"])
    assert (root / "outputs" / "top_alerts.csv").exists()
