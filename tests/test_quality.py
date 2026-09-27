from __future__ import annotations

import csv
import json
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from netanomaly import quality
from netanomaly.cli import main
from netanomaly.config import DQSettings
from netanomaly.ingest import ingest_directory, ingest_file
from netanomaly.quality import VolumeStatus, build_report, daily_volume, latest_batch

SETTINGS = DQSettings()


# --- helpers -------------------------------------------------------------------

def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _write(rows: list[dict[str, str]], out: Path) -> Path:
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return out


def _edited(sample_csv: Path, out: Path, edits: dict[int, dict[str, str]]) -> Path:
    """Copy sample_csv with field overrides for 1-based data rows."""
    rows = _rows(sample_csv)
    for r, changes in edits.items():
        rows[r - 1] = {**rows[r - 1], **changes}
    return _write(rows, out)


def _ts(text: str) -> datetime:
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", text))  # source has nanoseconds


def _num(text: str) -> float | None:
    return float(text) if text != "" else None


def _oracle(rows: list[dict[str, str]]) -> dict[str, tuple[int, int]]:
    """(applicable, violations) per check, computed row by row in Python from the raw CSV."""
    out = {c.name: [0, 0] for c in quality.CHECKS}

    def tally(name: str, applies: bool, bad: bool) -> None:
        out[name][0] += applies
        out[name][1] += applies and bad

    for r in rows:
        packets, n_bytes, proto = _num(r["num_packets"]), _num(r["num_bytes"]), _num(r["ip_protocol_id"])
        ttl_min, ttl_max = _num(r["ttl_min"]), _num(r["ttl_max"])
        tally("flow_end_before_start", True, _ts(r["flow_end_time"]) < _ts(r["flow_start_time"]))
        tally("ttl_min_gt_ttl_max", None not in (ttl_min, ttl_max), (ttl_min or 0) > (ttl_max or 0))
        tally("bytes_per_packet_gt_1514", (packets or 0) > 0 and n_bytes is not None,
              (n_bytes or 0) > 1514 * (packets or 0))
        tally("syn_only_gt_3_packets", proto == 6, r["tcp_flag"].split("-") == ["SYN"] and (packets or 0) > 3)
        tally("icmp_with_ports", proto == 1, (_num(r["src_port"]) or 0) != 0 or (_num(r["dist_port"]) or 0) != 0)
        tally("ingress_eq_egress", "" not in (r["ingress_id"], r["egress_id"]), r["ingress_id"] == r["egress_id"])
        for raw, name in (("vlad_id", "vlan_id"), ("vlad_id_dot", "vlan_id_dot1q"),
                          ("vlad_id_customer", "vlan_id_customer")):
            tally(f"{name}_gt_4094", r[raw] != "", (_num(r[raw]) or 0) > 4094)
    return {k: (a, b) for k, (a, b) in out.items()}


def _lake_with_daily_counts(con, lake: Path, counts: dict[date, int]) -> None:
    for day, n in counts.items():
        out = lake / "flows" / f"flow_date={day.isoformat()}"
        out.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY (SELECT range AS x FROM range({n})) TO '{(out / 'src_t_0.parquet').as_posix()}' (FORMAT parquet)")


# --- per-row checks and null rates ---------------------------------------------

VIOLATIONS = {
    1: {"flow_end_time": "2026-09-01 00:00:00+00:00", "flow_start_time": "2026-09-01 00:00:05+00:00"},
    2: {"ttl_min": "200", "ttl_max": "10"},
    3: {"num_bytes": "999999", "num_packets": "1"},
    4: {"ip_protocol_id": "6", "tcp_flag": "SYN", "num_packets": "10"},
    5: {"ip_protocol_id": "1", "tcp_flag": "", "src_port": "0", "dist_port": "2048"},
    6: {"ingress_id": "7", "egress_id": "7"},
    7: {"vlad_id": "5000", "vlad_id_dot": "5000.0"},
    8: {"ttl_max": "", "vlad_id": ""},  # nulls in optional columns
}


def test_every_check_matches_a_row_by_row_oracle(con, contract, sample_csv, tmp_path):
    bad = _edited(sample_csv, tmp_path / "bad.csv", VIOLATIONS)
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b1")

    report = build_report(con, contract, lake, tmp_path, "b1", SETTINGS)

    got = {c.name: (c.applicable, c.violations) for c in report.checks}
    assert got == _oracle(_rows(bad))
    for name in ("flow_end_before_start", "ttl_min_gt_ttl_max", "bytes_per_packet_gt_1514", "syn_only_gt_3_packets",
                 "icmp_with_ports", "ingress_eq_egress", "vlan_id_gt_4094", "vlan_id_dot1q_gt_4094"):
        assert got[name][1] >= 1, name  # the injected violation is seen
    end_before_start = next(c for c in report.checks if c.name == "flow_end_before_start")
    assert end_before_start.example == "bad.csv:1"  # traceable to the source record


def test_null_rates_match_empty_raw_values(con, contract, sample_csv, tmp_path):
    bad = _edited(sample_csv, tmp_path / "bad.csv", VIOLATIONS)
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b1")

    report = build_report(con, contract, lake, tmp_path, "b1", SETTINGS)

    rows = _rows(bad)
    assert {n.column: n.nulls for n in report.null_rates} == {
        c.name: sum(r[c.raw] == "" for r in rows) for c in contract.columns
    }
    assert {n.column: n.rate for n in report.null_rates}["ttl_max"] == pytest.approx(1 / 50)


# --- rejects, batch scope -------------------------------------------------------

def test_rejects_are_counted_by_reason_and_column(con, contract, sample_csv, tmp_path):
    bad = _edited(sample_csv, tmp_path / "bad.csv", {3: {"num_bytes": "abc"}, 4: {"num_bytes": "xyz"}})
    lines = bad.read_text(encoding="utf-8").splitlines()
    lines[10] += ",EXTRA"  # data row 10
    bad.write_text("\n".join(lines) + "\n", encoding="utf-8")
    lake = tmp_path / "lake"
    ingest_file(con, bad, contract, lake, tmp_path / "stage", "b1", max_reject_fraction=0.1)

    report = build_report(con, contract, lake, tmp_path, "b1", SETTINGS)

    assert [(r.reason, r.column_name, r.rows) for r in report.rejects] == [
        ("cast_failed", "num_bytes", 2), ("too_many_columns", None, 1)
    ]
    assert (report.flows, report.rejected_rows) == (47, 3)


def test_report_covers_only_its_own_batch(con, contract, sample_csv, tmp_path):
    lake = tmp_path / "lake"
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "b1")
    later = _edited(sample_csv, tmp_path / "later.csv", {1: {"ttl_min": "250", "ttl_max": "1"}})
    ingest_file(con, later, contract, lake, tmp_path / "stage", "b2")

    first, second = (build_report(con, contract, lake, tmp_path, b, SETTINGS) for b in ("b1", "b2"))

    def ttl(report: quality.QualityReport) -> int:
        return next(c.violations for c in report.checks if c.name == "ttl_min_gt_ttl_max")

    assert [f.source_file for f in first.files] == ["sample.csv"] and first.flows == 50
    assert [f.source_file for f in second.files] == ["later.csv"] and second.flows == 50
    assert ttl(second) == ttl(first) + 1


def test_latest_batch_is_the_newest_in_the_ledger(con, contract, sample_csv, tmp_path):
    lake = tmp_path / "lake"
    assert latest_batch(lake) is None
    ingest_file(con, sample_csv, contract, lake, tmp_path / "stage", "20260101T000000Z")
    ingest_file(con, _edited(sample_csv, tmp_path / "b.csv", {}), contract, lake, tmp_path / "stage", "20260102T000000Z")
    assert latest_batch(lake) == "20260102T000000Z"


# --- column drift ---------------------------------------------------------------

def test_column_drift_against_the_contract(con, contract, sample_csv, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    rows = _rows(sample_csv)
    _write([{**r, "surprise": "1"} for r in rows], raw / "extra.csv")
    _write([{k: v for k, v in r.items() if k != "ttl_max"} for r in rows], raw / "missing.csv")
    typed = _edited(sample_csv, tmp_path / "typed.csv", {4: {"dist_port": "https"}})
    con.execute(f"COPY (SELECT * FROM read_csv('{typed.as_posix()}')) TO '{(raw / 'typed.parquet').as_posix()}' "
                "(FORMAT parquet)")
    lake = tmp_path / "lake"
    ingest_directory(con, raw, contract, lake, tmp_path / "stage")

    report = build_report(con, contract, lake, raw, latest_batch(lake), SETTINGS)

    drift = {d.source_file: d for d in report.drift}
    assert drift["extra.csv"].unexpected == ["surprise"] and drift["extra.csv"].missing == []
    assert drift["missing.csv"].missing == ["ttl_max"] and drift["missing.csv"].status == "quarantined"
    mismatches = drift["typed.parquet"].type_mismatches
    assert ("dist_port", "INTEGER", "VARCHAR", "family") in {
        (m.column, m.contract_type, m.found_type, m.kind) for m in mismatches
    }
    assert "src_port" in {m.column for m in mismatches if m.kind == "width"}  # BIGINT in file, INTEGER in contract
    assert drift["extra.csv"].type_mismatches == []  # CSV is untyped: type problems show up as cast_failed rejects


def test_drift_notes_a_raw_file_that_is_gone(con, contract, sample_csv, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    shutil.copy(sample_csv, raw / "a.csv")
    lake = tmp_path / "lake"
    ingest_directory(con, raw, contract, lake, tmp_path / "stage")
    (raw / "a.csv").write_text("replaced", encoding="utf-8")  # same name, different content

    report = build_report(con, contract, lake, raw, latest_batch(lake), SETTINGS)
    assert report.drift[0].note.startswith("raw file not found")


# --- daily volume vs trailing median --------------------------------------------

D0 = date(2026, 9, 1)


def _days(*counts: int) -> dict[date, int]:
    return {D0 + timedelta(days=i): n for i, n in enumerate(counts)}


def test_volume_drop_is_flagged_against_trailing_median(con, tmp_path):
    _lake_with_daily_counts(con, tmp_path, _days(100, 100, 100, 100, 100, 100, 100, 20))
    (day,) = daily_volume(con, tmp_path, [D0 + timedelta(days=7)], SETTINGS)
    assert (day.flows, day.trailing_median, day.history_days) == (20, 100, 7)
    assert day.ratio == pytest.approx(0.2) and day.status is VolumeStatus.LOW


def test_missing_days_in_history_count_as_zero(con, tmp_path):
    counts = {**_days(100, 100, 100, 100, 100, 100, 100), D0 + timedelta(days=9): 100}  # days 7, 8 absent
    _lake_with_daily_counts(con, tmp_path, counts)
    (day,) = daily_volume(con, tmp_path, [D0 + timedelta(days=9)], SETTINGS)
    assert (day.history_days, day.trailing_median) == (7, 100)  # window d2..d8: five 100s, two 0s
    (gap,) = daily_volume(con, tmp_path, [D0 + timedelta(days=8)], SETTINGS)
    assert (gap.flows, gap.status) == (0, VolumeStatus.LOW)  # an empty day is itself a volume drop


def test_first_days_have_insufficient_history(con, tmp_path):
    _lake_with_daily_counts(con, tmp_path, _days(100, 5000))
    first, second = daily_volume(con, tmp_path, [D0, D0 + timedelta(days=1)], SETTINGS)
    assert first.status is VolumeStatus.INSUFFICIENT_HISTORY and first.trailing_median is None
    assert second.status is VolumeStatus.INSUFFICIENT_HISTORY and second.history_days == 1


def test_volume_uses_only_earlier_days(con, tmp_path):
    """No leakage: adding later days must not change an earlier day's baseline or status."""
    target = D0 + timedelta(days=5)
    _lake_with_daily_counts(con, tmp_path / "a", _days(100, 90, 110, 100, 95, 300))
    _lake_with_daily_counts(con, tmp_path / "b", _days(100, 90, 110, 100, 95, 300, 10_000, 1, 1))
    before = daily_volume(con, tmp_path / "a", [target], SETTINGS)
    assert before == daily_volume(con, tmp_path / "b", [target], SETTINGS)
    assert before[0].status is VolumeStatus.HIGH


# --- CLI ------------------------------------------------------------------------

def test_dq_command_writes_json_and_markdown(sample_csv, tmp_path):
    root = tmp_path / "ds"
    (root / "raw").mkdir(parents=True)
    shutil.copy(sample_csv, root / "raw" / "sample.csv")
    main(["--root", str(root), "ingest"])
    main(["--root", str(root), "dq"])

    batch = latest_batch(root / "lake")
    data = json.loads((root / "outputs" / "dq" / f"dq_{batch}.json").read_text(encoding="utf-8"))
    assert data["batch_id"] == batch and data["flows"] == 50
    assert {c["name"] for c in data["checks"]} == {c.name for c in quality.CHECKS}
    md = (root / "outputs" / "dq" / f"dq_{batch}.md").read_text(encoding="utf-8")
    for heading in ("## Files", "## Plausibility checks", "## Null rates", "## Rejects", "## Daily volume",
                    "## Column drift"):
        assert heading in md


def test_dq_command_without_any_batch_writes_nothing(tmp_path):
    main(["--root", str(tmp_path), "dq"])
    assert not (tmp_path / "outputs" / "dq").exists()
