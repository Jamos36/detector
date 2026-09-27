"""V2-3 host novelty: new-destination / new-port rates from a persistent seen set of strictly earlier windows."""

from __future__ import annotations

import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from netanomaly import novelty
from netanomaly.feature_registry import load_registry
from netanomaly.novelty import build_host_novelty, require_usable_inputs
from netanomaly.schema import Confidence

DAYS = [date(2026, 9, 1) + timedelta(days=i) for i in range(5)]
CUTOFF = datetime(2026, 9, 3, 12, tzinfo=UTC)  # on a window boundary: windows before it must never change
WINDOW = 5

Flow = tuple[str, datetime, str, int | None]  # src_ip, flow_start, dst_ip, dst_port


def _at(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=UTC)


def _write_day(lake: Path, day: date, flows: list[Flow], name: str = "src_test_0.parquet",
               sequence: list[int] | None = None) -> None:
    rows = [f for f in flows if f[1].date() == day]
    cols = {
        "src_ip": [r[0] for r in rows],
        "flow_start": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
        "dst_ip": [r[2] for r in rows],
        "dst_port": pa.array([r[3] for r in rows], pa.int32()),
    }
    if sequence is not None:
        cols["flow_sequence"] = pa.array(sequence[:len(rows)], pa.int64())
    part = lake / "flows" / f"flow_date={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(cols), part / name)


def _write_lake(lake: Path, flows: list[Flow]) -> Path:
    for day in sorted({f[1].date() for f in flows}):
        _write_day(lake, day, flows)
    return lake


def _run(con, root: Path, rebuild: bool = False, window: int = WINDOW) -> novelty.NoveltyRun:
    return build_host_novelty(con, root / "lake", root / "host_novelty", root / "state", window, rebuild=rebuild)


def _rows(con, root: Path) -> dict:
    rel = con.sql(f"SELECT * FROM read_parquet('{(root / 'host_novelty' / '**' / '*.parquet').as_posix()}')")
    return {(r[0], r[1]): dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()}


def _build(con, tmp_path: Path, name: str, flows: list[Flow]) -> dict:
    root = tmp_path / name
    _write_lake(root / "lake", flows)
    _run(con, root)
    return _rows(con, root)


def _history_flows() -> list[Flow]:
    """Flows before CUTOFF: three hosts that keep adding destinations and ports over days 1-3."""
    flows: list[Flow] = []
    for d, day in enumerate(DAYS[:3]):
        for hour in (1, 2, 11):
            for h in range(3):
                src = f"10.0.0.{h + 1}"
                flows.append((src, _at(day, hour), f"192.168.0.{h}", 443))                          # same peer
                flows.append((src, _at(day, hour, 1), f"192.168.{d + 1}.{hour}", 8000 + hour + d))  # new per day
    assert all(f[1] < CUTOFF for f in flows)
    return flows


def _future_flows() -> list[Flow]:
    """Flows at or after CUTOFF that would change earlier windows if they leaked: they re-contact every earlier
    destination and port (a lake-wide 'ever seen' rule would stop earlier firsts being new), add new peers, start
    exactly on CUTOFF (tied timestamps), and bring a new host."""
    flows: list[Flow] = []
    earlier = sorted({(s, d, p) for s, _, d, p in _history_flows()})
    for day_idx, day in enumerate(DAYS[2:]):
        start = CUTOFF if day_idx == 0 else _at(day, 0)
        flows += [(s, start, d, p) for s, d, p in earlier]
        for k in range(5):
            flows.append(("10.0.0.1", start, f"172.16.9.{k}", 20_000 + k))
            flows.append(("10.0.0.99", start + timedelta(minutes=7), f"172.16.8.{k}", 30_000 + k))
    assert all(f[1] >= CUTOFF for f in flows)
    return flows


# --- definitions --------------------------------------------------------------------------------------------

def test_novelty_definitions_on_a_small_example(con, tmp_path):
    d1, d2 = DAYS[0], DAYS[1]
    flows: list[Flow] = [
        ("h", _at(d1, 0, 0), "A", 80), ("h", _at(d1, 0, 2), "B", 80), ("h", _at(d1, 0, 4, 59), "A", 443),
        ("h", _at(d1, 0, 5), "A", 80), ("h", _at(d1, 0, 6), "C", 22),       # next window: C and 22 are new
        ("h", _at(d1, 0, 12), "D", None), ("h", _at(d1, 0, 13), "A", None),  # NULL ports only
        ("h", _at(d2, 9, 0), "A", 80), ("h", _at(d2, 9, 1), "E", 8080),
        ("g", _at(d2, 9, 0), "A", 80),                                      # another host: its own seen set
    ]
    rows = _build(con, tmp_path, "a", flows)
    got = {k: (r["uniq_dst_ip"], r["new_dst_ip"], r["new_dst_ip_rate"],
               r["uniq_dst_port"], r["new_dst_port"], r["new_dst_port_rate"]) for k, r in rows.items()}
    assert got == {
        ("h", _at(d1, 0, 0)): (2, 2, 1.0, 2, 2, 1.0),     # host's first window: everything is new
        ("h", _at(d1, 0, 5)): (2, 1, 0.5, 2, 1, 0.5),     # A and port 80 seen in the earlier window
        ("h", _at(d1, 0, 10)): (2, 1, 0.5, 0, 0, None),   # D new, A not; no port -> NULL rate
        ("h", _at(d2, 9, 0)): (2, 1, 0.5, 2, 1, 0.5),     # history crosses days: E and 8080 new
        ("g", _at(d2, 9, 0)): (1, 1, 1.0, 1, 1, 1.0),
    }
    assert rows[("h", _at(d2, 9, 0))]["host_first_seen"] == _at(d1, 0, 0)
    assert rows[("g", _at(d2, 9, 0))]["host_first_seen"] == _at(d2, 9, 0)
    assert {r["history_days"] for k, r in rows.items() if k[1].date() == d2} == {1}


def test_tied_timestamps_and_row_order_cannot_change_results(con, tmp_path):
    """Rows with the same flow_start share one window and one history: neither is the other's history, and the
    order of rows in the file (or flow_sequence) plays no role."""
    t = _at(DAYS[0], 3, 10)
    flows: list[Flow] = [("h", _at(DAYS[0], 3, 0), "A", 1), ("h", t, "X", 2), ("h", t, "Y", 2), ("h", t, "A", 3),
                         ("h", t + timedelta(minutes=4, seconds=59), "X", 2),  # same window as the tie
                         ("h", t + timedelta(minutes=5), "X", 2)]              # boundary: next window, X seen
    rows = _build(con, tmp_path, "a", flows)
    assert (rows[("h", t)]["new_dst_ip"], rows[("h", t)]["uniq_dst_ip"]) == (2, 3)  # X, Y new; A seen at 03:00
    assert (rows[("h", t)]["new_dst_port"], rows[("h", t)]["uniq_dst_port"]) == (2, 2)
    assert rows[("h", t + timedelta(minutes=5))]["new_dst_ip_rate"] == 0.0

    shuffled_root = tmp_path / "b"
    shuffled = flows[:]
    random.Random(1).shuffle(shuffled)
    _write_day(shuffled_root / "lake", DAYS[0], shuffled, sequence=list(range(len(shuffled), 0, -1)))
    _run(con, shuffled_root)
    assert _rows(con, shuffled_root) == rows


# --- temporal leakage ---------------------------------------------------------------------------------------

def test_future_rows_cannot_change_earlier_results(con, tmp_path):
    before = _build(con, tmp_path, "before", _history_flows())
    after = _build(con, tmp_path, "after", _history_flows() + _future_flows())
    earlier = {k: v for k, v in after.items() if k[1] < CUTOFF}
    assert earlier == before  # every column, including windows earlier on the cutoff day
    assert any(k[1].date() == CUTOFF.date() for k in before)


def test_the_leakage_test_data_does_change_later_results(con, tmp_path):
    """Guards the test above: once they are history, the future rows must move later results."""
    after = _build(con, tmp_path, "after", _history_flows() + _future_flows())
    at_cutoff = after[("10.0.0.1", CUTOFF)]
    assert at_cutoff["new_dst_ip"] == 5 and at_cutoff["uniq_dst_ip"] > 5  # 172.16.9.x new; earlier peers not
    assert after[("10.0.0.1", _at(DAYS[3], 0))]["new_dst_ip"] == 0  # the cutoff window's new peers are history now
    assert after[("10.0.0.99", CUTOFF + timedelta(minutes=5))]["host_first_seen"] == CUTOFF + timedelta(minutes=5)


# --- persistence and reruns ---------------------------------------------------------------------------------

def _mtimes(root: Path) -> dict[str, int]:
    return {p.relative_to(root).as_posix(): p.stat().st_mtime_ns
            for d in ("host_novelty", "state") for p in (root / d).rglob("*.parquet")}


def test_repeated_runs_are_idempotent_and_recompute_nothing(con, tmp_path):
    root = tmp_path / "a"
    _write_lake(root / "lake", _history_flows())
    first = _run(con, root)
    assert first.recomputed == tuple(DAYS[:3]) and first.days == 3
    rows, mtimes = _rows(con, root), _mtimes(root)
    for _ in range(2):
        again = _run(con, root)
        assert again.recomputed == () and again.rows == first.rows
    assert _rows(con, root) == rows and _mtimes(root) == mtimes  # nothing rewritten
    assert _run(con, root, rebuild=True).recomputed == tuple(DAYS[:3])
    assert _rows(con, root) == rows  # a full rebuild gives the same result


def test_new_day_is_appended_without_touching_earlier_days(con, tmp_path):
    flows = _history_flows() + _future_flows()
    root = tmp_path / "inc"
    _write_lake(root / "lake", [f for f in flows if f[1].date() < DAYS[3]])
    _run(con, root)
    mtimes = _mtimes(root)
    _write_day(root / "lake", DAYS[3], flows)
    assert _run(con, root).recomputed == (DAYS[3],)
    assert {k: v for k, v in _mtimes(root).items() if k in mtimes} == mtimes
    assert _rows(con, root) == _build(con, tmp_path, "full", [f for f in flows if f[1].date() <= DAYS[3]])


def test_late_flows_for_a_past_day_recompute_from_that_day(con, tmp_path):
    flows = _history_flows()
    root = tmp_path / "late"
    _write_lake(root / "lake", flows)
    _run(con, root)
    day1 = {k: v for k, v in _mtimes(root).items() if DAYS[0].isoformat() in k}
    late: list[Flow] = [("10.0.0.1", _at(DAYS[1], 23), "198.51.100.7", 25)]
    _write_day(root / "lake", DAYS[1], late, name="src_late_0.parquet")
    assert _run(con, root).recomputed == (DAYS[1], DAYS[2])
    assert {k: v for k, v in _mtimes(root).items() if DAYS[0].isoformat() in k} == day1  # day 1 untouched
    assert _rows(con, root) == _build(con, tmp_path, "full", flows + late)

    (root / "lake" / "flows" / f"flow_date={DAYS[1].isoformat()}" / "src_late_0.parquet").unlink()
    assert _run(con, root).recomputed == (DAYS[1], DAYS[2])  # removing a file is a change too
    assert _rows(con, root) == _build(con, tmp_path, "orig", flows)


def test_removed_day_missing_output_and_changed_window_trigger_recompute(con, tmp_path):
    root = tmp_path / "a"
    _write_lake(root / "lake", _history_flows())
    _run(con, root)
    (root / "host_novelty" / f"flow_date={DAYS[1].isoformat()}" / "part-0.parquet").unlink()
    assert _run(con, root).recomputed == (DAYS[1], DAYS[2])
    for p in (root / "lake" / "flows" / f"flow_date={DAYS[2].isoformat()}").glob("*.parquet"):
        p.unlink()
    run = _run(con, root)
    assert run.recomputed == () and run.days == 2  # nothing left to compute; day 3 partitions dropped
    assert not (root / "host_novelty" / f"flow_date={DAYS[2].isoformat()}").exists()
    assert not (root / "state" / "seen_dst_ip" / f"first_date={DAYS[2].isoformat()}").exists()
    assert _run(con, root, window=10).recomputed == tuple(DAYS[:2])
    assert json.loads((root / "state" / "manifest.json").read_text())["window_minutes"] == 10


def test_interrupted_run_resumes_at_the_first_unfinished_day(con, tmp_path, monkeypatch):
    flows = _history_flows()
    root = tmp_path / "crash"
    _write_lake(root / "lake", flows)
    real = novelty.compute_day

    def crash_on_day3(con_, files, day, *args):
        real(con_, files, day, *args)
        if day == DAYS[2]:
            raise RuntimeError("simulated crash after writing day 3, before the manifest")

    monkeypatch.setattr(novelty, "compute_day", crash_on_day3)
    with pytest.raises(RuntimeError):
        _run(con, root)
    monkeypatch.setattr(novelty, "compute_day", real)
    assert _run(con, root).recomputed == (DAYS[2],)
    assert _rows(con, root) == _build(con, tmp_path, "full", flows)


def test_window_must_divide_a_day(con, tmp_path):
    _write_lake(tmp_path / "a" / "lake", _history_flows())
    with pytest.raises(ValueError, match="divide a day"):
        _run(con, tmp_path / "a", window=7)


# --- inputs and eligibility ---------------------------------------------------------------------------------

def test_novelty_inputs_are_usable_in_the_registry(contract):
    require_usable_inputs(load_registry(), contract)


@pytest.mark.parametrize(("field", "feature"), [("dst_ip", "new_dst_ip_rate"), ("dst_port", "new_dst_port_rate")])
def test_novelty_refuses_inputs_the_contract_rates_low(contract, field, feature):
    cols = [c.model_copy(update={"confidence": Confidence.LOW}) if c.name == field else c for c in contract.columns]
    with pytest.raises(ValueError, match=feature):
        require_usable_inputs(load_registry(), contract.model_copy(update={"columns": cols}))
