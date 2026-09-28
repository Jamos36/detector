"""V2-4 timing regularity: interarrival CV per (src_ip, dst_ip) from flows strictly before the scored window."""

from __future__ import annotations

import random
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from netanomaly.config import TimingSettings
from netanomaly.feature_registry import load_registry
from netanomaly.schema import Confidence
from netanomaly.timing import TimingQuality, build_host_timing, require_usable_inputs

SETTINGS = TimingSettings(history_hours=1, min_events=4)
D1, D2, D3 = (date(2026, 9, 1) + timedelta(days=i) for i in range(3))
CUTOFF = datetime(2026, 9, 2, 12, tzinfo=UTC)  # on a window boundary: windows up to and including it never change
MIN = timedelta(minutes=1)
SEC = timedelta(seconds=1)

Flow = tuple[str, datetime, str]  # src_ip, flow_start, dst_ip


def _at(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=UTC)


def _every(src: str, dst: str, start: datetime, end: datetime, step: timedelta) -> list[Flow]:
    out, t = [], start
    while t < end:
        out.append((src, t, dst))
        t += step
    return out


def _write_lake(lake: Path, flows: list[Flow], files_per_day: int = 1, sequence: bool = False) -> Path:
    for day in sorted({f[1].date() for f in flows}):
        rows = [f for f in flows if f[1].date() == day]
        part = lake / "flows" / f"flow_date={day.isoformat()}"
        part.mkdir(parents=True)
        for i in range(files_per_day):
            chunk = rows[i::files_per_day]
            cols = {"src_ip": [r[0] for r in chunk],
                    "flow_start": pa.array([r[1] for r in chunk], pa.timestamp("us", tz="UTC")),
                    "dst_ip": [r[2] for r in chunk]}
            if sequence:  # exporter sequence numbers in reverse order: must play no role
                cols["flow_sequence"] = pa.array(range(len(chunk), 0, -1), pa.int64())
            pq.write_table(pa.table(cols), part / f"src_test_{i}.parquet")
    return lake


def _build(con, tmp_path: Path, name: str, flows: list[Flow], s: TimingSettings = SETTINGS, **write) -> dict:
    lake = _write_lake(tmp_path / name / "lake", flows, **write)
    out = tmp_path / name / "host_timing"
    build_host_timing(con, lake, out, tmp_path / name / "work", 5, s)
    rel = con.sql(f"SELECT * FROM read_parquet('{(out / '**' / '*.parquet').as_posix()}')")
    return {(r[0], r[1]): dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()}


def _cv(gaps: list[float]) -> float:
    return statistics.pstdev(gaps) / statistics.mean(gaps)


# --- definitions ------------------------------------------------------------------------------------------

def _known_flows() -> list[Flow]:
    w = _at(D1, 11)
    irregular = [0, 10, 60, 360, 380, 1000]  # seconds after 10:00
    return [
        *_every("b", "c2", _at(D1, 10), _at(D1, 11, 30), MIN),               # beacon, exactly 60 s
        *[("b", _at(D1, 10) + s * SEC, "web") for s in irregular],
        *[("r", _at(D1, 10) + s * SEC, "web") for s in irregular],
        ("r", w + 30 * SEC, "z"),                                            # r is active in W = 11:00
        ("e", _at(D1, 9, 59), "p"),                                          # lake start; just outside W's history
        *[("e", _at(D1, 10, m), "p") for m in (0, 10, 20, 30)],             # first one exactly at W - 1 h
        ("e", w, "p"), ("e", w + 2 * MIN, "p"),                              # inside W: never W's own history
        *[("i", _at(D1, 10, m), "q") for m in (0, 20, 40)],                 # 3 events < min_events
        ("i", w, "q"),
        ("n", w, "q"),                                                       # first flow ever
        *_every("m", "c2", _at(D1, 23, 20), _at(D2, 0, 45), 2 * MIN),       # beacon across midnight
    ]


def test_known_answers_for_cv_edges_and_quality(con, tmp_path):
    rows = _build(con, tmp_path, "a", _known_flows())
    w = _at(D1, 11)

    b = rows[("b", w)]
    assert (b["timing_quality"], b["timing_dst_ip"], b["timing_events"]) == ("ok", "c2", 60)
    assert b["interarrival_cv"] == pytest.approx(0, abs=1e-9) and b["timing_median_gap_s"] == 60
    assert (b["timing_pairs"], b["history_pairs"], b["history_events"]) == (2, 2, 66)

    r = rows[("r", w)]  # the minimum over one series is that series' CV
    assert (r["timing_dst_ip"], r["timing_events"]) == ("web", 6)
    assert r["interarrival_cv"] == pytest.approx(_cv([10, 50, 300, 20, 620]))
    assert r["timing_median_gap_s"] == 50

    e = rows[("e", w)]  # [10:00, 11:00): 10:00 is in, 09:59 and 11:00 are out
    assert (e["timing_events"], e["interarrival_cv"], e["timing_median_gap_s"]) == (4, 0, 600)
    assert e["history_complete"] is True
    first = rows[("e", _at(D1, 10))]  # history [09:00, 10:00) starts before the lake's first flow (09:59)
    assert (first["timing_quality"], first["history_events"], first["history_complete"]) == ("insufficient", 1, False)

    i = rows[("i", w)]
    assert (i["timing_quality"], i["history_events"], i["timing_pairs"]) == ("insufficient", 3, 0)
    assert i["interarrival_cv"] is None and i["timing_dst_ip"] is None and i["timing_events"] is None
    n = rows[("n", w)]
    assert (n["timing_quality"], n["history_events"], n["history_pairs"]) == ("none", 0, 0)
    assert n["interarrival_cv"] is None

    m = rows[("m", _at(D2, 0, 40))]  # history [23:40, 00:40) crosses the UTC day boundary
    assert (m["timing_events"], m["timing_median_gap_s"]) == (30, 120)
    assert m["interarrival_cv"] == pytest.approx(0, abs=1e-9)
    assert {r["timing_quality"] for r in rows.values()} == {q.value for q in TimingQuality}


def test_rows_are_the_v0_host_window_grid(con, tmp_path):
    flows = _known_flows()
    rows = _build(con, tmp_path, "a", flows)
    grid = {(ip, t.replace(minute=t.minute - t.minute % 5, second=0)) for ip, t, _ in flows}
    assert set(rows) == grid


def test_history_hours_bounds_the_series(con, tmp_path):
    rows = _build(con, tmp_path, "a", _known_flows(), TimingSettings(history_hours=2, min_events=4))
    e = rows[("e", _at(D1, 11))]  # [09:00, 11:00) now includes 09:59: 5 events, gaps 60 s + 3 x 600 s
    assert (e["timing_events"], e["history_complete"]) == (5, False)
    assert e["interarrival_cv"] == pytest.approx(_cv([60, 600, 600, 600]))


# --- ties and ordering ------------------------------------------------------------------------------------

def _tied_flows() -> list[Flow]:
    t = _at(D1, 10, 10)
    return [
        *[("t", _at(D1, 10, m), "a") for m in (0, 10, 20, 30)],
        ("t", t, "a"), ("t", t, "a"),                   # duplicates of an instant: one event, no zero gaps
        ("t", t, "b"), ("t", _at(D1, 10, 11), "b"),    # tie with another peer: separate series
        ("t", _at(D1, 11), "x"),
    ]


def test_tied_timestamps_are_one_event(con, tmp_path):
    rows = _build(con, tmp_path, "a", _tied_flows())
    r = rows[("t", _at(D1, 11))]
    assert (r["timing_dst_ip"], r["timing_events"], r["interarrival_cv"]) == ("a", 4, 0)  # 3 x 600 s
    assert (r["history_pairs"], r["history_events"]) == (2, 6)


def test_equal_cv_series_pick_the_lowest_dst_ip(con, tmp_path):
    w = _at(D1, 11)
    flows = [*_every("k", "z2", _at(D1, 10), w, 2 * MIN),   # written first
             *_every("k", "a2", _at(D1, 10), w, 2 * MIN),   # same rhythm: CV tie at 0
             ("k", w, "x")]
    r = _build(con, tmp_path, "a", flows)[("k", w)]
    assert (r["interarrival_cv"], r["timing_dst_ip"], r["timing_pairs"]) == (0, "a2", 2)


def test_shuffled_input_order_files_and_flow_sequence_cannot_change_results(con, tmp_path):
    flows = _known_flows() + _tied_flows()
    rows = _build(con, tmp_path, "a", flows)
    shuffled = flows[:]
    random.Random(4).shuffle(shuffled)
    assert _build(con, tmp_path, "b", shuffled, files_per_day=3, sequence=True) == rows


# --- temporal leakage -------------------------------------------------------------------------------------

def _history_flows() -> list[Flow]:
    """Flows before CUTOFF, spanning two days, plus one marker flow per host AT the cutoff, so the cutoff window
    exists in both lakes and its value (which must ignore its own flows) can be compared too."""
    flows = [
        *_every("b", "c2", _at(D2, 10), CUTOFF, MIN),
        *[("b", _at(D2, 11) + s * SEC, "web") for s in (0, 7, 90, 400, 410, 1500, 2000)],
        *_every("m", "c2", _at(D1, 23), _at(D2, 0, 30), 3 * MIN),
        *[("i", _at(D2, 11, m), "q") for m in (5, 35)],
    ]
    assert all(t < CUTOFF for _, t, _ in flows)
    return [*flows, *[(ip, CUTOFF, "q") for ip in ("b", "i", "m", "n", "f")]]


def _future_flows() -> list[Flow]:
    """Flows at or after CUTOFF, built to move every result if they leaked: more flows inside the cutoff window
    (incl. ties with the markers), off-rhythm beacon events, new peers and a new host."""
    flows = [
        ("b", CUTOFF, "c2"), ("b", CUTOFF, "web"), ("b", CUTOFF + 30 * SEC, "c2"),
        *[("i", CUTOFF + s * SEC, "q") for s in (1, 2, 3, 4)],
        *[("b", CUTOFF + 37 * k * SEC, "c2") for k in range(200)],
        *_every("i", "q", CUTOFF, _at(D3, 3), 5 * MIN),
        *_every("m", "new", CUTOFF, _at(D3, 1), 2 * MIN),
        *[("f", CUTOFF + k * MIN, "q") for k in range(90)],
    ]
    assert all(t >= CUTOFF for _, t, _ in flows)
    return flows


def test_future_rows_cannot_change_earlier_results(con, tmp_path):
    before = _build(con, tmp_path, "before", _history_flows())
    after = _build(con, tmp_path, "after", _history_flows() + _future_flows())
    earlier = {k: v for k, v in after.items() if k[1] <= CUTOFF}
    assert earlier == before  # every column, including the cutoff window whose own flows changed
    assert before[("b", CUTOFF)]["timing_quality"] == "ok" and before[("b", CUTOFF)]["interarrival_cv"] == 0
    assert {r["timing_quality"] for k, r in before.items() if k[1] == CUTOFF} == {"ok", "insufficient", "none"}


def test_the_leakage_test_data_does_change_later_results(con, tmp_path):
    """Guards the test above: once they are history, the future rows must move later results."""
    after = _build(con, tmp_path, "after", _history_flows() + _future_flows())
    assert after[("b", CUTOFF + 30 * MIN)]["interarrival_cv"] > 0.05  # the 37 s events broke the 60 s rhythm
    assert after[("i", CUTOFF + 30 * MIN)]["timing_quality"] == "ok"
    assert after[("f", CUTOFF + 30 * MIN)]["timing_quality"] == "ok"


# --- inputs and eligibility -------------------------------------------------------------------------------

def test_timing_inputs_are_usable_in_the_registry(contract):
    require_usable_inputs(load_registry(), contract)


@pytest.mark.parametrize("field", ["src_ip", "dst_ip", "flow_start"])
def test_timing_refuses_inputs_the_contract_rates_low(contract, field):
    cols = [c.model_copy(update={"confidence": Confidence.LOW}) if c.name == field else c for c in contract.columns]
    with pytest.raises(ValueError, match="interarrival_cv"):
        require_usable_inputs(load_registry(), contract.model_copy(update={"columns": cols}))


def test_window_must_divide_a_day(con, tmp_path):
    lake = _write_lake(tmp_path / "lake", _tied_flows())
    with pytest.raises(ValueError, match="divide a day"):
        build_host_timing(con, lake, tmp_path / "out", tmp_path / "work", 7, SETTINGS)
