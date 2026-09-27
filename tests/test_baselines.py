"""V2-2 host baselines: strictly earlier history, median/MAD, peer-group fallback, baseline_quality."""

from __future__ import annotations

import math
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from netanomaly.baselines import MAD_TO_SIGMA, BaselineQuality, build_host_baseline, require_usable_inputs
from netanomaly.config import BaselineSettings
from netanomaly.feature_registry import load_registry
from netanomaly.schema import Confidence

SETTINGS = BaselineSettings(lookback_days=3, min_windows=4, min_days=2, min_peer_hosts=3)
DAYS = [date(2026, 9, 1) + timedelta(days=i) for i in range(6)]
CUTOFF = datetime(2026, 9, 4, 12, tzinfo=UTC)  # "now": the leakage test adds rows at or after this instant
SUBNET_A, SUBNET_B = "10.0.1.0/24", "10.0.2.0/24"

Flow = tuple[str, datetime, int, str | None]  # src_ip, flow_start, bytes, src_subnet


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def _history_flows() -> list[Flow]:
    """Flows before CUTOFF. Hosts are chosen so that every baseline level occurs on day 4 (before CUTOFF):
    a1-a3 host, a_const peer (MAD 0), a_new peer (no history), b_new global (its subnet has one host),
    nosub global (no subnet), and every row on day 1 none (no earlier day)."""
    flows: list[Flow] = []
    for i, day in enumerate(DAYS[:4]):
        for hour in (1, 2, 3):
            for k, ip in enumerate(("10.0.1.1", "10.0.1.2", "10.0.1.3")):
                flows.append((ip, _at(day, hour), 1_000 * (k + 1) + 97 * i + 13 * hour ** 2, SUBNET_A))
            flows.append(("10.0.1.9", _at(day, hour), 500, SUBNET_A))
            flows.append(("10.0.2.1", _at(day, hour), 4_000 + 50 * i + 7 * hour, SUBNET_B))
    for hour in (1, 2):
        flows.append(("10.0.1.50", _at(DAYS[3], hour, 30), 2_500, SUBNET_A))
        flows.append(("10.0.2.50", _at(DAYS[3], hour, 30), 9_000, SUBNET_B))
        flows.append(("10.0.3.1", _at(DAYS[3], hour, 30), 300, None))
    assert all(f[1] < CUTOFF for f in flows)
    return flows


def _future_flows() -> list[Flow]:
    """Flows at or after CUTOFF, built to move every baseline if they leaked: extreme volumes on the rest of
    day 4 and later days, and 10 new hosts in subnet B, which would lift b_new from global to peer."""
    flows: list[Flow] = []
    for day in DAYS[3:]:
        for hour in (13, 14, 20):
            for ip in ("10.0.1.1", "10.0.1.2", "10.0.1.3", "10.0.1.9", "10.0.1.50", "10.0.2.1", "10.0.2.50"):
                subnet = SUBNET_A if ip.startswith("10.0.1.") else SUBNET_B
                flows.append((ip, _at(day, hour), 50_000_000 + hour, subnet))
            flows += [(f"10.0.2.{100 + n}", _at(day, hour), 10 + n, SUBNET_B) for n in range(10)]
            flows.append(("10.0.3.1", _at(day, hour), 1, None))
    assert all(f[1] >= CUTOFF for f in flows)
    return flows


def _write_lake(lake: Path, flows: list[Flow]) -> Path:
    for day in sorted({f[1].date() for f in flows}):
        rows = [f for f in flows if f[1].date() == day]
        table = pa.table({
            "src_ip": [r[0] for r in rows],
            "flow_start": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
            "bytes": pa.array([r[2] for r in rows], pa.int64()),
            "src_subnet": pa.array([r[3] for r in rows], pa.string()),
        })
        part = lake / "flows" / f"flow_date={day.isoformat()}"
        part.mkdir(parents=True)
        pq.write_table(table, part / "src_test_0.parquet")
    return lake


def _build(con, tmp_path: Path, name: str, flows: list[Flow], s: BaselineSettings = SETTINGS) -> dict:
    lake = _write_lake(tmp_path / name / "lake", flows)
    out = tmp_path / name / "host_baseline"
    build_host_baseline(con, lake, out, tmp_path / name / "work", 5, s)
    rel = con.sql(f"SELECT * EXCLUDE (flow_date) FROM read_parquet('{(out / '**' / '*.parquet').as_posix()}')")
    return {(r[0], r[1]): dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()}


def _row(rows: dict, ip: str, when: datetime) -> dict:
    return rows[(ip, when)]


def _v(b: int) -> float:
    return math.log1p(b)


# --- temporal leakage -------------------------------------------------------------------------------------

def test_future_rows_cannot_change_earlier_baselines_at_any_level(con, tmp_path):
    before = _build(con, tmp_path, "before", _history_flows())
    after = _build(con, tmp_path, "after", _history_flows() + _future_flows())

    earlier = {k: v for k, v in after.items() if k[1] < CUTOFF}
    assert earlier == before  # every column: value, baseline, quality and support counts

    levels_checked = {r["baseline_quality"] for r in before.values()}
    assert levels_checked == {q.value for q in BaselineQuality}  # host, peer, global AND none are covered
    assert _row(before, "10.0.2.50", _at(DAYS[3], 1, 30))["baseline_quality"] == "global"


def test_the_leakage_test_data_does_move_later_baselines(con, tmp_path):
    """Guards the test above: the future rows must be able to change baselines once they are history."""
    before = _build(con, tmp_path, "before", _history_flows())
    after = _build(con, tmp_path, "after", _history_flows() + _future_flows())
    day5 = _row(after, "10.0.2.50", _at(DAYS[4], 13))
    assert day5["baseline_quality"] == "peer"  # subnet B gained hosts on day 4, now history for day 5
    same_day = _row(after, "10.0.1.1", _at(DAYS[3], 13))  # day 4 after CUTOFF: same-day rows are not history
    assert same_day["baseline_median"] == _row(before, "10.0.1.1", _at(DAYS[3], 1))["baseline_median"]
    assert _row(after, "10.0.1.1", _at(DAYS[4], 13))["baseline_median"] > same_day["baseline_median"]


def test_history_is_the_lookback_days_before_the_row_and_never_its_own_day(con, tmp_path):
    flows = _history_flows()
    rows = _build(con, tmp_path, "a", flows)
    r = _row(rows, "10.0.1.1", _at(DAYS[3], 2))
    hist = [_v(b) for ip, t, b, _ in flows if ip == "10.0.1.1" and DAYS[0] <= t.date() < DAYS[3]]
    med = statistics.median(hist)
    mad = statistics.median(abs(x - med) for x in hist)
    assert (r["baseline_quality"], r["baseline_windows"], r["baseline_days"]) == ("host", 9, 3)
    assert r["baseline_median"] == pytest.approx(med)
    assert r["baseline_mad"] == pytest.approx(mad)
    assert r["bytes_out_robust_z"] == pytest.approx((_v(r["bytes_out"]) - med) / (MAD_TO_SIGMA * mad))

    # day 1 falls out of the 3-day lookback for day 5 rows: changing it moves day 4 but not day 5
    day5_flow = ("10.0.1.1", _at(DAYS[4], 1), 1_234, SUBNET_A)
    moved = [(ip, t, b * 7 if t.date() == DAYS[0] else b, sub) for ip, t, b, sub in flows] + [day5_flow]
    rows_moved = _build(con, tmp_path, "b", moved)
    rows_day5 = _build(con, tmp_path, "c", [*flows, day5_flow])
    assert _row(rows_moved, "10.0.1.1", _at(DAYS[4], 1)) == _row(rows_day5, "10.0.1.1", _at(DAYS[4], 1))
    assert _row(rows_moved, "10.0.1.1", _at(DAYS[3], 2))["baseline_median"] != r["baseline_median"]


# --- fallback and quality ---------------------------------------------------------------------------------

@pytest.mark.parametrize(("ip", "when", "quality", "hosts"), [
    ("10.0.1.1", _at(DAYS[3], 1), "host", 1),
    ("10.0.1.9", _at(DAYS[3], 1), "peer", 4),         # own history constant: MAD 0
    ("10.0.1.50", _at(DAYS[3], 1, 30), "peer", 4),    # new host, subnet A has history
    ("10.0.2.50", _at(DAYS[3], 1, 30), "global", 5),  # subnet B history has 1 host < min_peer_hosts
    ("10.0.3.1", _at(DAYS[3], 1, 30), "global", 5),   # no src_subnet
    ("10.0.1.1", _at(DAYS[0], 1), "none", 0),         # first day: no earlier data at any level
    ("10.0.1.1", _at(DAYS[1], 1), "none", 0),         # one earlier day < min_days everywhere
])
def test_fallback_order_and_quality(con, tmp_path, ip, when, quality, hosts):
    r = _row(_build(con, tmp_path, "a", _history_flows()), ip, when)
    assert (r["baseline_quality"], r["baseline_hosts"]) == (quality, hosts)
    if quality == "none":
        assert r["bytes_out_robust_z"] is None and r["baseline_median"] is None and r["baseline_windows"] == 0
    else:
        assert r["bytes_out_robust_z"] is not None and r["baseline_mad"] > 0
        assert r["baseline_windows"] >= SETTINGS.min_windows and r["baseline_days"] >= SETTINGS.min_days


def test_peer_baseline_uses_the_subnet_history(con, tmp_path):
    flows = _history_flows()
    r = _row(_build(con, tmp_path, "a", flows), "10.0.1.50", _at(DAYS[3], 1, 30))
    peer = [_v(b) for _, t, b, sub in flows if sub == SUBNET_A and t.date() < DAYS[3]]
    assert (r["host_windows"], r["baseline_windows"], r["baseline_days"]) == (0, len(peer), 3)
    assert r["baseline_median"] == pytest.approx(statistics.median(peer))


# --- inputs and eligibility -------------------------------------------------------------------------------

def test_baseline_inputs_are_usable_in_the_registry(contract):
    require_usable_inputs(load_registry(), contract)


@pytest.mark.parametrize("field", ["bytes", "src_subnet"])
def test_baseline_refuses_inputs_the_contract_rates_low(contract, field):
    cols = [c.model_copy(update={"confidence": Confidence.LOW}) if c.name == field else c for c in contract.columns]
    with pytest.raises(ValueError, match="bytes_out_robust_z"):
        require_usable_inputs(load_registry(), contract.model_copy(update={"columns": cols}))
