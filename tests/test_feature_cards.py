"""V2-5 feature cards: statistics (PSI, AUROC, Spearman, binning), feature selection and label alignment."""

from __future__ import annotations

import csv
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score

from netanomaly import feature_cards as fc
from netanomaly.feature_cards import Direction, auroc, auroc_by_attack, psi, psi_bin_counts, select_features, spearman
from netanomaly.feature_registry import Status, load_registry, usable_features
from netanomaly.feature_report import rounded
from netanomaly.labels import verify_truth, window_labels_sql

D1, D2 = date(2026, 9, 1), date(2026, 9, 2)
T0 = datetime(2026, 9, 1, 10, tzinfo=UTC)
MIN = timedelta(minutes=1)


def _card_rows(con, **columns) -> None:
    """A hand-made card_rows table (the table every statistic reads)."""
    con.register("_src", pa.table(columns))
    con.execute("CREATE OR REPLACE TEMP TABLE card_rows AS SELECT * FROM _src")
    con.unregister("_src")


# --- PSI ------------------------------------------------------------------------------------------------------

def test_psi_is_zero_for_identical_shares_and_matches_the_formula():
    assert psi([10, 20, 70], [1, 2, 7]) == pytest.approx(0.0)
    expected = sum((a - e) * math.log(a / e) for e, a in ((0.5, 0.3), (0.5, 0.7)))
    assert psi([50, 50], [30, 70]) == pytest.approx(expected)


def test_psi_floors_empty_bins_instead_of_dividing_by_zero():
    value = psi([100, 0], [50, 50])
    assert value == pytest.approx((0.5 - 1) * math.log(0.5 / 1) + (0.5 - 1e-4) * math.log(0.5 / 1e-4))


def test_psi_rejects_empty_or_misaligned_counts():
    for expected, actual in (([1, 2], [1]), ([0, 0], [1, 1]), ([1, 1], [0, 0])):
        with pytest.raises(ValueError):
            psi(expected, actual)


def test_psi_bins_are_reference_quantiles_right_closed_with_a_null_bin(con):
    ref = [float(v) for v in range(1, 11)]  # D1: 1..10 -> one median edge (5) with 2 bins
    _card_rows(con, flow_date=pa.array([D1] * 10 + [D2] * 4, pa.date32()),
               x=pa.array([*ref, 5.0, 5.5, None, 100.0], pa.float64()))
    edges, counts = psi_bin_counts(con, "x", [D1], bins=2)
    assert edges == [5.0]
    assert counts[D1] == {0: 5, 1: 5}
    assert counts[D2] == {0: 1, 1: 2, -1: 1}  # 5 falls in the closed bin (<= 5); NULL has its own bin


def test_psi_moves_when_only_missingness_changes(con):
    values = [float(v) for v in range(20)]
    _card_rows(con, flow_date=pa.array([D1] * 20 + [D2] * 20, pa.date32()),
               x=pa.array(values + values[::2] + [None] * 10, pa.float64()))
    by_day = {p.flow_date: p for p in fc.psi_by_day(con, "x", [D1, D2], [D1], bins=4)}
    assert by_day[D1].psi == pytest.approx(0.0) and by_day[D1].role == "reference"
    assert by_day[D2].null_rate == 0.5 and by_day[D2].reading == "shift"


# --- AUROC ----------------------------------------------------------------------------------------------------

def test_auroc_matches_sklearn_with_ties_and_nulls_ranked_lowest(con):
    rng = np.random.default_rng(0)
    n = 400
    y = rng.random(n) < 0.2
    x = np.round(rng.normal(y * 0.8, 1.0), 1)  # rounding creates ties
    null = rng.random(n) < 0.15
    _card_rows(con, x=pa.array([None if m else v for v, m in zip(x, null, strict=True)], pa.float64()),
               y=pa.array(y.tolist()))
    auc, pos, neg = auroc(con, "SELECT * FROM card_rows", "x", "y")
    assert (pos, neg) == (y.sum(), n - y.sum())
    assert auc == pytest.approx(roc_auc_score(y, np.where(null, x.min() - 1, x)))  # NULL = below every value


def test_auroc_edge_cases(con):
    _card_rows(con, x=pa.array([1.0, 2.0, 3.0, 4.0]), y=pa.array([False, False, True, True]))
    assert auroc(con, "SELECT * FROM card_rows", "x", "y")[0] == 1.0
    assert auroc(con, "SELECT * FROM card_rows", "-x", "y")[0] == 0.0
    assert auroc(con, "SELECT * FROM card_rows", "1", "y")[0] == 0.5  # all tied
    assert auroc(con, "SELECT * FROM card_rows WHERE NOT y", "x", "y") == (None, 0, 2)


def test_per_attack_population_excludes_other_attacks_and_uses_declared_direction(con):
    """Type A: its windows (incl. one holding A and B) vs clean windows; B-only windows and other days left out."""
    _card_rows(con, flow_date=pa.array([D2] * 6 + [D1], pa.date32()),
               x=pa.array([0.1, 0.2, 5.0, 0.3, 0.4, 0.5, 9.0]),
               attack_types=pa.array([["A"], ["A", "B"], ["B"], None, None, None, None], pa.list_(pa.string())))
    by_type = {a.attack_type: a for a in auroc_by_attack(con, "x", Direction.LOW, ["A", "B"], [D2])}
    assert (by_type["A"].positives, by_type["A"].negatives, by_type["A"].auroc) == (2, 3, 1.0)  # low = anomalous
    assert (by_type["B"].positives, by_type["B"].negatives) == (2, 3)
    assert (by_type[fc.ANY_ATTACK].positives, by_type[fc.ANY_ATTACK].negatives) == (3, 3)


# --- redundancy -----------------------------------------------------------------------------------------------

def test_spearman_matches_scipy_and_counts_pairwise_complete_rows(con):
    rng = np.random.default_rng(1)
    a = rng.integers(0, 20, 300).astype(float)  # heavy ties
    b = a + rng.normal(0, 5, 300)
    missing = rng.random(300) < 0.3
    _card_rows(con, a=pa.array(a), b=pa.array(b),
               c=pa.array([None if m else -v for v, m in zip(b, missing, strict=True)], pa.float64()))
    result = spearman(con, ["a", "b", "c"])
    assert result[("a", "b")][0] == pytest.approx(spearmanr(a, b).statistic)
    assert result[("a", "b")][1] == 300
    assert result[("b", "c")] == (pytest.approx(-1.0), int((~missing).sum()))  # c = -b wherever present
    c = -b[~missing]
    assert result[("a", "c")][0] == pytest.approx(spearmanr(a[~missing], c).statistic)  # re-ranked subset


def test_spearman_of_a_constant_feature_is_undefined_not_nan(con):
    _card_rows(con, a=pa.array([1.0, 2.0, 3.0]), k=pa.array([7.0, 7.0, 7.0]))
    assert spearman(con, ["a", "k"]) == {("a", "k"): (None, 3)}


# --- feature selection ----------------------------------------------------------------------------------------

def test_only_usable_implemented_features_are_analysed(contract):
    registry = load_registry()
    chosen, skipped = select_features(registry, contract)
    usable = set(usable_features(registry, contract))
    implemented = {f.name for f in registry.features if f.status is Status.IMPLEMENTED}
    assert {e.feature.name for e in chosen} == usable & implemented
    reasons = {s.name: s.reason for s in skipped}
    assert reasons["syn_only_ratio"].startswith("not usable") and reasons["rst_ratio"].startswith("not usable")
    assert "not implemented" in reasons["bytes_per_packet"]
    assert {e.feature.name for e in chosen} | set(reasons) == {f.name for f in registry.features}


def test_a_usable_implemented_feature_without_a_source_is_refused(contract, monkeypatch):
    monkeypatch.delitem(fc.FEATURE_SOURCES, "interarrival_cv")
    with pytest.raises(ValueError, match="interarrival_cv"):
        select_features(load_registry(), contract)


# --- truth mapping and label alignment ------------------------------------------------------------------------

def _lake(root: Path, flows: list[tuple[str, datetime, int]]) -> Path:
    for day in sorted({f[1].date() for f in flows}):
        part = root / "lake" / "flows" / f"flow_date={day.isoformat()}"
        part.mkdir(parents=True)
        rows = [f for f in flows if f[1].date() == day]
        pq.write_table(pa.table({"src_ip": [r[0] for r in rows],
                                 "flow_start": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
                                 "flow_sequence": pa.array([r[2] for r in rows], pa.int64())}),
                       part / "src_t_0.parquet")
    return root / "lake"


def _truth(root: Path, injected: dict[int, str], injections: list[dict]) -> Path:
    truth = root / "truth"
    truth.mkdir()
    with (truth / "injected_flows.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["flow_sequence", "injection_id"])
        w.writerows(injected.items())
    with (truth / "injections.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["injection_id", "attack_type", "src_ip", "n_flows"])
        w.writeheader()
        w.writerows(injections)
    return truth


FLOWS = [
    ("h", T0 + 5 * MIN - timedelta(microseconds=1), 1),  # 10:04:59.999999 -> window 10:00 (injected, A)
    ("h", T0 + 5 * MIN, 2),                               # 10:05:00 exactly -> window 10:05 (injected, B)
    ("h", T0 + 6 * MIN, 3),                               # benign flow in the injected window 10:05
    ("h", T0 + 7 * MIN, 4),                               # 10:05 again, injected A: the window holds A and B
    ("h", T0 + 20 * MIN, 5),                              # same host, another window: negative
    ("o", T0 + 1 * MIN, 6),                               # another host, same window as flow 1: negative
]
INJECTIONS = [{"injection_id": "a", "attack_type": "A", "src_ip": "h", "n_flows": 2},
              {"injection_id": "b", "attack_type": "B", "src_ip": "h", "n_flows": 1}]
INJECTED = {1: "a", 2: "b", 4: "a"}


def test_host_window_labels_follow_the_window_of_each_injected_flow(con, tmp_path):
    lake, truth = _lake(tmp_path, FLOWS), _truth(tmp_path, INJECTED, INJECTIONS)
    check = verify_truth(con, lake, truth)
    assert (check.injections, check.truth_flows, check.matched_flows, check.attack_days) == (2, 3, 3, (D1,))
    rows = con.execute(f"SELECT src_ip, window_start, attack_types, injected_flows "
                       f"FROM ({window_labels_sql(lake, truth, 5)}) ORDER BY window_start").fetchall()
    assert rows == [("h", T0, ["A"], 1), ("h", T0 + 5 * MIN, ["A", "B"], 2)]  # 10:20 and host o: no label


@pytest.mark.parametrize(("injected", "injections", "problem"), [
    ({1: "a", 4: "a"}, [{**INJECTIONS[0], "src_ip": "o"}], "src_ip differs"),
    ({1: "a"}, [INJECTIONS[0]], "count differs from n_flows"),
    ({1: "a", 4: "a", 5: "zzz"}, [INJECTIONS[0]], "unknown injection_id"),
])
def test_truth_that_does_not_map_onto_the_lake_is_refused(con, tmp_path, injected, injections, problem):
    lake = _lake(tmp_path, FLOWS)
    with pytest.raises(ValueError, match=problem):
        verify_truth(con, lake, _truth(tmp_path, injected, injections))


def test_truth_flows_missing_from_the_lake_are_refused(con, tmp_path):
    lake = _lake(tmp_path, FLOWS)
    with pytest.raises(ValueError, match="no lake flow"):
        verify_truth(con, lake, _truth(tmp_path, {1: "a", 99: "a"}, [INJECTIONS[0]]))


# --- grid coverage --------------------------------------------------------------------------------------------

def _features(root: Path, windows: list[tuple[str, datetime]], timing_rows: int) -> Path:
    features = root / "features"
    for table, n in (("host_window", len(windows)), ("host_timing", timing_rows)):
        part = features / table / f"flow_date={D1.isoformat()}"
        part.mkdir(parents=True)
        cols = ({"flows": pa.array([1] * n, pa.int64())} if table == "host_window"
                else {"interarrival_cv": pa.array([0.5] * n), "timing_quality": ["ok"] * n})
        pq.write_table(pa.table({"src_ip": [w[0] for w in windows[:n]],
                                 "window_start": pa.array([w[1] for w in windows[:n]], pa.timestamp("us", tz="UTC")),
                                 **cols}), part / "part-0.parquet")
    return features


def test_card_rows_join_labels_onto_the_grid(con, tmp_path):
    lake, truth = _lake(tmp_path, FLOWS), _truth(tmp_path, INJECTED, INJECTIONS)
    grid = [("h", T0), ("h", T0 + 5 * MIN), ("h", T0 + 20 * MIN), ("o", T0)]
    features = _features(tmp_path, grid, timing_rows=len(grid))
    assert fc.create_card_rows(con, features, ["flows", "interarrival_cv"], window_labels_sql(lake, truth, 5)) == 4
    rows = con.execute("SELECT attack_types, injected_flows, grid_flows FROM card_rows ORDER BY src_ip, window_start")
    assert rows.fetchall() == [(["A"], 1, 1), (["A", "B"], 2, 1), (None, 0, 1), (None, 0, 1)]


def test_card_rows_refuse_a_feature_table_that_does_not_cover_the_grid(con, tmp_path):
    features = _features(tmp_path, [("h", T0), ("h", T0 + 5 * MIN)], timing_rows=1)  # stale timing output
    with pytest.raises(ValueError, match="host_timing .* does not cover"):
        fc.create_card_rows(con, features, ["flows", "interarrival_cv"], None)


def test_card_rows_refuse_injected_windows_missing_from_the_grid(con, tmp_path):
    lake, truth = _lake(tmp_path, FLOWS), _truth(tmp_path, INJECTED, INJECTIONS)
    features = _features(tmp_path, [("h", T0), ("o", T0)], timing_rows=2)  # grid lacks window 10:05
    with pytest.raises(ValueError, match="1 injected host-windows are not on the"):
        fc.create_card_rows(con, features, ["flows", "interarrival_cv"], window_labels_sql(lake, truth, 5))


def test_report_json_rounds_floats_so_reruns_are_byte_identical():
    run_a, run_b = {"rho": [0.9694647820393368], "n": 3}, {"rho": [0.9694647820393381], "n": 3}
    assert rounded(run_a) == rounded(run_b) == {"rho": [0.969464782], "n": 3}
