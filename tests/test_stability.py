"""V3 stability report (ADR-023): Spearman with ties, per-day top-K overlap, variant plan, label-free end to end."""

from __future__ import annotations

import inspect
from datetime import timedelta

import numpy as np
import pytest
from scipy.stats import spearmanr

from netanomaly import iforest, stability
from netanomaly.config import ModelSettings, StabilitySettings
from netanomaly.db import sql_literal
from netanomaly.feature_registry import load_registry
from tests.test_model_split import D1, V3_FEATURES, _write_days


def _write_scores(con, path, variant: str, rows: list[tuple[str, int, int, float]]) -> None:
    """rows: (src_ip, day offset, window index, score)."""
    values = ", ".join(
        f"({sql_literal(ip)}, TIMESTAMPTZ '{(D1 + timedelta(d)).isoformat()} 00:00:00+00' + INTERVAL 5 MINUTE * {w}, "
        f"DATE '{(D1 + timedelta(d)).isoformat()}', {float(s)!r}, {sql_literal(variant)})" for ip, d, w, s in rows)
    con.execute(f"COPY (SELECT * FROM (VALUES {values}) t(src_ip, window_start, flow_date, anomaly_score, "
                f"model_version)) TO {sql_literal(path)} (FORMAT parquet)")


def test_spearman_uses_average_ranks_for_ties_like_scipy(con, tmp_path):
    rng = np.random.default_rng(1)
    a = rng.integers(0, 6, 200).astype(float)  # many ties
    b = a + rng.integers(0, 4, 200)
    keys = [(f"10.0.0.{i % 50}", 0, i // 50) for i in range(200)]
    _write_scores(con, tmp_path / "a.parquet", "a", [(*k, s) for k, s in zip(keys, a, strict=True)])
    _write_scores(con, tmp_path / "b.parquet", "b", [(*k, s) for k, s in zip(keys, b, strict=True)])
    stability.rank_tables(con, (tmp_path / "*.parquet").as_posix(), ["a", "b"], top_k=10)
    result = stability.agreement(con, [("a", "b"), ("a", stability.REFERENCE)])
    assert result[("a", "b")]["spearman"] == pytest.approx(spearmanr(a, b).statistic, abs=1e-12)
    assert result[("a", stability.REFERENCE)]["spearman"] == pytest.approx(spearmanr(a, (a + b) / 2).statistic)


def test_topk_overlap_is_per_day_and_breaks_ties_deterministically(con, tmp_path):
    # day 0: a ranks w0 > w1 > w2, b ranks w2 > w1 > w0 -> top-2 share only w1: 0.5
    # day 1: all scores tied in both -> tie-break by src_ip, window_start picks the same rows: 1.0
    a = [("h", 0, 0, 3.0), ("h", 0, 1, 2.0), ("h", 0, 2, 1.0), ("h", 1, 0, 1.0), ("h", 1, 1, 1.0), ("h", 1, 2, 1.0)]
    b = [("h", 0, 0, 1.0), ("h", 0, 1, 2.0), ("h", 0, 2, 3.0), ("h", 1, 0, 1.0), ("h", 1, 1, 1.0), ("h", 1, 2, 1.0)]
    _write_scores(con, tmp_path / "a.parquet", "a", a)
    _write_scores(con, tmp_path / "b.parquet", "b", b)
    stability.rank_tables(con, (tmp_path / "*.parquet").as_posix(), ["a", "b"], top_k=2)
    assert stability.agreement(con, [("a", "b")])[("a", "b")]["topk_overlap_by_day"] == [0.5, 1.0]


def test_plan_caps_sample_sizes_at_the_training_rows_and_keeps_seeds_disjoint():
    model = ModelSettings(train_sample_rows=200_000, seed=42)
    st = StabilitySettings(seeds=3, sample_sizes=[500, 2_000, 9_999_999], curve_seeds=2)
    variants = stability.plan_variants(model, st, train_rows=1_500)
    seeds = [v for v in variants if v.group == "seed"]
    curve = [v for v in variants if v.group == "curve"]
    assert [(v.seed, v.sample_rows) for v in seeds] == [(42, 1_500), (43, 1_500), (44, 1_500)]
    assert sorted({v.sample_rows for v in curve}) == [500, 1_500]  # 2,000 and 9,999,999 capped to all 1,500 rows
    assert len(curve) == 4 and not {v.seed for v in curve} & {v.seed for v in seeds}
    assert len({v.name for v in variants}) == len(variants)


def test_report_is_label_free_deterministic_and_on_held_out_days_only(con, tmp_path):
    hw = tmp_path / "host_window"
    _write_days(con, hw, range(4))
    split = iforest.time_split(iforest.feature_dates(con, hw), 0.5)
    registry = load_registry()
    model = ModelSettings(train_sample_rows=100_000, n_estimators=20, max_samples=64, seed=3, n_jobs=1)
    st = StabilitySettings(seeds=3, sample_sizes=[100, 400], curve_seeds=2)
    work = tmp_path / "work"

    def build() -> dict:
        return stability.build_report(con, hw, V3_FEATURES, iforest.log1p_features(registry, V3_FEATURES), split,
                                      model, st, 25, work, 1_000, registry.registry_version)

    report = build()
    assert not work.exists()
    assert report["labels_used"] is False and report["score_rows"] == 2 * 20 * 24
    assert report["split"]["score_dates"] == ["2026-09-03", "2026-09-04"]
    assert report["seed_stability"]["pairs"] == 3 and report["seed_stability"]["train_rows"] == 960
    assert [c["train_rows"] for c in report["sample_size_curve"]] == [100, 400, 960]
    for block in (report["seed_stability"], *report["sample_size_curve"]):
        assert -1 <= block["spearman"]["min"] <= block["spearman"]["max"] <= 1
        assert 0 <= block["topk_overlap_worst_day"] <= block["topk_overlap_mean"]["min"] <= 1
    assert build() == report  # rerun is identical
    md = stability.to_markdown(report)
    assert "SYNTHETIC DATA" in md and "Labels used: no" in md


def test_stability_never_reads_truth():
    assert not hasattr(stability, "labels")  # the truth module is not imported
    src = inspect.getsource(stability)
    assert all(name not in src for name in ("truth_dir", "injected_flows", "injections.csv", "window_labels"))
