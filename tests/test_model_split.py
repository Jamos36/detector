"""V3 time-based split (ADR-022): training sees only earlier days, scoring writes only later days, and data after a
row's day can change neither the model nor that row's score. Model inputs come from the registry."""

from __future__ import annotations

import inspect
import json
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from netanomaly import cli, iforest, labels
from netanomaly.config import ModelSettings
from netanomaly.db import sql_literal
from netanomaly.feature_registry import load_registry
from netanomaly.features import HOST_WINDOW_FEATURES
from netanomaly.schema import Confidence

D1 = date(2026, 9, 1)
SETTINGS = ModelSettings(train_sample_rows=100_000, n_estimators=25, max_samples=64, seed=7, n_jobs=1)
V3_FEATURES = [f for f in HOST_WINDOW_FEATURES if f not in ("syn_only_ratio", "rst_ratio")]
RATIOS = ("syn_only_ratio", "rst_ratio", "internal_ratio")


def _write_days(con, out: Path, days: range, *, scale: float = 1.0, hosts: int = 20, windows: int = 24) -> None:
    """Host-window rows partitioned by flow_date, like features.build_host_window, with hash-derived values."""
    cols = ", ".join(
        f"(abs(hash(h, w, '{name}')) % 100) / 100.0 AS {name}" if name in RATIOS
        else f"(abs(hash(h, w, '{name}')) % 50 + 1) * {float(scale)} AS {name}"
        for name in HOST_WINDOW_FEATURES)
    for d in days:
        day = D1 + timedelta(days=d)
        con.execute(f"""
COPY (
  SELECT '10.0.0.' || h AS src_ip,
         TIMESTAMPTZ '{day.isoformat()} 00:00:00+00' + INTERVAL 5 MINUTE * w AS window_start,
         {cols},
         DATE '{day.isoformat()}' AS flow_date
  FROM range({hosts}) t(h), range({windows}) u(w)
) TO {sql_literal(out)} (FORMAT parquet, PARTITION_BY (flow_date), OVERWRITE_OR_IGNORE true)""")


@pytest.fixture
def hw(con, tmp_path) -> Path:
    out = tmp_path / "host_window"
    _write_days(con, out, range(4))
    return out


def _fit(con, hw: Path, split: iforest.TimeSplit, **kw):
    registry = load_registry()
    return iforest.fit(con, hw, V3_FEATURES, iforest.log1p_features(registry, V3_FEATURES), split, SETTINGS,
                       version="test", registry_version=registry.registry_version, **kw)


def _probe(n: int = 500) -> np.ndarray:
    return np.random.default_rng(0).uniform(0, 5, size=(n, len(V3_FEATURES)))


# --- the split ---------------------------------------------------------------

def test_time_split_trains_on_the_first_days_and_scores_only_later_days():
    days = [D1 + timedelta(days=i) for i in (3, 0, 5, 1, 2, 4, 0)]  # unsorted, with a duplicate
    split = iforest.time_split(days, 0.5)
    assert split.train_dates == (D1, D1 + timedelta(1), D1 + timedelta(2))
    assert split.score_dates == tuple(D1 + timedelta(i) for i in (3, 4, 5))
    assert max(split.train_dates) < min(split.score_dates) and split.train_end == D1 + timedelta(2)
    five = [D1 + timedelta(i) for i in range(5)]
    assert iforest.time_split(five, 0.5).train_dates == (D1, D1 + timedelta(1))  # floor(5 * 0.5) = 2


@pytest.mark.parametrize(("n_days", "fraction"), [(1, 0.5), (3, 0.2), (2, 0.4), (0, 0.5)])
def test_time_split_refuses_an_empty_side(n_days, fraction):
    with pytest.raises(ValueError, match="time split needs"):
        iforest.time_split([D1 + timedelta(i) for i in range(n_days)], fraction)


def test_feature_dates_come_from_the_partitions(con, hw):
    assert iforest.feature_dates(con, hw) == [D1 + timedelta(i) for i in range(4)]


# --- training never sees later days -------------------------------------------

def test_training_uses_only_rows_on_or_before_train_end(con, hw):
    split = iforest.time_split(iforest.feature_dates(con, hw), 0.5)
    rows = con.execute(iforest.training_sample_sql(hw, V3_FEATURES, split.train_end, 10**6, 7)).fetchall()
    assert len(rows) == 2 * 20 * 24
    assert {r[1].date() for r in rows} == set(split.train_dates)
    _, manifest = _fit(con, hw, split)
    assert manifest.train_rows == manifest.train_period_rows == 960
    assert manifest.train_dates == ["2026-09-01", "2026-09-02"] and manifest.score_after == "2026-09-02"
    assert manifest.train_period[1].startswith("2026-09-02")


def test_future_rows_cannot_change_the_trained_model(con, hw, tmp_path):
    split = iforest.time_split(iforest.feature_dates(con, hw), 0.5)
    for sample_rows in (10**6, 300):  # all training rows, and a hash sample of them
        before, m_before = _fit(con, hw, split, sample_rows=sample_rows)
        # same training days; the score period rewritten with extreme values plus one more day
        changed = tmp_path / f"changed_{sample_rows}"
        _write_days(con, changed, range(2))
        _write_days(con, changed, range(2, 5), scale=1000.0)
        after, m_after = _fit(con, changed, split, sample_rows=sample_rows)
        assert m_after.train_rows == m_before.train_rows
        assert np.array_equal(before.score_samples(_probe()), after.score_samples(_probe()))


def test_training_sample_does_not_depend_on_row_or_file_order(con, hw, tmp_path):
    split = iforest.time_split(iforest.feature_dates(con, hw), 0.5)
    shuffled = tmp_path / "shuffled"
    con.execute(f"""COPY (SELECT * FROM read_parquet({sql_literal(hw.as_posix() + '/**/*.parquet')},
                                                     hive_partitioning = true) ORDER BY hash(src_ip, window_start) DESC)
                    TO {sql_literal(shuffled)} (FORMAT parquet, PARTITION_BY (flow_date))""")
    a, _ = _fit(con, hw, split, sample_rows=300)
    b, _ = _fit(con, shuffled, split, sample_rows=300)
    assert np.array_equal(a.score_samples(_probe()), b.score_samples(_probe()))


# --- scoring writes only later days; earlier scores are fixed ---------------------

def _scores(con, path: Path) -> list[tuple]:
    return con.execute(f"SELECT src_ip, window_start, flow_date, anomaly_score FROM read_parquet({sql_literal(path)}) "
                       "ORDER BY flow_date, src_ip, window_start").fetchall()


def test_scoring_writes_only_days_after_training_and_later_rows_do_not_change_them(con, hw, tmp_path):
    split = iforest.time_split(iforest.feature_dates(con, hw), 0.5)
    model, manifest = _fit(con, hw, split)
    n = iforest.score(con, hw, model, manifest, tmp_path / "s1.parquet", batch_rows=1_000)
    first = _scores(con, tmp_path / "s1.parquet")
    assert n == len(first) == 2 * 20 * 24
    assert {r[2] for r in first} == set(split.score_dates)
    _write_days(con, hw, range(4, 6), scale=1000.0)  # later data arrives
    iforest.score(con, hw, model, manifest, tmp_path / "s2.parquet", batch_rows=1_000)
    second = _scores(con, tmp_path / "s2.parquet")
    assert [r for r in second if r[2] <= split.score_dates[-1]] == first
    assert {r[2] for r in second} == {D1 + timedelta(i) for i in range(2, 6)}


def test_scoring_refuses_when_no_day_follows_training(con, hw, tmp_path):
    split = iforest.TimeSplit(tuple(D1 + timedelta(i) for i in range(4)), (), 1.0)
    model, manifest = _fit(con, hw, split)
    (tmp_path / "stale.parquet").write_bytes(b"old scores")
    with pytest.raises(ValueError, match="no feature rows after the training days"):
        iforest.score(con, hw, model, manifest, tmp_path / "stale.parquet", batch_rows=1_000)
    assert not (tmp_path / "stale.parquet").exists()


def test_v0_artifacts_load_but_are_refused_for_scoring(con, hw, tmp_path, contract):
    registry = load_registry()
    manifests = Path(__file__).parent.parent / "data" / "synth" / "models"
    v0 = json.loads(next(manifests.glob("iforest-20260927T191244Z/manifest.json")).read_text(encoding="utf-8"))
    manifest = iforest.ModelManifest(**v0)  # V0 manifests still load
    assert manifest.score_after is None and "syn_only_ratio" in manifest.features
    with pytest.raises(ValueError, match="no time split"):
        iforest.check_scorable(manifest, registry, contract)
    with pytest.raises(ValueError, match="no time split"):
        iforest.score(con, hw, None, manifest, tmp_path / "s.parquet", batch_rows=1_000)
    split = iforest.time_split(iforest.feature_dates(con, hw), 0.5)
    _, v3 = _fit(con, hw, split)
    iforest.check_scorable(v3, registry, contract)
    with pytest.raises(ValueError, match="not usable"):
        iforest.check_scorable(iforest.ModelManifest(**{**asdict(v3), "features": [*v3.features, "rst_ratio"]}),
                               registry, contract)


# --- model inputs come from the registry --------------------------------------

def test_model_features_are_the_usable_window_features_of_the_registry(contract):
    registry = load_registry()
    names = iforest.model_features(registry, contract)
    assert names == V3_FEATURES
    assert "syn_only_ratio" not in names and "rst_ratio" not in names  # tcp_flags, confidence low (ADR-017)
    assert "bytes_out_robust_z" not in names and "interarrival_cv" not in names  # prior-history tables, not inputs
    assert iforest.log1p_features(registry, names) == sorted(
        ["flows", "bytes_out", "packets_out", "uniq_dst_ip", "uniq_dst_port", "max_flow_bytes"])


def test_model_features_follow_the_contract_not_a_hardcoded_list(contract):
    registry = load_registry()
    cols = [c.model_copy(update={"confidence": Confidence.HIGH}) if c.name == "tcp_flags" else c
            for c in contract.columns]
    assert "syn_only_ratio" in iforest.model_features(registry, contract.model_copy(update={"columns": cols}))
    cols = [c.model_copy(update={"confidence": Confidence.LOW}) if c.name == "bytes" else c for c in contract.columns]
    assert "bytes_out" not in iforest.model_features(registry, contract.model_copy(update={"columns": cols}))


def test_model_code_never_reads_synthetic_truth():
    """Truth labels are for held-out diagnostics only; training and scoring code must not touch them."""
    for obj in (iforest, cli.cmd_train, cli.cmd_score):
        src = inspect.getsource(obj)
        assert "truth" not in src and "labels" not in src


# --- held-out diagnostic --------------------------------------------------------

def test_injected_windows_by_date_counts_windows_per_day(con, tmp_path):
    root = tmp_path / "synth"
    cli.main(["--root", str(root), "generate", "--days", "2", "--clean-days", "1", "--hosts", "30", "--seed", "4"])
    cli.main(["--root", str(root), "ingest"])
    by_day = labels.injected_windows_by_date(con, root / "lake", root / "truth", 5)
    assert list(by_day) == [D1 + timedelta(1)] and by_day[D1 + timedelta(1)] > 0
