"""End-to-end tests of the two workflows around a frozen model bundle, on tiny Parquet fixtures:

A. historical: development (train + validation) -> bundle -> final holdout test (ledger: first / repeat / reused);
B. new data: a separate scoring run on different Parquet files that must not refit anything, an incompatible schema,
and the optional relationship analysis in its three config modes (off, report-only, model inputs)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from netanomaly import experiment as exp
from netanomaly import models, relationships, scoring
from netanomaly.bundle import BundleError, load_bundle
from netanomaly.cli import main
from netanomaly.config import BandSettings, load_poc_config
from netanomaly.db import sql_literal
from netanomaly.featureset import FEATURE_BY_NAME
from netanomaly.source import SourceError, repo_root
from tests.fixtures import scan_rows, write_config, write_days

REL = """relationship_analysis:
  enabled: true
  include_model_features: {include}
  recent_lookback_days: 1
  baseline_lookback_days: 1
  warmup_days: 1
  min_support_windows: 3
"""


def _cfg(tmp: Path, name: str, rel: str = ""):
    data = tmp / "data_hist"
    if not data.exists():
        write_days(data, range(8), {6: scan_rows(6, 10)})
    return load_poc_config(write_config(tmp / f"{name}.yaml", data, tmp / f"work_{name}", extra=rel))


def _develop(cfg) -> tuple[Path, Path]:
    report = exp.run_experiment(cfg)
    return report.parent, scoring.bundle_for_config(cfg)


def _rows(con, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def _rel(path: Path) -> str:
    return f"read_parquet({sql_literal(path)})"


@pytest.fixture(scope="module")
def dev(tmp_path_factory):
    """One development run without and one with the report-only relationship analysis, on the same data."""
    tmp = tmp_path_factory.mktemp("wf")
    off = _cfg(tmp, "off")
    on = _cfg(tmp, "on", REL.format(include="false"))
    off_dir, off_bundle = _develop(off)
    on_dir, on_bundle = _develop(on)
    new_data = write_days(tmp / "data_new", range(10, 12), {11: scan_rows(11, 4, host="10.0.0.42")})
    return {"tmp": tmp, "off": off, "on": on, "off_dir": off_dir, "on_dir": on_dir, "off_bundle": off_bundle,
            "on_bundle": on_bundle, "new_data": new_data}


def test_bundle_is_self_contained_outside_the_experiment_and_holds_no_raw_rows(dev):
    b = load_bundle(dev["off_bundle"])
    assert dev["off_bundle"].parent == dev["off"].work_dir / "bundles"
    files = sorted(p.relative_to(b.path).as_posix() for p in b.path.rglob("*") if p.is_file())
    assert files == ["bundle.json", "models/iforest.joblib", "models/iforest.json", "models/ocsvm.joblib",
                     "models/ocsvm.json", "train_entities.parquet", "train_stats.parquet"]
    m = b.meta
    assert b.model_features == exp.open_context(dev["off"]).features  # ordered model inputs
    assert m["window_minutes"] == 60 and m["field_map"]["src_ip"] == "src_id_addr"
    assert set(m["periods"]) == {"train", "validation", "test"} and m["bands"]["reference"] == "validation"
    assert {c["model_id"] for c in m["bands"]["cutoffs"]} == {"iforest", "ocsvm"} and m["percentile_grids"]
    assert {"python", "scikit-learn", "duckdb"} <= set(m["versions"])
    assert m["relationship_analysis"]["enabled"] is False
    assert pq.read_table(b.train_stats).num_rows == 1  # summaries only, no flow rows


def test_holdout_scores_only_the_test_period_with_frozen_models_and_cutoffs(dev):
    cfg, b = dev["off"], load_bundle(dev["off_bundle"])
    report = scoring.run_holdout(cfg, dev["off_bundle"])
    run = json.loads((report.parent / "run.json").read_text(encoding="utf-8"))
    assert run["kind"] == "holdout-test" and run["holdout"]["status"] in ("first", "repeat")
    assert run["period"] == b.meta["periods"]["test"] and run["bundle"]["bundle_id"] == b.bundle_id
    ctx = exp.open_context(cfg)
    days = _rows(ctx.con, f"SELECT DISTINCT flow_date::VARCHAR FROM {_rel(report.parent / 'scores.parquet')} "
                          "ORDER BY 1")
    assert days == [("2026-01-11",), ("2026-01-12",)]
    # the frozen pipelines give exactly what the development models give on the same rows
    direct = report.parent / "direct.parquet"
    models.score_to_parquet(ctx.con, ctx.table, exp.load_models(ctx), direct, 10_000, b.period("test").where())
    assert pq.read_table(direct).equals(pq.read_table(report.parent / "scores_raw.parquet"))
    text = report.read_text(encoding="utf-8")
    assert b.bundle_id in text and "2026-01-11 .. 2026-01-13" in text and "Final test report" in text


def test_holdout_ledger_labels_first_repeat_and_reuse_by_another_bundle(dev, tmp_path):
    cfg = load_poc_config(Path(dev["tmp"] / "off.yaml"), {"work_dir": str(tmp_path / "ledger")})
    exp.run_experiment(cfg)
    bundle = scoring.bundle_for_config(cfg)
    assert "Untouched holdout" in scoring.run_holdout(cfg, bundle).read_text(encoding="utf-8")
    assert "Repeat of the same frozen evaluation" in scoring.run_holdout(cfg, bundle).read_text(encoding="utf-8")
    exp.finalize(exp.open_context(cfg), BandSettings(quantiles={"Critical": 0.99, "High": 0.98, "Medium": 0.97,
                                                                "Low": 0.9}))
    other = scoring.bundle_for_config(cfg)
    assert other != bundle  # re-banding makes a new bundle id
    assert "PREVIOUSLY VIEWED TEST PERIOD" in scoring.run_holdout(cfg, other).read_text(encoding="utf-8")
    ledger = json.loads((cfg.work_dir / scoring.LEDGER).read_text(encoding="utf-8"))
    assert [e["status"] for e in ledger] == ["first", "repeat", "reused"]


def test_new_data_is_scored_by_a_separate_run_without_refitting(dev, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("scoring must not fit anything")

    monkeypatch.setattr(models, "fit_model", refuse)
    monkeypatch.setattr(models.IsolationForest, "fit", refuse)
    monkeypatch.setattr(models.OneClassSVM, "fit", refuse)
    monkeypatch.setattr(models.Pipeline, "fit", refuse)
    bundle_dir, cfg_path = dev["off_bundle"], dev["tmp"] / "off.yaml"
    before = {p.name: p.stat().st_mtime_ns for p in (bundle_dir / "models").iterdir()}
    main(["score-new", "--config", str(cfg_path), "--bundle", str(bundle_dir), "--input", str(dev["new_data"])])
    runs = list((dev["off"].work_dir / "scoring").glob("new-data-*"))
    assert len(runs) == 1
    run_dir, b = runs[0], load_bundle(bundle_dir)
    run = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert run["model_inputs"] == b.model_features and run["period"]["start"] == "2026-01-15"
    assert run["cutoffs"] == b.meta["bands"]["cutoffs"] and run["holdout"] is None
    con = exp.connect_and_open(dev["off"])[0]
    scores = _rel(run_dir / "scores.parquet")
    for c in b.cutoffs:  # bands are the bundle's validation cutoffs applied to the new raw scores
        for raw, band in _rows(con, f"SELECT {c.model_id}_raw, {c.model_id}_band FROM {scores}"):
            expected = next((k for k in ("Critical", "High", "Medium", "Low") if raw >= c.thresholds[k]), "Benign")
            assert band == expected
    assert {p.name: p.stat().st_mtime_ns for p in (bundle_dir / "models").iterdir()} == before
    text = (run_dir / "report.md").read_text(encoding="utf-8")
    assert "New-data scoring report" in text and b.bundle_id in text and "2026-01-15 .. 2026-01-17" in text
    assert "not accuracy" in text
    top = _rows(con, f"SELECT src_ip FROM {_rel(run_dir / 'alerts.parquet')} ORDER BY alert_rank LIMIT 3")
    assert ("10.0.0.42",) in top  # the port sweep in the new data ranks at the top


def test_incompatible_new_files_are_refused_before_anything_is_scored(dev, tmp_path):
    bad = tmp_path / "bad"
    bad.mkdir()
    table = pq.read_table(next(dev["new_data"].glob("*.parquet")))
    pq.write_table(table.rename_columns(["dport" if c == "dist_port" else c for c in table.column_names]),
                   bad / "renamed.parquet")
    scoring_dir = dev["off"].work_dir / "scoring"
    before = set(scoring_dir.iterdir()) if scoring_dir.exists() else set()
    with pytest.raises(BundleError, match=r"dst_port: needs column 'dist_port'.*field_map.*Nothing was scored"):
        scoring.score_new(dev["off"], dev["off_bundle"], [str(bad)])
    assert (set(scoring_dir.iterdir()) if scoring_dir.exists() else set()) == before


def test_report_only_relationships_leave_model_inputs_and_scores_unchanged(dev):
    off, on = dev["off_dir"], dev["on_dir"]
    assert off.name == on.name  # same experiment id: the models are identical
    assert not (dev["off"].work_dir / "relationships").exists()
    ctx = exp.open_context(dev["on"])
    assert ctx.relationships is not None and not any(f.startswith("rel_") for f in ctx.features)
    assert pq.read_table(off / "scores_raw.parquet").equals(pq.read_table(on / "scores_raw.parquet"))
    for name in (relationships.PAIR_WINDOWS, relationships.HOST_WINDOWS, relationships.EVIDENCE):
        assert ctx.relationships.path(name).exists()
    text = (on / "report.md").read_text(encoding="utf-8")
    assert "Source -> destination relationships" in text and "report-only" in text
    assert (on / "charts" / "relationship_trends.svg").exists()
    # the development analysis stops at the test start; the bundle carries it as history
    b = load_bundle(dev["on_bundle"])
    assert str(b.rel["history_end"]).startswith("2026-01-11") and (b.rel_state / "recent_pairs.parquet").exists()
    pairs = _rel(ctx.relationships.path(relationships.PAIR_WINDOWS))
    assert _rows(ctx.con, f"SELECT max(window_start) < TIMESTAMPTZ '2026-01-11' FROM {pairs}")[0][0]


def test_relationship_history_continues_across_scoring_runs_and_refuses_overlap(dev):
    cfg, bundle = dev["on"], dev["on_bundle"]
    holdout = scoring.run_holdout(cfg, bundle)
    hist = json.loads((holdout.parent / "relationships" / "summary.json").read_text(encoding="utf-8"))
    assert hist["carried_from"].endswith("state") and hist["history_start"].startswith("2026-01-05")
    first = scoring.score_new(cfg, bundle, [str(dev["new_data"])])
    later = write_days(dev["tmp"] / "data_later", range(12, 13))
    second = scoring.score_new(cfg, bundle, [str(later)], history=first.parent)
    s = json.loads((second.parent / "relationships" / "summary.json").read_text(encoding="utf-8"))
    assert Path(s["carried_from"]) == first.parent / "relationships" / "state"
    # data that precedes the carried history: report-only analysis is skipped with a warning, models still score
    overlap = scoring.score_new(cfg, bundle, [str(dev["new_data"])], history=second.parent)
    run = json.loads((overlap.parent / "run.json").read_text(encoding="utf-8"))
    assert run["relationships"] is None and any("overlaps or precedes" in w for w in run["warnings"])
    assert run["scored_windows"] > 0
    con = exp.connect_and_open(cfg)[0]
    evidence = _rel(first.parent / "relationships" / relationships.EVIDENCE)
    reasons = _rows(con, f"SELECT reason FROM {evidence} WHERE src_ip = '10.0.0.42'")
    assert reasons and all(r[0].startswith("never seen before") for r in reasons)  # a host new since history start
    assert "Source -> destination relationships" in first.read_text(encoding="utf-8")


def test_relationship_summaries_become_model_inputs_only_when_asked(dev):
    cfg = _cfg(dev["tmp"], "incl", REL.format(include="true"))
    exp_dir, bundle_dir = _develop(cfg)
    assert exp_dir.name != dev["off_dir"].name  # different model inputs -> different experiment
    b = load_bundle(bundle_dir)
    rel_inputs = [f for f in b.model_features if f.startswith("rel_")]
    assert rel_inputs and set(rel_inputs) <= set(relationships.MODEL_COLUMNS)
    base_inputs = load_bundle(dev["off_bundle"]).model_features
    assert b.model_features == [*base_inputs, *rel_inputs]  # base inputs unchanged, rel_* appended
    assert all(f in FEATURE_BY_NAME for f in b.model_features)  # numeric behavioural features only
    assert not {"src_ip", "dst_ip", "dst_port", "protocol", "window_start"} & set(b.model_features)
    for f in b.load_models():
        assert f.pipeline.named_steps["impute"].n_features_in_ == len(b.model_features)
    report = scoring.score_new(cfg, bundle_dir, [str(dev["new_data"])])
    raw = pq.read_table(report.parent / "scores_raw.parquet")
    assert raw.num_rows > 0 and np.isfinite(raw.column("iforest_raw").to_numpy()).all()
    # the summaries are model inputs here, so data that precedes the carried history is refused outright
    with pytest.raises(relationships.RelationshipError, match="overlaps or precedes"):
        scoring.score_new(cfg, bundle_dir, [str(dev["new_data"])], history=report.parent)


def test_external_input_cannot_write_outputs_into_the_checkout(tmp_path):
    root = repo_root()
    data = write_days(tmp_path / "ext", range(2))
    cfg = load_poc_config(write_config(tmp_path / "c.yaml", data, root / "guard_probe_ext"),
                          {"allow_inside_repo": True})
    with pytest.raises(SourceError, match="possibly real data"):
        exp.run_profile(cfg)
    assert not (root / "guard_probe_ext").exists()  # refused before anything (even the spill folder) is created
