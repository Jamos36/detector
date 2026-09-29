"""End-to-end tests of the Parquet-only PoC on tiny fixtures: the full experiment, traceability, reproducibility,
chronological leakage, re-banding without retraining, experiment identity, search, CLI, and the data boundary."""

from __future__ import annotations

import ast
import json
import re
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from netanomaly import experiment as exp
from netanomaly import scoring
from netanomaly.cli import main
from netanomaly.config import BandSettings, load_poc_config
from netanomaly.db import connect, sql_literal
from netanomaly.source import SourceError, repo_root
from tests.fixtures import START, scan_rows, write_config, write_days

SCAN = ("10.0.0.99", START + timedelta(days=6, hours=10))  # a port sweep in the test period
DAYS = range(8)  # 2026-01-05 .. 2026-01-12: train 4 days, validation 2, test 2
SPLIT = {"train": {"start": "2026-01-05", "end": "2026-01-09"},
         "validation": {"start": "2026-01-09", "end": "2026-01-11"},
         "test": {"start": "2026-01-11", "end": "2026-01-13"}}


def _annotations(path: Path) -> Path:
    path.write_text("""intervals:
  - {name: pt-test, start: 2026-01-11, end: 2026-01-12, source: fixture, confidence: medium, notes: broad}
  - {name: pt-early, start: 2026-01-07T06:00:00Z, end: 2026-01-07T09:00:00Z, source: fixture}
""", encoding="utf-8")
    return path


def _setup(tmp_path: Path, name: str = "run", extra_rows: dict | None = None) -> Path:
    data = write_days(tmp_path / f"data_{name}", DAYS, {6: scan_rows(6, 10), **(extra_rows or {})})
    return write_config(tmp_path / f"{name}.yaml", data, tmp_path / f"work_{name}",
                        annotations=_annotations(tmp_path / f"ann_{name}.yaml"))


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("poc")
    cfg = load_poc_config(_setup(tmp))
    report = exp.run_experiment(cfg)
    test_report = scoring.run_holdout(cfg, scoring.bundle_for_config(cfg))
    return {"tmp": tmp, "cfg": cfg, "report": report, "ctx": exp.open_context(cfg), "dir": report.parent,
            "test_dir": test_report.parent}


def _q(ctx, sql: str):
    return ctx.con.execute(sql).fetchall()


def _rel(path: Path) -> str:
    return f"read_parquet({sql_literal(path)})"


def _manifest(d: Path) -> dict:
    return json.loads((d / "manifest.json").read_text(encoding="utf-8"))


def test_experiment_writes_every_output_and_a_complete_manifest(run):
    d = run["dir"]
    for name in ("manifest.json", "scores.parquet", "alerts.parquet", "daily_summary.parquet",
                 "entity_summary.parquet", "diagnostics.json", "robustness.json", "report.md",
                 "train_stats.parquet", "train_entities.parquet",
                 "models/iforest.joblib", "models/ocsvm.joblib", "charts/overview.svg", "charts/model_comparison.svg"):
        assert (d / name).exists(), name
    m = _manifest(d)
    assert Path(m["bundle"]["path"]).is_dir() and m["bundle"]["bundle_id"].startswith(d.name)
    assert m["experiment_id"] == d.name == run["ctx"].experiment_id
    assert [p["name"] for p in m["periods"]] == ["train", "validation", "test"]
    assert m["input"]["files"] == len(DAYS) and len(m["input"]["fingerprint"]) == 64
    assert {"python", "scikit-learn", "duckdb"} <= set(m["versions"]) and "commit" in m["code"]
    assert m["bands"]["reference"] == "validation" and len(m["bands"]["cutoffs"]) == 2
    assert all(x["score_direction"] == "higher = more anomalous" for x in m["models"])
    assert any("pt-early" in w for w in m["warnings"])  # the training period overlaps a supplied interval
    assert m["features"]["model_inputs"] and set(m["features"]["model_inputs"]) <= set(m["feature_table"]["computed"])


def test_development_scores_cover_train_and_validation_but_never_the_test_period(run):
    ctx, d = run["ctx"], run["dir"]
    scores = _rel(d / "scores.parquet")
    dev_rows = _q(ctx, f"SELECT count(*) FROM {ctx.table.relation()} WHERE {ctx.dev_where()}")[0][0]
    assert _q(ctx, f"SELECT count(*) FROM {scores}")[0][0] == dev_rows < ctx.table.rows
    periods = dict(_q(ctx, f"SELECT period, count(DISTINCT flow_date) FROM {scores} GROUP BY 1"))
    assert periods == {"train": 4, "validation": 2}
    rob = json.loads((d / "robustness.json").read_text(encoding="utf-8"))
    assert rob["seed_period"] == rob["variant_period"] == "validation"
    assert "reserved" in run["report"].read_text(encoding="utf-8")
    bands = {b for (b,) in _q(ctx, f"SELECT DISTINCT iforest_band FROM {scores}")}
    assert bands <= {"Critical", "High", "Medium", "Low", "Benign"} and "Benign" in bands
    cats = dict(_q(ctx, f"SELECT annotation_category, count(*) FROM {scores} GROUP BY 1"))
    assert set(cats) == {"inside", "buffer", "outside"}
    # validation calibrates the bands: ~0.1 % of validation windows reach Critical by construction
    crit = _q(ctx, f"SELECT avg((iforest_band = 'Critical')::INT) FROM {scores} WHERE period = 'validation'")[0][0]
    assert crit <= 0.01


def test_the_held_out_scan_is_flagged_by_the_final_test_and_traceable_to_its_source_rows(run):
    ctx, d = run["ctx"], run["test_dir"]
    row = _q(ctx, f"SELECT ocsvm_band, beyond_train_range, alert_rank, trace_flow_count, trace_rows "
                  f"FROM {_rel(d / 'alerts.parquet')} WHERE src_ip = '{SCAN[0]}' AND window_start = "
                  f"TIMESTAMPTZ '{SCAN[1].isoformat()}'")
    assert row, "scan window missing from alerts"
    band, beyond, rank, n_flows, trace = row[0]
    assert band == "Critical" and "uniq_dst_port" in beyond and rank <= 5
    assert n_flows == 200 and len(trace) == run["cfg"].report.trace_rows_per_window
    for t in trace:  # every traced (file, row) really is a flow of this host inside the window
        rec = pq.read_table(t["file"]).slice(t["row_index"], 1).to_pylist()[0]
        assert rec["src_id_addr"] == SCAN[0]
        assert SCAN[1] <= rec["flow_start_time"] < SCAN[1] + timedelta(hours=1)


def test_report_states_limitations_and_draws_aggregated_charts(run):
    text = run["report"].read_text(encoding="utf-8")
    for phrase in ("not probabilities", "not a confirmed attack", "below the review threshold",
                   "no reliable labels", "Isolation Forest cannot extrapolate", "in-sample"):
        assert phrase in text, phrase
    assert text.count("![") >= 8


def test_rerun_with_same_rows_and_config_is_reproducible(run, tmp_path):
    report = exp.run_experiment(load_poc_config(_setup(tmp_path, "again")))
    a = pq.read_table(run["dir"] / "scores_raw.parquet")
    b = pq.read_table(report.parent / "scores_raw.parquet")
    assert a.equals(b)


def test_future_rows_cannot_change_the_models_or_the_calibration(run, tmp_path):
    loud_future = {7: scan_rows(7, 3, host="10.0.0.77", ports=900)}
    cfg = load_poc_config(_setup(tmp_path, "future", extra_rows=loud_future))
    other = exp.run_experiment(cfg).parent
    base = run["dir"]
    assert base.name != other.name  # the input changed, so it is a different experiment
    q = "SELECT * FROM {} WHERE window_start < TIMESTAMPTZ '2026-01-11' ORDER BY ALL"
    con = connect(cfg.duckdb)
    assert con.execute(q.format(_rel(base / "scores_raw.parquet"))).fetchall() == \
        con.execute(q.format(_rel(other / "scores_raw.parquet"))).fetchall()
    assert _manifest(base)["bands"]["cutoffs"] == _manifest(other)["bands"]["cutoffs"]


def test_rebanding_reuses_models_and_records_a_revision(run):
    ctx, d = run["ctx"], run["dir"]
    model_mtime = (d / "models" / "iforest.joblib").stat().st_mtime_ns
    strict = BandSettings(mode="budget", budget_per_day={"Critical": 0.5, "High": 1, "Medium": 2, "Low": 3})
    try:
        exp.finalize(ctx, strict)
        m = _manifest(d)
        assert m["experiment_id"] == d.name and len(m["band_revisions"]) >= 2
        assert m["bands"]["settings"]["mode"] == "budget"
        assert (d / "models" / "iforest.joblib").stat().st_mtime_ns == model_mtime
        n = _q(ctx, f"SELECT count(*) FILTER (WHERE iforest_band <> 'Benign') FROM {_rel(d / 'scores.parquet')} "
                    "WHERE period = 'validation'")[0][0]
        assert n <= 3 * 2 + 2  # about 3 review windows per day on the 2 reference days
    finally:
        exp.finalize(ctx)  # restore the configured bands for the other tests


def test_feature_window_and_model_changes_create_new_experiment_ids(tmp_path):
    cfg_path = _setup(tmp_path, "ids")
    ids = {"base": exp.open_context(load_poc_config(cfg_path)).experiment_id}
    for key, override in (("features", {"features": {"exclude": ["bytes_per_packet"]}}),
                          ("window", {"window_minutes": 30}),
                          ("model", {"models": {"iforest": {"n_estimators": 60, "max_train_rows": 2000},
                                                "ocsvm": {"max_train_rows": 400}, "n_jobs": 1}})):
        ids[key] = exp.open_context(load_poc_config(cfg_path, override)).experiment_id
    assert len(set(ids.values())) == 4 and ids["base"].startswith("fixture-")


def test_contamination_variants_and_training_exclusion(run, tmp_path):
    rob = json.loads((run["dir"] / "robustness.json").read_text(encoding="utf-8"))
    variants = {v["variant"]: v for v in rob["variants"]}
    assert {"iforest_excl_annotated", "ocsvm_excl_annotated", "iforest_trim990"} <= set(variants)
    full = _manifest(run["dir"])["models"][0]["train_rows_available"]
    assert variants["iforest_excl_annotated"]["train_rows_available"] < full
    assert {r["variant"] for r in rob["seed_stability"]} == {"iforest_seed43", "ocsvm_seed43"}
    cfg = load_poc_config(_setup(tmp_path, "excl"), {"split": {**SPLIT, "exclude_annotated_from_train": True}})
    ctx = exp.open_context(cfg)
    assert "outside" in ctx.train_where() and ctx.experiment_id != run["ctx"].experiment_id


def test_search_compares_candidates_on_validation_only(run):
    ctx = run["ctx"]
    ctx.cfg.search.iforest.append({"n_estimators": 20})
    try:
        md = exp.run_search(ctx)
    finally:
        ctx.cfg.search.iforest.clear()
    results = json.loads((md.parent / "search.json").read_text(encoding="utf-8"))
    assert [r["candidate"] for r in results] == ["iforest_c0", "iforest_c1", "ocsvm_c0"]
    assert results[0]["vs_base"]["spearman"] == pytest.approx(1.0)
    days = _q(ctx, f"SELECT DISTINCT flow_date::VARCHAR FROM {_rel(md.parent / 'iforest_validation.parquet')} "
                   "ORDER BY 1")
    assert days == [("2026-01-09",), ("2026-01-10",)]
    assert "validation period only" in md.read_text(encoding="utf-8")


def test_cli_profile_and_experiment_smoke(tmp_path):
    cfg = _setup(tmp_path, "cli")
    main(["profile", "--config", str(cfg)])
    profile = (tmp_path / "work_cli" / "profile" / "profile.md").read_text(encoding="utf-8")
    assert "Field mapping" in profile and "src_id_addr" in profile
    main(["run", "--config", str(cfg)])
    assert len(list((tmp_path / "work_cli" / "experiments").glob("*/report.md"))) == 1
    tests = list((tmp_path / "work_cli" / "scoring").glob("holdout-test-*/report.md"))
    assert len(tests) == 1 and "Final test report" in tests[0].read_text(encoding="utf-8")


def test_real_data_cannot_be_read_from_or_written_into_the_checkout(tmp_path):
    root = repo_root()
    cfg = load_poc_config(_setup(tmp_path, "guard"), {"work_dir": str(root / "guard_probe_outputs")})
    with pytest.raises(SourceError, match="ADR-012"):
        exp.run_profile(cfg)
    assert not (root / "guard_probe_outputs").exists()
    cfg = load_poc_config(_setup(tmp_path, "guard2"), {"input": {"paths": [str(root / "data" / "synth" / "raw")]}})
    with pytest.raises(SourceError, match="input data inside the repository"):
        exp.run_profile(cfg)


def test_poc_path_does_not_use_the_synthetic_generator_or_truth_files():
    forbidden = {"netanomaly.synth", "netanomaly.inject", "netanomaly.labels", "netanomaly.alerts"}
    for path in (Path(__file__).parents[1] / "src" / "netanomaly" / "poc").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert not imported & forbidden, path.name
        assert not re.search(r"truth_dir|injected_flows|injections\.csv|flow_sequence", text), path.name
