"""Scoring with a frozen model bundle - the only way the reserved test period and new data are ever scored.

    run_holdout(cfg, bundle)                 final test: the bundle's reserved test period of the configured input
    score_new(cfg, bundle, paths, history)   entirely new Parquet files (any period after the bundle's history)

Nothing is fitted or tuned here: the pipelines (imputer, scaler, model), the ordered model inputs, the field mapping,
the window, the band cutoffs and percentile grids and the training reference all come from the bundle. The new data
only has to match the bundle's field contract (checked first, bundle.check_compatible).

Relationship history (when the bundle has it) is continued from the bundle's state, or from an earlier scoring run
(`history`), and each run writes its own continued state; the bundle itself is never modified. The first window of
new data may not start before the history ends.

Output: `<work_dir>/scoring/<run_id>/` with scores_raw / scores / alerts / daily_summary / entity_summary Parquet,
run.json (bundle id, data period, holdout status), report.md + report.html, charts/ and relationships/.

Holdout ledger (`<work_dir>/holdout_ledger.json`): every final-test run is recorded with its bundle, test period and
input fingerprint. The report labels a run *first* (untouched holdout), *repeat* (same bundle and data again:
identical results, still the original holdout) or *reused* (another bundle already looked at this test period: a new
experiment whose test figures are optimistic).
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from netanomaly import outputs
from netanomaly import relationships as rel
from netanomaly.annotations import Annotation, load_annotations
from netanomaly.bundle import Bundle, BundleError, check_compatible, load_bundle
from netanomaly.config import PocConfig
from netanomaly.featureset import FeatureTable, build_feature_table
from netanomaly.models import score_to_parquet
from netanomaly.source import Source
from netanomaly.splits import Period

log = logging.getLogger("netanomaly")
HOLDOUT, NEW_DATA = "holdout-test", "new-data"
LEDGER, RUN_JSON, SCORES_RAW = "holdout_ledger.json", "run.json", "scores_raw.parquet"
HOLDOUT_LABELS = {
    "first": "Untouched holdout: the first evaluation of this test period recorded in this work_dir.",
    "repeat": ("Repeat of the same frozen evaluation (same bundle, same data): results are identical to the first "
               "run; still the original holdout."),
    "reused": ("NEW EXPERIMENT ON A PREVIOUSLY VIEWED TEST PERIOD: other bundles ({others}) were already evaluated "
               "on it. Settings chosen after that look are not independent of the test period; treat these test "
               "figures as optimistic."),
}


class ScoringError(ValueError):
    pass


@dataclass
class ScoringRun:
    """One scoring run. Attribute names match the development Context where report helpers are shared."""

    cfg: PocConfig
    con: duckdb.DuckDBPyConnection
    source: Source
    table: FeatureTable
    bundle: Bundle
    kind: str
    run_id: str
    exp_dir: Path  # the run directory
    period: Period
    annotations: list[Annotation]
    warnings: list[str] = field(default_factory=list)
    relationships: rel.RelationshipResult | None = None
    holdout: dict | None = None

    @property
    def model_ids(self) -> list[str]:
        return self.bundle.model_ids

    @property
    def features(self) -> list[str]:
        return self.bundle.model_features

    @property
    def periods(self) -> list[Period]:
        return [self.period]


def bundle_for_config(cfg: PocConfig) -> Path:
    """The bundle written by this config's development experiment (run `netanomaly` or `report` first)."""
    from netanomaly.experiment import MANIFEST, open_context

    ctx = open_context(cfg)
    manifest = ctx.exp_dir / MANIFEST
    bundle = json.loads(manifest.read_text(encoding="utf-8")).get("bundle") if manifest.exists() else None
    if not bundle or not Path(bundle["path"]).exists():
        raise ScoringError(f"no model bundle for experiment {ctx.experiment_id} yet: run `uv run netanomaly` (or "
                           "`train`, `score`, `report`) first, or pass --bundle")
    return Path(bundle["path"])


def run_holdout(cfg: PocConfig, bundle_path: Path) -> Path:
    """Score the bundle's reserved test period of the configured input once, with everything frozen."""
    bundle = load_bundle(bundle_path)
    test = bundle.period("test")
    if test is None:
        raise ScoringError(f"bundle {bundle.bundle_id} has no reserved test period (split.test)")
    return _score(cfg, bundle, cfg.input.paths, HOLDOUT, test, bundle.rel_state)


def score_new(cfg: PocConfig, bundle_path: Path, paths: list[str], history: Path | None = None) -> Path:
    """Score new Parquet files with a bundle. `history`: a bundle or an earlier scoring run to continue the
    relationship history from (default: the bundle's own state)."""
    bundle = load_bundle(bundle_path)
    return _score(cfg, bundle, paths, NEW_DATA, None, _history_dir(bundle, history))


def _history_dir(bundle: Bundle, history: Path | None) -> Path:
    if history is None or bundle.rel_params is None:  # no relationship analysis: no history to continue
        return bundle.rel_state
    for candidate in (history / "relationships" / rel.STATE_DIR, history):
        if (candidate / "state.json").exists():
            return candidate
    raise ScoringError(f"--history {history}: no relationship state found (expected a bundle or a scoring run "
                       "directory with relationships/state/state.json)")


def _data_period(con: duckdb.DuckDBPyConnection, table: FeatureTable) -> Period:
    # min/max over the hive partition column crashes DuckDB 1.5.5 when there is a single partition: use window_start
    lo, hi = con.execute(f"SELECT CAST(min(window_start) AS DATE), CAST(max(window_start) AS DATE) "
                         f"FROM {table.relation()}").fetchone()
    if lo is None:
        raise ScoringError("the new Parquet files contain no usable flows (flow_start and src_ip are required)")
    return Period("new", lo, hi + timedelta(days=1))


def _score(cfg: PocConfig, bundle: Bundle, paths: list[str], kind: str, period: Period | None,
           history_dir: Path) -> Path:
    from netanomaly.experiment import connect_and_open

    con, source = connect_and_open(cfg, paths, bundle.field_map, bundle.epoch_unit)
    fitted = bundle.load_models()
    base = build_feature_table(con, source, bundle.window_minutes, cfg.work_dir / "features")
    warnings = check_compatible(bundle, source, base.features)
    period = period or _data_period(con, base)
    n_rows = con.execute(f"SELECT count(*) FROM {base.relation()} WHERE {period.where()}").fetchone()[0]
    if not n_rows:
        raise ScoringError(f"no host-windows in {period.name} {period.start} .. {period.end} (exclusive)")
    key = json.dumps([source.fingerprint, period.to_dict(), str(history_dir)])
    run_id = f"{kind}-{bundle.bundle_id}-{hashlib.sha256(key.encode()).hexdigest()[:8]}"
    run_dir = cfg.work_dir / "scoring" / run_id
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True)
    run = ScoringRun(cfg, con, source, base, bundle, kind, run_id, run_dir, period, load_annotations(cfg.annotations),
                     warnings + _period_warnings(bundle, period, source, kind))
    _relationships(run, history_dir)
    missing = [f for f in bundle.model_features if f not in run.table.features]
    if missing:
        raise BundleError(f"model inputs {missing} are not available for this data; nothing was scored")
    n = score_to_parquet(con, run.table, fitted, run_dir / SCORES_RAW, cfg.batch_rows, period.where())
    log.info("%s: scored %d windows %s .. %s with frozen bundle %s", kind, n, period.start, period.end,
             bundle.bundle_id)
    n_alerts = _write_outputs(run)
    if kind == HOLDOUT:
        run.holdout = _ledger(cfg, bundle, period, source.fingerprint, run_id)
    manifest = _write_run_json(run, n, n_alerts)
    from netanomaly import run_report

    return run_report.write_run_report(run, manifest)


def _period_warnings(bundle: Bundle, period: Period, source: Source, kind: str) -> list[str]:
    out = []
    for name in ("train", "validation"):
        p = bundle.period(name)
        if p and period.start < p.end and period.end > p.start:
            out.append(f"the scored period overlaps the bundle's {name} period {p.start} .. {p.end}: those windows "
                       "were used to fit or calibrate the models (in-sample)")
    if kind == HOLDOUT and source.fingerprint != bundle.meta["input"]["fingerprint"]:
        out.append("the configured input differs from the data the bundle was developed on (different files, sizes "
                   "or modification times): this is not the reserved holdout of the same dataset")
    return out


def _relationships(run: ScoringRun, history_dir: Path) -> None:
    params = run.bundle.rel_params
    if params is None:
        return
    history = rel.load_history(history_dir, params)
    holdout = run.kind == HOLDOUT
    as_inputs = bool(run.bundle.rel.get("include_model_features"))
    try:
        run.relationships = rel.analyze(run.con, run.source, params, run.exp_dir / "relationships",
                                        start=run.period.start_dt if holdout else None,
                                        end=run.period.end_dt if holdout else None, history=history)
    except rel.RelationshipError as exc:
        if as_inputs:  # the models need the summaries: refuse rather than score with a made-up history
            raise
        run.warnings.append(f"relationship analysis skipped (report-only, model scores unaffected): {exc}")
        log.warning("relationship analysis skipped: %s", exc)
        return
    if as_inputs:
        run.table = rel.extend_feature_table(run.con, run.table, run.relationships, run.cfg.work_dir / "features",
                                             run.period.where())


def _write_outputs(run: ScoringRun) -> int:
    b, d, rpt = run.bundle, run.exp_dir, run.cfg.report
    buffer = run.cfg.annotation_buffer_hours
    outputs.write_scores(run.con, run.table, d / SCORES_RAW, [run.period], run.annotations, buffer, b.cutoffs,
                         b.grids, d / outputs.SCORES, b.model_features, b.train_stats)
    n_alerts = outputs.write_alerts(run.con, d / outputs.SCORES, run.source, b.model_ids, b.model_features,
                                    run.table.features, d / outputs.ALERTS, rpt.trace_top_n,
                                    rpt.trace_rows_per_window, b.train_stats, b.train_entities)
    outputs.write_daily_summary(run.con, d / outputs.SCORES, b.model_ids, run.annotations, buffer,
                                b.meta["bands"]["settings"]["below_label"], d / outputs.DAILY)
    outputs.write_entity_summary(run.con, d / outputs.SCORES, b.model_ids, d / outputs.ENTITY)
    return n_alerts


def _ledger(cfg: PocConfig, bundle: Bundle, period: Period, fingerprint: str, run_id: str) -> dict:
    path = cfg.work_dir / LEDGER
    entries = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    same = [e for e in entries if e["test_period"] == period.to_dict() and e["input_fingerprint"] == fingerprint]
    others = sorted({e["bundle_id"] for e in same if e["bundle_id"] != bundle.bundle_id})
    status = "reused" if others else "repeat" if same else "first"
    entry = {"at": datetime.now(UTC).isoformat(), "run_id": run_id, "bundle_id": bundle.bundle_id,
             "experiment_id": bundle.meta["experiment_id"], "test_period": period.to_dict(),
             "input_fingerprint": fingerprint, "status": status}
    path.write_text(json.dumps([*entries, entry], indent=2), encoding="utf-8")
    return {**entry, "previous_runs": len(same), "previous_bundles": others,
            "label": HOLDOUT_LABELS[status].format(others=", ".join(others))}


def _write_run_json(run: ScoringRun, n_windows: int, n_alerts: int) -> dict:
    from netanomaly.experiment import _versions, code_revision

    b = run.bundle
    manifest = {
        "run_id": run.run_id, "kind": run.kind, "created_at": datetime.now(UTC).isoformat(),
        "bundle": {"bundle_id": b.bundle_id, "path": str(b.path), "created_at": b.meta.get("created_at"),
                   "experiment_id": b.meta["experiment_id"], "code": b.meta.get("code"),
                   "models": {m["model_id"]: m.get("artifact_sha256") for m in b.meta["models"]}},
        "code": code_revision(), "versions": _versions(),
        "input": {"files": len(run.source.files), "bytes": sum(f.size for f in run.source.files),
                  "fingerprint": run.source.fingerprint, "paths": [f.path for f in run.source.files]},
        "field_mapping": run.source.mapping.to_dict(),
        "period": run.period.to_dict(), "scored_windows": n_windows, "review_windows": n_alerts,
        "model_inputs": b.model_features, "cutoffs": b.meta["bands"]["cutoffs"],
        "holdout": run.holdout, "warnings": run.warnings,
        "relationships": run.relationships.to_dict() if run.relationships else None,
        "notes": {"refit": "none: models, imputers, scalers, feature order and band cutoffs come from the bundle",
                  "scores": "rankings, higher = more anomalous; not probabilities",
                  "labels": "no individual labels: nothing here is accuracy, precision or recall"},
    }
    (run.exp_dir / RUN_JSON).write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return manifest
