"""Experiment orchestration: the Python API behind `netanomaly <stage>`.

    ctx = open_context(cfg)          # source + mapping, feature table (cached), split, feature screening, id
    fitted = train_models(ctx)       # fit IF / OCSVM on the training period, save pipelines
    score_models(ctx, fitted)        # score every window (scores_raw.parquet)
    robustness(ctx, fitted)          # seed stability + contamination variants (label-free)
    finalize(ctx)                    # bands, output tables, diagnostics, report, manifest

Layout under `work_dir` (outside the checkout, ADR-012):
    profile/                         profile.json, profile.md
    features/<key>/                  cached host-window table + meta.json
    experiments/<experiment_id>/     manifest.json, models/, scores_raw.parquet, scores.parquet, alerts.parquet,
                                     daily_summary.parquet, entity_summary.parquet, diagnostics.json,
                                     robustness.json, report.md, charts/, search/

The experiment id hashes everything that changes the fitted models or their inputs (input fingerprint, mapping,
window, selected features, split, training exclusion, model settings, seeds). Band cutoffs are not part of it:
re-banding reuses the experiment and is recorded in `band_revisions`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import matplotlib
import numpy as np
import pyarrow
import sklearn

from netanomaly import bands as bands_mod
from netanomaly import diagnostics as diag
from netanomaly import outputs
from netanomaly.annotations import OUTSIDE, Annotation, category_sql, load_annotations, overlaps
from netanomaly.config import BandSettings, PocConfig
from netanomaly.db import connect
from netanomaly.featureset import FeatureTable, build_feature_table, feature_quality, select_features
from netanomaly.models import (
    FittedModel,
    ModelSpec,
    fit_model,
    load_model,
    save_model,
    score_to_parquet,
    specs_from_config,
)
from netanomaly.profile import build_profile, write_profile
from netanomaly.source import Source, check_outside_repo, open_source, repo_root
from netanomaly.splits import Period, resolve_split

log = logging.getLogger("netanomaly")
POC_VERSION = 1
SCORES_RAW = "scores_raw.parquet"
MANIFEST = "manifest.json"
DIAGNOSTICS = "diagnostics.json"
ROBUSTNESS = "robustness.json"
TEST_NOTE = ("The test period is independent only until settings are changed after looking at its results; every "
             "such change makes later test figures optimistic. Tune on validation.")


class ExperimentError(ValueError):
    pass


@dataclass
class Context:
    cfg: PocConfig
    con: duckdb.DuckDBPyConnection
    source: Source
    table: FeatureTable
    annotations: list[Annotation]
    periods: list[Period]
    selected: list[str]  # configured model inputs
    features: list[str]  # after screening on training rows
    quality: list[dict]
    experiment_id: str
    exp_dir: Path
    warnings: list[str] = field(default_factory=list)

    def period(self, name: str) -> Period | None:
        return next((p for p in self.periods if p.name == name), None)

    @property
    def train(self) -> Period:
        return self.periods[0]

    def train_where(self, *, exclude_annotated: bool | None = None) -> str:
        exclude = self.cfg.split.exclude_annotated_from_train if exclude_annotated is None else exclude_annotated
        where = self.train.where()
        if exclude and self.annotations:
            cat = category_sql("window_start", "window_end", self.annotations, self.cfg.annotation_buffer_hours)
            where += f" AND ({cat}) = '{OUTSIDE}'"
        return where

    @property
    def model_ids(self) -> list[str]:
        return [s.model_id for s in specs_from_config(self.cfg.models)]


# --- setup -------------------------------------------------------------------------------------------------------

def _connect_and_open(cfg: PocConfig) -> tuple[duckdb.DuckDBPyConnection, Source]:
    """Boundary checks first (connect() creates the spill directory under work_dir), then the source."""
    check_outside_repo([cfg.work_dir, cfg.duckdb.temp_directory], "work_dir (outputs, models, spill)",
                       cfg.allow_inside_repo)
    con = connect(cfg.duckdb)
    source = open_source(con, cfg.input.paths, cfg.input.field_map, cfg.input.epoch_unit)
    check_outside_repo([Path(f.path) for f in source.files], "input data", cfg.allow_inside_repo)
    return con, source


def run_profile(cfg: PocConfig) -> Path:
    con, source = _connect_and_open(cfg)
    return write_profile(build_profile(con, source, cfg.window_minutes), cfg.work_dir / "profile")


def open_context(cfg: PocConfig, *, rebuild_features: bool = False) -> Context:
    con, source = _connect_and_open(cfg)
    table = build_feature_table(con, source, cfg.window_minutes, cfg.work_dir / "features", rebuild=rebuild_features)
    annotations = load_annotations(cfg.annotations)
    days = [d for (d,) in con.execute(f"SELECT DISTINCT flow_date FROM {table.relation()} ORDER BY 1").fetchall()]
    periods = resolve_split(cfg.split, days)
    selected = select_features(table.features, cfg.features.include, cfg.features.exclude)
    ctx = Context(cfg, con, source, table, annotations, periods, selected, [], [], "", Path())
    ctx.quality = feature_quality(con, table, selected, ctx.train_where())
    ctx.features = [q["feature"] for q in ctx.quality if q["status"] == "ok"]
    for q in ctx.quality:
        if q["status"] != "ok":
            ctx.warnings.append(f"feature {q['feature']} dropped: {q['status']} on training rows")
    if not ctx.features:
        raise ExperimentError("every selected feature is all-NULL or constant on the training rows")
    for o in overlaps(annotations, ctx.train.start_dt, ctx.train.end_dt, cfg.annotation_buffer_hours):
        ctx.warnings.append(f"training period overlaps supplied interval {o['name']} ({o['kind']}); the baseline may "
                            "contain pentest activity. Consider split.exclude_annotated_from_train or another "
                            "training period (see contamination variants in the report).")
    ctx.experiment_id = experiment_id(ctx)
    ctx.exp_dir = cfg.work_dir / "experiments" / ctx.experiment_id
    for w in ctx.warnings:
        log.warning(w)
    return ctx


def experiment_id(ctx: Context) -> str:
    specs = specs_from_config(ctx.cfg.models)
    excluding = ctx.cfg.split.exclude_annotated_from_train
    payload = {"poc_version": POC_VERSION, "feature_table": ctx.table.key, "features": ctx.features,
               "window_minutes": ctx.cfg.window_minutes, "periods": [p.to_dict() for p in ctx.periods],
               "exclude_annotated": excluding,
               "annotations": [a.to_dict() for a in ctx.annotations] if excluding else None,
               "annotation_buffer_hours": ctx.cfg.annotation_buffer_hours if excluding else None,
               "models": [{"kind": s.kind, "settings": s.settings, "seed": s.seed} for s in specs]}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:10]
    return f"{ctx.cfg.name}-{digest}"


# --- stages ------------------------------------------------------------------------------------------------------

def _fit(ctx: Context, spec: ModelSpec, train_where: str | None = None) -> FittedModel:
    m = ctx.cfg.models
    return fit_model(ctx.con, ctx.table, ctx.features, train_where or ctx.train_where(), spec, n_jobs=m.n_jobs,
                     max_matrix_mb=m.max_matrix_mb)


def train_models(ctx: Context) -> list[FittedModel]:
    specs = specs_from_config(ctx.cfg.models)
    if not specs:
        raise ExperimentError("no model enabled (models.iforest.enabled / models.ocsvm.enabled)")
    fitted = []
    for spec in specs:
        f = _fit(ctx, spec)
        save_model(f, ctx.exp_dir / "models")
        log.info("fitted %s on %d of %d training windows in %.1fs", f.model_id, f.train_rows, f.train_rows_available,
                 f.fit_seconds)
        fitted.append(f)
    write_manifest(ctx, {"models": [f.summary() for f in fitted]})
    return fitted


def load_models(ctx: Context) -> list[FittedModel]:
    models_dir = ctx.exp_dir / "models"
    if not all((models_dir / f"{m}.joblib").exists() for m in ctx.model_ids):
        raise ExperimentError(f"no trained models in {models_dir}; run `netanomaly train` first")
    return [load_model(models_dir, m) for m in ctx.model_ids]


def score_models(ctx: Context, fitted: list[FittedModel] | None = None) -> Path:
    fitted = fitted or load_models(ctx)
    out = ctx.exp_dir / SCORES_RAW
    n = score_to_parquet(ctx.con, ctx.table, fitted, out, ctx.cfg.batch_rows)
    log.info("scored %d windows with %s -> %s", n, [f.model_id for f in fitted], out)
    write_manifest(ctx, {"scored_windows": n})
    return out


def robustness(ctx: Context, fitted: list[FittedModel] | None = None) -> dict:
    """Seed stability (on validation) and contamination variants (on test), compared with the primary models."""
    fitted = fitted or load_models(ctx)
    raw = outputs.parquet(ctx.exp_dir / SCORES_RAW)
    d = ctx.cfg.diagnostics
    tmp = ctx.exp_dir / "tmp"
    seed_period = ctx.period("validation") or ctx.period("test") or ctx.train
    eval_period = ctx.period("test") or ctx.period("validation") or ctx.train
    result: dict = {"seed_period": seed_period.name, "variant_period": eval_period.name, "seed_stability": [],
                    "variants": []}
    for f in fitted:
        for i in range(1, d.seed_repeats + 1):
            spec = f.spec.with_overrides({}, label=f"{f.model_id}_seed{f.spec.seed + i}", seed=f.spec.seed + i)
            _compare(ctx, raw, f, _fit(ctx, spec), seed_period, tmp, result["seed_stability"], "seed")
        for name, where in _variant_wheres(ctx, f, raw):
            spec = f.spec.with_overrides({}, label=f"{f.model_id}_{name}")
            _compare(ctx, raw, f, _fit(ctx, spec, where), eval_period, tmp, result["variants"], name)
    shutil.rmtree(tmp, ignore_errors=True)
    (ctx.exp_dir / ROBUSTNESS).write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    return result


def _variant_wheres(ctx: Context, f: FittedModel, raw: str) -> list[tuple[str, str]]:
    """Alternative training selections: with/without supplied intervals (when they touch training) and a refit
    without the primary model's own highest-scoring training windows (trimmed baseline)."""
    out = []
    if ctx.annotations and overlaps(ctx.annotations, ctx.train.start_dt, ctx.train.end_dt,
                                    ctx.cfg.annotation_buffer_hours):
        flip = not ctx.cfg.split.exclude_annotated_from_train
        out.append(("excl_annotated" if flip else "incl_annotated", ctx.train_where(exclude_annotated=flip)))
    q = ctx.cfg.diagnostics.trim_refit_quantile
    if q is not None:
        m = f.model_id
        cut = ctx.con.execute(f"SELECT quantile_disc({m}_raw, {q}) FROM {raw} "
                              f"WHERE {ctx.train_where()}").fetchone()[0]
        keep = (f"struct_pack(s := src_ip, w := window_start) IN (SELECT struct_pack(s := src_ip, w := window_start) "
                f"FROM {raw} WHERE {ctx.train.where()} AND {m}_raw < {cut!r})")
        out.append((f"trim{round(q * 1000)}", f"{ctx.train_where()} AND {keep}"))
    return out


def _compare(ctx: Context, raw: str, primary: FittedModel, variant: FittedModel, period: Period, tmp: Path,
             sink: list, kind: str) -> None:
    path = tmp / f"{variant.model_id}.parquet"
    score_to_parquet(ctx.con, ctx.table, [variant], path, ctx.cfg.batch_rows, period.where())
    rel = outputs.parquet(path)
    agree = diag.rank_agreement(ctx.con, raw, f"{primary.model_id}_raw", rel, f"{variant.model_id}_raw",
                                ctx.cfg.diagnostics.top_n)
    entry = {"model": primary.model_id, "variant": variant.model_id, "kind": kind, "period": period.name,
             "train_rows": variant.train_rows, "train_rows_available": variant.train_rows_available, **agree}
    if ctx.annotations:
        b, n = ctx.cfg.annotation_buffer_hours, ctx.cfg.diagnostics.top_n
        primary_rel = f"(SELECT * FROM {raw} WHERE {period.where()})"
        entry["top_n_categories_primary"] = diag.top_categories(ctx.con, primary_rel, f"{primary.model_id}_raw", n,
                                                                ctx.annotations, b)
        entry["top_n_categories_variant"] = diag.top_categories(ctx.con, rel, f"{variant.model_id}_raw", n,
                                                                ctx.annotations, b)
    sink.append(entry)
    log.info("%s %s vs %s on %s: spearman %s, top-%d jaccard %s", kind, variant.model_id, primary.model_id,
             period.name, _f(entry["spearman"]), ctx.cfg.diagnostics.top_n, _f(entry["top_n_jaccard"]))


def finalize(ctx: Context, band_settings: BandSettings | None = None) -> Path:
    """Calibrate bands, write output tables, diagnostics and the report. Re-runnable with other bands."""
    from netanomaly import report  # matplotlib's pyplot is imported only when a report is written

    settings = band_settings or ctx.cfg.bands
    raw_path = ctx.exp_dir / SCORES_RAW
    if not raw_path.exists():
        raise ExperimentError(f"{raw_path} missing; run `netanomaly score` first")
    raw = outputs.parquet(raw_path)
    models = ctx.model_ids
    ref_period = ctx.period(settings.reference) or ctx.train
    if ref_period.name != settings.reference:
        ctx.warnings.append(f"no {settings.reference} period: bands calibrated on {ref_period.name} (in-sample)")
    cutoffs, grids, refs = [], {}, {}
    for m in models:
        ref, per_day = bands_mod.reference_scores(ctx.con, raw, f"{m}_raw", ref_period.where())
        cutoffs.append(bands_mod.calibrate(m, ref, per_day, settings, ref_period.name))
        grids[m], refs[m] = bands_mod.percentile_grid(ref), ref
    d, b = ctx.exp_dir, ctx.cfg.annotation_buffer_hours
    outputs.write_scores(ctx.con, ctx.table, raw_path, ctx.periods, ctx.annotations, b, cutoffs, grids,
                         d / outputs.SCORES, ctx.features, ctx.train_where())
    n_alerts = outputs.write_alerts(ctx.con, d / outputs.SCORES, ctx.source, models, ctx.features,
                                    ctx.table.features, d / outputs.ALERTS, ctx.cfg.report.trace_top_n,
                                    ctx.cfg.report.trace_rows_per_window)
    outputs.write_daily_summary(ctx.con, d / outputs.SCORES, models, ctx.annotations, b, settings.below_label,
                                d / outputs.DAILY)
    outputs.write_entity_summary(ctx.con, d / outputs.SCORES, models, d / outputs.ENTITY)
    diagnostics = compute_diagnostics(ctx, models, refs)
    (d / DIAGNOSTICS).write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    rob_path = d / ROBUSTNESS
    rob = json.loads(rob_path.read_text(encoding="utf-8")) if rob_path.exists() else None
    manifest = write_manifest(ctx, {"review_windows": n_alerts}, band_revision={
        "at": datetime.now(UTC).isoformat(), "settings": settings.model_dump(), "reference": ref_period.name,
        "cutoffs": [c.to_dict() for c in cutoffs]})
    path = report.write_report(ctx, manifest, cutoffs, diagnostics, rob)
    log.info("%d windows in a review band of at least one model", n_alerts)
    return path


def compute_diagnostics(ctx: Context, models: list[str], refs: dict[str, np.ndarray]) -> dict:
    rel = outputs.parquet(ctx.exp_dir / outputs.SCORES)
    d, con = ctx.cfg.diagnostics, ctx.con
    buffers = sorted({*d.buffer_hours_sensitivity, ctx.cfg.annotation_buffer_hours})
    out = {"score_distribution": diag.score_distribution(con, rel, models),
           "band_volume": diag.band_volume(con, rel, models),
           "cutoff_curve": [r for m in models for r in diag.cutoff_curve(con, rel, m, refs[m])],
           "concentration": {m: diag.concentration(con, rel, m, d.top_k_per_day) for m in models},
           "training_concentration": {m: diag.training_concentration(con, rel, m) for m in models},
           "annotation_overlap": diag.annotation_overlap(con, rel, models, ctx.annotations, buffers,
                                                          d.top_k_per_day),
           "feature_drift": diag.feature_drift(con, ctx.table, ctx.features, ctx.train,
                                                  diag.time_bucket(ctx.periods)[0])}
    if len(models) >= 2:
        a, b = models[:2]
        out["model_agreement"] = diag.model_agreement(con, rel, a, b, d.top_n, d.top_k_per_day)
        focus = "test" if ctx.period("test") else ctx.periods[-1].name
        out["disagreements"] = {"period": focus, "rows": diag.disagreements(con, rel, a, b, focus, 15)}
    return out


def run_experiment(cfg: PocConfig, *, rebuild_features: bool = False) -> Path:
    ctx = open_context(cfg, rebuild_features=rebuild_features)
    log.info("experiment %s: %d windows, features %s, periods %s", ctx.experiment_id, ctx.table.rows, ctx.features,
             [(p.name, str(p.start), str(p.end)) for p in ctx.periods])
    fitted = train_models(ctx)
    score_models(ctx, fitted)
    robustness(ctx, fitted)
    return finalize(ctx)


# --- search ------------------------------------------------------------------------------------------------------

def run_search(ctx: Context) -> Path:
    """Fit each configured candidate on the training period and compare them on VALIDATION only (test untouched).

    Label-free: score quantiles, daily top-k concentration, agreement with the base configuration, and (weak) the
    annotation categories of the validation top-N. Every candidate and its diagnostics is stored; nothing is chosen
    automatically.
    """
    val = ctx.period("validation")
    if val is None:
        raise ExperimentError("search needs a validation period (split.validation or a non-zero fraction)")
    out_dir = ctx.exp_dir / "search"
    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for base in specs_from_config(ctx.cfg.models):
        overrides = [{}, *getattr(ctx.cfg.search, base.kind)]
        candidates = [base.with_overrides(o, label=f"{base.kind}_c{i}") for i, o in enumerate(overrides)]
        fitted = [_fit(ctx, c) for c in candidates]
        path = out_dir / f"{base.kind}_validation.parquet"
        score_to_parquet(ctx.con, ctx.table, fitted, path, ctx.cfg.batch_rows, val.where())
        rel = f"(SELECT *, 'validation' AS period FROM {outputs.parquet(path)})"
        for o, f in zip(overrides, fitted, strict=True):
            results.append(_search_entry(ctx, rel, base.kind, o, f, fitted[0]))
    (out_dir / "search.json").write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    md = out_dir / "search.md"
    md.write_text(_search_markdown(results), encoding="utf-8")
    write_manifest(ctx, {"search": {"path": str(md), "candidates": len(results)}})
    return md


def _search_entry(ctx: Context, rel: str, kind: str, overrides: dict, f: FittedModel, base: FittedModel) -> dict:
    d, col = ctx.cfg.diagnostics, f"{f.model_id}_raw"
    q = diag.rows(ctx.con, f"SELECT quantile_cont({col}, [0.5, 0.99, 0.999]) AS q FROM {rel}")[0]["q"]
    entry = {"kind": kind, "candidate": f.model_id, "overrides": overrides, "settings": f.spec.settings,
             "seed": f.spec.seed, "fit_seconds": round(f.fit_seconds, 2), "train_rows": f.train_rows,
             "validation_q50": q[0], "validation_q99": q[1], "validation_q999": q[2],
             "concentration": diag.concentration(ctx.con, rel, f.model_id, d.top_k_per_day),
             "vs_base": diag.rank_agreement(ctx.con, rel, f"{base.model_id}_raw", rel, col, d.top_n)}
    if ctx.annotations:
        entry["validation_top_n_categories"] = diag.top_categories(ctx.con, rel, col, d.top_n, ctx.annotations,
                                                                   ctx.cfg.annotation_buffer_hours)
    return entry


def _search_markdown(results: list[dict]) -> str:
    lines = ["# Parameter search (validation period only)", "",
             "Label-free comparison; nothing is selected automatically. `vs base` = agreement with candidate 0 (the "
             "configured settings). Top-N categories are weak annotation context, not detection rates. " + TEST_NOTE,
             "", ("| candidate | overrides | fit s | q50 | q99 | q99.9 | top-k distinct hosts | top-5 host share | "
             "vs base spearman | vs base top-N jaccard | top-N inside / buffer / outside |"),
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        conc = (r["concentration"] or [{}])[0]
        cats = r.get("validation_top_n_categories") or {}
        cat_txt = " / ".join(f"{cats.get(k, 0):.2f}" for k in ("inside", "buffer", "outside")) if cats else "-"
        lines.append(f"| {r['candidate']} | `{json.dumps(r['overrides'])}` | {r['fit_seconds']} | "
                     f"{r['validation_q50']:.4g} | {r['validation_q99']:.4g} | {r['validation_q999']:.4g} | "
                     f"{conc.get('distinct_entities', '-')} | {_f(conc.get('top5_entity_share'))} | "
                     f"{_f(r['vs_base']['spearman'])} | {_f(r['vs_base']['top_n_jaccard'])} | {cat_txt} |")
    return "\n".join(lines) + "\n"


def _f(x: float | None) -> str:
    return "-" if x is None else f"{x:.3f}"


# --- manifest ----------------------------------------------------------------------------------------------------

def code_revision() -> dict:
    root = repo_root()
    if root is None:
        return {"commit": None, "dirty": None}
    try:
        commit = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True,
                                check=True).stdout.strip()
        dirty = bool(subprocess.run(["git", "-C", str(root), "status", "--porcelain", "--", "src"],
                                    capture_output=True, text=True, check=True).stdout.strip())
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "dirty": None}
    return {"commit": commit, "src_dirty": dirty}


def write_manifest(ctx: Context, extra: dict, band_revision: dict | None = None) -> dict:
    path = ctx.exp_dir / MANIFEST
    ctx.exp_dir.mkdir(parents=True, exist_ok=True)
    old = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    now = datetime.now(UTC).isoformat()
    manifest = {
        **old,
        "experiment_id": ctx.experiment_id,
        "created_at": old.get("created_at", now),
        "updated_at": now,
        "poc_version": POC_VERSION,
        "code": code_revision(),
        "versions": {"python": platform.python_version(), "scikit-learn": sklearn.__version__,
                     "numpy": np.__version__, "duckdb": duckdb.__version__, "pyarrow": pyarrow.__version__,
                     "matplotlib": matplotlib.__version__},
        "config": json.loads(ctx.cfg.model_dump_json()),
        "input": {"files": len(ctx.source.files), "bytes": sum(f.size for f in ctx.source.files),
                  "fingerprint": ctx.source.fingerprint, "paths": [f.path for f in ctx.source.files]},
        "field_mapping": ctx.source.mapping.to_dict(),
        "feature_table": {"key": ctx.table.key, "path": str(ctx.table.path), "rows": ctx.table.rows,
                          "window_minutes": ctx.table.window_minutes, "computed": ctx.table.features},
        "features": {"selected": ctx.selected, "model_inputs": ctx.features, "quality": ctx.quality},
        "periods": [p.to_dict() for p in ctx.periods],
        "training_filter": {"exclude_annotated": ctx.cfg.split.exclude_annotated_from_train,
                            "where": ctx.train_where()},
        "annotations": [a.to_dict() for a in ctx.annotations],
        "annotation_buffer_hours": ctx.cfg.annotation_buffer_hours,
        "warnings": ctx.warnings,
        "notes": {"scores": "higher = more anomalous; rankings within one fitted model, not probabilities",
                  "bands": "rank/review bands calibrated on a reference period; Benign = below the review "
                           "threshold, not proven safe",
                  "annotations": "weak temporal context, never labels", "test_period": TEST_NOTE},
        **extra,
    }
    if band_revision:
        manifest["band_revisions"] = [*old.get("band_revisions", []), band_revision]
        manifest["bands"] = band_revision
    path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return manifest
