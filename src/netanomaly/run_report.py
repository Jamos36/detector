"""Report of a scoring run with a frozen bundle: the final holdout test or new data (`report.md` + `report.html` +
`charts/` in the run directory). Every figure names the bundle and the data period, so results trace back to the
exact model used. All chart data is aggregated in DuckDB first."""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from netanomaly import charts
from netanomaly import diagnostics as diag
from netanomaly.annotations import CATEGORY_LABELS
from netanomaly.config import BAND_ORDER
from netanomaly.outputs import DAILY, SCORES, parquet
from netanomaly.report import DISCLAIMER, HIST_BINS, _candidates, _table, write_html

TITLES = {"holdout-test": "Final test report (reserved holdout period, frozen bundle)",
          "new-data": "New-data scoring report (frozen bundle)"}
QUANTILES = (0.5, 0.9, 0.99, 0.999)


def write_run_report(run, manifest: dict) -> Path:
    charts_dir = run.exp_dir / "charts"
    shutil.rmtree(charts_dir, ignore_errors=True)
    models = run.model_ids
    sections = [_header(run, manifest), _overview(run, models, charts_dir), _distribution(run, models, charts_dir),
                _bands(run, models, charts_dir), _annotations(run, models),
                _candidates(run, models).replace("## 6.", "## 5.", 1), _relationships(run, charts_dir),
                _trace(run, manifest)]
    path = run.exp_dir / "report.md"
    text = "\n\n".join(s for s in sections if s) + "\n"
    path.write_text(text, encoding="utf-8")
    write_html(text, path.with_suffix(".html"), f"{run.kind} {run.bundle.bundle_id}")
    return path


def _intervals(run) -> list[tuple]:
    pad = timedelta(hours=run.cfg.annotation_buffer_hours)
    return [(a.start - pad, a.start, a.end, a.end + pad) for a in run.annotations]


def _header(run, manifest: dict) -> str:
    b, p = run.bundle, run.period
    s = diag.rows(run.con, f"SELECT count(*) AS windows, count(DISTINCT src_ip) AS hosts, sum(flows) AS flows, "
                           f"min(window_start)::VARCHAR AS first, max(window_end)::VARCHAR AS last "
                           f"FROM {parquet(run.exp_dir / SCORES)}")[0]
    train, val = b.period("train"), b.period("validation")
    holdout = run.holdout or {}
    status = [f"> **Holdout status: {holdout['status']}.** {holdout['label']}", ""] if holdout else [
        ("> **New data.** No labels: these are rankings against the bundle's training baseline, not accuracy, "
         "precision or recall. Nothing was fitted or tuned on this data."), ""]
    mock = ("\n\n> **Mock/synthetic input** (`allow_inside_repo: true`): results describe fixture or demo data, "
            "not any real network." if run.cfg.allow_inside_repo else "")
    trained = (f"- Trained on {train.start} .. {train.end}; bands calibrated on validation {val.start} .. {val.end} "
               "(frozen; nothing refitted).") if train and val else "- Trained / calibrated periods: see bundle.json."
    return "\n".join([
        f"# {TITLES[run.kind]}", "",
        f"**Model bundle `{b.bundle_id}`** - data period **{p.start} .. {p.end} (UTC, end exclusive)**.", "",
        DISCLAIMER.format(below=b.meta["bands"]["settings"]["below_label"]) + mock, "", *status,
        f"- Run: `{run.run_id}` ({manifest['created_at']} UTC) -> `{run.exp_dir}`.",
        (f"- Bundle: created {b.meta.get('created_at')}, experiment `{b.meta['experiment_id']}`, code "
         f"{(b.meta.get('code') or {}).get('commit') or 'unknown'}; path `{b.path}`."), trained,
        (f"- Input: {manifest['input']['files']} Parquet files; {s['flows']:,} flows in {s['windows']:,} host x "
         f"{b.window_minutes}-min windows, {s['hosts']:,} hosts, {s['first']} .. {s['last']} (UTC)."),
        f"- Model inputs ({len(b.model_features)}, in this order): {', '.join(f'`{f}`' for f in b.model_features)}.",
        "", "**Warnings**", "", "\n".join(f"- {w}" for w in run.warnings) or "- none"])


def _overview(run, models: list[str], out: Path) -> str:
    cols = ", ".join(f"{' + '.join(f'{m}_{b.lower()}' for b in BAND_ORDER)} AS {m}" for m in models)
    got = diag.rows(run.con, f"SELECT flow_date, flows, {cols} FROM {parquet(run.exp_dir / DAILY)} ORDER BY 1")
    days = [datetime.combine(r["flow_date"], datetime.min.time()) for r in got]
    fmt = run.cfg.report.chart_format
    charts.overview(out / f"overview.{fmt}", days, [r["flows"] for r in got], {m: [r[m] for r in got] for m in models},
                    _intervals(run), [(run.period.name, run.period.start_dt, run.period.end_dt)], "day")
    return "\n".join(["## 1. Overview", "", f"![overview](charts/overview.{fmt})", "",
                      "Daily flow volume (top) and windows in any review band per model (bottom)."])


def _reference_quantile(grid: list[tuple[float, float]], q: float) -> float | None:
    """Smallest validation score whose reference share reaches q (from the bundle's percentile grid)."""
    return next((s for s, share in grid if share >= q), None)


def _histogram(run, rel: str, m: str, lo: float, hi: float) -> tuple[np.ndarray, np.ndarray]:
    width = (hi - lo) / HIST_BINS or 1.0
    edges = np.array([lo + i * width for i in range(HIST_BINS + 1)])
    counts = np.zeros(HIST_BINS)
    for b, n in run.con.execute(f"SELECT least(CAST(floor(({m}_raw - {lo!r}) / {width!r}) AS INTEGER), "
                                f"{HIST_BINS - 1}), count(*) FROM {rel} GROUP BY 1").fetchall():
        counts[b] = n
    return edges, counts


def _distribution(run, models: list[str], out: Path) -> str:
    rel = parquet(run.exp_dir / SCORES)
    qs = "[" + ", ".join(map(str, QUANTILES)) + "]"
    body, hists = [], {}
    for m in models:
        q, lo, hi = run.con.execute(f"SELECT quantile_cont({m}_raw, {qs}), min({m}_raw), max({m}_raw) "
                                    f"FROM {rel}").fetchone()
        body.append([m, "validation (bundle)", *[_reference_quantile(run.bundle.grids[m], x) for x in QUANTILES]])
        body.append([m, run.period.name, *q])
        hists[m] = {run.period.name: _histogram(run, rel, m, lo, hi)}
    fmt = run.cfg.report.chart_format
    charts.score_distributions(out / f"score_distribution.{fmt}", hists,
                               {c.model_id: c.thresholds for c in run.bundle.cutoffs})
    return "\n".join(["## 2. Score distribution against the validation reference", "",
                      f"![scores](charts/score_distribution.{fmt})", "",
                      ("Raw-score quantiles (higher = more anomalous). A distribution shifted above validation means "
                       "more windows look unusual than during calibration (drift or new activity), so bands fire more "
                       "often."), "", _table(["model", "period", *[f"q{q}" for q in QUANTILES]], body)])


def _bands(run, models: list[str], out: Path) -> str:
    rel = parquet(run.exp_dir / SCORES)
    parts = ", ".join(f"count(*) FILTER (WHERE {m}_band = '{b}') AS {m}_{b}" for m in models for b in BAND_ORDER)
    got = diag.rows(run.con, f"SELECT date_trunc('day', window_start) AS d, {parts} FROM {rel} GROUP BY 1 ORDER BY 1")
    fmt = run.cfg.report.chart_format
    charts.bands_over_time(out / f"bands_over_time.{fmt}", [r["d"] for r in got],
                           {m: {b: [r[f"{m}_{b}"] for r in got] for b in BAND_ORDER} for m in models},
                           _intervals(run), "day", timedelta(days=1))
    total, days = run.con.execute(f"SELECT count(*), count(DISTINCT flow_date) FROM {rel}").fetchone()
    body = []
    for c in run.bundle.cutoffs:
        cum = 0
        for b in BAND_ORDER:
            k = sum(r[f"{c.model_id}_{b}"] for r in got)
            cum += k
            body.append([c.model_id, b, c.thresholds[b], k, round(k / max(days, 1), 2), cum / total,
                         1 - c.quantiles[b]])
    return "\n".join(["## 3. Review bands (validation cutoffs, frozen)", "", f"![bands](charts/bands_over_time.{fmt})",
                      "", ("*Expected share* is the share of validation windows at or above the band. A much larger "
                           "observed share means this period differs from validation - not a detection rate."), "",
                      _table(["model", "band", "raw cutoff >=", "windows", "per day", "observed share (cumulative)",
                              "expected share (cumulative)"], body)])


def _annotations(run, models: list[str]) -> str:
    if not run.annotations:
        return "## 4. Supplied pentest ranges\n\nNo annotation file configured (`annotations:`)."
    b, k = run.cfg.annotation_buffer_hours, run.cfg.diagnostics.top_k_per_day
    over = diag.annotation_overlap(run.con, parquet(run.exp_dir / SCORES), models, run.annotations, [b], k)
    return "\n".join([
        "## 4. Supplied pentest ranges (weak annotations)", "",
        (f"Where review-band and daily top-{k} windows fall versus all windows (buffer {b} h). Ratio > 1 = "
         "over-represented. Broad date ranges are context, not labels: this is co-occurrence, not a detection rate."),
        "", _table(["model", "category", "windows", "base share", "review share", "review ratio", f"top-{k} ratio"],
                   [[r["model"], CATEGORY_LABELS[r["category"]], r["windows"], r["base_share"], r["review_share"],
                     r["review_ratio"], r["topk_ratio"]] for r in over if r["period"] == run.period.name])])


def _relationships(run, out: Path) -> str:
    if run.relationships is None:
        return ""
    from netanomaly import relationship_report

    return relationship_report.section(
        "## 6. Source -> destination relationships", run.con, run.relationships, run.cfg, _intervals(run), out,
        model_features=[f for f in run.bundle.model_features if f.startswith("rel_")])


def _trace(run, manifest: dict) -> str:
    models = manifest["bundle"]["models"]
    return "\n".join([
        "## 7. Traceability and limits", "",
        f"- Bundle `{run.bundle.bundle_id}`; model artifacts (sha256): "
        + ", ".join(f"{m} `{(h or '')[:12]}`" for m, h in models.items()) + ".",
        f"- Input fingerprint `{manifest['input']['fingerprint'][:16]}`; files and field mapping in `run.json`.",
        ("- Outputs: `scores.parquet`, `alerts.parquet` (with source file / row traces), `daily_summary.parquet`, "
         "`entity_summary.parquet`, `run.json`."),
        ("- Nothing was fitted, re-scaled or re-calibrated on this data; features were computed with the bundle's "
         "window and mapping and passed to the models in the bundle's order."),
        ("- No reliable labels: these are rankings for review, not accuracy, precision, recall or a false-positive "
         "rate. Pentest ranges are broad context.")])
