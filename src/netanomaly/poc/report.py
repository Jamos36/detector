"""Human-readable experiment report: `report.md` + `charts/*.svg` (or .png, `report.chart_format`) in the experiment directory.

All chart data is aggregated in DuckDB first (per day, week, window bucket, histogram bin, or a bounded hash sample);
the report never loads every scored window into Python.
"""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from netanomaly.poc import charts
from netanomaly.poc.annotations import CATEGORY_LABELS, OUTSIDE, overlaps
from netanomaly.poc.bands import Cutoffs, bands_markdown
from netanomaly.poc.config import BAND_ORDER
from netanomaly.poc.diagnostics import rows, time_bucket
from netanomaly.poc.outputs import ALERTS, DAILY, ENTITY, SCORES, parquet

REPORT = "report.md"
FLAGGED = ", ".join(repr(b) for b in BAND_ORDER)
HIST_BINS = 60
SCATTER_ROWS = 50_000
DISCLAIMER = (
    "> **How to read this report.** Scores are *rankings* from unsupervised models (higher = more anomalous), not "
    "probabilities. Critical/High/Medium/Low are review bands calibrated on a reference period, not severities; "
    "`{below}` means *below the review threshold*, not *safe*. Supplied pentest date ranges are broad, weak "
    "temporal context: a window inside one is not a confirmed attack, and activity outside them is not cleared. "
    "Nothing here is accuracy, precision, recall or a false-positive rate - there are no reliable labels.")


def _table(headers: list[str], body: list[list]) -> str:
    def cell(v: object) -> str:
        if v is None:
            return "-"
        if isinstance(v, float):
            return f"{v:.4g}"
        return str(v).replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(cell(v) for v in r) + " |" for r in body]
    return "\n".join(lines)


def _intervals(ctx) -> list[tuple]:
    pad = timedelta(hours=ctx.cfg.annotation_buffer_hours)
    return [(a.start - pad, a.start, a.end, a.end + pad) for a in ctx.annotations]


def _periods(ctx) -> list[tuple]:
    return [(p.name, p.start_dt, p.end_dt) for p in ctx.periods]


def _band_q(cutoffs: list[Cutoffs]) -> dict[str, dict[str, float]]:
    return {c.model_id: c.quantiles for c in cutoffs}


def _floors(cutoffs: list[Cutoffs]) -> dict[str, float]:
    """Rarity cap per model: a percentile of 1.0 only says 'above every reference score'."""
    return {c.model_id: 1 / (c.reference_rows + 1) for c in cutoffs}


def write_report(ctx, manifest: dict, cutoffs: list[Cutoffs], diagnostics: dict, rob: dict | None) -> Path:
    d = ctx.exp_dir
    charts_dir = d / "charts"
    shutil.rmtree(charts_dir, ignore_errors=True)  # never mix charts of an earlier banding or format
    models = [c.model_id for c in cutoffs]
    sections = [
        _header(ctx, manifest),
        _overview(ctx, models, charts_dir),
        _zooms(ctx, models, cutoffs, charts_dir),
        _bands(ctx, models, cutoffs, diagnostics, charts_dir),
        _distributions(ctx, models, cutoffs, charts_dir),
        _entities(ctx, models, cutoffs, charts_dir),
        _candidates(ctx, models),
        _comparison(ctx, models, cutoffs, diagnostics, charts_dir),
        _annotation_section(ctx, diagnostics),
        _robustness(ctx, diagnostics, rob, charts_dir),
        _coverage(ctx, manifest),
        _limitations(ctx),
    ]
    path = d / REPORT
    path.write_text("\n\n".join(s for s in sections if s) + "\n", encoding="utf-8")
    return path


def _header(ctx, manifest: dict) -> str:
    rel = parquet(ctx.exp_dir / SCORES)
    s = rows(ctx.con, f"SELECT count(*) AS windows, count(DISTINCT src_ip) AS hosts, min(window_start)::VARCHAR AS "
                      f"first, max(window_end)::VARCHAR AS last, sum(flows) AS flows FROM {rel}")[0]
    per = {r["period"]: r for r in rows(ctx.con, f"SELECT period, count(*) AS windows, count(DISTINCT flow_date) AS "
                                                 f"days, count(DISTINCT src_ip) AS hosts FROM {rel} GROUP BY 1")}
    body = [[p.name, p.start, p.end, *[per.get(p.name, {}).get(k, 0) for k in ("windows", "days", "hosts")]]
            for p in ctx.periods]
    if "unassigned" in per:
        u = per["unassigned"]
        body.append(["unassigned (between/after periods)", "-", "-", u["windows"], u["days"], u["hosts"]])
    mock = ("\n\n> **Mock/synthetic input** (`allow_inside_repo: true`): results describe fixture or demo data, "
            "not any real network." if ctx.cfg.allow_inside_repo else "")
    code = manifest.get("code", {})
    return "\n".join([
        f"# Anomaly-ranking experiment `{ctx.experiment_id}`", "",
        DISCLAIMER.format(below=ctx.cfg.bands.below_label) + mock, "",
        (f"- Generated: {manifest['updated_at']} (UTC). Code: {code.get('commit') or 'unknown'}"
        f"{' (src has uncommitted changes)' if code.get('src_dirty') else ''}."),
        (f"- Input: {manifest['input']['files']} Parquet files, {s['flows']:,} flows in {s['windows']:,} host x "
        f"{ctx.cfg.window_minutes}-min windows, {s['hosts']:,} hosts, {s['first']} .. {s['last']} (UTC)."),
        f"- Model inputs ({len(ctx.features)}): {', '.join(f'`{f}`' for f in ctx.features)}.",
        (f"- Models: {', '.join(ctx.model_ids)}. Bands calibrated on: {manifest['bands']['reference']} "
        f"({manifest['bands']['settings']['mode']} mode)."), "",
        _table(["period", "UTC start", "UTC end (excl.)", "windows", "days with data", "hosts"], body),
        "", ("Scores on the training period are in-sample (exploratory only); validation calibrates the bands; test "
        "simulates later deployment."), "", "**Warnings**", "", "\n".join(f"- {w}" for w in ctx.warnings) or "- none"])


def _overview(ctx, models: list[str], out: Path) -> str:
    rel = parquet(ctx.exp_dir / DAILY)
    cols = ", ".join(f"{' + '.join(f'{m}_{b.lower()}' for b in BAND_ORDER)} AS {m}" for m in models)
    got = rows(ctx.con, f"SELECT flow_date, flows, {cols} FROM {rel} ORDER BY flow_date")
    days = [datetime.combine(r["flow_date"], datetime.min.time()) for r in got]
    charts.overview(out / f"overview.{ctx.cfg.report.chart_format}", days, [r["flows"] for r in got], {m: [r[m] for r in got] for m in models},
                    _intervals(ctx), _periods(ctx), "day")
    return "\n".join(["## 1. Year overview", "", f"![overview](charts/overview.{ctx.cfg.report.chart_format})", "",
                      ("Daily flow volume (top) and windows in any review band per model (bottom). Shaded ranges are "
                      "the supplied pentest dates (grey) and their buffers (hatched).")])


def _zoom_centres(ctx, models: list[str]) -> list[dict]:
    rel = parquet(ctx.exp_dir / SCORES)
    best = f"greatest({', '.join(f'{m}_pct' for m in models)})"
    cands = rows(ctx.con, f"SELECT window_start, src_ip, {best} AS pct FROM {rel} ORDER BY pct DESC, "
                          f"{' + '.join(f'{m}_raw' for m in models)} DESC, src_ip LIMIT 500")
    chosen, gap = [], timedelta(hours=ctx.cfg.report.zoom_hours)
    for c in cands:
        if all(abs(c["window_start"] - x["window_start"]) >= gap for x in chosen):
            chosen.append(c)
        if len(chosen) >= ctx.cfg.report.zoom_periods:
            break
    return chosen


def _zooms(ctx, models: list[str], cutoffs: list[Cutoffs], out: Path) -> str:
    rel = parquet(ctx.exp_dir / SCORES)
    half = timedelta(hours=ctx.cfg.report.zoom_hours / 2)
    lines = ["## 2. Highest-ranked periods (finer resolution)", "",
             (f"Up to {ctx.cfg.report.zoom_periods} periods of {ctx.cfg.report.zoom_hours} h centred on the highest "
             f"reference percentile across models, at least {ctx.cfg.report.zoom_hours} h apart. Bucket = one "
             f"{ctx.cfg.window_minutes}-minute window; each point is the highest-ranked host in that bucket.")]
    for i, c in enumerate(_zoom_centres(ctx, models), start=1):
        lo, hi = c["window_start"] - half, c["window_start"] + half
        got = rows(ctx.con, f"SELECT window_start, sum(flows) AS flows, "
                            f"{', '.join(f'max({m}_pct) AS {m}' for m in models)} FROM {rel} "
                            f"WHERE window_start >= TIMESTAMPTZ '{lo.isoformat()}' AND "
                            f"window_start < TIMESTAMPTZ '{hi.isoformat()}' GROUP BY 1 ORDER BY 1")
        near = overlaps(ctx.annotations, lo, hi, ctx.cfg.annotation_buffer_hours)
        title = f"#{i}: {c['window_start']:%Y-%m-%d %H:%M} UTC, top host {c['src_ip']}"
        charts.zoom(out / f"zoom_{i}.{ctx.cfg.report.chart_format}", title, [r["window_start"] for r in got], [r["flows"] for r in got],
                    {m: [r[m] for r in got] for m in models}, cutoffs[0].quantiles, _floors(cutoffs),
                    _intervals(ctx), f"{ctx.cfg.window_minutes} min")
        context = ", ".join(f"{o['name']} ({CATEGORY_LABELS[o['kind']]})" for o in near) or "no supplied interval"
        lines += ["", f"![zoom {i}](charts/zoom_{i}.{ctx.cfg.report.chart_format})", "",
                  (f"Top window: `{c['src_ip']}` at {c['window_start']:%Y-%m-%d %H:%M} UTC (max reference percentile "
                  f"{c['pct']:.5f}). Annotation context within the plotted range: {context}.")]
    return "\n".join(lines)


def _bands(ctx, models: list[str], cutoffs: list[Cutoffs], diagnostics: dict, out: Path) -> str:
    rel = parquet(ctx.exp_dir / SCORES)
    parts = ", ".join(f"count(*) FILTER (WHERE {m}_band = '{b}') AS {m}_{b}" for m in models for b in BAND_ORDER)
    bucket, step = time_bucket(ctx.periods)
    got = rows(ctx.con, f"SELECT date_trunc('{bucket}', window_start) AS wk, {parts} FROM {rel} "
                        "GROUP BY 1 ORDER BY 1")
    charts.bands_over_time(out / f"bands_over_time.{ctx.cfg.report.chart_format}", [r["wk"] for r in got],
                           {m: {b: [r[f"{m}_{b}"] for r in got] for b in BAND_ORDER} for m in models},
                           _intervals(ctx), bucket, step)
    charts.cutoff_curve(out / f"cutoff_curve.{ctx.cfg.report.chart_format}", diagnostics["cutoff_curve"], _band_q(cutoffs))
    vol = sorted((v for v in diagnostics["band_volume"] if v["band"] in BAND_ORDER),
                 key=lambda v: (v["model"], v["period"], BAND_ORDER.index(v["band"])))
    body = [[v["model"], v["period"], v["band"], v["windows"], v["per_day"]] for v in vol]
    return "\n".join([
        "## 3. Review bands over time and alert volume", "",
        f"![bands](charts/bands_over_time.{ctx.cfg.report.chart_format})", "", f"![cutoffs](charts/cutoff_curve.{ctx.cfg.report.chart_format})", "",
        ("Band cutoffs (raw-score thresholds from reference-period quantiles; change them with "
        "`netanomaly poc report --bands FILE` without retraining):"), "", bands_markdown(cutoffs), "",
        _table(["model", "period", "band", "windows", "per day"], body)])


def _histograms(ctx, m: str) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    rel = parquet(ctx.exp_dir / SCORES)
    lo, hi = ctx.con.execute(f"SELECT min({m}_raw), max({m}_raw) FROM {rel}").fetchone()
    width = (hi - lo) / HIST_BINS or 1.0
    edges = np.array([lo + i * width for i in range(HIST_BINS + 1)])
    got = rows(ctx.con, f"SELECT period, least(CAST(floor(({m}_raw - {lo!r}) / {width!r}) AS INTEGER), "
                        f"{HIST_BINS - 1}) AS b, count(*) AS n FROM {rel} GROUP BY 1, 2")
    out = {}
    for period in [p.name for p in ctx.periods]:
        counts = np.zeros(HIST_BINS)
        for r in got:
            if r["period"] == period:
                counts[r["b"]] = r["n"]
        if counts.sum():
            out[period] = (edges, counts)
    return out


def _distributions(ctx, models: list[str], cutoffs: list[Cutoffs], out: Path) -> str:
    charts.score_distributions(out / f"score_distribution.{ctx.cfg.report.chart_format}", {m: _histograms(ctx, m) for m in models},
                               {c.model_id: c.thresholds for c in cutoffs})
    return "\n".join(["## 4. Score distributions", "", f"![scores](charts/score_distribution.{ctx.cfg.report.chart_format})", "",
                      ("Score direction for both models: `raw = -score_samples(x)`, higher = more anomalous. A test "
                      "distribution shifted right of validation means more windows look unusual than during "
                      "calibration (drift or new activity), so the bands fire more often.")])


def _heatmap(ctx, m: str, out: Path, floor: float) -> str | None:
    rel, ent = parquet(ctx.exp_dir / SCORES), parquet(ctx.exp_dir / ENTITY)
    top = [r["src_ip"] for r in rows(ctx.con, f"SELECT src_ip FROM {ent} WHERE {m}_review > 0 ORDER BY {m}_review "
                                              f"DESC, {m}_max_pct DESC, src_ip LIMIT {ctx.cfg.report.heatmap_entities}")]
    if not top:
        return None
    bucket, step = time_bucket(ctx.periods)
    ctx.con.execute("CREATE OR REPLACE TEMP TABLE poc_heat_hosts (src_ip VARCHAR)")
    ctx.con.executemany("INSERT INTO poc_heat_hosts VALUES (?)", [[t] for t in top])
    got = rows(ctx.con, f"SELECT src_ip, date_trunc('{bucket}', window_start) AS col, max({m}_pct) AS pct "
                        f"FROM {rel} WHERE src_ip IN (SELECT src_ip FROM poc_heat_hosts) GROUP BY 1, 2")
    cols, c = [], min(r["col"] for r in got)
    while c <= max(r["col"] for r in got):
        cols.append(c)
        c += step
    index, pos = {c: j for j, c in enumerate(cols)}, {h: i for i, h in enumerate(top)}
    values = np.full((len(top), len(cols)), np.nan)
    for r in got:
        values[pos[r["src_ip"]], index[r["col"]]] = float(charts.rarity(r["pct"], floor))
    flags = [bool(overlaps(ctx.annotations, c, c + step, ctx.cfg.annotation_buffer_hours)) for c in cols]
    charts.heatmap(out / f"heatmap_{m}.{ctx.cfg.report.chart_format}", f"{m}: highest rarity per host and {bucket}", top, cols, values, flags,
                   bucket, float(charts.rarity(1.0, floor)))
    return f"![{m} heatmap](charts/heatmap_{m}.{ctx.cfg.report.chart_format})"


def _entities(ctx, models: list[str], cutoffs: list[Cutoffs], out: Path) -> str:
    ent = parquet(ctx.exp_dir / ENTITY)
    lines = ["## 5. Hosts x time", ""]
    for m in models:
        img = _heatmap(ctx, m, out, _floors(cutoffs)[m])
        lines += [img, ""] if img else [f"({m}: no review-band windows)", ""]
    body = rows(ctx.con, f"SELECT * FROM {ent} ORDER BY {' + '.join(f'{m}_review' for m in models)} DESC, src_ip "
                         "LIMIT 15")
    lines += ["Hosts with the most review-band windows (all periods):", "",
              _table(["host", "windows", "train windows", *[f"{m} review" for m in models],
                      *[f"{m} review inside ranges" for m in models], *[f"{m} max pct" for m in models]],
                     [[r["src_ip"], r["windows"], r["train_windows"], *[r[f"{m}_review"] for m in models],
                       *[r[f"{m}_review_inside"] for m in models], *[r[f"{m}_max_pct"] for m in models]]
                      for r in body])]
    return "\n".join(lines)


def _candidates(ctx, models: list[str]) -> str:
    rel = parquet(ctx.exp_dir / ALERTS)
    columns = {d[0] for d in ctx.con.execute(f"SELECT * FROM {rel} LIMIT 0").description}
    ctx_cols = [c for c in ("entity_train_median_flows", "entity_train_median_uniq_dst_ip") if c in columns]
    got = rows(ctx.con, f"SELECT * FROM {rel} ORDER BY alert_rank LIMIT {ctx.cfg.report.top_candidates}")
    body = []
    for r in got:
        trace = (f"{r['trace_flow_count']} flows; {len(r['trace_source_files'] or [])} file(s); first "
                 f"{Path(r['trace_rows'][0]['file']).name}#{r['trace_rows'][0]['row_index']}"
                 if r.get("trace_rows") else "not traced (beyond trace_top_n)")
        baseline = ("host not in training" if not r["entity_seen_in_train"] else
                    f"{r['entity_train_windows']} train windows; " + ", ".join(
                        f"{c.removeprefix('entity_train_median_')} median {r[c]:.3g}" for c in ctx_cols
                        if r[c] is not None))
        body.append([r["alert_rank"], r["src_ip"], f"{r['window_start']:%Y-%m-%d %H:%M}", r["period"],
                     CATEGORY_LABELS[r["annotation_category"]], r["review_band"], ",".join(r["models_in_review"]),
                     *[f"{r[f'{m}_pct']:.5f}" for m in models], r["flows"], r["top_deviations"],
                     ", ".join(r["beyond_train_range"] or []) or "-", baseline, trace])
    return "\n".join([
        "## 6. Ranked candidate windows", "",
        (f"Top {len(got)} review-band windows by highest reference percentile across models (all of them: "
        "`alerts.parquet`). *Largest deviations* are robust z-scores against training medians on the model's input "
        "scale - context for the analyst, not model attributions. Traces give source files and 0-based row indexes "
        "(`trace_rows` in alerts.parquet)."), "",
        _table(["#", "host", "window (UTC)", "period", "annotation context", "best band", "models",
                *[f"{m} pct" for m in models], "flows", "largest deviations", "beyond training range",
                "host baseline", "trace"], body),
        "", _beyond_note(ctx)])


def _beyond_note(ctx) -> str:
    rel = parquet(ctx.exp_dir / SCORES)
    n, low = ctx.con.execute(f"SELECT count(*) FILTER (WHERE len(beyond_train_range) > 0), count(*) FILTER (WHERE "
                             f"len(beyond_train_range) > 0 AND NOT ({' OR '.join(f'{m}_band IN ({FLAGGED})' for m in ctx.model_ids)})) "
                             f"FROM {rel} WHERE period <> 'train'").fetchone()
    return (f"*Beyond training range*: model inputs outside the training min/max. Isolation Forest cannot extrapolate "
            f"(a value far beyond the training range scores like the most extreme training value), so such windows can "
            f"rank lower than their novelty suggests. Outside training: {n:,} windows have at least one input beyond the "
            f"training range; {low:,} of them are in no review band - filter `scores.parquet` on "
            "`len(beyond_train_range) > 0` to review them.")


def _comparison(ctx, models: list[str], cutoffs: list[Cutoffs], diagnostics: dict, out: Path) -> str:
    if len(models) < 2 or "model_agreement" not in diagnostics:
        return ""
    a, b = models[:2]
    focus = diagnostics["disagreements"]["period"]
    rel = parquet(ctx.exp_dir / SCORES)
    total = ctx.con.execute(f"SELECT count(*) FROM {rel} WHERE period = '{focus}'").fetchone()[0]
    sample = ctx.con.execute(f"SELECT {a}_pct, {b}_pct FROM {rel} WHERE period = '{focus}' ORDER BY "
                             f"hash(src_ip, window_start) LIMIT {SCATTER_ROWS}").fetchnumpy()
    charts.model_scatter(out / f"model_comparison.{ctx.cfg.report.chart_format}", sample[f"{a}_pct"], sample[f"{b}_pct"], (a, b), total, focus,
                         (_floors(cutoffs)[a], _floors(cutoffs)[b]))
    d = ctx.cfg.diagnostics
    return "\n".join([
        f"## 7. {a} vs {b}", "", f"![comparison](charts/model_comparison.{ctx.cfg.report.chart_format})", "",
        (f"Spearman = rank correlation over all windows of the period; top-N Jaccard = overlap of each model's "
        f"{d.top_n} highest windows; daily top-{d.top_k_per_day} overlap = mean share of each day's top windows the "
        "models share. Both see the same inputs but define 'unusual' differently (isolation depth vs distance from "
        "a learned boundary), so moderate agreement is expected; windows both rank high are the strongest "
        "candidates for review, not confirmed findings."), "",
        _table(["period", "windows", "spearman", f"top-{d.top_n} jaccard", f"daily top-{d.top_k_per_day} overlap",
                "both in review", f"only {a}", f"only {b}"],
               [[r["period"], r["windows"], r["spearman"], r["top_n_jaccard"], r["daily_topk_overlap"],
                 r["both_review"], r[f"only_{a}_review"], r[f"only_{b}_review"]]
                for r in diagnostics["model_agreement"]]),
        "", f"Largest disagreements in the {focus} period (in one model's top 15, not the other's):", "",
        _table(["kind", "host", "window (UTC)", f"{a} pct", f"{b} pct", "annotation"],
               [[r["kind"], r["src_ip"], r["window_start"], r[f"{a}_pct"], r[f"{b}_pct"], r["annotation_category"]]
                for r in diagnostics["disagreements"]["rows"]])])


def _annotation_section(ctx, diagnostics: dict) -> str:
    if not ctx.annotations:
        return "## 8. Supplied pentest ranges\n\nNo annotation file configured (`annotations:`)."
    over = diagnostics["annotation_overlap"]
    b, k = ctx.cfg.annotation_buffer_hours, ctx.cfg.diagnostics.top_k_per_day
    main = [r for r in over if r["buffer_hours"] == b and r["period"] != "unassigned"]
    sens = _buffer_sensitivity(over)
    ann = [[a.name, a.start.isoformat(), a.end.isoformat(), a.source, a.confidence, a.notes,
            ", ".join(p.name for p in ctx.periods if overlaps([a], p.start_dt, p.end_dt, 0)) or "none"]
           for a in ctx.annotations]
    return "\n".join([
        "## 8. Supplied pentest ranges (weak annotations)", "",
        _table(["name", "start (UTC)", "end (UTC, excl.)", "source", "confidence", "notes", "periods touched"], ann),
        "", (f"Where review-band and daily top-{k} windows fall, versus all windows (buffer {b} h). *Ratio* = share "
        "among flagged windows / share among all windows; > 1 = over-represented. This describes co-occurrence "
        "with broad date ranges; it is not a detection rate, and any unusual activity that happens to coincide with "
        "the ranges would look the same."), "",
        _table(["model", "period", "category", "windows", "base share", "review share", "review ratio",
                f"top-{k} share", f"top-{k} ratio"],
               [[r["model"], r["period"], CATEGORY_LABELS[r["category"]], r["windows"], r["base_share"],
                 r["review_share"], r["review_ratio"], r["topk_share"], r["topk_ratio"]] for r in main]),
        "", ("Sensitivity to the buffer width, for windows *inside a range or its buffer* (a pattern that appears "
        "for one buffer only is fragile; a ratio that grows with a wider buffer suggests activity near, not in, "
        "the supplied dates):"), "",
        _table(["model", "period", "buffer h", "inside+buffer base share", "review share", "review ratio",
                f"top-{k} share", f"top-{k} ratio"],
               [[r["model"], r["period"], r["buffer_hours"], r["base_share"], r["review_share"], r["review_ratio"],
                 r["topk_share"], r["topk_ratio"]] for r in sens])])


def _buffer_sensitivity(over: list[dict]) -> list[dict]:
    """Combine inside + buffer shares per (model, period, buffer) for the validation and test periods."""
    out = []
    keys = sorted({(r["model"], r["period"], r["buffer_hours"]) for r in over
                   if r["period"] in ("validation", "test")})
    for model, period, buffer_hours in keys:
        sub = [r for r in over if (r["model"], r["period"], r["buffer_hours"]) == (model, period, buffer_hours)
               and r["category"] != OUTSIDE]
        base = sum(r["base_share"] or 0 for r in sub)
        review = sum(r["review_share"] or 0 for r in sub)
        top = sum(r["topk_share"] or 0 for r in sub)
        out.append({"model": model, "period": period, "buffer_hours": buffer_hours, "base_share": base,
                    "review_share": review, "review_ratio": review / base if base else None, "topk_share": top,
                    "topk_ratio": top / base if base else None})
    return out


def _cats(c: dict | None) -> str:
    return " / ".join(f"{c.get(k, 0):.2f}" for k in ("inside", "buffer", "outside")) if c else "-"


def _robustness(ctx, diagnostics: dict, rob: dict | None, out: Path) -> str:
    n = ctx.cfg.diagnostics.top_n
    lines = ["## 9. Robustness, contamination and drift", ""]
    if rob:
        seeds = _table(["model", "variant", "spearman", f"top-{n} jaccard"],
                       [[r["model"], r["variant"], r["spearman"], r["top_n_jaccard"]] for r in rob["seed_stability"]])
        variants = _table(["model", "variant", "train rows", "spearman vs primary", f"top-{n} jaccard",
                           "top-N inside/buffer/outside (primary)", "(variant)"],
                          [[r["model"], r["variant"], r["train_rows"], r["spearman"], r["top_n_jaccard"],
                            _cats(r.get("top_n_categories_primary")), _cats(r.get("top_n_categories_variant"))]
                           for r in rob["variants"]])
        lines += [(f"**Seed stability** ({rob['seed_period']} period): each model refitted with other seeds (the seed "
                  "also changes the training sample)."), "",
                  seeds if rob["seed_stability"] else "(diagnostics.seed_repeats = 0)", "",
                  (f"**Contamination sensitivity** ({rob['variant_period']} period): the same model trained on "
                  "alternative baselines. `excl_annotated` / `incl_annotated` flip whether windows in the supplied "
                  "ranges (and buffers) are training data; `trimNNN` refits without the primary model's own "
                  "highest-scoring training windows. Large changes mean the baseline choice matters: a baseline "
                  "containing attack-like activity can absorb it as normal."), "",
                  variants if rob["variants"] else "(no variants applicable)", ""]
    for m, conc in diagnostics["training_concentration"].items():
        lines += [(f"Hosts over-represented in {m}'s top 1 % of *training* windows (possible baseline contamination, "
                  "or unusual-but-normal hosts; worth a look):"), "",
                  _table(["host", "train windows", "top windows", "share of top", "share of windows"],
                         [[r["src_ip"], r["windows"], r["top_windows"], r["share_of_top"], r["share_of_windows"]]
                          for r in conc[:5]]), ""]
    for m, conc in diagnostics["concentration"].items():
        lines += [(f"Daily top-{ctx.cfg.diagnostics.top_k_per_day} concentration for {m} (few hosts dominating = one "
                  "noisy host may crowd out others):"), "",
                  _table(["period", "top windows", "distinct hosts", "top-5 host share", "most frequent host", "count"],
                         [[r["period"], r["top_windows"], r["distinct_entities"], r["top5_entity_share"],
                           r["most_frequent_entity"], r["most_frequent_count"]] for r in conc]), ""]
    drift = diagnostics["feature_drift"]
    if drift:
        feats = list(dict.fromkeys(r["feature"] for r in drift))
        weeks = sorted({r["week"] for r in drift})
        psi = np.full((len(feats), len(weeks)), np.nan)
        for r in drift:
            psi[feats.index(r["feature"]), weeks.index(r["week"])] = r["psi"]
        train_weeks = [ctx.train.start.isoformat() <= w < ctx.train.end.isoformat() for w in weeks]
        charts.drift(out / f"drift.{ctx.cfg.report.chart_format}", feats, weeks, psi, train_weeks,
                     time_bucket(ctx.periods)[0])
        lines += [f"![drift](charts/drift.{ctx.cfg.report.chart_format})"]
    return "\n".join(lines)


def _coverage(ctx, manifest: dict) -> str:
    m = manifest["field_mapping"]
    return "\n".join([
        "## 10. Coverage, field mapping and data quality", "",
        _table(["canonical", "source column", "type", "status", "conversion", "note"],
               [[f["name"], f["source"], f["source_type"], f["status"], f["conversion"], f["note"]]
                for f in m["fields"]]), "",
        (f"Unmapped source columns (never modelled): {len(m['unmapped_columns'])}. Full profile: "
        "`netanomaly poc profile` -> `profile/profile.md`."), "", "Assumptions:", "",
        *[f"- {a}" for a in m["assumptions"]], "",
        _table(["feature", "null share", "train null share", "train non-finite", "status", "handling"],
               [[q["feature"], q["null_share"], q["train_null_share"], q["train_nonfinite"], q["status"],
                 q["handling"]] for q in ctx.quality]), "",
        (f"Training filter: {'excluding' if ctx.cfg.split.exclude_annotated_from_train else 'including'} windows in "
        "supplied ranges. Training samples (bounded, spread over days):"), "",
        _table(["model", "train rows", "available", "sample rule"],
               [[x["model_id"], x["train_rows"], x["train_rows_available"], x["sample_rule"]]
                for x in manifest.get("models", [])])])


def _limitations(ctx) -> str:
    return "## 11. Limitations of this run\n\n- No reliable labels: nothing here measures detection quality. Pentest ranges are broad; overlap with them is descriptive.\n- A high rank means *unusual relative to the chosen training baseline*, which may be benign change (new services, backups, your own scanners) - and a baseline that contains attack-like activity can hide it.\n- Features describe one host and one window only (no history, no peer baselines, no periodicity), so slow, low-volume or beacon-like activity may rank low.\n- Field meanings are unvalidated (see mapping). Timestamps without offset were assumed UTC where noted.\n- Changing settings after looking at test results makes later test results optimistic.\n- Raw scores are only comparable within one fitted model; use percentiles/bands to compare models."
