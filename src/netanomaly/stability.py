"""Stability of the V3 Isolation Forest (V3-2, ADR-023): sample-size curve and seed stability.

Label-free. Every model is trained with the V3 time split (`iforest.fit`, training days only) and scores the held-out
score days; stability compares those rankings, never truth labels.

- Reference: the mean `anomaly_score` of the seed-stability models (seeds `model.seed + i`, `i < seeds`) trained on
  `min(train_sample_rows, training rows)` rows. Averaging removes most single-forest noise.
- Seed stability: every pair of those seed models (seed changes both the hash sample and the forest's randomness;
  with all training rows in the sample only the forest changes).
- Sample-size curve: for each size in `sample_sizes` (capped at the training rows, which is always included),
  `curve_seeds` models with seeds `model.seed + 1000 + j` (disjoint from the reference) against the reference.
- Agreement: Spearman rho over all scored rows (average ranks for ties) and top-K overlap per UTC day =
  |top-K(a) ∩ top-K(b)| / K with K = `alert_budget_per_day` (ties broken by `src_ip`, `window_start`).

Scores are written per model to Parquet under `work_dir` in Arrow batches, compared in DuckDB, then deleted.
"""

from __future__ import annotations

import json
import shutil
import statistics
from collections.abc import Collection
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path

import duckdb

from netanomaly import iforest
from netanomaly.config import ModelSettings, StabilitySettings
from netanomaly.db import sql_literal

REPORT_DIR = "stability"
REFERENCE = "reference"
CURVE_SEED_OFFSET = 1000
SYNTHETIC_NOTE = ("SYNTHETIC DATA — stability of rankings on held-out synthetic days, not detection performance and "
                  "not a real-world estimate (ADR-012).")


@dataclass(frozen=True)
class Variant:
    name: str
    group: str  # "seed" (reference ensemble, seed stability) or "curve" (sample-size curve)
    sample_rows: int
    seed: int


def plan_variants(model: ModelSettings, st: StabilitySettings, train_rows: int) -> list[Variant]:
    size = min(model.train_sample_rows, train_rows)
    seeds = [Variant(f"seed-{model.seed + i}", "seed", size, model.seed + i) for i in range(st.seeds)]
    sizes = sorted({min(n, train_rows) for n in st.sample_sizes} | {train_rows})
    curve = [Variant(f"n{n}-seed-{model.seed + CURVE_SEED_OFFSET + j}", "curve", n, model.seed + CURVE_SEED_OFFSET + j)
             for n in sizes for j in range(st.curve_seeds)]
    return seeds + curve


def _score_variants(con: duckdb.DuckDBPyConnection, features_dir: Path, features: list[str], log1p: Collection[str],
                    split: iforest.TimeSplit, model: ModelSettings, variants: list[Variant], work_dir: Path,
                    batch_rows: int, registry_version: int) -> dict[str, int]:
    """Fit and score every variant; returns the rows each was trained on."""
    trained = {}
    for v in variants:
        fitted, manifest = iforest.fit(con, features_dir, features, log1p, split, model, version=v.name,
                                       registry_version=registry_version, sample_rows=v.sample_rows, seed=v.seed)
        iforest.score(con, features_dir, fitted, manifest, work_dir / f"{v.name}.parquet", batch_rows)
        trained[v.name] = manifest.train_rows
    return trained


def rank_tables(con: duckdb.DuckDBPyConnection, scores_glob: str, seed_names: list[str], top_k: int) -> None:
    """TEMP TABLE `stab_ranked`: per variant (and the seed-mean reference) average rank `r` and top-K flag `top`."""
    names = ", ".join(sql_literal(n) for n in seed_names)
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE stab_ranked AS
WITH s AS (
  SELECT model_version AS variant, src_ip, window_start, flow_date, anomaly_score AS score
  FROM read_parquet({sql_literal(scores_glob)})
),
scores AS (
  SELECT * FROM s
  UNION ALL
  SELECT {sql_literal(REFERENCE)}, src_ip, window_start, flow_date, avg(score ORDER BY variant)  -- fixed sum order
  FROM s WHERE variant IN ({names}) GROUP BY ALL
)
SELECT variant, src_ip, window_start, flow_date,
       rank() OVER (PARTITION BY variant ORDER BY score)
         + (count(*) OVER (PARTITION BY variant, score) - 1) / 2.0 AS r,
       row_number() OVER (PARTITION BY variant, flow_date ORDER BY score DESC, src_ip, window_start) <= {int(top_k)}
         AS top
FROM scores""")


def agreement(con: duckdb.DuckDBPyConnection, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
    """Spearman rho and per-day top-K overlap for each (a, b) pair of variants in TEMP TABLE `stab_ranked`."""
    con.execute("CREATE OR REPLACE TEMP TABLE stab_pairs (a VARCHAR, b VARCHAR)")
    con.executemany("INSERT INTO stab_pairs VALUES (?, ?)", pairs)
    rows = con.execute("""
WITH joined AS (
  SELECT p.a, p.b, x.flow_date, x.r AS ra, y.r AS rb, x.top AS ta, y.top AS tb
  FROM stab_pairs p
  JOIN stab_ranked x ON x.variant = p.a
  JOIN stab_ranked y ON y.variant = p.b AND y.src_ip = x.src_ip AND y.window_start = x.window_start
),
per_day AS (
  SELECT a, b, flow_date, count(*) FILTER (WHERE ta AND tb) / count(*) FILTER (WHERE ta) AS overlap
  FROM joined GROUP BY ALL
),
rho AS (SELECT a, b, corr(ra, rb) AS spearman FROM joined GROUP BY ALL)
SELECT a, b, any_value(spearman), list(overlap ORDER BY flow_date)
FROM rho JOIN per_day USING (a, b)
GROUP BY a, b""").fetchall()
    return {(a, b): {"spearman": rho, "topk_overlap_by_day": days} for a, b, rho, days in rows}


def _round(x: float) -> float:
    return round(float(x), 6)


def _summary(values: list[float]) -> dict:
    return {"min": _round(min(values)), "median": _round(statistics.median(values)), "max": _round(max(values))}


def _summarise(result: dict, pairs: list[tuple[str, str]]) -> dict:
    overlaps = [result[p]["topk_overlap_by_day"] for p in pairs]
    return {"spearman": _summary([result[p]["spearman"] for p in pairs]),
            "topk_overlap_mean": _summary([statistics.fmean(o) for o in overlaps]),
            "topk_overlap_worst_day": _round(min(min(o) for o in overlaps))}


def build_report(con: duckdb.DuckDBPyConnection, features_dir: Path, features: list[str], log1p: Collection[str],
                 split: iforest.TimeSplit, model: ModelSettings, st: StabilitySettings, top_k: int, work_dir: Path,
                 batch_rows: int, registry_version: int) -> dict:
    (train_rows,) = con.execute(
        f"SELECT count(*) FROM read_parquet({sql_literal((features_dir / '**' / '*.parquet').as_posix())}, "
        f"hive_partitioning = true) WHERE flow_date <= DATE {sql_literal(split.train_end.isoformat())}").fetchone()
    variants = plan_variants(model, st, train_rows)
    seeds = [v for v in variants if v.group == "seed"]
    curve_variants = [v for v in variants if v.group == "curve"]
    seed_pairs = list(combinations([v.name for v in seeds], 2))
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True)
    try:
        trained = _score_variants(con, features_dir, features, log1p, split, model, variants, work_dir, batch_rows,
                                  registry_version)
        rank_tables(con, (work_dir / "*.parquet").as_posix(), [v.name for v in seeds], top_k)
        (score_rows,) = con.execute(
            f"SELECT count(*) FROM stab_ranked WHERE variant = {sql_literal(REFERENCE)}").fetchone()
        result = agreement(con, seed_pairs + [(v.name, REFERENCE) for v in curve_variants])
    finally:
        con.execute("DROP TABLE IF EXISTS stab_ranked; DROP TABLE IF EXISTS stab_pairs")
        shutil.rmtree(work_dir, ignore_errors=True)

    curve = []
    for n in sorted({v.sample_rows for v in curve_variants}):
        members = [v for v in curve_variants if v.sample_rows == n]
        curve.append({"sample_rows": n, "train_rows": trained[members[0].name], "seeds": [v.seed for v in members],
                      **_summarise(result, [(v.name, REFERENCE) for v in members])})
    return {
        "note": SYNTHETIC_NOTE,
        "labels_used": False,
        "split": {"train_fraction": split.train_fraction, "train_dates": [d.isoformat() for d in split.train_dates],
                  "score_dates": [d.isoformat() for d in split.score_dates]},
        "features": list(features),
        "model_settings": model.model_dump(),
        "train_period_rows": train_rows,
        "score_rows": score_rows,
        "top_k": top_k,
        "reference": {"definition": "mean anomaly_score of the seed-stability models", "seeds": [v.seed for v in seeds],
                      "train_rows": trained[seeds[0].name]},
        "seed_stability": {"train_rows": trained[seeds[0].name], "seeds": [v.seed for v in seeds],
                           "pairs": len(seed_pairs), **_summarise(result, seed_pairs)},
        "sample_size_curve": curve,
    }


def to_markdown(report: dict) -> str:
    s, ss, k = report["split"], report["seed_stability"], report["top_k"]

    def rng(d: dict) -> str:
        return f"{d['median']:.4f} ({d['min']:.4f}–{d['max']:.4f})"

    ref = report["reference"]
    lines = [
        "# Isolation Forest stability (V3)",
        "",
        f"**{report['note']}**",
        "",
        "<!-- GENERATED by `uv run netanomaly stability`. -->",
        "",
        (f"- Split: train {s['train_dates'][0]}..{s['train_dates'][-1]} ({len(s['train_dates'])} days, "
         f"{report['train_period_rows']:,} rows); scored {s['score_dates'][0]}..{s['score_dates'][-1]} "
         f"({len(s['score_dates'])} days, {report['score_rows']:,} rows). Train fraction {s['train_fraction']}."),
        f"- Features ({len(report['features'])}): {', '.join(f'`{f}`' for f in report['features'])}.",
        (f"- Forest: {report['model_settings']['n_estimators']} trees, max_samples "
         f"{report['model_settings']['max_samples']}. Labels used: no."),
        (f"- Reference: {ref['definition']} (seeds {ref['seeds'][0]}–{ref['seeds'][-1]}, "
         f"{ref['train_rows']:,} training rows each)."),
        (f"- Agreement: Spearman rho over all scored rows; top-{k} overlap = share of a day's top-{k} host-windows "
         "shared with the other ranking (mean over scored days; worst single day separately). "
         "Cells: median (min–max) over seeds or pairs."),
        "",
        "## Seed stability",
        "",
        f"{ss['pairs']} pairs of {len(ss['seeds'])} seeds, {ss['train_rows']:,} training rows each.",
        "",
        f"| Spearman rho | top-{k} overlap (mean over days) | worst day |",
        "|---|---|---:|",
        f"| {rng(ss['spearman'])} | {rng(ss['topk_overlap_mean'])} | {ss['topk_overlap_worst_day']:.2f} |",
        "",
        "## Sample-size curve (vs reference)",
        "",
        f"| training rows | seeds | Spearman rho | top-{k} overlap (mean over days) | worst day |",
        "|---:|---:|---|---|---:|",
        *(f"| {c['train_rows']:,} | {len(c['seeds'])} | {rng(c['spearman'])} | {rng(c['topk_overlap_mean'])} "
          f"| {c['topk_overlap_worst_day']:.2f} |" for c in report["sample_size_curve"]),
        "",
    ]
    return "\n".join(lines)


def write_report(report: dict, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path, md_path = out_dir / "stability.json", out_dir / "stability.md"
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(to_markdown(report), encoding="utf-8")
    return json_path, md_path
