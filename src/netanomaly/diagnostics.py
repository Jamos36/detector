"""Unsupervised and weak-annotation diagnostics, computed in DuckDB over the scored window table.

None of these is accuracy, precision, recall or a false-positive rate: there are no reliable per-window labels.
Annotation figures compare *where* high-ranked windows fall (inside / buffer / outside the supplied date ranges)
with where all windows fall (the base rate). A ratio above 1 means top-ranked windows are over-represented in the
supplied ranges; it does not show that they are attacks, and a broad range makes it weak evidence either way.
"""

from __future__ import annotations

from datetime import timedelta

import duckdb
import numpy as np

from netanomaly.annotations import BUFFER, INSIDE, OUTSIDE, Annotation, category_sql
from netanomaly.config import BAND_ORDER
from netanomaly.featureset import FeatureTable
from netanomaly.splits import Period

QUANTILES = (0.5, 0.9, 0.99, 0.999)
CURVE_QUANTILES = (0.9, 0.95, 0.975, 0.99, 0.995, 0.999, 0.9995)
PSI_BINS = 10
PSI_FLOOR = 1e-4
FLAGGED = ", ".join(repr(b) for b in BAND_ORDER)


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def score_distribution(con: duckdb.DuckDBPyConnection, rel: str, models: list[str]) -> list[dict]:
    out = []
    qs = "[" + ", ".join(map(str, QUANTILES)) + "]"
    for m in models:
        for r in rows(con, f"SELECT period, count(*) AS n, min({m}_raw) AS min, quantile_cont({m}_raw, {qs}) AS q, "
                           f"max({m}_raw) AS max FROM {rel} GROUP BY period ORDER BY period"):
            out.append({"model": m, "period": r["period"], "n": r["n"], "min": r["min"], "max": r["max"],
                        **{f"q{q}": v for q, v in zip(QUANTILES, r["q"], strict=True)}})
    return out


def band_volume(con: duckdb.DuckDBPyConnection, rel: str, models: list[str]) -> list[dict]:
    """Windows per band, and per day of the period, by model and period."""
    out = []
    for m in models:
        out += [{"model": m, **r} for r in rows(con, f"""
WITH days AS (SELECT period, count(DISTINCT flow_date) AS d FROM {rel} GROUP BY period),
b AS (SELECT period, {m}_band AS band, count(*) AS windows FROM {rel} GROUP BY ALL)
SELECT b.period, band, windows, windows / d::DOUBLE AS per_day FROM b JOIN days USING (period)
ORDER BY b.period, band""")]
    return out


def cutoff_curve(con: duckdb.DuckDBPyConnection, rel: str, model: str, reference: np.ndarray) -> list[dict]:
    """Mean windows per day at or above a reference quantile, per period: how alert volume moves with the cutoff."""
    out = []
    for q in CURVE_QUANTILES:
        t = float(np.quantile(reference, q, method="higher"))
        for r in rows(con, f"SELECT period, count(*) FILTER (WHERE {model}_raw >= {t!r}) / "
                           f"count(DISTINCT flow_date)::DOUBLE AS per_day FROM {rel} GROUP BY period ORDER BY 1"):
            out.append({"model": model, "quantile": q, "threshold": t, **r})
    return out


def topk_sql(rel: str, model: str, k: int, where: str = "TRUE") -> str:
    """Top-k windows per UTC day by raw score; ties broken by (src_ip, window_start) so it is deterministic."""
    return (f"SELECT * FROM {rel} WHERE {where} QUALIFY row_number() OVER (PARTITION BY flow_date "
            f"ORDER BY {model}_raw DESC, src_ip, window_start) <= {int(k)}")


def concentration(con: duckdb.DuckDBPyConnection, rel: str, model: str, k: int) -> list[dict]:
    """How concentrated each period's daily top-k is on few hosts (a few noisy hosts dominating = low diversity)."""
    return rows(con, f"""
WITH top AS ({topk_sql(rel, model, k)}),
per_entity AS (SELECT period, src_ip, count(*) AS n FROM top GROUP BY ALL),
ranked AS (SELECT *, row_number() OVER (PARTITION BY period ORDER BY n DESC, src_ip) AS r FROM per_entity)
SELECT period, sum(n) AS top_windows, count(*) AS distinct_entities,
  sum(n) FILTER (WHERE r <= 5) / sum(n)::DOUBLE AS top5_entity_share,
  arg_min(src_ip, r) AS most_frequent_entity, max(n) AS most_frequent_count
FROM ranked GROUP BY period ORDER BY period""")


def annotation_overlap(con: duckdb.DuckDBPyConnection, rel: str, models: list[str], annotations: list[Annotation],
                       buffers: list[int], k: int) -> list[dict]:
    """Share of review-band and daily-top-k windows by annotation category vs the base share of all windows."""
    if not annotations:
        return []
    out = []
    for b in buffers:
        cat = category_sql("window_start", "window_end", annotations, b)
        for m in models:
            counts = rows(con, f"""
WITH c AS (SELECT period, flow_date, src_ip, window_start, {m}_raw, {m}_band IN ({FLAGGED}) AS flagged, {cat} AS cat
           FROM {rel}),
top AS ({topk_sql('c', m, k)}),
base AS (SELECT period, cat, count(*) AS windows, count(*) FILTER (WHERE flagged) AS review_windows FROM c
         GROUP BY ALL),
tk AS (SELECT period, cat, count(*) AS topk_windows FROM top GROUP BY ALL)
SELECT base.*, coalesce(tk.topk_windows, 0) AS topk_windows FROM base LEFT JOIN tk USING (period, cat)""")
            out += _shares(counts, m, b)
    return out


def _shares(counts: list[dict], model: str, buffer_hours: int) -> list[dict]:
    out = []
    for period in sorted({r["period"] for r in counts}):
        sub = [r for r in counts if r["period"] == period]
        tot = {key: sum(r[key] for r in sub) for key in ("windows", "review_windows", "topk_windows")}
        for r in sub:
            base = r["windows"] / tot["windows"] if tot["windows"] else None
            rev = r["review_windows"] / tot["review_windows"] if tot["review_windows"] else None
            top = r["topk_windows"] / tot["topk_windows"] if tot["topk_windows"] else None
            out.append({"model": model, "buffer_hours": buffer_hours, "period": period, "category": r["cat"],
                        "windows": r["windows"], "base_share": base, "review_share": rev,
                        "review_ratio": rev / base if rev is not None and base else None,
                        "topk_share": top, "topk_ratio": top / base if top is not None and base else None})
    return out


def model_agreement(con: duckdb.DuckDBPyConnection, rel: str, a: str, b: str, top_n: int, k: int) -> list[dict]:
    """Rank agreement of two models per period: Spearman (Pearson of ranks), overall top-N Jaccard, mean daily
    top-k overlap, and review-band agreement counts."""
    return rows(con, f"""
WITH r AS (
  SELECT period, flow_date, {a}_band IN ({FLAGGED}) AS fa, {b}_band IN ({FLAGGED}) AS fb,
    rank() OVER (PARTITION BY period ORDER BY {a}_raw) AS ra, rank() OVER (PARTITION BY period ORDER BY {b}_raw) AS rb,
    row_number() OVER (PARTITION BY period ORDER BY {a}_raw DESC, src_ip, window_start) AS na,
    row_number() OVER (PARTITION BY period ORDER BY {b}_raw DESC, src_ip, window_start) AS nb,
    row_number() OVER (PARTITION BY period, flow_date ORDER BY {a}_raw DESC, src_ip, window_start) AS da,
    row_number() OVER (PARTITION BY period, flow_date ORDER BY {b}_raw DESC, src_ip, window_start) AS db
  FROM {rel}),
daily AS (SELECT period, avg(o) AS daily_topk_overlap FROM (
  SELECT period, flow_date, count(*) FILTER (WHERE da <= {k} AND db <= {k}) / least({k}, count(*))::DOUBLE AS o
  FROM r GROUP BY ALL) GROUP BY period),
agg AS (SELECT period, count(*) AS windows, corr(ra, rb) AS spearman,
  count(*) FILTER (WHERE na <= {top_n} AND nb <= {top_n})::DOUBLE /
    nullif(count(*) FILTER (WHERE na <= {top_n} OR nb <= {top_n}), 0) AS top_n_jaccard,
  count(*) FILTER (WHERE fa AND fb) AS both_review, count(*) FILTER (WHERE fa AND NOT fb) AS only_{a}_review,
  count(*) FILTER (WHERE fb AND NOT fa) AS only_{b}_review
  FROM r GROUP BY period)
SELECT agg.*, daily.daily_topk_overlap FROM agg LEFT JOIN daily USING (period) ORDER BY period""")


def disagreements(con: duckdb.DuckDBPyConnection, rel: str, a: str, b: str, period: str, n: int) -> list[dict]:
    """Windows in one model's top-n of the period but not in the other's (percentiles shown for both)."""
    return rows(con, f"""
WITH r AS (SELECT src_ip, window_start, {a}_pct, {b}_pct, annotation_category,
  row_number() OVER (ORDER BY {a}_raw DESC, src_ip, window_start) AS na,
  row_number() OVER (ORDER BY {b}_raw DESC, src_ip, window_start) AS nb FROM {rel} WHERE period = '{period}')
SELECT CASE WHEN na <= {n} THEN 'top in {a} only' ELSE 'top in {b} only' END AS kind, src_ip,
  window_start::VARCHAR AS window_start, {a}_pct, {b}_pct, annotation_category
FROM r WHERE (na <= {n}) <> (nb <= {n}) ORDER BY least(na, nb) LIMIT {2 * n}""")


def rank_agreement(con: duckdb.DuckDBPyConnection, rel_a: str, col_a: str, rel_b: str, col_b: str,
                   top_n: int) -> dict:
    """Spearman and top-N Jaccard between two score columns on the same rows (joined on src_ip, window_start)."""
    return rows(con, f"""
WITH j AS (SELECT a.{col_a} AS x, b.{col_b} AS y, src_ip, window_start FROM {rel_a} a
           JOIN {rel_b} b USING (src_ip, window_start)),
r AS (SELECT rank() OVER (ORDER BY x) AS rx, rank() OVER (ORDER BY y) AS ry,
        row_number() OVER (ORDER BY x DESC, src_ip, window_start) AS nx,
        row_number() OVER (ORDER BY y DESC, src_ip, window_start) AS ny FROM j)
SELECT count(*) AS rows, corr(rx, ry) AS spearman,
  count(*) FILTER (WHERE nx <= {top_n} AND ny <= {top_n})::DOUBLE /
    nullif(count(*) FILTER (WHERE nx <= {top_n} OR ny <= {top_n}), 0) AS top_n_jaccard FROM r""")[0]


def top_categories(con: duckdb.DuckDBPyConnection, rel: str, col: str, top_n: int, annotations: list[Annotation],
                   buffer_hours: int) -> dict:
    """Annotation categories of the overall top-N windows of a score column (descriptive shares)."""
    if not annotations:
        return {}
    cat = category_sql("window_start", "window_end", annotations, buffer_hours)
    got = rows(con, f"SELECT {cat} AS cat, count(*) AS n FROM (SELECT * FROM {rel} ORDER BY {col} DESC, src_ip, "
                    f"window_start LIMIT {int(top_n)}) GROUP BY 1")
    total = sum(r["n"] for r in got) or 1
    return {c: next((r["n"] for r in got if r["cat"] == c), 0) / total for c in (INSIDE, BUFFER, OUTSIDE)}


def training_concentration(con: duckdb.DuckDBPyConnection, rel: str, model: str, top_share: float = 0.01,
                           limit: int = 10) -> list[dict]:
    """Hosts over-represented among the highest-scoring TRAINING windows. A baseline where a few hosts dominate its
    own top scores may contain activity the model partly absorbed as normal (contamination check, inspired by the
    per-client divergence of Kamiguchi & Nishio 2026). A pointer for review, not a finding."""
    return rows(con, f"""
WITH t AS (SELECT src_ip, {model}_raw AS s FROM {rel} WHERE period = 'train'),
cut AS (SELECT quantile_disc(s, {1 - top_share}) AS c, count(*) AS n FROM t),
per AS (SELECT src_ip, count(*) AS windows, count(*) FILTER (WHERE s >= (SELECT c FROM cut)) AS top_windows FROM t
        GROUP BY src_ip)
SELECT src_ip, windows, top_windows,
  top_windows / nullif((SELECT sum(top_windows) FROM per), 0)::DOUBLE AS share_of_top,
  windows / (SELECT n FROM cut)::DOUBLE AS share_of_windows
FROM per WHERE top_windows > 0 ORDER BY top_windows DESC, src_ip LIMIT {int(limit)}""")


def _bin_sql(feature: str, edges: list[float]) -> str:
    whens = " ".join(f"WHEN {feature} <= {e!r} THEN {i}" for i, e in enumerate(edges))
    return f"CASE WHEN {feature} IS NULL OR NOT isfinite({feature}::DOUBLE) THEN -1 {whens} ELSE {len(edges)} END"


def time_bucket(periods: list[Period]) -> tuple[str, timedelta]:
    """Chart/drift bucket for the analysed span: UTC days up to 120 days, else ISO weeks."""
    span = (periods[-1].end - periods[0].start).days
    return ("day", timedelta(days=1)) if span <= 120 else ("week", timedelta(days=7))


def feature_drift(con: duckdb.DuckDBPyConnection, table: FeatureTable, features: list[str], train: Period,
                  bucket: str = "week") -> list[dict]:
    """PSI per day or week of each model input against the training distribution (decile bins of training values plus a
    NULL bin; shares floored at 1e-4). A drift indicator for the baseline, in the spirit of the drift gate of
    Sheela & Dey 2026: it says the traffic mix moved away from training, not that the change is malicious."""
    out = []
    rel = table.relation()
    probs = ", ".join(str(i / PSI_BINS) for i in range(1, PSI_BINS))
    for f in features:
        edges = con.execute(f"SELECT quantile_cont({f}::DOUBLE, [{probs}]) FROM {rel} "
                            f"WHERE {train.where()} AND isfinite({f}::DOUBLE)").fetchone()[0]
        if not edges:
            continue
        p = f"greatest(coalesce(r.p, 0), {PSI_FLOOR})"
        q = f"greatest(coalesce(c.q, 0), {PSI_FLOOR})"
        got = con.execute(f"""
WITH b AS (SELECT date_trunc('{bucket}', window_start)::DATE AS week, {train.where()} AS is_train,
             {_bin_sql(f, sorted(set(edges)))} AS bin FROM {rel}),
ref AS (SELECT bin, count(*) / sum(count(*)) OVER () AS p FROM b WHERE is_train GROUP BY bin),
cur AS (SELECT week, bin, count(*) / sum(count(*)) OVER (PARTITION BY week) AS q FROM b GROUP BY week, bin),
grid AS (SELECT week, bin FROM (SELECT DISTINCT week FROM b) CROSS JOIN (SELECT DISTINCT bin FROM b))
SELECT g.week::VARCHAR, sum(({q} - {p}) * ln({q} / {p}))
FROM grid g LEFT JOIN cur c USING (week, bin) LEFT JOIN ref r USING (bin) GROUP BY 1 ORDER BY 1""").fetchall()
        out += [{"feature": f, "week": w, "psi": psi} for w, psi in got]
    return out
