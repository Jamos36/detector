"""Machine-readable experiment outputs (Parquet), all built in DuckDB:

- scores.parquet: every host-window with trace columns, period, annotation category/names, all computed feature
  values, and per model `<m>_raw` (higher = more anomalous), `<m>_pct` (reference percentile) and `<m>_band`.
- alerts.parquet: windows in a review band of at least one model, with the host's training-period context, the
  largest deviations from training medians, and for the top `trace_top_n` a trace to source files / row indexes.
- daily_summary.parquet, entity_summary.parquet: aggregates used by the report.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from netanomaly.db import sql_literal
from netanomaly.poc.annotations import Annotation, category_sql, names_sql
from netanomaly.poc.bands import Cutoffs, band_sql
from netanomaly.poc.config import BAND_ORDER
from netanomaly.poc.featureset import FEATURE_BY_NAME, FeatureTable
from netanomaly.poc.source import Source
from netanomaly.poc.splits import Period, period_case_sql

SCORES, ALERTS, DAILY, ENTITY = "scores.parquet", "alerts.parquet", "daily_summary.parquet", "entity_summary.parquet"
FLAGGED = ", ".join(repr(b) for b in BAND_ORDER)
CONTEXT_FEATURES = ("flows", "bytes_total", "uniq_dst_ip", "uniq_dst_port")


def parquet(path: Path) -> str:
    return f"read_parquet({sql_literal(path)})"


def _copy(con: duckdb.DuckDBPyConnection, sql: str, out: Path) -> int:
    out.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO {sql_literal(out)} (FORMAT parquet, COMPRESSION zstd)")
    return con.execute(f"SELECT count(*) FROM {parquet(out)}").fetchone()[0]


def beyond_range_sql(features: list[str]) -> tuple[str, str]:
    """(training min/max select list, per-window list of model inputs outside the training range).

    Isolation Forest cannot extrapolate: its split thresholds lie inside the training range, so a value far beyond
    it scores like the most extreme training value. This flag makes such windows visible regardless of the score.
    """
    stats = ", ".join(f"min({f}) AS min_{f}, max({f}) AS max_{f}" for f in features)
    flags = ", ".join(f"CASE WHEN f.{f} > tr.max_{f} OR f.{f} < tr.min_{f} THEN '{f}' END" for f in features)
    return stats, f"list_filter([{flags}], x -> x IS NOT NULL)"


def write_scores(con: duckdb.DuckDBPyConnection, table: FeatureTable, scores_raw: Path, periods: list[Period],
                 annotations: list[Annotation], buffer_hours: int, cutoffs: list[Cutoffs],
                 grids: dict[str, list[tuple[float, float]]], out: Path, model_features: list[str],
                 train_where: str) -> int:
    joins, cols = [], []
    for c in cutoffs:
        m = c.model_id
        score, pct = zip(*grids[m], strict=True)
        con.register(f"grid_{m}", pa.table({"score": list(score), "pct": list(pct)}))
        joins.append(f"ASOF LEFT JOIN grid_{m} g_{m} ON s.{m}_raw >= g_{m}.score")
        cols += [f"s.{m}_raw", f"coalesce(g_{m}.pct, 0.0) AS {m}_pct", f"{band_sql(f's.{m}_raw', c)} AS {m}_band"]
    feats = ", ".join(f"f.{x}" for x in table.features)
    stats, beyond = beyond_range_sql(model_features)
    sql = f"""
WITH tr AS (SELECT {stats} FROM {table.relation()} WHERE {train_where})
SELECT f.src_ip, f.window_start, f.window_end, f.flow_date, {period_case_sql(periods, 'f.window_start')} AS period,
  {category_sql('f.window_start', 'f.window_end', annotations, buffer_hours)} AS annotation_category,
  {names_sql('f.window_start', 'f.window_end', annotations, buffer_hours)} AS annotation_names,
  f.first_flow_start, f.last_flow_start, f.n_source_files, {feats}, {beyond} AS beyond_train_range, {', '.join(cols)}
FROM {table.relation()} f JOIN {parquet(scores_raw)} s USING (src_ip, window_start) CROSS JOIN tr
{' '.join(joins)}
ORDER BY f.window_start, f.src_ip"""
    return _copy(con, sql, out)


def _transformed(feature: str, col: str) -> str:
    return f"ln(1 + greatest({col}, 0))" if FEATURE_BY_NAME[feature].log1p else f"{col}::DOUBLE"


def _training_stats_sql(scores: str, features: list[str]) -> str:
    parts = []
    for f in features:
        t = _transformed(f, f)
        parts += [f"median({t}) AS med_{f}",
                  (f"coalesce(nullif((quantile_cont({t}, 0.75) - quantile_cont({t}, 0.25)) / 1.349, 0), "
                  f"nullif(stddev_pop({t}), 0)) AS scale_{f}")]
    return f"SELECT {', '.join(parts)} FROM {scores} WHERE period = 'train'"


def _deviation_sql(features: list[str]) -> str:
    """Top-3 |robust z| of the window's features against training medians, on the model's input scale."""
    def z(f: str) -> str:
        return f"(({_transformed(f, 's.' + f)} - t.med_{f}) / t.scale_{f})"

    items = ", ".join(f"struct_pack(a := abs({z(f)}), f := '{f}', z := {z(f)}, v := s.{f}::DOUBLE)"
                      for f in features)
    label = ("x.f || '=' || round(x.v, 2)::VARCHAR || ' (z ' || CASE WHEN x.z >= 0 THEN '+' ELSE '' END || "
             "round(x.z, 1)::VARCHAR || ')'")
    return (f"array_to_string(list_transform(list_slice(list_reverse_sort(list_filter([{items}], "
            f"x -> x.a IS NOT NULL AND isfinite(x.a))), 1, 3), x -> {label}), '; ')")


def write_alerts(con: duckdb.DuckDBPyConnection, scores: Path, source: Source, models: list[str],
                 model_features: list[str], all_features: list[str], out: Path, trace_top_n: int,
                 trace_rows: int) -> int:
    rel = parquet(scores)
    ctx = [f for f in CONTEXT_FEATURES if f in all_features]
    entity = "".join(f", median({f}) AS train_median_{f}" for f in ctx)
    entity_cols = "".join(f"e.train_median_{f} AS entity_train_median_{f}, " for f in ctx)
    flagged_any = " OR ".join(f"{m}_band IN ({FLAGGED})" for m in models)
    best = "CASE " + " ".join(f"WHEN {' OR '.join(f'{m}_band = {b!r}' for m in models)} THEN '{b}'"
                              for b in BAND_ORDER) + " END"
    in_review = ("list_filter([" + ", ".join(f"CASE WHEN {m}_band IN ({FLAGGED}) THEN '{m}' END" for m in models)
                 + "], x -> x IS NOT NULL)")
    max_pct = f"greatest({', '.join(f'{m}_pct' for m in models)})"
    pct_sum = " + ".join(f"{m}_pct" for m in models)
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE poc_alerts AS
WITH t AS ({_training_stats_sql(rel, model_features)}),
e AS (SELECT src_ip, count(*) AS train_windows{entity} FROM {rel} WHERE period = 'train' GROUP BY src_ip)
SELECT s.*, {best} AS review_band, {in_review} AS models_in_review, {max_pct} AS max_pct,
  row_number() OVER (ORDER BY {max_pct} DESC, {pct_sum} DESC, s.src_ip, s.window_start) AS alert_rank,
  e.train_windows IS NOT NULL AS entity_seen_in_train, coalesce(e.train_windows, 0) AS entity_train_windows,
  {entity_cols}{_deviation_sql(model_features)} AS top_deviations
FROM {rel} s CROSS JOIN t LEFT JOIN e USING (src_ip)
WHERE {flagged_any}""")
    trace = _trace_sql(source, trace_top_n, trace_rows)
    return _copy(con, f"SELECT a.*, tr.trace_flow_count, tr.trace_source_files, tr.trace_rows FROM poc_alerts a "
                      f"LEFT JOIN ({trace}) tr USING (src_ip, window_start) ORDER BY a.alert_rank", out)


def _trace_sql(source: Source, top_n: int, n_rows: int) -> str:
    """One pass over the flows for the top-N alerts: flow count, source files, first `n_rows` (file, row index)."""
    if top_n <= 0:
        return ("SELECT NULL::VARCHAR AS src_ip, NULL::TIMESTAMPTZ AS window_start, NULL::BIGINT AS trace_flow_count, "
                "NULL::VARCHAR[] AS trace_source_files, "
                "NULL::STRUCT(file VARCHAR, row_index BIGINT)[] AS trace_rows WHERE false")
    return f"""
SELECT k.src_ip, k.window_start, count(*) AS trace_flow_count, list(DISTINCT fl.source_file) AS trace_source_files,
  list_slice(list(struct_pack(file := fl.source_file, row_index := fl.source_row_index)
                  ORDER BY fl.flow_start, fl.source_file, fl.source_row_index), 1, {int(n_rows)}) AS trace_rows
FROM (SELECT src_ip, window_start, window_end FROM poc_alerts WHERE alert_rank <= {int(top_n)}) k
JOIN ({source.flows_sql()}) fl ON fl.src_ip = k.src_ip AND fl.flow_start >= k.window_start
  AND fl.flow_start < k.window_end
GROUP BY k.src_ip, k.window_start"""


def write_daily_summary(con: duckdb.DuckDBPyConnection, scores: Path, models: list[str],
                        annotations: list[Annotation], buffer_hours: int, below_label: str, out: Path) -> int:
    day_cat = category_sql("flow_date::TIMESTAMPTZ", "(flow_date + 1)::TIMESTAMPTZ", annotations, buffer_hours)
    per_model = []
    for m in models:
        per_model += [f"count(*) FILTER (WHERE {m}_band = '{b}') AS {m}_{b.lower()}" for b in BAND_ORDER]
        per_model += [f"count(*) FILTER (WHERE {m}_band = {below_label!r}) AS {m}_below",
                      f"max({m}_raw) AS {m}_max_raw", f"max({m}_pct) AS {m}_max_pct"]
    sql = f"""
SELECT flow_date, max(period) AS period, {day_cat} AS day_category, count(*) AS windows,
  count(DISTINCT src_ip) AS hosts, sum(flows) AS flows, {', '.join(per_model)}
FROM {parquet(scores)} GROUP BY flow_date ORDER BY flow_date"""
    return _copy(con, sql, out)


def write_entity_summary(con: duckdb.DuckDBPyConnection, scores: Path, models: list[str], out: Path) -> int:
    per_model = []
    for m in models:
        per_model += [f"max({m}_pct) AS {m}_max_pct", f"count(*) FILTER (WHERE {m}_band IN ({FLAGGED})) AS {m}_review",
                      f"count(*) FILTER (WHERE {m}_band IN ('Critical', 'High')) AS {m}_critical_high",
                      (f"count(*) FILTER (WHERE {m}_band IN ({FLAGGED}) AND annotation_category = 'inside') "
                      f"AS {m}_review_inside"), f"arg_max(window_start, {m}_raw) AS {m}_top_window"]
    sql = f"""
SELECT src_ip, count(*) AS windows, min(window_start) AS first_window, max(window_start) AS last_window,
  sum(flows) AS flows, count(*) FILTER (WHERE period = 'train') AS train_windows, {', '.join(per_model)}
FROM {parquet(scores)} GROUP BY src_ip ORDER BY {' + '.join(f'{m}_review' for m in models)} DESC, src_ip"""
    return _copy(con, sql, out)
