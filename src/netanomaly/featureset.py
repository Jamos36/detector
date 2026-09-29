"""PoC feature table: one row per source host (`src_ip`) x fixed UTC window (`window_minutes`).

Why host x window: pentest activity is attributed to machines and periods, a window row is small enough to model a
year (tens of millions of flows become ~hosts x windows rows), and every row traces back to its flows through
(`src_ip`, [`window_start`, `window_end`)) plus `source_file`/`source_row_index` in the flow relation.

Every feature is computed from the flows inside its own window only (no history), so a row cannot see the future
and chronological splits cannot leak through the features. Identifiers (IPs, ports, file names) are never features;
they are trace columns. Definitions live in `FEATURES` below and are rendered into FEATURES.md (`uv run netanomaly docs`).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from netanomaly.db import sql_literal
from netanomaly.source import Source

TRACE_COLUMNS = ("src_ip", "window_start", "window_end", "flow_date", "first_flow_start", "last_flow_start",
                 "n_source_files")
FEATURE_TABLE_VERSION = 1


@dataclass(frozen=True)
class FeatureDef:
    name: str
    sql: str  # aggregate over the window's flows (canonical columns of source.flows_sql)
    sources: tuple[str, ...]  # canonical fields it needs; dropped when any is unavailable
    log1p: bool  # heavy-tailed count/volume: log1p before imputation/scaling (stateless)
    description: str
    nulls: str  # when the value is NULL and what that means


FEATURES = (
    FeatureDef("flows", "count(*)", ("src_ip", "flow_start"), True,
               "flows started by the host in the window", "never (a row exists only with >= 1 flow)"),
    FeatureDef("bytes_total", "sum(bytes)", ("bytes",), True, "sum of flow bytes",
               "every flow in the window has NULL/negative bytes"),
    FeatureDef("packets_total", "sum(packets)", ("packets",), True, "sum of flow packets",
               "every flow has NULL/negative packets"),
    FeatureDef("max_flow_bytes", "max(bytes)", ("bytes",), True, "largest single flow (bytes)",
               "every flow has NULL bytes"),
    FeatureDef("bytes_per_packet", "sum(bytes) / nullif(sum(packets), 0)", ("bytes", "packets"), True,
               "mean bytes per packet over the window", "packets sum to 0 or are NULL"),
    FeatureDef("uniq_dst_ip", "count(DISTINCT dst_ip)", ("dst_ip",), True,
               "distinct destination addresses (fan-out; scans raise it)", "never (0 when every dst_ip is NULL)"),
    FeatureDef("uniq_dst_port", "count(DISTINCT dst_port)", ("dst_port",), True,
               "distinct destination ports (port sweeps raise it)", "never (0 when every dst_port is NULL)"),
    FeatureDef("internal_share", "avg(dst_is_private::DOUBLE)", ("dst_ip",), False,
               "share of flows to RFC 1918 IPv4 destinations (lateral vs external)",
               "no destination is a dotted IPv4 address"),
    FeatureDef("tcp_share", "avg((protocol = 6)::DOUBLE)", ("protocol",), False,
               "share of TCP flows (protocol 6)", "every protocol is NULL"),
    FeatureDef("udp_share", "avg((protocol = 17)::DOUBLE)", ("protocol",), False,
               "share of UDP flows (protocol 17)", "every protocol is NULL"),
    FeatureDef("icmp_share", "avg((protocol = 1)::DOUBLE)", ("protocol",), False,
               "share of ICMP flows (protocol 1; ping sweeps raise it)", "every protocol is NULL"),
    FeatureDef("mean_duration_s", "avg(duration_s)", ("flow_start", "flow_end"), True,
               "mean flow duration in seconds", "no flow has flow_end >= flow_start"),
)
# Optional history-dependent host-window summaries from relationships.py. They are model inputs only when
# `relationship_analysis.include_model_features` is true; they are never part of the base feature table.
REL_SQL = "relationships.py host-window summary (strictly earlier windows only)"
REL_SOURCES = ("src_ip", "dst_ip", "flow_start")
RELATIONSHIP_FEATURES = (
    FeatureDef("rel_new_dst", REL_SQL, REL_SOURCES, True,
               "destinations the host contacts for the first time since history began (never seen before)",
               "warm-up: less than `warmup_days` of history"),
    FeatureDef("rel_new_dst_share", REL_SQL, REL_SOURCES, False,
               "share of the window's destinations that are never seen before (0..1)",
               "warm-up, or no destination in the window"),
    FeatureDef("rel_recently_unseen_dst", REL_SQL, REL_SOURCES, True,
               "destinations contacted before, but not within `recent_lookback_days`", "never"),
    FeatureDef("rel_freq_increase", REL_SQL, REL_SOURCES, True,
               "pairs whose flow count is >= 2^threshold x their own recent median (enough support)", "never"),
    FeatureDef("rel_freq_decrease", REL_SQL, REL_SOURCES, True,
               "pairs whose flow count is <= 2^-threshold x their own recent median (enough support)", "never"),
    FeatureDef("rel_max_abs_log2_change", REL_SQL, REL_SOURCES, False,
               "largest |log2((flows+1)/(median+1))| among the window's pairs with enough support",
               "no pair in the window has `min_support_windows` earlier active windows"),
)
FEATURE_BY_NAME = {f.name: f for f in (*FEATURES, *RELATIONSHIP_FEATURES)}


class FeatureError(ValueError):
    pass


def available_features(source: Source) -> list[FeatureDef]:
    usable = source.mapping.usable
    return [f for f in FEATURES if all(s in usable for s in f.sources)]


def select_features(available: list[str], include: list[str] | None, exclude: list[str]) -> list[str]:
    """Configured feature subset, in definition order. Unknown or unavailable names are errors, not skips."""
    unknown = sorted(n for n in {*(include or []), *exclude} if n not in FEATURE_BY_NAME)
    if unknown:
        raise FeatureError(f"unknown feature(s) {unknown}; defined: {list(FEATURE_BY_NAME)}")
    names = list(available)
    if include is not None:
        missing = [n for n in include if n not in names]
        if any(n.startswith("rel_") for n in missing):
            raise FeatureError(f"feature(s) {missing} are relationship summaries: set "
                               "relationship_analysis.enabled and include_model_features to use them")
        if missing:
            raise FeatureError(f"feature(s) {missing} need source fields that are not mapped (see profile)")
        names = [n for n in names if n in include]
    names = [n for n in names if n not in exclude]
    if not names:
        raise FeatureError("no features selected")
    return names


def feature_sql(source: Source, features: list[FeatureDef], window_minutes: int) -> str:
    w = int(window_minutes)
    bucket = f"time_bucket(INTERVAL '{w} minutes', flow_start)"
    aggs = ",\n  ".join(f"{f.sql} AS {f.name}" for f in features)
    return f"""
SELECT
  src_ip,
  {bucket} AS window_start,
  {bucket} + INTERVAL '{w} minutes' AS window_end,
  CAST({bucket} AS DATE) AS flow_date,
  min(flow_start) AS first_flow_start,
  max(flow_start) AS last_flow_start,
  count(DISTINCT source_file) AS n_source_files,
  {aggs}
FROM ({source.flows_sql()})
WHERE flow_start IS NOT NULL AND src_ip IS NOT NULL AND src_ip <> ''
GROUP BY ALL"""


def cache_key(source: Source, features: list[FeatureDef], window_minutes: int) -> str:
    payload = {"version": FEATURE_TABLE_VERSION, "input": source.fingerprint, "window_minutes": window_minutes,
               "mapping": {k: asdict(v) for k, v in sorted(source.mapping.usable.items())},
               "epoch_unit": source.epoch_unit, "features": [asdict(f) for f in features]}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


@dataclass
class FeatureTable:
    path: Path  # directory of flow_date=YYYY-MM-DD partitions
    key: str
    window_minutes: int
    features: list[str]  # every computed feature (the model may use a subset)
    rows: int

    @property
    def glob(self) -> str:
        return (self.path / "**" / "*.parquet").as_posix()

    def relation(self) -> str:
        return f"read_parquet({sql_literal(self.glob)}, hive_partitioning = true)"


def build_feature_table(con: duckdb.DuckDBPyConnection, source: Source, window_minutes: int, cache_dir: Path,
                        *, rebuild: bool = False) -> FeatureTable:
    """Aggregate flows to host-window rows (all available features), cached by input + mapping + window + defs."""
    features = available_features(source)
    key = cache_key(source, features, window_minutes)
    out = cache_dir / key
    meta_path = out / "meta.json"
    if meta_path.exists() and not rebuild:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return FeatureTable(out / "table", key, meta["window_minutes"], meta["features"], meta["rows"])
    shutil.rmtree(out, ignore_errors=True)
    table = out / "table"
    table.mkdir(parents=True)
    con.execute(f"COPY ({feature_sql(source, features, window_minutes)}) TO {sql_literal(table)} "
                "(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (flow_date), OVERWRITE true)")
    ft = FeatureTable(table, key, window_minutes, [f.name for f in features], 0)
    ft.rows = con.execute(f"SELECT count(*) FROM {ft.relation()}").fetchone()[0]
    meta_path.write_text(json.dumps({"key": key, "window_minutes": window_minutes, "features": ft.features,
                                     "rows": ft.rows, "input_fingerprint": source.fingerprint,
                                     "definitions": [asdict(f) for f in features]}, indent=2), encoding="utf-8")
    return ft


def feature_quality(con: duckdb.DuckDBPyConnection, table: FeatureTable, features: list[str],
                    train_where: str) -> list[dict]:
    """Per feature: missing share overall and on training rows, non-finite values, spread on training rows.

    status: ok | all_null (on training rows) | constant (on training rows). Non-finite values are counted and
    treated as missing (the imputer replaces them with the training median).
    """
    out = []
    for name in features:
        n, nn, tn, tnn, nonfinite, std, distinct = con.execute(f"""
SELECT count(*), count({name}),
  count(*) FILTER (WHERE {train_where}), count({name}) FILTER (WHERE {train_where}),
  count(*) FILTER (WHERE {train_where} AND NOT isfinite({name}::DOUBLE)),
  stddev_pop({name}::DOUBLE) FILTER (WHERE {train_where} AND isfinite({name}::DOUBLE)),
  approx_count_distinct({name}) FILTER (WHERE {train_where})
FROM {table.relation()}""").fetchone()
        status = "all_null" if not tnn else "constant" if not std else "ok"
        out.append({"feature": name, "null_share": round(1 - nn / n, 6) if n else None,
                    "train_null_share": round(1 - tnn / tn, 6) if tn else None, "train_nonfinite": nonfinite,
                    "train_std": std, "train_distinct_approx": distinct, "status": status,
                    "handling": f"dropped from the model ({status} on training rows)" if status != "ok" else
                    ("log1p (stateless), then " if FEATURE_BY_NAME[name].log1p else "")
                    + "median imputation fitted on training rows"})
    return out


def features_document() -> str:
    """Whole FEATURES.md (generated by `uv run netanomaly docs`)."""
    return f"# Features\n\n<!-- GENERATED by `uv run netanomaly docs` from src/netanomaly/featureset.py. Do not edit by hand; tests fail if this file is stale. -->\n\n{features_markdown()}"


def features_markdown() -> str:
    """The feature table section of FEATURES.md."""
    rows = [f"| `{f.name}` | {', '.join(f.sources)} | `{f.sql}` | {'log1p' if f.log1p else '-'} | "
            f"{f.description} | {f.nulls} |" for f in FEATURES]
    return "\n".join([
        "## Model inputs (`src/netanomaly/featureset.py`)", "",
        ("Row unit: one source host (`src_ip`) x one fixed UTC window of `window_minutes` (config, default 60; must "
        "divide a day). Every value comes from the flows whose `flow_start` falls in [`window_start`, "
        "`window_end`) of that host only: no history, no later rows, no labels or annotations. Trace columns "
        f"({', '.join(f'`{c}`' for c in TRACE_COLUMNS)}) identify the row and are never model inputs. A feature is "
        "computed only when every canonical source field is mapped (see the profile); `features.include/exclude` "
        "selects the model inputs, and a different selection or window is a different experiment id."), "",
        ("Missing values: NULLs (and non-finite values) are imputed with the training-period median; the imputer, "
        "any scaler and feature screening (all-NULL or constant on training rows -> dropped and reported) are "
        "fitted on training rows only."), "",
        "| feature | canonical sources | aggregate | transform | meaning | NULL when |",
        "|---|---|---|---|---|---|", *rows, "",
        ("Known redundancy: `bytes_total`, `packets_total` and `max_flow_bytes` are strongly correlated in most "
        "traffic; keep or exclude them per experiment. Canonical field meanings are unvalidated (0/42 contract "
        "fields) - see SCHEMA.md."), "",
        "## Optional relationship summaries (`src/netanomaly/relationships.py`)", "",
        ("Model inputs only when `relationship_analysis.include_model_features: true` (default false: report-only). "
         "Unlike the features above they use the host's history, but strictly earlier windows only (and, when "
         "scoring a model bundle, the history carried in the bundle). Raw IPs and pair keys are never model inputs."),
        "", "| feature | transform | meaning | NULL when |", "|---|---|---|---|",
        *[f"| `{f.name}` | {'log1p' if f.log1p else '-'} | {f.description} | {f.nulls} |"
          for f in RELATIONSHIP_FEATURES], "",
    ])
