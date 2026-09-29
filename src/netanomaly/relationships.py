"""Optional source -> destination behaviour tracking (`relationship_analysis:` in config.yaml).

A pair is a directed (src_ip, dst_ip) - or (src_ip, dst_ip, dst_port, protocol) with `group_by_port_protocol` -
observed in a UTC window of `window_minutes` (the host-window convention of featureset.py). The pair-by-time table is
sparse: a row exists only for windows in which the pair had at least one flow. IPs are string keys and evidence;
they are never numeric model inputs.

Signals per active pair-window, computed from STRICTLY EARLIER windows of the same pair (window frames that end
before the current row, plus the carried history state), so a later row can never change an earlier result:

- never seen before: the source has no earlier window with this destination since `history_start`. Not judged
  (status `warmup`) until `warmup_days` of history exist - early on, everything would look new.
- recently unseen: the pair was seen before, but its last earlier window started more than `recent_lookback_days`
  before this window (a contact exactly `recent_lookback_days` earlier still counts as seen recently).
- frequency change: log2((flows + 1) / (median flows + 1)), the median taken over the pair's own earlier active
  windows that start within `baseline_lookback_days` (at most `baseline_lookback_days` earlier, inclusive). With
  fewer than `min_support_windows` such windows the status is `insufficient_history` and no change is claimed. The
  +1 smoothing keeps zero-heavy pairs finite; the baseline describes the pair's intensity when active.

History state: after a run, `state/` keeps what later periods need - one row per pair (first seen, last seen, active
windows) plus the pair-windows of the last `baseline_lookback_days`. Scoring a later period starts from that state
(from a model bundle or an earlier scoring run), so "normal" is only ever established by earlier data. Data that
starts before the state ends is refused.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from netanomaly.config import PocConfig
from netanomaly.db import sql_literal
from netanomaly.featureset import RELATIONSHIP_FEATURES, FeatureTable
from netanomaly.source import Source

STATE_VERSION = 1
PAIR_WINDOWS, HOST_WINDOWS, EVIDENCE, STATE_DIR = ("pair_windows.parquet", "host_windows.parquet",
                                                   "pair_evidence.parquet", "state")
NOVELTY = ("never_seen", "recently_unseen", "seen_recently", "warmup")
CHANGE = ("increase", "decrease", "stable", "insufficient_history")
MODEL_COLUMNS = tuple(f.name for f in RELATIONSHIP_FEATURES)


class RelationshipError(ValueError):
    pass


@dataclass(frozen=True)
class RelParams:
    """Everything that changes the analysis output (frozen into model bundles and history states)."""

    window_minutes: int
    recent_lookback_days: float
    baseline_lookback_days: float
    min_support_windows: int
    warmup_days: float
    change_log2_threshold: float
    group_by_port_protocol: bool

    @property
    def pair_cols(self) -> tuple[str, ...]:
        return ("src_ip", "dst_ip", "dst_port", "protocol") if self.group_by_port_protocol else ("src_ip", "dst_ip")

    @property
    def key(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:12]

    def to_dict(self) -> dict:
        return asdict(self)


def params_from(cfg: PocConfig) -> RelParams:
    r = cfg.relationship_analysis
    return RelParams(cfg.relationship_window, r.recent_lookback_days, r.baseline_lookback_days,
                     r.min_support_windows, r.warmup_days, r.change_log2_threshold, r.group_by_port_protocol)


def _seconds(days: float) -> int:
    return round(days * 86400)


def _ts(value: datetime) -> str:
    return f"TIMESTAMPTZ {sql_literal(value.isoformat())}"


def _rel(path: Path) -> str:
    return f"read_parquet({sql_literal(path)})"


def _copy(con: duckdb.DuckDBPyConnection, sql: str, out: Path) -> int:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO {sql_literal(out)} (FORMAT parquet, COMPRESSION zstd)")
    return con.execute(f"SELECT count(*) FROM {_rel(out)}").fetchone()[0]


# --- history state -----------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class History:
    """Carried pair history: everything before `end` (exclusive) is summarised in `path`."""

    path: Path
    history_start: datetime
    end: datetime
    params: RelParams

    @property
    def summary(self) -> str:
        return _rel(self.path / "pair_summary.parquet")

    @property
    def recent(self) -> str:
        return _rel(self.path / "recent_pairs.parquet")


def load_history(path: Path, params: RelParams) -> History:
    meta_path = path / "state.json"
    if not meta_path.exists():
        raise RelationshipError(f"no relationship history state in {path} (state.json missing)")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if meta.get("version") != STATE_VERSION or meta.get("params") != params.to_dict():
        raise RelationshipError(f"relationship history in {path} was built with other settings "
                                f"({meta.get('params')}); it cannot be continued with {params.to_dict()}")
    return History(path, datetime.fromisoformat(meta["history_start"]), datetime.fromisoformat(meta["end"]), params)


# --- analysis ----------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class RelationshipResult:
    out_dir: Path
    params: RelParams
    history_start: datetime | None
    end: datetime | None
    pair_windows: int
    host_windows: int
    evidence_rows: int
    carried_from: str | None

    @property
    def state(self) -> Path:
        return self.out_dir / STATE_DIR

    def path(self, name: str) -> Path:
        return self.out_dir / name

    def to_dict(self) -> dict:
        return {"out_dir": str(self.out_dir), "params": self.params.to_dict(), "params_key": self.params.key,
                "history_start": self.history_start.isoformat() if self.history_start else None,
                "end": self.end.isoformat() if self.end else None, "pair_windows": self.pair_windows,
                "host_windows": self.host_windows, "evidence_rows": self.evidence_rows,
                "carried_from": self.carried_from}


def load_result(out_dir: Path) -> RelationshipResult:
    """A finished analysis written by `analyze` (its summary.json)."""
    d = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))

    def ts(v: str | None) -> datetime | None:
        return datetime.fromisoformat(v) if v else None

    return RelationshipResult(out_dir, RelParams(**d["params"]), ts(d["history_start"]), ts(d["end"]),
                              d["pair_windows"], d["host_windows"], d["evidence_rows"], d["carried_from"])


def pair_windows_sql(source: Source, p: RelParams, start: datetime | None, end: datetime | None) -> str:
    """Active pair-windows of the source in [start, end): flows, bytes, packets (bounded aggregation in DuckDB)."""
    bucket = f"time_bucket(INTERVAL '{p.window_minutes} minutes', flow_start)"
    where = ["flow_start IS NOT NULL", "src_ip IS NOT NULL", "src_ip <> ''", "dst_ip IS NOT NULL", "dst_ip <> ''"]
    if start is not None:
        where.append(f"flow_start >= {_ts(start)}")
    if end is not None:
        where.append(f"flow_start < {_ts(end)}")
    return (f"SELECT {', '.join(p.pair_cols)}, {bucket} AS window_start, count(*) AS flows, sum(bytes) AS bytes, "
            f"sum(packets) AS packets FROM ({source.flows_sql()}) WHERE {' AND '.join(where)} GROUP BY ALL")


def _same_pair(a: str, b: str, cols: tuple[str, ...]) -> str:
    return " AND ".join(f"{a}.{c} IS NOT DISTINCT FROM {b}.{c}" for c in cols)


def _signals_sql(p: RelParams, history: History | None, history_start: datetime) -> str:
    """Per new pair-window: first/last seen, baseline, novelty and change status (earlier windows only)."""
    cols = ", ".join(p.pair_cols)
    part = f"PARTITION BY {cols} ORDER BY window_start"
    recent_rows = (f"SELECT {cols}, window_start, flows, bytes, packets, false AS is_new FROM {history.recent} "
                   "UNION ALL " if history else "")
    summary_join = (f"LEFT JOIN {history.summary} ps ON {_same_pair('w', 'ps', p.pair_cols)}" if history else
                    "LEFT JOIN (SELECT NULL::TIMESTAMPTZ AS first_seen, NULL::TIMESTAMPTZ AS last_seen) ps ON false")
    recent, base, warm = (_seconds(p.recent_lookback_days), _seconds(p.baseline_lookback_days),
                          _seconds(p.warmup_days))
    thr, w = p.change_log2_threshold, p.window_minutes
    return f"""
WITH combined AS ({recent_rows}SELECT {cols}, window_start, flows, bytes, packets, true AS is_new FROM rel_new),
w AS (
  SELECT *,
    lag(window_start) OVER ({part}) AS prev_seen,
    min(window_start) OVER ({part} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_in_rows,
    count(*) OVER base AS baseline_support,
    median(flows) OVER base AS baseline_flows_median,
    median(bytes) OVER base AS baseline_bytes_median
  FROM combined
  WINDOW base AS ({part} RANGE BETWEEN INTERVAL '{base} seconds' PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)
),
s AS (
  SELECT w.*, coalesce(ps.first_seen, w.first_in_rows) AS first_seen,
    coalesce(w.prev_seen, ps.last_seen) AS last_seen_before,
    w.window_start < {_ts(history_start)} + INTERVAL '{warm} seconds' AS warmup,
    CASE WHEN w.baseline_support >= {p.min_support_windows}
         THEN log2((w.flows + 1) / (w.baseline_flows_median + 1)) END AS log2_change
  FROM w {summary_join}
  WHERE w.is_new
)
SELECT {cols}, window_start, window_start + INTERVAL '{w} minutes' AS window_end,
  CAST(window_start AS DATE) AS flow_date, flows, bytes, packets, first_seen, last_seen_before,
  round(epoch(window_start - last_seen_before) / 86400, 3) AS days_since_last_seen,
  baseline_support, baseline_flows_median, baseline_bytes_median, log2_change,
  CASE WHEN last_seen_before IS NULL THEN CASE WHEN warmup THEN 'warmup' ELSE 'never_seen' END
       WHEN last_seen_before < window_start - INTERVAL '{recent} seconds' THEN 'recently_unseen'
       ELSE 'seen_recently' END AS novelty_status,
  CASE WHEN log2_change IS NULL THEN 'insufficient_history'
       WHEN log2_change >= {thr!r} THEN 'increase'
       WHEN log2_change <= {-thr!r} THEN 'decrease'
       ELSE 'stable' END AS change_status,
  warmup
FROM s"""


def _host_windows_sql(pairs: str) -> str:
    return f"""
SELECT src_ip, window_start, any_value(window_end) AS window_end, any_value(flow_date) AS flow_date,
  count(*) AS rel_pairs, any_value(warmup) AS rel_warmup,
  CASE WHEN any_value(warmup) THEN NULL ELSE count(*) FILTER (WHERE novelty_status = 'never_seen') END AS rel_new_dst,
  CASE WHEN any_value(warmup) THEN NULL
       ELSE count(*) FILTER (WHERE novelty_status = 'never_seen') / count(*)::DOUBLE END AS rel_new_dst_share,
  count(*) FILTER (WHERE novelty_status = 'recently_unseen') AS rel_recently_unseen_dst,
  count(*) FILTER (WHERE change_status = 'increase') AS rel_freq_increase,
  count(*) FILTER (WHERE change_status = 'decrease') AS rel_freq_decrease,
  max(abs(log2_change)) AS rel_max_abs_log2_change,
  count(*) FILTER (WHERE change_status = 'insufficient_history') AS rel_pairs_insufficient_history
FROM {pairs} GROUP BY src_ip, window_start ORDER BY window_start, src_ip"""


def _evidence_sql(pairs: str, p: RelParams, history_start: datetime | None) -> str:
    """Notable pair-windows with a readable reason (IPs kept as evidence, never as model inputs)."""
    since = history_start.strftime("%Y-%m-%d %H:%M") if history_start else "-"
    novelty = (f"CASE novelty_status WHEN 'never_seen' THEN 'never seen before (history since {since} UTC)' "
               "WHEN 'recently_unseen' THEN 'not contacted for ' || days_since_last_seen::VARCHAR || ' days (lookback "
               f"{p.recent_lookback_days:g} d); first seen ' || strftime(first_seen, '%Y-%m-%d') END")
    change = ("CASE WHEN change_status IN ('increase', 'decrease') THEN change_status || ': ' || flows::VARCHAR || "
              "' flows vs median ' || round(baseline_flows_median, 1)::VARCHAR || ' over ' || "
              f"baseline_support::VARCHAR || ' earlier active windows in {p.baseline_lookback_days:g} d (log2 ' || "
              "round(log2_change, 2)::VARCHAR || ')' END")
    return (f"SELECT *, concat_ws('; ', {novelty}, {change}) AS reason FROM {pairs} "
            "WHERE novelty_status IN ('never_seen', 'recently_unseen') OR change_status IN ('increase', 'decrease') "
            f"ORDER BY window_start, {', '.join(p.pair_cols)}")


def _write_state(con: duckdb.DuckDBPyConnection, p: RelParams, history: History | None, out: Path,
                 history_start: datetime, end: datetime) -> None:
    cols = ", ".join(p.pair_cols)
    old_summary = (f"SELECT {cols}, first_seen, last_seen, active_windows FROM {history.summary} UNION ALL "
                   if history else "")
    _copy(con, f"SELECT {cols}, min(first_seen) AS first_seen, max(last_seen) AS last_seen, "
               f"sum(active_windows)::BIGINT AS active_windows FROM ({old_summary}SELECT {cols}, "
               "min(window_start) AS first_seen, max(window_start) AS last_seen, count(*) AS active_windows "
               f"FROM rel_new GROUP BY ALL) GROUP BY ALL ORDER BY {cols}", out / "pair_summary.parquet")
    keep_from = end - timedelta(seconds=_seconds(p.baseline_lookback_days))
    old_recent = f"SELECT {cols}, window_start, flows, bytes, packets FROM {history.recent} UNION ALL " if history else ""
    _copy(con, f"SELECT * FROM ({old_recent}SELECT {cols}, window_start, flows, bytes, packets FROM rel_new) "
               f"WHERE window_start >= {_ts(keep_from)} ORDER BY window_start, {cols}", out / "recent_pairs.parquet")
    (out / "state.json").write_text(json.dumps({
        "version": STATE_VERSION, "params": p.to_dict(), "history_start": history_start.isoformat(),
        "end": end.isoformat(), "note": "derived per-pair aggregates (contain IP addresses): keep outside the "
        "repository, like the data they come from"}, indent=2), encoding="utf-8")


def analyze(con: duckdb.DuckDBPyConnection, source: Source, p: RelParams, out_dir: Path, *,
            start: datetime | None = None, end: datetime | None = None,
            history: History | None = None) -> RelationshipResult:
    """Pair-by-time table, host-window summary, evidence and the continued history state for flows in [start, end).

    `start`/`end` must lie on window boundaries (UTC day boundaries always do). With `history`, every analysed
    window must start at or after `history.end`; otherwise earlier-period results would depend on later data.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"CREATE OR REPLACE TEMP TABLE rel_new AS {pair_windows_sql(source, p, start, end)}")
    first, last = con.execute("SELECT min(window_start), max(window_start) FROM rel_new").fetchone()
    if history is not None and first is not None and first < history.end:
        raise RelationshipError(
            f"relationship history ends {history.end.isoformat()} but the data starts {first.isoformat()}: scoring "
            "data that overlaps or precedes the carried history would let later rows define what was normal "
            "earlier. Score only data that starts at or after the history end (continue a chain of scoring runs "
            "with --history <earlier run>).")
    history_start = history.history_start if history else (start or first)
    state_end = end or (last + timedelta(minutes=p.window_minutes) if last else (history.end if history else None))
    pairs_path = out_dir / PAIR_WINDOWS
    empty = history_start is None  # no flows and no history: same columns, no rows
    signals = _signals_sql(p, history, history_start or datetime.fromisoformat("1970-01-01T00:00:00+00:00"))
    n_pairs = _copy(con, f"SELECT * FROM ({signals}) WHERE {'false' if empty else 'true'} "
                         f"ORDER BY window_start, {', '.join(p.pair_cols)}", pairs_path)
    n_hosts = _copy(con, _host_windows_sql(_rel(pairs_path)), out_dir / HOST_WINDOWS)
    n_evidence = _copy(con, _evidence_sql(_rel(pairs_path), p, history_start), out_dir / EVIDENCE)
    if history_start is not None and state_end is not None:
        _write_state(con, p, history, out_dir / STATE_DIR, history_start, state_end)
    con.execute("DROP TABLE IF EXISTS rel_new")
    result = RelationshipResult(out_dir, p, history_start, state_end, n_pairs, n_hosts, n_evidence,
                                str(history.path) if history else None)
    (out_dir / "summary.json").write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")
    return result


# --- optional model inputs ---------------------------------------------------------------------------------------

def extend_feature_table(con: duckdb.DuckDBPyConnection, base: FeatureTable, result: RelationshipResult,
                         cache_dir: Path, where: str = "TRUE") -> FeatureTable:
    """Base host-window rows (filtered by `where`) + the numeric rel_* summaries as extra columns. Only used when
    `include_model_features` is true. Host-windows without a destination get NULL (imputed like any missing value).
    Rebuilt on every call: the summaries depend on the carried history, which a cache key cannot see cheaply."""
    key = hashlib.sha256(json.dumps([base.key, result.params.key, where, str(result.out_dir)]).encode()
                         ).hexdigest()[:16]
    table = cache_dir / f"{base.key}-rel-{key}" / "table"
    table.parent.mkdir(parents=True, exist_ok=True)
    rel_cols = ", ".join(f"h.{c}" for c in MODEL_COLUMNS)
    con.execute(f"COPY (SELECT f.*, {rel_cols} FROM {base.relation()} f LEFT JOIN "
                f"{_rel(result.path(HOST_WINDOWS))} h USING (src_ip, window_start) WHERE {where}) TO "
                f"{sql_literal(table)} (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (flow_date), OVERWRITE true)")
    ft = FeatureTable(table, f"{base.key}-rel-{key}", base.window_minutes, [*base.features, *MODEL_COLUMNS], 0)
    ft.rows = con.execute(f"SELECT count(*) FROM {ft.relation()}").fetchone()[0]
    return ft
