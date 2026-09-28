"""Timing regularity over the hours before a window (V2-4): `interarrival_cv`, for beaconing.

Definitions (see ARCHITECTURE.md → Timing regularity, ADR-020):

- **Scored row**: one per host-window of the V0 grid, `W = time_bucket(window_minutes, flow_start)` per `src_ip`
  (the host is active in W). The row only marks *when* to look; W's own flows never enter its value.
- **History window**: flows of the same `src_ip` with `flow_start` in `[W - history_hours, W)` (default 2 h, at
  least 1 h, at most 24 h). A flow at or after `W` (in the scored window or later) cannot change the row (tested).
- **Event order**: UTC `flow_start` only. Exporter sequence numbers and file/row order are never used (ADR-016).
- **Peer grouping (series)**: one series per destination peer, `(src_ip, dst_ip)`, over all ports and protocols.
  Its events are the pair's *distinct* `flow_start` instants: flows with a tied timestamp are one event, so ties
  and duplicated records neither add zero gaps nor depend on row order. There is no `src_subnet`/global fallback:
  regularity belongs to one conversation, and pooling other hosts' flows would not form a series.
- **Gaps**: differences between consecutive events of a series, both inside the history window. Consecutive is
  judged on the whole lake, so a gap only ever depends on its two endpoints, both before `W`.
- **Minimum support**: a series qualifies with at least `min_events` events (default 10, i.e. 9 gaps) inside the
  history window. With a 2 h history this reaches periods up to ~13 min; slower beacons are not visible.
- **Statistic**: per qualifying series `cv = stddev_pop(gap) / mean(gap)` (0 = perfectly periodic, ~1 = Poisson-like
  random arrivals). `interarrival_cv` is the minimum over the host's qualifying series (ties: lowest `dst_ip`);
  that series is reported as `timing_dst_ip`, `timing_events`, `timing_median_gap_s` (its period estimate).
  Low values rank first: a ranking signal, not a probability of beaconing.
- **Missing / insufficient history**: `timing_quality` is `ok` (a series qualified), `insufficient` (the host has
  flows in the history window but no series reaches `min_events`) or `none` (no flows in the history window);
  `interarrival_cv` and the `timing_*` series columns are NULL unless `ok`. `history_complete` is false when the
  history window starts before the lake's earliest `flow_start`, i.e. the lake cannot cover the full history.
  Support columns: `timing_pairs` (qualifying series), `history_pairs`, `history_events`.

Inputs are checked against the feature registry before anything is computed (`src_ip`, `dst_ip`, `flow_start`).
Memory: one lake pass writes a slim event table (`temp_directory/timing_events`, deleted afterwards), then one
query per UTC day reads that day and the day before. Output is staged and swapped in when every day succeeded.
"""

from __future__ import annotations

import shutil
from datetime import date
from enum import StrEnum
from pathlib import Path

import duckdb

from netanomaly.config import TimingSettings
from netanomaly.db import sql_literal
from netanomaly.feature_registry import Registry, require_usable
from netanomaly.features import flows_glob
from netanomaly.schema import Contract

TIMING_FEATURES = ("interarrival_cv",)
MINUTES_PER_DAY = 24 * 60
PART = "part-0.parquet"


class TimingQuality(StrEnum):
    """How much history stood behind a row, best first."""

    OK = "ok"
    INSUFFICIENT = "insufficient"
    NONE = "none"


def require_usable_inputs(registry: Registry, contract: Contract) -> None:
    """Refuse to build a timing feature the registry does not rate usable against the contract."""
    require_usable(registry, contract, TIMING_FEATURES)


def events_sql(lake: Path) -> str:
    """One row per distinct (src_ip, dst_ip, flow_start) with the pair's previous distinct instant (`prev_t`).

    DISTINCT makes tied timestamps one event, so ORDER BY t is a total order within a pair: no tie-break exists
    that row order or exporter sequence numbers could influence. `prev_t` depends only on instants before `t`.
    """
    return f"""
WITH ev AS (
  SELECT DISTINCT src_ip, dst_ip, flow_start AS t
  FROM read_parquet({sql_literal(flows_glob(lake))}, hive_partitioning = true)
  WHERE src_ip IS NOT NULL AND dst_ip IS NOT NULL AND flow_start IS NOT NULL
)
SELECT src_ip, dst_ip, t, lag(t) OVER (PARTITION BY src_ip, dst_ip ORDER BY t) AS prev_t,
       CAST(t AS DATE) AS flow_date  -- UTC day: db.connect() pins the session zone to UTC
FROM ev
"""


def day_sql(events_dir: Path, day: date, lake_start: str, window_minutes: int, s: TimingSettings) -> str:
    """`interarrival_cv` and support for every host-window on `day`, from flows in [W - history_hours, W)."""
    src = f"read_parquet({sql_literal(events_dir / '**' / '*.parquet')}, hive_partitioning = true)"
    d = f"DATE {sql_literal(day.isoformat())}"
    hist_start = f"win.window_start - INTERVAL {int(s.history_hours)} HOUR"
    return f"""
WITH ev AS (  -- history_hours <= 24, so the day before is enough
  SELECT src_ip, dst_ip, t, prev_t, flow_date FROM {src}
  WHERE flow_date BETWEEN {d} - INTERVAL 1 DAY AND {d}
),
win AS (  -- the V0 host-window grid on this day
  SELECT DISTINCT src_ip, time_bucket(INTERVAL '{int(window_minutes)} minutes', t) AS window_start
  FROM ev WHERE flow_date = {d}
),
hist AS (  -- events strictly before the window, within the history window
  SELECT win.src_ip, win.window_start, ev.dst_ip,
         CASE WHEN ev.prev_t >= {hist_start}  -- gap only when both endpoints are inside the history window
              THEN (epoch_us(ev.t) - epoch_us(ev.prev_t)) / 1e6 END AS gap_s
  FROM win JOIN ev ON ev.src_ip = win.src_ip AND ev.t >= {hist_start} AND ev.t < win.window_start
),
pair AS (
  SELECT src_ip, window_start, dst_ip, count(*) AS events,
         stddev_pop(gap_s) / avg(gap_s) AS cv, median(gap_s) AS median_gap_s
  FROM hist GROUP BY ALL
),
host AS (
  SELECT src_ip, window_start, count(*) AS history_pairs, sum(events) AS history_events,
         count(*) FILTER (WHERE events >= {int(s.min_events)}) AS timing_pairs
  FROM pair GROUP BY ALL
),
best AS (
  SELECT src_ip, window_start, cv AS interarrival_cv, dst_ip AS timing_dst_ip, events AS timing_events,
         median_gap_s AS timing_median_gap_s
  FROM pair WHERE events >= {int(s.min_events)}
  QUALIFY row_number() OVER (PARTITION BY src_ip, window_start ORDER BY cv, dst_ip) = 1
)
SELECT
  win.src_ip, win.window_start, best.interarrival_cv,
  CASE WHEN best.src_ip IS NOT NULL THEN '{TimingQuality.OK}'
       WHEN host.history_events > 0 THEN '{TimingQuality.INSUFFICIENT}'
       ELSE '{TimingQuality.NONE}' END AS timing_quality,
  best.timing_dst_ip, best.timing_events, best.timing_median_gap_s,
  coalesce(host.timing_pairs, 0) AS timing_pairs,
  coalesce(host.history_pairs, 0) AS history_pairs,
  coalesce(host.history_events, 0)::BIGINT AS history_events,
  {hist_start} >= TIMESTAMPTZ {sql_literal(lake_start)} AS history_complete
FROM win
LEFT JOIN host USING (src_ip, window_start)
LEFT JOIN best USING (src_ip, window_start)
ORDER BY win.src_ip, win.window_start
"""


def build_host_timing(con: duckdb.DuckDBPyConnection, lake: Path, out_dir: Path, work_dir: Path,
                      window_minutes: int, s: TimingSettings) -> int:
    """Write `out_dir/flow_date=D/part-0.parquet` for every day in the lake. Returns the row count.

    Days are computed one at a time, so memory is bounded by two days of events and their history joins.
    """
    if MINUTES_PER_DAY % window_minutes:
        raise ValueError(f"window_minutes={window_minutes} must divide a day so windows never span two UTC days")
    events_dir = work_dir / "timing_events"
    staging = out_dir.with_name(out_dir.name + "._staging")
    for p in (events_dir, staging):
        shutil.rmtree(p, ignore_errors=True)
    events_dir.mkdir(parents=True)
    staging.mkdir(parents=True)
    try:
        con.execute(f"COPY ({events_sql(lake)}) TO {sql_literal(events_dir)} "
                    "(FORMAT parquet, PARTITION_BY (flow_date), OVERWRITE true)")
        glob = sql_literal(events_dir / "**" / "*.parquet")
        days = [r[0] for r in con.execute(
            f"SELECT DISTINCT flow_date FROM read_parquet({glob}, hive_partitioning = true) ORDER BY 1").fetchall()]
        if days:
            (lake_start,) = con.execute(f"SELECT min(t)::VARCHAR FROM read_parquet({glob})").fetchone()
        for day in days:
            part = staging / f"flow_date={day.isoformat()}"
            part.mkdir()
            con.execute(f"COPY ({day_sql(events_dir, day, lake_start, window_minutes, s)}) "
                        f"TO {sql_literal(part / PART)} (FORMAT parquet, COMPRESSION zstd)")
        shutil.rmtree(out_dir, ignore_errors=True)
        staging.rename(out_dir)
    finally:
        shutil.rmtree(events_dir, ignore_errors=True)
        shutil.rmtree(staging, ignore_errors=True)
    if not days:
        return 0
    return con.execute(f"SELECT count(*) FROM read_parquet({sql_literal(out_dir / '**' / PART)})").fetchone()[0]
