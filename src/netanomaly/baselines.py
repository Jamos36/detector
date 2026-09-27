"""Host baselines from strictly earlier data (V2-2): `bytes_out_robust_z` with a peer-group fallback.

Definitions (see ARCHITECTURE.md → Host baselines, ADR-018):

- **Value**: `v = ln(1 + bytes_out)` per (src_ip, window), where `bytes_out = sum(bytes)` over the window, as in V0.
  The log makes a deviation multiplicative ("10x the usual volume"), comparable across hosts of different size.
- **Baseline window**: every row on UTC day D is compared with the rows of the `lookback_days` whole UTC days
  before D, i.e. `flow_date` in [D - lookback_days, D - 1]. Rows of day D itself, including its earlier windows,
  are never used. A baseline value on day D is therefore a function of strictly earlier days only; rows at or
  after D cannot change it (tested). Only active windows exist, so the baseline describes the host when active.
- **Levels and fallback**: `host` (the host's own history) → `peer` (all hosts whose windows carry the same
  `src_subnet`) → `global` (all hosts) → `none`. The first level that qualifies is used. A level qualifies when its
  history has at least `min_windows` windows on at least `min_days` distinct days and MAD > 0; `peer` and `global`
  also need at least `min_peer_hosts` distinct hosts.
- **Score**: `bytes_out_robust_z = (v - median) / (1.4826 * MAD)` of the chosen level's history; NULL at `none`.
  1.4826 scales MAD to a standard deviation for normal data. It is a ranking signal, not a probability.
- **Quality**: `baseline_quality` is the chosen level (host > peer > global > none), with its support
  (`baseline_windows`, `baseline_days`, `baseline_hosts`) and the host's own support (`host_windows`,
  `host_days`), so a consumer sees both which history was used and why the host's own history was not.

Inputs are checked against the feature registry before anything is computed: every feature built here must be
usable (all sources high/medium confidence, model_use not exclude/provenance). Low-confidence fields are never read.
"""

from __future__ import annotations

import shutil
from datetime import date
from enum import StrEnum
from pathlib import Path

import duckdb

from netanomaly.config import BaselineSettings
from netanomaly.db import sql_literal
from netanomaly.feature_registry import Registry, require_usable
from netanomaly.features import flows_glob
from netanomaly.schema import Contract

BASELINE_FEATURES = ("bytes_out_robust_z",)
MAD_TO_SIGMA = 1.4826  # MAD * 1.4826 estimates the standard deviation of normally distributed data


class BaselineQuality(StrEnum):
    """Which history a row's baseline came from, best first."""

    HOST = "host"
    PEER = "peer"
    GLOBAL = "global"
    NONE = "none"


def require_usable_inputs(registry: Registry, contract: Contract) -> None:
    """Refuse to build a baseline feature the registry does not rate usable against the contract."""
    require_usable(registry, contract, BASELINE_FEATURES)


def input_sql(lake: Path, window_minutes: int) -> str:
    """Host-window rows with the baseline inputs only: src_ip, window, bytes_out, src_subnet (peer group)."""
    bucket = f"time_bucket(INTERVAL '{int(window_minutes)} minutes', flow_start)"
    return f"""
SELECT
  src_ip,
  {bucket} AS window_start,
  sum(bytes) AS bytes_out,
  min(src_subnet) AS peer_group,  -- one subnet per host-window expected; min() keeps it deterministic
  CAST({bucket} AS DATE) AS flow_date
FROM read_parquet({sql_literal(flows_glob(lake))}, hive_partitioning = true)
GROUP BY ALL
"""


def day_sql(input_dir: Path, day: date, s: BaselineSettings) -> str:
    """Baseline and robust z for every host-window on `day`, from the `s.lookback_days` days before it."""
    src = f"read_parquet({sql_literal(input_dir / '**' / '*.parquet')}, hive_partitioning = true)"
    d = f"DATE {sql_literal(day.isoformat())}"
    value = "CASE WHEN bytes_out >= 0 THEN ln(1 + bytes_out::DOUBLE) END"

    def qualifies(p: str, hosts: bool) -> str:
        cond = f"{p}_windows >= {int(s.min_windows)} AND {p}_days >= {int(s.min_days)} AND {p}_mad > 0"
        return cond + (f" AND {p}_hosts >= {int(s.min_peer_hosts)}" if hosts else "")

    def pick(stat: str) -> str:
        return (f"CASE baseline_quality WHEN 'host' THEN host_{stat} WHEN 'peer' THEN peer_{stat} "
                f"WHEN 'global' THEN global_{stat} END")

    return f"""
WITH hist AS (  -- strictly earlier whole days only: [D - lookback, D - 1]
  SELECT src_ip, peer_group, flow_date, {value} AS v
  FROM {src}
  WHERE flow_date >= {d} - INTERVAL {int(s.lookback_days)} DAY AND flow_date < {d} AND {value} IS NOT NULL
),
host AS (
  SELECT src_ip, median(v) AS host_median, mad(v) AS host_mad, count(*) AS host_windows,
         count(DISTINCT flow_date) AS host_days
  FROM hist GROUP BY src_ip
),
peer AS (
  SELECT peer_group, median(v) AS peer_median, mad(v) AS peer_mad, count(*) AS peer_windows,
         count(DISTINCT flow_date) AS peer_days, count(DISTINCT src_ip) AS peer_hosts
  FROM hist WHERE peer_group IS NOT NULL GROUP BY peer_group
),
all_hosts AS (
  SELECT median(v) AS global_median, mad(v) AS global_mad, count(*) AS global_windows,
         count(DISTINCT flow_date) AS global_days, count(DISTINCT src_ip) AS global_hosts
  FROM hist
),
cur AS (
  SELECT src_ip, window_start, peer_group, bytes_out, {value} AS v FROM {src} WHERE flow_date = {d}
),
joined AS (
  SELECT cur.*, host.* EXCLUDE (src_ip), peer.* EXCLUDE (peer_group), all_hosts.*,
    CASE WHEN {qualifies('host', False)} THEN 'host'
         WHEN {qualifies('peer', True)} THEN 'peer'
         WHEN {qualifies('global', True)} THEN 'global'
         ELSE 'none' END AS baseline_quality
  FROM cur
  LEFT JOIN host USING (src_ip)
  LEFT JOIN peer USING (peer_group)
  CROSS JOIN all_hosts
),
picked AS (
  SELECT *, {pick('median')} AS baseline_median, {pick('mad')} AS baseline_mad
  FROM joined
)
SELECT
  src_ip, window_start, peer_group, bytes_out,
  (v - baseline_median) / ({MAD_TO_SIGMA} * baseline_mad) AS bytes_out_robust_z,
  baseline_quality, baseline_median, baseline_mad,
  coalesce({pick('windows')}, 0) AS baseline_windows,
  coalesce({pick('days')}, 0) AS baseline_days,
  coalesce(CASE baseline_quality WHEN 'host' THEN 1 WHEN 'peer' THEN peer_hosts
                WHEN 'global' THEN global_hosts END, 0) AS baseline_hosts,
  coalesce(host_windows, 0) AS host_windows,
  coalesce(host_days, 0) AS host_days
FROM picked
ORDER BY src_ip, window_start
"""


def build_host_baseline(con: duckdb.DuckDBPyConnection, lake: Path, out_dir: Path, work_dir: Path,
                        window_minutes: int, s: BaselineSettings) -> int:
    """Write `out_dir/flow_date=D/part-0.parquet` for every day in the lake. Returns the row count.

    Days are computed one at a time, so memory is bounded by `lookback_days + 1` days of host-window rows.
    The output is built in a staging directory and swapped in only when every day succeeded.
    """
    input_dir = work_dir / "baseline_input"
    staging = out_dir.with_name(out_dir.name + "._staging")
    for d in (input_dir, staging):
        shutil.rmtree(d, ignore_errors=True)
    input_dir.mkdir(parents=True)
    staging.mkdir(parents=True)
    try:
        con.execute(f"COPY ({input_sql(lake, window_minutes)}) TO {sql_literal(input_dir)} "
                    "(FORMAT parquet, PARTITION_BY (flow_date), OVERWRITE true)")
        days = [r[0] for r in con.execute(
            f"SELECT DISTINCT flow_date FROM read_parquet({sql_literal(input_dir / '**' / '*.parquet')}, "
            "hive_partitioning = true) ORDER BY 1").fetchall()]
        for day in days:
            part = staging / f"flow_date={day.isoformat()}"
            part.mkdir()
            con.execute(f"COPY ({day_sql(input_dir, day, s)}) TO {sql_literal(part / 'part-0.parquet')} "
                        "(FORMAT parquet, COMPRESSION zstd)")
        shutil.rmtree(out_dir, ignore_errors=True)
        staging.rename(out_dir)
    finally:
        shutil.rmtree(input_dir, ignore_errors=True)
        shutil.rmtree(staging, ignore_errors=True)
    if not days:
        return 0
    return con.execute(f"SELECT count(*) FROM read_parquet({sql_literal(out_dir / '**' / '*.parquet')})").fetchone()[0]
