"""V0 behavioural features: source host x time window.

Deliberately small. V2 replaces this with a feature registry, host-relative
baselines and per-feature evaluation.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from netanomaly.db import sql_literal

HOST_WINDOW_FEATURES = (
    "flows", "bytes_out", "packets_out", "uniq_dst_ip", "uniq_dst_port",
    "syn_only_ratio", "rst_ratio", "internal_ratio", "max_flow_bytes",
)


def flows_glob(lake: Path) -> str:
    return (lake / "flows" / "**" / "*.parquet").as_posix()


def host_window_sql(lake: Path, window_minutes: int) -> str:
    return f"""
SELECT
  src_ip,
  time_bucket(INTERVAL '{int(window_minutes)} minutes', flow_start) AS window_start,
  count(*) AS flows,
  sum(bytes) AS bytes_out,
  sum(packets) AS packets_out,
  count(DISTINCT dst_ip) AS uniq_dst_ip,
  count(DISTINCT dst_port) AS uniq_dst_port,
  avg((tcp_syn AND NOT tcp_ack)::DOUBLE) AS syn_only_ratio,
  avg(tcp_rst::DOUBLE) AS rst_ratio,
  avg(dst_is_private::DOUBLE) AS internal_ratio,
  max(bytes) AS max_flow_bytes,
  CAST(time_bucket(INTERVAL '{int(window_minutes)} minutes', flow_start) AS DATE) AS flow_date
FROM read_parquet({sql_literal(flows_glob(lake))}, hive_partitioning = true)
GROUP BY ALL
"""


def build_host_window(con: duckdb.DuckDBPyConnection, lake: Path, out_dir: Path, window_minutes: int) -> int:
    """Aggregate flows to host-window rows, written partitioned by date. Returns row count.

    The output directory is rebuilt from scratch on every run.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({host_window_sql(lake, window_minutes)}) TO {sql_literal(out_dir)} "
        "(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (flow_date), OVERWRITE true)"
    )
    return con.execute(f"SELECT count(*) FROM read_parquet({sql_literal(out_dir / '**' / '*.parquet')})").fetchone()[0]
