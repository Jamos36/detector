"""Alert-budget selection (top-K per day) with provenance, and V0 recall@K.

Thresholds are an alert budget, not semantic severities: "the K most
anomalous host-windows per day" makes no claim about maliciousness.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from netanomaly.db import sql_literal
from netanomaly.features import flows_glob

SAMPLE_FLOW_IDS = 20


def _ranked_sql(scores: Path, k: int) -> str:
    return f"""
SELECT *, row_number() OVER (PARTITION BY flow_date ORDER BY anomaly_score DESC) AS rank_in_day
FROM read_parquet({sql_literal(scores)})
QUALIFY rank_in_day <= {int(k)}
"""


def write_top_alerts(con: duckdb.DuckDBPyConnection, scores: Path, lake: Path, k: int, window_minutes: int,
                     out_csv: Path) -> int:
    """Top-K host-windows per day, each linked to its source files and flow_ids."""
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
COPY (
  WITH ranked AS ({_ranked_sql(scores, k)}),
  prov AS (
    SELECT r.src_ip, r.window_start,
           list(DISTINCT f.source_file) AS source_files,
           list(f.flow_id ORDER BY f.flow_start)[1:{SAMPLE_FLOW_IDS}] AS sample_flow_ids
    FROM ranked r
    JOIN read_parquet({sql_literal(flows_glob(lake))}) f
      ON f.src_ip = r.src_ip
     AND time_bucket(INTERVAL '{int(window_minutes)} minutes', f.flow_start) = r.window_start
    GROUP BY ALL
  )
  SELECT ranked.*, prov.source_files, prov.sample_flow_ids
  FROM ranked JOIN prov USING (src_ip, window_start)
  ORDER BY flow_date, rank_in_day
) TO {sql_literal(out_csv)} (HEADER, DELIMITER ',')""")
    return con.execute(f"SELECT count(*) FROM read_csv({sql_literal(out_csv)})").fetchone()[0]


def recall_at_k(con: duckdb.DuckDBPyConnection, scores: Path, lake: Path, truth_dir: Path, k: int,
                window_minutes: int) -> list[tuple]:
    """Per attack type: injections with >=1 window inside the day's top-K, over all injections."""
    return con.execute(f"""
WITH truth AS (
  SELECT DISTINCT i.injection_id, m.attack_type, f.src_ip,
         time_bucket(INTERVAL '{int(window_minutes)} minutes', f.flow_start) AS window_start
  FROM read_csv({sql_literal(truth_dir / 'injected_flows.csv')}) i
  JOIN read_parquet({sql_literal(flows_glob(lake))}) f USING (flow_sequence)
  JOIN read_csv({sql_literal(truth_dir / 'injections.csv')}) m USING (injection_id)
),
hits AS (
  SELECT t.injection_id, t.attack_type, bool_or(r.rank_in_day IS NOT NULL) AS detected,
         min(r.rank_in_day) AS best_rank
  FROM truth t LEFT JOIN ({_ranked_sql(scores, k)}) r USING (src_ip, window_start)
  GROUP BY ALL
)
SELECT attack_type, sum(detected::INT) AS detected, count(*) AS injected, min(best_rank) AS best_rank
FROM hits GROUP BY 1 ORDER BY 1""").fetchall()
