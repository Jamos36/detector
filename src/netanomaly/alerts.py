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


def _check_truth_join(con: duckdb.DuckDBPyConnection, lake: Path, truth_dir: Path) -> None:
    """Every truth flow_sequence must match exactly one lake flow.

    flow_sequence is unique only within one synthetic `generate` run; real exporters give no such guarantee
    (unverified), and a lake holding other files (another generate run, other exports) can repeat it.
    """
    missing, repeated = con.execute(f"""
WITH truth AS (SELECT DISTINCT flow_sequence FROM read_csv({sql_literal(truth_dir / 'injected_flows.csv')})),
matches AS (
  SELECT t.flow_sequence, count(f.flow_sequence) AS n
  FROM truth t LEFT JOIN read_parquet({sql_literal(flows_glob(lake))}) f ON f.flow_sequence = t.flow_sequence
  GROUP BY ALL
)
SELECT count(*) FILTER (WHERE n = 0), count(*) FILTER (WHERE n > 1) FROM matches""").fetchone()
    problems = [f"{n} truth flow_sequence value(s) {what}"
                for n, what in ((missing, "match no lake flow"), (repeated, "match more than one lake flow")) if n]
    if problems:
        raise ValueError("; ".join(problems) + f" in {lake}. recall@K needs a lake built only from the "
                         "synthetic files of the generate run that wrote the truth.")


def recall_at_k(con: duckdb.DuckDBPyConnection, scores: Path, lake: Path, truth_dir: Path, k: int,
                window_minutes: int) -> list[tuple]:
    """Per attack type: injections with >=1 window inside the day's top-K, over all injections.

    Synthetic evaluation only: injected flows are joined to the lake by flow_sequence, which the generator
    makes unique per run. Production code must not rely on flow_sequence; flow_id is the traceability key.
    """
    _check_truth_join(con, lake, truth_dir)
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
