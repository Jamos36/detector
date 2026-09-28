"""Synthetic ground truth for evaluation (V2-5): truth-to-lake mapping check and host-window labels.

Evaluation only; never an input to features, scores or alerts. The generator's truth files name injected flows by
`flow_sequence`, which is unique within one synthetic `generate` run only (ADR-016), so every use starts with
`verify_truth`, which refuses a lake the truth does not map onto one-to-one.

Label alignment:

- **Flow level**: a flow is positive for attack type A when its `flow_sequence` is in `truth/injected_flows.csv` with
  an injection of type A. No flow-level feature is implemented yet (V2-1 candidates only), so no flow-level metric
  is computed.
- **Host-window level**: a host-window (`src_ip`, `time_bucket(window_minutes, flow_start)`) is positive for A when it
  contains at least one injected flow of A. It is **negative** only when it contains no injected flow of any type.
  A positive window can also hold the host's own benign flows (`purity` = injected / all flows in the window).
- The label marks the window the injected flows fall in. Prior-history features describe earlier data (timing:
  the 2 h before the window), so they can lag the label; that is a property of the feature, reported, not corrected.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from netanomaly.alerts import check_truth_join
from netanomaly.db import sql_literal
from netanomaly.features import flows_glob

TRUTH_FLOWS = "injected_flows.csv"
INJECTIONS = "injections.csv"


@dataclass(frozen=True)
class TruthCheck:
    """Counts from mapping truth onto the lake; every mismatch count must be 0."""

    injections: int
    truth_flows: int
    matched_flows: int
    unknown_injections: int  # injection_id in injected_flows.csv without a row in injections.csv
    src_mismatches: int  # matched lake flow whose src_ip differs from the injection's src_ip
    count_mismatches: int  # injections whose matched flow count differs from n_flows
    attack_days: tuple[date, ...]

    @property
    def problems(self) -> list[str]:
        checks = (
            (self.truth_flows - self.matched_flows, "truth flows without a lake flow"),
            (self.unknown_injections, "truth flows naming an unknown injection_id"),
            (self.src_mismatches, "lake flows whose src_ip differs from their injection's src_ip"),
            (self.count_mismatches, "injections whose lake flow count differs from n_flows"),
        )
        return [f"{n} {what}" for n, what in checks if n]


def has_truth(truth_dir: Path) -> bool:
    return (truth_dir / TRUTH_FLOWS).is_file() and (truth_dir / INJECTIONS).is_file()


def _truth_flows_sql(lake: Path, truth_dir: Path) -> str:
    return f"""
SELECT t.injection_id, m.attack_type, m.src_ip AS truth_src_ip, f.src_ip, f.flow_start
FROM read_csv({sql_literal(truth_dir / TRUTH_FLOWS)}) t
LEFT JOIN read_parquet({sql_literal(flows_glob(lake))}) f USING (flow_sequence)
LEFT JOIN read_csv({sql_literal(truth_dir / INJECTIONS)}) m USING (injection_id)"""


def verify_truth(con: duckdb.DuckDBPyConnection, lake: Path, truth_dir: Path) -> TruthCheck:
    """Check that the truth maps onto this lake one-to-one and consistently; raise ValueError otherwise."""
    check_truth_join(con, lake, truth_dir)  # every truth flow_sequence matches exactly one lake flow
    injections, truth_flows, matched, unknown, src_bad, days = con.execute(f"""
WITH tf AS ({_truth_flows_sql(lake, truth_dir)})
SELECT (SELECT count(*) FROM read_csv({sql_literal(truth_dir / INJECTIONS)})),
       count(*), count(src_ip), count(*) FILTER (WHERE attack_type IS NULL),
       count(*) FILTER (WHERE src_ip IS DISTINCT FROM truth_src_ip AND src_ip IS NOT NULL),
       list(DISTINCT CAST(flow_start AS DATE) ORDER BY CAST(flow_start AS DATE))
         FILTER (WHERE flow_start IS NOT NULL)
FROM tf""").fetchone()
    (count_bad,) = con.execute(f"""
WITH tf AS ({_truth_flows_sql(lake, truth_dir)}),
got AS (SELECT injection_id, count(src_ip) AS n FROM tf GROUP BY ALL)
SELECT count(*) FROM read_csv({sql_literal(truth_dir / INJECTIONS)}) m LEFT JOIN got USING (injection_id)
WHERE coalesce(got.n, 0) <> m.n_flows""").fetchone()
    check = TruthCheck(injections, truth_flows, matched, unknown, src_bad, count_bad, tuple(days or ()))
    if check.problems:
        raise ValueError("truth does not map onto the lake: " + "; ".join(check.problems))
    return check


def window_labels_sql(lake: Path, truth_dir: Path, window_minutes: int) -> str:
    """One row per host-window containing injected flows: `attack_types` (sorted list) and `injected_flows`.

    Windows absent from this result contain no injected flow. The window is the V0 grid bucket of `flow_start`.
    """
    bucket = f"time_bucket(INTERVAL '{int(window_minutes)} minutes', flow_start)"
    return f"""
SELECT src_ip, {bucket} AS window_start,
       list(DISTINCT attack_type ORDER BY attack_type) AS attack_types, count(*) AS injected_flows
FROM ({_truth_flows_sql(lake, truth_dir)})
GROUP BY ALL"""
