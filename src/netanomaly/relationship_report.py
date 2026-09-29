"""Report section for the optional source -> destination analysis (used by the development report and by the
holdout-test / new-data scoring reports). Every number is aggregated in DuckDB (per day or ISO week, status counts,
LIMITed top-N tables); no pair table is loaded into Python."""

from __future__ import annotations

from pathlib import Path

import duckdb

from netanomaly import charts
from netanomaly.config import PocConfig
from netanomaly.db import sql_literal
from netanomaly.diagnostics import rows
from netanomaly.relationships import EVIDENCE, PAIR_WINDOWS, RelationshipResult

WEEKLY_AFTER_DAYS = 120  # longer spans are plotted per ISO week (a year -> ~52 points)
SIGNALS = (("never_seen", "novelty_status = 'never_seen'"), ("recently_unseen", "novelty_status = 'recently_unseen'"),
           ("frequency increase", "change_status = 'increase'"), ("frequency decrease", "change_status = 'decrease'"))


def _table(headers: list[str], body: list[list]) -> str:
    from netanomaly.report import _table as table

    return table(headers, body)


def _rel(path: Path) -> str:
    return f"read_parquet({sql_literal(path)})"


def _when(v) -> str:
    return "-" if v is None else f"{v:%Y-%m-%d %H:%M}"


def section(title: str, con: duckdb.DuckDBPyConnection, result: RelationshipResult, cfg: PocConfig,
            intervals: list, out: Path, *, model_features: list[str]) -> str:
    p, n = result.params, cfg.relationship_analysis.top_pairs
    pairs, evidence = _rel(result.path(PAIR_WINDOWS)), _rel(result.path(EVIDENCE))
    key = ", ".join(p.pair_cols)
    pair_kind = "(src, dst, dst_port, protocol)" if p.group_by_port_protocol else "(src, dst)"
    carried = f" (carried from `{result.carried_from}`)" if result.carried_from else ""
    head = [title, "",
            (f"Directed source -> destination pairs {pair_kind} per UTC window of {p.window_minutes} min, computed from "
             f"strictly earlier windows only. History starts {_when(result.history_start)} UTC{carried}."), "",
            (f"- *never seen*: no earlier window of the pair since the history start (not judged during the first "
             f"{p.warmup_days:g} days of history: `warmup`)."),
            (f"- *recently unseen*: seen before, but not within the last {p.recent_lookback_days:g} days (a separate "
             "signal from never seen)."),
            (f"- *frequency change*: log2((flows + 1) / (median flows + 1)) against the pair's own active windows in "
             f"the previous {p.baseline_lookback_days:g} days; judged only with >= {p.min_support_windows} such windows "
             f"(else `insufficient_history`); |log2| >= {p.change_log2_threshold:g} is an increase / decrease."),
            ("- Model inputs: " + (", ".join(f"`{f}`" for f in model_features) if model_features else
                                    "none (report-only: the anomaly scores do not use these signals)") + "."),
            "",
            ("These are review signals, not probabilities or confirmed attacks: new destinations and changed volumes "
             "are common in normal operations (new services, updates, backups)."), ""]
    if not result.pair_windows:
        return "\n".join([*head, "No pair activity in this period."])
    counts = {}
    for r in rows(con, f"SELECT novelty_status, change_status, count(*) AS n FROM {pairs} GROUP BY ALL"):
        for k in (("novelty", r["novelty_status"]), ("change", r["change_status"])):
            counts[k] = counts.get(k, 0) + r["n"]
    status_table = _table(["signal", "status", "pair-windows"], [[k, s, c] for (k, s), c in sorted(counts.items())])
    return "\n".join([*head, status_table, "", _trend(con, pairs, cfg, intervals, out), "",
                      _top_novel(con, evidence, key, n), "", _top_changes(con, evidence, key, n), "",
                      _top_hosts(con, pairs), "",
                      ("All pair-windows: `relationships/pair_windows.parquet`; notable ones with reasons: "
                       "`relationships/pair_evidence.parquet`; per host and window: "
                       "`relationships/host_windows.parquet`.")])


def _trend(con: duckdb.DuckDBPyConnection, pairs: str, cfg: PocConfig, intervals: list, out: Path) -> str:
    span = con.execute(f"SELECT datediff('day', min(window_start), max(window_start)) FROM {pairs}").fetchone()[0]
    bucket = "week" if span > WEEKLY_AFTER_DAYS else "day"
    parts = ", ".join(f"count(*) FILTER (WHERE {cond}) AS s{i}" for i, (_, cond) in enumerate(SIGNALS))
    got = rows(con, f"SELECT date_trunc('{bucket}', window_start) AS b, {parts} FROM {pairs} GROUP BY 1 ORDER BY 1")
    fmt = cfg.report.chart_format
    charts.relationship_trends(out / f"relationship_trends.{fmt}", [r["b"] for r in got],
                               {name: [r[f"s{i}"] for r in got] for i, (name, _) in enumerate(SIGNALS)},
                               intervals, bucket)
    return (f"![relationship trends](charts/relationship_trends.{fmt})\n\nPair-windows per UTC {bucket} with each "
            "signal (a warm-up period shows no never-seen counts by design).")


def _pair_cells(r: dict, key: str) -> list:
    return [r["src_ip"], r["dst_ip"], *([r["dst_port"], r["protocol"]] if "dst_port" in key else [])]


def _pair_headers(key: str) -> list[str]:
    return ["source", "destination", *(["dst port", "protocol"] if "dst_port" in key else [])]


def _top_novel(con: duckdb.DuckDBPyConnection, evidence: str, key: str, n: int) -> str:
    got = rows(con, f"SELECT * FROM {evidence} WHERE novelty_status IN ('never_seen', 'recently_unseen') "
                    f"ORDER BY flows DESC, coalesce(bytes, 0) DESC, window_start, {key} LIMIT {int(n)}")
    body = [[*_pair_cells(r, key), _when(r["window_start"]), r["novelty_status"], r["flows"], r["bytes"],
             _when(r["last_seen_before"]), r["reason"]] for r in got]
    return "\n".join([f"New and recently-unseen destinations with the most flows (top {len(got)}):", "",
                      _table([*_pair_headers(key), "window (UTC)", "status", "flows", "bytes", "last seen before",
                              "reason"], body) if body else "(none)"])


def _top_changes(con: duckdb.DuckDBPyConnection, evidence: str, key: str, n: int) -> str:
    got = rows(con, f"SELECT * FROM {evidence} WHERE change_status IN ('increase', 'decrease') "
                    f"ORDER BY abs(log2_change) DESC, flows DESC, window_start, {key} LIMIT {int(n)}")
    body = [[*_pair_cells(r, key), _when(r["window_start"]), r["change_status"], r["flows"],
             r["baseline_flows_median"], r["baseline_support"], r["log2_change"], r["reason"]] for r in got]
    return "\n".join([f"Largest pair-frequency changes (top {len(got)} by |log2 change|):", "",
                      _table([*_pair_headers(key), "window (UTC)", "change", "flows", "baseline median",
                              "support", "log2", "reason"], body) if body else "(none)"])


def _top_hosts(con: duckdb.DuckDBPyConnection, pairs: str) -> str:
    got = rows(con, f"""SELECT src_ip, count(*) FILTER (WHERE novelty_status = 'never_seen') AS never_seen,
  count(*) FILTER (WHERE novelty_status = 'recently_unseen') AS recently_unseen,
  count(*) FILTER (WHERE change_status = 'increase') AS increases,
  count(*) FILTER (WHERE change_status = 'decrease') AS decreases, count(DISTINCT window_start) AS windows
FROM {pairs} GROUP BY src_ip ORDER BY never_seen + recently_unseen DESC, increases DESC, src_ip LIMIT 10""")
    return "\n".join(["Sources with the most new / recently-unseen destinations:", "",
                      _table(["source", "never-seen pair-windows", "recently-unseen", "increases", "decreases",
                              "active windows"],
                             [[r["src_ip"], r["never_seen"], r["recently_unseen"], r["increases"], r["decreases"],
                               r["windows"]] for r in got])])
