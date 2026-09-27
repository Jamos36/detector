"""Per-batch data-quality report (V1-2).

For one ingest batch: plausibility checks on its flows, null rates, rejects by
reason, column drift of its raw files against the schema contract, and daily
volume against the median of strictly earlier days. DuckDB does every scan;
Python only receives aggregates (one row per check, column, reason or day).

Counts are observations, not verdicts: most checks rest on field meanings that
are still unverified (SCHEMA.md), and several can be legitimate traffic.
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path

import duckdb

from netanomaly.config import DQSettings
from netanomaly.db import sql_literal
from netanomaly.features import flows_glob
from netanomaly.ingest import (
    FLOWS_DIR,
    FileFormat,
    LedgerEntry,
    Status,
    detect_format,
    file_sha256,
    flow_files,
    read_header,
    read_ledger,
    rejects_path,
)
from netanomaly.schema import Contract

REPORT_DIR = "dq"
VLAN_MAX = 4094  # highest valid 802.1Q VLAN ID
MAX_BYTES_PER_PACKET = 1514  # full untagged Ethernet frame without FCS
SYN_ONLY_MAX_PACKETS = 3  # one SYN plus a few retransmissions
INTEGER_TYPES = frozenset({"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
                           "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"})


@dataclass(frozen=True)
class Check:
    name: str
    applies_to: str  # SQL predicate over lake columns: rows for which the check is meaningful
    violation: str  # SQL predicate, evaluated only where applies_to holds
    note: str


CHECKS: tuple[Check, ...] = (
    Check("flow_end_before_start", "flow_start IS NOT NULL AND flow_end IS NOT NULL", "flow_end < flow_start",
          "Impossible for first/last packet times from one clock."),
    Check("ttl_min_gt_ttl_max", "ttl_min IS NOT NULL AND ttl_max IS NOT NULL", "ttl_min > ttl_max",
          "Impossible by definition of min/max."),
    Check("bytes_per_packet_gt_1514", "packets > 0 AND bytes IS NOT NULL",
          f"bytes > {MAX_BYTES_PER_PACKET} * packets",
          "Above a full untagged Ethernet frame: jumbo frames, tags or offload, otherwise a counter/unit problem."),
    Check("syn_only_gt_3_packets", "protocol = 6",
          f"tcp_syn AND NOT (tcp_ack OR tcp_fin OR tcp_rst OR tcp_psh) AND packets > {SYN_ONLY_MAX_PACKETS}",
          "Unanswered handshakes rarely exceed a few SYNs. tcp_flag is low confidence (one label, maybe sampled)."),
    Check("icmp_with_ports", "protocol = 1", "coalesce(src_port, 0) <> 0 OR coalesce(dst_port, 0) <> 0",
          "ICMP has no ports; some exporters encode type/code in dst_port (type*256+code)."),
    Check("ingress_eq_egress", "ingress_if IS NOT NULL AND egress_if IS NOT NULL", "ingress_if = egress_if",
          "Hairpinning is possible but rare; a high rate suggests the interface fields mean something else."),
    *(Check(f"{col}_gt_{VLAN_MAX}", f"{col} IS NOT NULL", f"{col} > {VLAN_MAX}", "802.1Q VLAN IDs end at 4094.")
      for col in ("vlan_id", "vlan_id_dot1q", "vlan_id_customer")),
)


class VolumeStatus(StrEnum):
    OK = "ok"
    LOW = "low"
    HIGH = "high"
    INSUFFICIENT_HISTORY = "insufficient_history"


@dataclass(frozen=True)
class CheckResult:
    name: str
    applicable: int
    violations: int
    rate: float | None  # violations / applicable
    example: str | None  # "source_file:source_row_number" of the first violating row
    note: str


@dataclass(frozen=True)
class NullRate:
    column: str
    nulls: int
    rate: float | None


@dataclass(frozen=True)
class RejectCount:
    reason: str
    column_name: str | None
    rows: int


@dataclass(frozen=True)
class DayVolume:
    flow_date: date
    flows: int
    trailing_median: float | None
    history_days: int  # calendar days in the trailing window (days without flows count as 0)
    ratio: float | None
    status: VolumeStatus


@dataclass(frozen=True)
class TypeMismatch:
    column: str
    contract_type: str
    found_type: str
    kind: str  # "width": same family (cast on ingest; out-of-range values are rejected) | "family": different


@dataclass(frozen=True)
class FileDrift:
    source_file: str
    status: str
    missing: list[str]
    unexpected: list[str]
    type_mismatches: list[TypeMismatch]
    note: str


@dataclass(frozen=True)
class FileSummary:
    source_file: str
    status: str
    rows: int
    rejected_rows: int
    reason: str


@dataclass(frozen=True)
class QualityReport:
    batch_id: str
    generated_at: str
    flows: int
    rejected_rows: int
    files: list[FileSummary]
    checks: list[CheckResult]
    null_rates: list[NullRate]
    rejects: list[RejectCount]
    volume: list[DayVolume]
    drift: list[FileDrift]


# --- batch selection -------------------------------------------------------------

def batch_entries(lake: Path, batch_id: str) -> list[LedgerEntry]:
    """Files whose latest ledger entry belongs to this batch (a later re-ingest moves a file to its batch)."""
    entries = (e for e in read_ledger(lake).values() if e.ingest_batch_id == batch_id)
    return sorted((e for e in entries if e.status != Status.IN_PROGRESS), key=lambda e: e.source_file)


def latest_batch(lake: Path) -> str | None:
    """Newest batch id in the ledger (ids are UTC timestamps, so they sort in time order)."""
    return max((e.ingest_batch_id for e in read_ledger(lake).values() if e.status != Status.IN_PROGRESS), default=None)


# --- flow checks, null rates, rejects ---------------------------------------------

def _file_list(files: list[Path]) -> str:
    return "[" + ", ".join(sql_literal(f) for f in files) + "]"


def _rate(n: int, d: int) -> float | None:
    return n / d if d else None


def _flow_stats(
    con: duckdb.DuckDBPyConnection, contract: Contract, files: list[Path], batch_id: str
) -> tuple[int, list[CheckResult], list[NullRate]]:
    """One DuckDB pass over the batch's flows: row count, every check, every column's nulls."""
    if not files:
        return 0, [CheckResult(c.name, 0, 0, None, None, c.note) for c in CHECKS], [
            NullRate(c.name, 0, None) for c in contract.columns
        ]
    exprs = ["count(*)"]
    for c in CHECKS:
        bad = f"({c.applies_to}) AND ({c.violation})"
        exprs += [
            f"count_if({c.applies_to})",
            f"count_if({bad})",
            (f"arg_min(source_file || ':' || source_row_number, {{'f': source_file, 'r': source_row_number}}) "
             f"FILTER (WHERE {bad})"),
        ]
    exprs += [f'count(*) - count("{col.name}")' for col in contract.columns]
    total, *values = con.execute(
        f"SELECT {', '.join(exprs)} FROM read_parquet({_file_list(files)}) "
        f"WHERE ingest_batch_id = {sql_literal(batch_id)}"
    ).fetchone()
    per_check = [values[3 * i: 3 * i + 3] for i in range(len(CHECKS))]
    checks = [
        CheckResult(c.name, app, bad, _rate(bad, app), example, c.note)
        for c, (app, bad, example) in zip(CHECKS, per_check, strict=True)
    ]
    nulls = [
        NullRate(col.name, n, _rate(n, total))
        for col, n in zip(contract.columns, values[3 * len(CHECKS):], strict=True)
    ]
    return total, checks, nulls


def _reject_counts(
    con: duckdb.DuckDBPyConnection, lake: Path, entries: list[LedgerEntry], batch_id: str
) -> list[RejectCount]:
    files = [p for e in entries if (p := rejects_path(lake, e.file_hash)).exists()]
    if not files:
        return []
    rows = con.execute(f"""
SELECT reason, column_name, count(DISTINCT source_file_hash || ':' || source_row_number) AS n
FROM read_parquet({_file_list(files)})
WHERE ingest_batch_id = {sql_literal(batch_id)}
GROUP BY ALL
ORDER BY n DESC, reason, column_name NULLS FIRST""").fetchall()
    return [RejectCount(reason, column, n) for reason, column, n in rows]


# --- daily volume ---------------------------------------------------------------

def _daily_counts(con: duckdb.DuckDBPyConnection, lake: Path) -> dict[date, int]:
    """Flows per UTC day across the whole lake (all batches): one small row per day."""
    if not any((lake / FLOWS_DIR).glob("flow_date=*/*.parquet")):
        return {}
    return dict(con.execute(
        f"SELECT flow_date, count(*) FROM read_parquet({sql_literal(flows_glob(lake))}, hive_partitioning = true) "
        "GROUP BY flow_date"
    ).fetchall())


def _day_volume(day: date, counts: dict[date, int], first_day: date, s: DQSettings) -> DayVolume:
    # Strictly earlier calendar days, not before the lake's first day; missing days count as 0 flows.
    start = max(first_day, day - timedelta(days=s.trailing_days))
    history = [counts.get(start + timedelta(days=i), 0) for i in range((day - start).days)]
    flows = counts.get(day, 0)
    median = float(statistics.median(history)) if history else None
    ratio = flows / median if median else None
    if len(history) < s.min_history_days:
        status = VolumeStatus.INSUFFICIENT_HISTORY
    elif ratio is None:  # trailing window has no flows at all
        status = VolumeStatus.HIGH if flows else VolumeStatus.OK
    elif ratio < s.volume_ratio_low:
        status = VolumeStatus.LOW
    elif ratio > s.volume_ratio_high:
        status = VolumeStatus.HIGH
    else:
        status = VolumeStatus.OK
    return DayVolume(day, flows, median, len(history), ratio, status)


def daily_volume(con: duckdb.DuckDBPyConnection, lake: Path, days: list[date], s: DQSettings) -> list[DayVolume]:
    """Each day's flow count against the median of the previous `trailing_days` days (no future data)."""
    counts = _daily_counts(con, lake)
    if not counts:
        return [DayVolume(d, 0, None, 0, None, VolumeStatus.INSUFFICIENT_HISTORY) for d in sorted(days)]
    first_day = min(counts)
    return [_day_volume(d, counts, first_day, s) for d in sorted(days)]


def _partition_date(path: Path) -> date:
    return date.fromisoformat(path.parent.name.removeprefix("flow_date="))


# --- column drift ---------------------------------------------------------------

def _type_family(duck_type: str) -> str:
    if duck_type in INTEGER_TYPES:
        return "integer"
    if duck_type.startswith(("FLOAT", "DOUBLE", "DECIMAL")):
        return "float"
    return duck_type  # VARCHAR, TIMESTAMP WITH TIME ZONE, TIMESTAMP (no zone), ... compared exactly


def _contract_types(con: duckdb.DuckDBPyConnection, contract: Contract) -> dict[str, tuple[str, str]]:
    """raw column -> (type as written in the contract, DuckDB's canonical spelling of it)."""
    spelled = {t: con.execute(f"SELECT typeof(CAST(NULL AS {t}))").fetchone()[0] for t in {c.type for c in contract.columns}}
    return {c.raw: (c.type, spelled[c.type]) for c in contract.columns}


def _file_drift(
    con: duckdb.DuckDBPyConnection,
    contract: Contract,
    types: dict[str, tuple[str, str]],
    entry: LedgerEntry,
    candidates: list[Path],
) -> FileDrift:
    def drift(missing: list[str], unexpected: list[str], mismatches: list[TypeMismatch], note: str) -> FileDrift:
        return FileDrift(entry.source_file, entry.status, missing, unexpected, mismatches, note)

    path = next((p for p in candidates if file_sha256(p) == entry.file_hash), None)
    if path is None:
        return drift([], [], [], f"raw file not found under the raw directory (hash {entry.file_hash[:12]}); not checked")
    fmt = detect_format(path)
    if fmt is FileFormat.UNKNOWN:
        return drift([], [], [], "not parquet or csv; not checked")
    missing, unexpected = contract.check_columns(read_header(path, fmt))
    if fmt is FileFormat.CSV:
        return drift(missing, unexpected, [], "CSV is untyped: type problems appear as cast_failed rejects")
    found = {row[0]: row[1] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_literal(path)})").fetchall()}
    mismatches = [
        TypeMismatch(col, written, got, "width" if _type_family(got) == _type_family(canonical) else "family")
        for col, (written, canonical) in types.items()
        if (got := found.get(col)) is not None and got != canonical
    ]
    return drift(missing, unexpected, mismatches, "")


def column_drift(
    con: duckdb.DuckDBPyConnection, contract: Contract, raw_dir: Path, entries: list[LedgerEntry]
) -> list[FileDrift]:
    """Header and (Parquet) physical types of each batch file against the contract.

    The ledger stores file names and hashes, not paths, so raw files are found by name and confirmed by hash.
    """
    by_name: dict[str, list[Path]] = defaultdict(list)
    for p in raw_dir.rglob("*") if raw_dir.exists() else ():
        if p.is_file():
            by_name[p.name].append(p)
    types = _contract_types(con, contract)
    return [_file_drift(con, contract, types, e, by_name[e.source_file]) for e in entries]


# --- report -----------------------------------------------------------------------

def build_report(
    con: duckdb.DuckDBPyConnection, contract: Contract, lake: Path, raw_dir: Path, batch_id: str, s: DQSettings
) -> QualityReport:
    entries = batch_entries(lake, batch_id)
    ingested = [e for e in entries if e.status == Status.INGESTED]
    files = [f for e in ingested for f in flow_files(lake, e.file_hash)]
    flows, checks, nulls = _flow_stats(con, contract, files, batch_id)
    return QualityReport(
        batch_id=batch_id,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        flows=flows,
        rejected_rows=sum(e.rejected_rows for e in ingested),
        files=[FileSummary(e.source_file, e.status, e.rows, e.rejected_rows, e.reason) for e in entries],
        checks=checks,
        null_rates=nulls,
        rejects=_reject_counts(con, lake, ingested, batch_id),
        volume=daily_volume(con, lake, sorted({_partition_date(f) for f in files}), s),
        drift=column_drift(con, contract, raw_dir, entries),
    )


def write_report(report: QualityReport, out_dir: Path) -> tuple[Path, Path]:
    """Write dq_<batch>.json (machine-readable) and dq_<batch>.md (for people)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path, md_path = out_dir / f"dq_{report.batch_id}.json", out_dir / f"dq_{report.batch_id}.md"
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


# --- markdown ---------------------------------------------------------------------

def _pct(rate: float | None) -> str:
    return "–" if rate is None else f"{rate:.2%}"


def _table(header: list[str], rows: list[list[object]]) -> list[str]:
    def cell(v: object) -> str:
        return "–" if v is None else str(v).replace("|", "\\|").replace("\n", " ")

    return [
        "| " + " | ".join(header) + " |",
        "|" + "---|" * len(header),
        *("| " + " | ".join(cell(v) for v in row) + " |" for row in rows),
        "",
    ]


def _md_files(r: QualityReport) -> list[str]:
    rows = [[f.source_file, f.status, f.rows, f.rejected_rows, f.reason] for f in r.files]
    return ["## Files", "", *_table(["file", "status", "rows", "rejected", "reason"], rows)]


def _md_checks(r: QualityReport) -> list[str]:
    rows = [[c.name, c.applicable, c.violations, _pct(c.rate), c.example, c.note] for c in r.checks]
    return ["## Plausibility checks", "",
            *_table(["check", "applicable", "violations", "rate", "example", "note"], rows)]


def _md_nulls(r: QualityReport) -> list[str]:
    rows = [[n.column, n.nulls, _pct(n.rate)] for n in r.null_rates if n.nulls]
    body = _table(["column", "nulls", "rate"], rows) if rows else []
    clean = sum(1 for n in r.null_rates if not n.nulls)
    return ["## Null rates", "", *body, f"Columns without nulls: {clean} of {len(r.null_rates)}.", ""]


def _md_rejects(r: QualityReport) -> list[str]:
    rows = [[x.reason, x.column_name, x.rows] for x in r.rejects]
    body = _table(["reason", "column", "rows"], rows) if rows else ["No rejected rows in ingested files.", ""]
    return ["## Rejects", "", "Rows in `lake/rejects/` for this batch; quarantined files are listed under Files.", "",
            *body]


def _md_volume(r: QualityReport) -> list[str]:
    rows = [[v.flow_date, v.flows, None if v.trailing_median is None else f"{v.trailing_median:.1f}", v.history_days,
             None if v.ratio is None else f"{v.ratio:.2f}", v.status] for v in r.volume]
    return ["## Daily volume", "",
            ("Flows per UTC day in the whole lake vs the median of strictly earlier days (missing days count as 0). "
             "The first and last day of a capture are often partial."), "",
            *_table(["date", "flows", "trailing median", "history days", "ratio", "status"], rows)]


def _md_drift(r: QualityReport) -> list[str]:
    rows = []
    for d in r.drift:
        family = ", ".join(f"{m.column} {m.found_type} (contract {m.contract_type})"
                           for m in d.type_mismatches if m.kind == "family")
        width = sum(m.kind == "width" for m in d.type_mismatches)
        rows.append([d.source_file, d.status, ", ".join(d.missing), ", ".join(d.unexpected), family, width, d.note])
    header = ["file", "status", "missing", "unexpected", "type family differs", "width differs", "note"]
    return ["## Column drift", "", *_table(header, rows)]


def render_markdown(r: QualityReport) -> str:
    intro = [
        f"# Data-quality report — batch {r.batch_id}",
        "",
        (f"Generated {r.generated_at}. {r.flows} flows ingested, {r.rejected_rows} rows rejected "
         f"({_pct(_rate(r.rejected_rows, r.flows + r.rejected_rows))})."),
        "",
        ("Counts are observations, not verdicts: field meanings are unverified (see SCHEMA.md) and some "
         "violations are legitimate traffic."),
        "",
    ]
    sections = _md_files(r) + _md_checks(r) + _md_nulls(r) + _md_rejects(r) + _md_volume(r) + _md_drift(r)
    return "\n".join(intro + sections)
