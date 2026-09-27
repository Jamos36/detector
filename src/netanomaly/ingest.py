"""Raw file -> normalized, date-partitioned Parquet lake with row-level provenance.

Rules:
- File type is detected from content (Parquet magic bytes), not the extension.
- Raw files are never modified; unreadable files are recorded as quarantined.
- Bad rows are rejected one by one with a reason code (lake/rejects/), not by
  quarantining the whole file; a file is quarantined only when the rejected
  fraction exceeds `max_reject_fraction` (then it is a file problem, not a row problem).
- Types come from the schema contract (no inference): CSV is staged as text and
  cast with TRY_CAST, so one bad value rejects one row.
- Every output row carries flow_id, source_file, source_row_number,
  source_file_hash and ingest_batch_id, so any anomaly traces back to its
  original record.
- A ledger keyed by file hash makes re-runs skip finished files (resume).
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import shutil
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from netanomaly.db import sql_literal as _sql_str
from netanomaly.schema import Contract

log = logging.getLogger(__name__)

PARQUET_MAGIC = b"PAR1"
HASH_CHUNK = 1 << 20
LEDGER_NAME = "_ingest_ledger.jsonl"
FLOWS_DIR = "flows"
PRIVATE_IP_REGEX = r"^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)"
TCP_FLAGS = ("SYN", "ACK", "FIN", "RST", "PSH")
HASH_PREFIX = 32  # hex chars (128 bits) of the file hash used in output file names
STAGING_DIR = "_staging"
REJECTS_DIR = "rejects"
DEFAULT_MAX_REJECT_FRACTION = 0.05
MAX_RAW_VALUE_CHARS = 1000  # raw text kept per reject: enough to diagnose without copying huge lines
# Canonical columns without which a flow cannot be dated (partition) or attributed to hosts.
REQUIRED_COLUMNS = ("flow_start", "flow_end", "src_ip", "dst_ip")


class FileFormat(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"
    UNKNOWN = "unknown"


class RejectReason(StrEnum):
    """Why a row was rejected. The CSV structure codes are DuckDB reader error types, snake_cased."""

    TOO_MANY_COLUMNS = "too_many_columns"
    MISSING_COLUMNS = "missing_columns"
    UNQUOTED_VALUE = "unquoted_value"
    LINE_SIZE_OVER_MAXIMUM = "line_size_over_maximum"
    INVALID_ENCODING = "invalid_encoding"
    INVALID_STATE = "invalid_state"
    CAST_FAILED = "cast_failed"
    MISSING_REQUIRED = "missing_required"


class Status(StrEnum):
    IN_PROGRESS = "in_progress"
    INGESTED = "ingested"
    QUARANTINED = "quarantined"
    SKIPPED_DUPLICATE = "skipped_duplicate"


@dataclass(frozen=True)
class LedgerEntry:
    source_file: str
    file_hash: str
    fmt: str
    status: str
    rows: int
    reason: str
    ingest_batch_id: str
    recorded_at: str
    rejected_rows: int = 0  # default keeps ledger lines written before row-level rejects readable


def detect_format(path: Path) -> FileFormat:
    """Classify by content: Parquet starts and ends with PAR1; CSV is UTF-8 text with commas."""
    size = path.stat().st_size
    if size == 0:
        return FileFormat.UNKNOWN
    with path.open("rb") as fh:
        head = fh.read(4096)
        if size >= 12 and head[:4] == PARQUET_MAGIC:
            fh.seek(-4, 2)
            if fh.read(4) == PARQUET_MAGIC:
                return FileFormat.PARQUET
    try:
        text = head.decode("utf-8")
    except UnicodeDecodeError:
        return FileFormat.UNKNOWN
    first_line = text.splitlines()[0] if text else ""
    return FileFormat.CSV if "," in first_line and "\x00" not in text else FileFormat.UNKNOWN


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(HASH_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def read_header(path: Path, fmt: FileFormat) -> list[str]:
    if fmt is FileFormat.PARQUET:
        return list(pq.read_schema(path).names)
    # Decode only the header line: a bad byte in a data row is a row reject, not a crash here.
    with path.open("rb") as fh:
        first = fh.readline().decode("utf-8", errors="replace")
    return next(csv.reader([first]), [])


def select_sources(paths: Iterable[Path]) -> tuple[list[Path], list[Path]]:
    """Pick one file per stem: Parquet wins over a same-named CSV (already converted).

    Returns (to_ingest, skipped_duplicates).
    """
    by_stem: dict[tuple[Path, str], list[Path]] = {}
    for p in sorted(paths):
        by_stem.setdefault((p.parent, p.stem), []).append(p)
    chosen, skipped = [], []
    for group in by_stem.values():
        parquet = [p for p in group if p.suffix.lower() == ".parquet"]
        keep = parquet[0] if parquet else group[0]
        chosen.append(keep)
        skipped.extend(p for p in group if p != keep)
    return chosen, skipped


def read_ledger(lake: Path) -> dict[str, LedgerEntry]:
    """Latest ledger entry per file hash."""
    ledger = lake / LEDGER_NAME
    entries: dict[str, LedgerEntry] = {}
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = LedgerEntry(**json.loads(line))
            except (json.JSONDecodeError, TypeError) as exc:
                # e.g. a line truncated by a crash mid-append: skip it, so that file is simply retried
                log.warning("skipping unreadable ledger line in %s: %s", ledger, exc)
                continue
            entries[entry.file_hash] = entry
    return entries


def append_ledger(lake: Path, entry: LedgerEntry) -> None:
    lake.mkdir(parents=True, exist_ok=True)
    with (lake / LEDGER_NAME).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry)) + "\n")
        fh.flush()


def _flow_id_sql(file_hash: str) -> str:
    return f"left(sha256({_sql_str(file_hash)} || ':' || source_row_number::VARCHAR), 32)"


def _problem_checks(contract: Contract) -> str:
    """SQL list of a row's problems (NULLs filtered out): uncastable values and empty required columns."""
    checks = []
    for c in contract.columns:
        raw = f'"{c.raw}"'
        if c.type != "VARCHAR":
            checks.append(
                f"CASE WHEN {raw} IS NOT NULL AND TRY_CAST({raw} AS {c.type}) IS NULL THEN "
                f"{{'reason': '{RejectReason.CAST_FAILED}', 'column_name': {_sql_str(c.raw)}, "
                f"'raw_value': left(CAST({raw} AS VARCHAR), {MAX_RAW_VALUE_CHARS}), "
                f"'detail': {_sql_str(f'not castable to {c.type}')}}} END"
            )
        if c.name in REQUIRED_COLUMNS:
            # a blank string is no more an IP than NULL; blank non-text values already fail TRY_CAST
            empty = f"{raw} IS NULL OR trim(CAST({raw} AS VARCHAR)) = ''" if c.type == "VARCHAR" else f"{raw} IS NULL"
            checks.append(
                f"CASE WHEN {empty} THEN {{'reason': '{RejectReason.MISSING_REQUIRED}', "
                f"'column_name': {_sql_str(c.raw)}, 'raw_value': CAST(NULL AS VARCHAR), "
                f"'detail': 'required column is empty'}} END"
            )
    return "list_filter([\n    " + ",\n    ".join(checks) + "\n  ], p -> p IS NOT NULL)"


def _checked_rows_ctes(contract: Contract, source: str, structural_rejects: str | None) -> str:
    """CTEs ending in `checked`: every source row with its original 1-based row number and its `_problems`.

    CSV records DuckDB could not split into columns never reach the staged file, so staged
    position `_k` is shifted past the rejected records before it: ASOF join on `kept_before`
    (good rows preceding each run of rejects). Row numbers count CSV records, so a quoted
    field spanning lines does not shift them.
    """
    src = f"SELECT *, file_row_number + 1 AS _k FROM read_parquet({_sql_str(source)}, file_row_number = true)"
    if structural_rejects is None:
        offsets = ""
        numbered = "SELECT *, _k AS source_row_number FROM src"
    else:
        offsets = f"""
bad AS (
  SELECT source_row_number, row_number() OVER (ORDER BY source_row_number) AS skipped
  FROM (SELECT DISTINCT source_row_number FROM {structural_rejects})
),
offsets AS (SELECT source_row_number - skipped AS kept_before, max(skipped) AS skipped FROM bad GROUP BY 1),"""
        numbered = """SELECT s.*, s._k + coalesce(o.skipped, 0) AS source_row_number
  FROM src s ASOF LEFT JOIN offsets o ON s._k > o.kept_before"""
    return f"""
WITH src AS ({src}),{offsets}
numbered AS ({numbered}),
checked AS (SELECT *, {_problem_checks(contract)} AS _problems FROM numbered)"""


def _normalize_select(contract: Contract, ctes: str, source_name: str, file_hash: str, batch_id: str) -> str:
    flag_bits = ",\n  ".join(
        f"coalesce(list_contains(string_split(tcp_flags, '-'), '{f}'), false) AS tcp_{f.lower()}" for f in TCP_FLAGS
    )
    return f"""{ctes},
typed AS (
  SELECT
  {contract.rename_select()},
  source_row_number
  FROM checked
  WHERE len(_problems) = 0
)
SELECT
  {_flow_id_sql(file_hash)} AS flow_id,
  * EXCLUDE (source_row_number),
  epoch(flow_end - flow_start) AS duration_s,
  {flag_bits},
  regexp_matches(src_ip, {_sql_str(PRIVATE_IP_REGEX)}) AS src_is_private,
  regexp_matches(dst_ip, {_sql_str(PRIVATE_IP_REGEX)}) AS dst_is_private,
  CAST(flow_start AS DATE) AS flow_date,
  {_sql_str(source_name)} AS source_file,
  source_row_number,
  {_sql_str(file_hash)} AS source_file_hash,
  {_sql_str(batch_id)} AS ingest_batch_id
FROM typed
"""


def _rejects_select(ctes: str, structural_rejects: str | None, source_name: str, file_hash: str, batch_id: str) -> str:
    """One row per (source row, problem); a row can be rejected for several reasons."""
    problems = """SELECT source_row_number, p.reason, p.column_name, p.raw_value, p.detail
  FROM (SELECT source_row_number, unnest(_problems) AS p FROM checked WHERE len(_problems) > 0)"""
    if structural_rejects is not None:
        problems += f"""
  UNION ALL
  SELECT source_row_number, reason, column_name, raw_value, detail FROM {structural_rejects}"""
    return f"""{ctes},
problems AS (
  {problems}
)
SELECT
  {_flow_id_sql(file_hash)} AS flow_id,
  {_sql_str(source_name)} AS source_file,
  source_row_number, reason, column_name, raw_value, detail,
  {_sql_str(file_hash)} AS source_file_hash,
  {_sql_str(batch_id)} AS ingest_batch_id
FROM problems
"""


def _stage_csv(con: duckdb.DuckDBPyConnection, path: Path, stage_dir: Path, file_hash: str) -> tuple[Path, str]:
    """Copy CSV to all-VARCHAR Parquet in record order; records DuckDB cannot parse go to a temp table.

    Returns (staged parquet, name of the temp table of structural rejects). Typing is left to
    TRY_CAST in `_checked_rows_ctes`, so a bad value rejects its row instead of failing the file.
    """
    stage_dir.mkdir(parents=True, exist_ok=True)
    key = file_hash[:HASH_PREFIX]  # hex only: safe inside identifiers
    staged = stage_dir / f"{key}.parquet"
    errors, scans, rejects = f"_csv_errors_{key}", f"_csv_scans_{key}", f"_csv_rejects_{key}"
    con.execute("SET preserve_insertion_order = true")
    try:
        con.execute(
            f"COPY (SELECT * FROM read_csv({_sql_str(path)}, header = true, all_varchar = true, "
            f"auto_detect = true, sample_size = -1, store_rejects = true, "
            f"rejects_table = {_sql_str(errors)}, rejects_scan = {_sql_str(scans)})) TO {_sql_str(staged)} (FORMAT parquet)"
        )
        # `line` counts CSV records with the header as 1, so the data row number is line - 1.
        # DuckDB reports a short record once per missing column: keep one row per (record, error
        # type), naming the first failing column.
        con.execute(f"""
CREATE OR REPLACE TEMP TABLE {rejects} AS
SELECT
  CAST(line - 1 AS BIGINT) AS source_row_number,
  lower(replace(CAST(error_type AS VARCHAR), ' ', '_')) AS reason,  -- no CAST errors: all columns are text here
  arg_min(column_name, column_idx) AS column_name,
  left(trim(any_value(csv_line), chr(13) || chr(10)), {MAX_RAW_VALUE_CHARS}) AS raw_value,  -- keeps the line break
  arg_min(error_message, column_idx) AS detail
FROM {errors}
GROUP BY line, error_type""")
    finally:
        con.execute(f"DROP TABLE IF EXISTS {errors}")
        con.execute(f"DROP TABLE IF EXISTS {scans}")
    return staged, rejects


def _parquet_rows(files: Iterator[Path]) -> int:
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def _reject_summary(con: duckdb.DuckDBPyConnection, rejects_file: Path) -> tuple[int, str]:
    """(distinct rejected rows, 'reason=rows, ...' most frequent first)."""
    src = f"read_parquet({_sql_str(rejects_file)})"
    total = con.execute(f"SELECT count(DISTINCT source_row_number) FROM {src}").fetchone()[0]
    by_reason = con.execute(
        f"SELECT reason, count(DISTINCT source_row_number) AS n FROM {src} GROUP BY reason ORDER BY n DESC, reason"
    ).fetchall()
    return total, ", ".join(f"{reason}={n}" for reason, n in by_reason)


def rejects_path(lake: Path, file_hash: str) -> Path:
    """Reject table of one source file (exists only if some of its rows were rejected)."""
    return lake / REJECTS_DIR / f"src_{file_hash[:HASH_PREFIX]}.parquet"


def flow_files(lake: Path, file_hash: str) -> list[Path]:
    """Lake Parquet files holding the flows of one source file, one per UTC date."""
    return sorted((lake / FLOWS_DIR).glob(f"flow_date=*/src_{file_hash[:HASH_PREFIX]}_*.parquet"))


def _clear_previous_output(lake: Path, file_hash: str) -> None:
    for old in flow_files(lake, file_hash):
        old.unlink()
    rejects_path(lake, file_hash).unlink(missing_ok=True)


def _swap_in(new_flows: Path, lake: Path, file_hash: str, rejects_file: Path | None) -> None:
    """Replace this file's previous lake output (flows and rejects) with freshly written files (renames only)."""
    _clear_previous_output(lake, file_hash)
    for f in new_flows.glob("flow_date=*/*.parquet"):
        dest = lake / FLOWS_DIR / f.parent.name / f.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        f.replace(dest)
    if rejects_file is not None:
        dest = rejects_path(lake, file_hash)
        dest.parent.mkdir(parents=True, exist_ok=True)
        rejects_file.replace(dest)


def ingest_file(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    contract: Contract,
    lake: Path,
    stage_dir: Path,
    batch_id: str,
    file_hash: str | None = None,
    max_reject_fraction: float = DEFAULT_MAX_REJECT_FRACTION,
) -> LedgerEntry:
    file_hash = file_hash or file_sha256(path)
    fmt = detect_format(path)

    def record(status: Status, rows: int = 0, reason: str = "", rejected_rows: int = 0) -> LedgerEntry:
        entry = LedgerEntry(
            source_file=path.name, file_hash=file_hash, fmt=fmt.value, status=status.value, rows=rows,
            reason=reason, ingest_batch_id=batch_id, recorded_at=datetime.now(UTC).isoformat(),
            rejected_rows=rejected_rows,
        )
        append_ledger(lake, entry)
        return entry

    if fmt is FileFormat.UNKNOWN:
        return record(Status.QUARANTINED, reason="not parquet or csv")
    missing, unexpected = contract.check_columns(read_header(path, fmt))
    if missing:
        return record(Status.QUARANTINED, reason=f"missing columns: {missing}; unexpected: {unexpected}")

    new_dir = lake / STAGING_DIR / file_hash[:HASH_PREFIX]
    shutil.rmtree(new_dir, ignore_errors=True)  # leftovers from a crashed attempt on this same file
    new_dir.mkdir(parents=True)  # DuckDB COPY does not create parents
    # Until the final INGESTED entry is written, a crash anywhere leaves this hash retryable.
    record(Status.IN_PROGRESS)
    staged: Path | None = None
    structural: str | None = None
    try:
        if fmt is FileFormat.CSV:
            staged, structural = _stage_csv(con, path, stage_dir, file_hash)
        ctes = _checked_rows_ctes(contract, (staged or path).as_posix(), structural)
        new_flows, rejects_file = new_dir / FLOWS_DIR, new_dir / "rejects.parquet"
        con.execute(
            f"COPY ({_rejects_select(ctes, structural, path.name, file_hash, batch_id)}) "
            f"TO {_sql_str(rejects_file)} (FORMAT parquet, COMPRESSION zstd)"
        )
        select = _normalize_select(contract, ctes, path.name, file_hash, batch_id)
        con.execute(
            f"COPY ({select}) TO {_sql_str(new_flows)} (FORMAT parquet, COMPRESSION zstd, "
            f"PARTITION_BY (flow_date), FILENAME_PATTERN 'src_{file_hash[:HASH_PREFIX]}_{{i}}')"
        )
        rows = _parquet_rows(new_flows.glob("flow_date=*/*.parquet"))
        rejected, by_reason = _reject_summary(con, rejects_file)
        total = rows + rejected
        if rejected > max_reject_fraction * total:
            # So many bad rows means a file problem (wrong export, wrong format), not a few bad records.
            return record(Status.QUARANTINED, rejected_rows=rejected, reason=(
                f"{rejected} of {total} rows rejected ({rejected / total:.1%}) > "
                f"max_reject_fraction {max_reject_fraction:.1%}: {by_reason}")[:500])
        # old output is removed only after the new write succeeded
        _swap_in(new_flows, lake, file_hash, rejects_file if rejected else None)
    except duckdb.Error as exc:
        return record(Status.QUARANTINED, reason=f"{type(exc).__name__}: {exc}"[:500])
    finally:
        shutil.rmtree(new_dir, ignore_errors=True)
        if staged is not None:
            staged.unlink(missing_ok=True)
        if structural is not None:
            con.execute(f"DROP TABLE IF EXISTS {structural}")
    return record(Status.INGESTED, rows=rows, rejected_rows=rejected)


def ingest_directory(
    con: duckdb.DuckDBPyConnection,
    raw_dir: Path,
    contract: Contract,
    lake: Path,
    stage_dir: Path,
    max_reject_fraction: float = DEFAULT_MAX_REJECT_FRACTION,
) -> list[LedgerEntry]:
    """Ingest every CSV/Parquet file under raw_dir, skipping hashes already ingested."""
    batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    candidates = [p for p in raw_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".csv", ".parquet")]
    chosen, duplicates = select_sources(candidates)
    done = {h for h, e in read_ledger(lake).items() if e.status == Status.INGESTED}
    results: list[LedgerEntry] = []
    for dup in duplicates:
        results.append(LedgerEntry(dup.name, "", "", Status.SKIPPED_DUPLICATE.value, 0,
                                   "same-named parquet ingested instead", batch_id, datetime.now(UTC).isoformat()))
    for path in chosen:
        file_hash = file_sha256(path)
        if file_hash in done:
            continue
        results.append(ingest_file(con, path, contract, lake, stage_dir, batch_id, file_hash, max_reject_fraction))
    return results
