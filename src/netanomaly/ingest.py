"""Raw file -> normalized, date-partitioned Parquet lake with row-level provenance.

Rules:
- File type is detected from content (Parquet magic bytes), not the extension.
- Raw files are never modified; bad files are recorded as quarantined.
- CSV is read with explicit types from the schema contract (no inference).
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
from collections.abc import Iterable
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


class FileFormat(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"
    UNKNOWN = "unknown"


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
    with path.open(newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh), [])


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


def _normalize_select(contract: Contract, source: str, source_name: str, file_hash: str, batch_id: str) -> str:
    flag_bits = ",\n  ".join(
        f"coalesce(list_contains(string_split(tcp_flags, '-'), '{f}'), false) AS tcp_{f.lower()}" for f in TCP_FLAGS
    )
    return f"""
WITH typed AS (
  SELECT
  {contract.rename_select()},
  file_row_number + 1 AS source_row_number
  FROM read_parquet({_sql_str(source)}, file_row_number = true)
)
SELECT
  left(sha256({_sql_str(file_hash)} || ':' || source_row_number::VARCHAR), 32) AS flow_id,
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


def _stage_csv(con: duckdb.DuckDBPyConnection, path: Path, contract: Contract, stage_dir: Path, file_hash: str) -> Path:
    """Copy CSV to Parquet with explicit types, preserving row order for provenance."""
    stage_dir.mkdir(parents=True, exist_ok=True)
    staged = stage_dir / f"{file_hash[:HASH_PREFIX]}.parquet"
    types = "{" + ", ".join(f"{_sql_str(k)}: {_sql_str(v)}" for k, v in contract.duckdb_types().items()) + "}"
    con.execute("SET preserve_insertion_order = true")
    con.execute(
        f"COPY (SELECT * FROM read_csv({_sql_str(path)}, header = true, types = {types}, "
        f"auto_detect = true, sample_size = -1)) TO {_sql_str(staged)} (FORMAT parquet)"
    )
    return staged


def _clear_previous_output(flows_dir: Path, file_hash: str) -> None:
    for old in flows_dir.glob(f"flow_date=*/src_{file_hash[:HASH_PREFIX]}_*.parquet"):
        old.unlink()


def _swap_in(new_dir: Path, flows_dir: Path, file_hash: str) -> None:
    """Replace this file's previous lake output with freshly written files (renames only)."""
    _clear_previous_output(flows_dir, file_hash)
    for f in new_dir.glob("flow_date=*/*.parquet"):
        dest = flows_dir / f.parent.name / f.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        f.replace(dest)


def ingest_file(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    contract: Contract,
    lake: Path,
    stage_dir: Path,
    batch_id: str,
    file_hash: str | None = None,
) -> LedgerEntry:
    file_hash = file_hash or file_sha256(path)
    fmt = detect_format(path)

    def record(status: Status, rows: int = 0, reason: str = "") -> LedgerEntry:
        entry = LedgerEntry(
            source_file=path.name, file_hash=file_hash, fmt=fmt.value, status=status.value, rows=rows,
            reason=reason, ingest_batch_id=batch_id, recorded_at=datetime.now(UTC).isoformat(),
        )
        append_ledger(lake, entry)
        return entry

    if fmt is FileFormat.UNKNOWN:
        return record(Status.QUARANTINED, reason="not parquet or csv")
    missing, unexpected = contract.check_columns(read_header(path, fmt))
    if missing:
        return record(Status.QUARANTINED, reason=f"missing columns: {missing}; unexpected: {unexpected}")

    flows_dir = lake / FLOWS_DIR
    new_dir = lake / STAGING_DIR / file_hash[:HASH_PREFIX]
    shutil.rmtree(new_dir, ignore_errors=True)  # leftovers from a crashed attempt on this same file
    new_dir.mkdir(parents=True)  # DuckDB COPY does not create parents
    # Until the final INGESTED entry is written, a crash anywhere leaves this hash retryable.
    record(Status.IN_PROGRESS)
    staged: Path | None = None
    try:
        source = path if fmt is FileFormat.PARQUET else (staged := _stage_csv(con, path, contract, stage_dir, file_hash))
        select = _normalize_select(contract, source.as_posix(), path.name, file_hash, batch_id)
        con.execute(
            f"COPY ({select}) TO {_sql_str(new_dir)} (FORMAT parquet, COMPRESSION zstd, "
            f"PARTITION_BY (flow_date), FILENAME_PATTERN 'src_{file_hash[:HASH_PREFIX]}_{{i}}')"
        )
        rows = con.execute(f"SELECT count(*) FROM read_parquet({_sql_str(source)})").fetchone()[0]
        _swap_in(new_dir, flows_dir, file_hash)  # old output is removed only after the new write succeeded
    except duckdb.Error as exc:
        return record(Status.QUARANTINED, reason=f"{type(exc).__name__}: {exc}"[:500])
    finally:
        shutil.rmtree(new_dir, ignore_errors=True)
        if staged is not None:
            staged.unlink(missing_ok=True)
    return record(Status.INGESTED, rows=rows)


def ingest_directory(
    con: duckdb.DuckDBPyConnection, raw_dir: Path, contract: Contract, lake: Path, stage_dir: Path
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
        results.append(ingest_file(con, path, contract, lake, stage_dir, batch_id, file_hash))
    return results
