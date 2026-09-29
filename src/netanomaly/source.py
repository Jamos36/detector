"""External Parquet inputs: discovery, explicit source -> canonical field mapping, and the flow relation.

Nothing is copied: every stage reads the source files through `flows_sql`, which also exposes `source_file` and
`source_row_index` (0-based row in that file) so any result can be traced back to the original records.

Field meanings are never inferred from names. The default mapping uses the raw names of the netflow_v1 contract
(whose meanings are unvalidated, 0/42); every field actually used is reported with its source column, source type,
how it was converted, and the assumptions that conversion makes.
"""

from __future__ import annotations

import glob
import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

from netanomaly import timestamps
from netanomaly.db import sql_literal

PRIVATE_IP_REGEX = r"^(10.|192.168.|172.(1[6-9]|2[0-9]|3[01]).)"  # RFC 1918 IPv4
from netanomaly.config import EpochUnit
from netanomaly.schema import load_contract

PARQUET_MAGIC = b"PAR1"
INTEGER_TYPES = frozenset({"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
                           "UINTEGER", "UBIGINT"})
FLOAT_TYPES = frozenset({"FLOAT", "DOUBLE"})
IPV4_PATTERN = r"\d{1,3}(\.\d{1,3}){3}"
EPOCH_TO_MICROS = {"s": "* 1000000", "ms": "* 1000", "us": "", "ns": "// 1000"}
NULL_TYPES = {"timestamp": "TIMESTAMPTZ", "ip": "VARCHAR", "integer": "BIGINT", "number": "DOUBLE"}


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str  # timestamp | ip | integer | number
    required: bool
    purpose: str


FIELDS = (
    FieldSpec("flow_start", "timestamp", True, "assigns the flow to a time window (first-packet time assumed)"),
    FieldSpec("src_ip", "ip", True, "entity key: groups flows per host; never a numeric feature"),
    FieldSpec("flow_end", "timestamp", False, "flow duration = flow_end - flow_start"),
    FieldSpec("dst_ip", "ip", False, "distinct peers and internal/external share; never a numeric feature"),
    FieldSpec("dst_port", "integer", False, "distinct destination ports (a count, never a magnitude)"),
    FieldSpec("protocol", "integer", False, "IANA protocol number -> TCP/UDP/ICMP shares"),
    FieldSpec("bytes", "number", False, "bytes per flow (one direction assumed)"),
    FieldSpec("packets", "number", False, "packets per flow (one direction assumed)"),
)
FIELD_BY_NAME = {f.name: f for f in FIELDS}


class SourceError(ValueError):
    """The input cannot be used as configured (not Parquet, required field missing, incompatible type)."""


@dataclass(frozen=True)
class SourceFile:
    path: str
    size: int
    mtime_ns: int


@dataclass
class FieldMapping:
    """One canonical field: where it comes from and whether it can be used."""

    name: str
    source: str
    source_type: str | None  # DuckDB type in the union schema, None when absent
    status: str  # ok | missing | incompatible
    conversion: str = ""
    files_present: int = 0
    note: str = ""


@dataclass
class MappingReport:
    fields: list[FieldMapping]
    unmapped_columns: list[str]  # source columns no canonical field uses (reported, never modelled)
    n_files: int
    assumptions: list[str] = field(default_factory=list)

    @property
    def usable(self) -> dict[str, FieldMapping]:
        return {f.name: f for f in self.fields if f.status == "ok"}

    def to_dict(self) -> dict:
        return asdict(self)


# --- repository boundary (ADR-012) -------------------------------------------------------------------------------

def repo_root() -> Path | None:
    """The checkout this package runs from, if it is one (an installed wheel has no .git)."""
    root = Path(__file__).resolve().parents[2]
    return root if (root / ".git").exists() else None


def check_outside_repo(paths: list[Path], what: str, allow: bool) -> None:
    """Refuse data inputs/outputs inside the checkout unless the config asserts they are mock/synthetic."""
    root = repo_root()
    if root is None or allow:
        return
    inside = [p for p in paths if Path(p).resolve().is_relative_to(root)]
    if inside:
        raise SourceError(
            f"{what} inside the repository checkout ({inside[0]}). This checkout must not hold real data or "
            "artifacts computed from it (ADR-012): point the config at a location outside the repository, or set "
            "`allow_inside_repo: true` only for mock/synthetic data.")


# --- discovery ---------------------------------------------------------------------------------------------------

def resolve_files(patterns: list[str]) -> list[SourceFile]:
    """Expand files / directories / globs to Parquet files. Directories and globs pick `*.parquet` only; every
    picked file must carry the Parquet magic bytes (CSV and other formats are not supported by the PoC)."""
    found: set[Path] = set()
    for pattern in patterns:
        p = Path(pattern)
        if p.is_dir():
            found.update(x for x in p.rglob("*.parquet") if x.is_file())
        elif any(ch in pattern for ch in "*?["):
            found.update(Path(x) for x in glob.glob(pattern, recursive=True)
                         if Path(x).is_file() and x.lower().endswith(".parquet"))
        elif p.is_file():
            found.add(p)
        else:
            raise SourceError(f"input path does not exist: {pattern}")
    files = sorted({f.resolve() for f in found}, key=lambda x: x.as_posix())
    if not files:
        raise SourceError(f"no Parquet files found for {patterns}")
    not_parquet = [f for f in files if not _is_parquet(f)]
    if not_parquet:
        raise SourceError(f"not Parquet (the PoC reads Parquet only): {[str(f) for f in not_parquet[:5]]}")
    return [SourceFile(f.as_posix(), f.stat().st_size, f.stat().st_mtime_ns) for f in files]


def _is_parquet(path: Path) -> bool:
    with path.open("rb") as fh:
        return fh.read(4) == PARQUET_MAGIC


def fingerprint(files: list[SourceFile]) -> str:
    """Identity of the input set (paths, sizes, modification times); a content hash of a year is too costly."""
    blob = json.dumps([asdict(f) for f in files], sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()


def _listing(files: list[SourceFile]) -> str:
    return "[" + ", ".join(sql_literal(f.path) for f in files) + "]"


def read_parquet_sql(files: list[SourceFile]) -> str:
    return f"read_parquet({_listing(files)}, union_by_name = true, filename = true, file_row_number = true)"


# --- schema and mapping ------------------------------------------------------------------------------------------

def source_schema(con: duckdb.DuckDBPyConnection, files: list[SourceFile]) -> dict[str, str]:
    """Union schema (column -> DuckDB type) from the Parquet footers; no rows are read."""
    rows = con.execute(f"DESCRIBE SELECT * EXCLUDE (filename, file_row_number) FROM {read_parquet_sql(files)}")
    return {name: typ for name, typ, *_ in rows.fetchall()}


def columns_per_file(con: duckdb.DuckDBPyConnection, files: list[SourceFile]) -> dict[str, int]:
    """How many files contain each column (a column missing from some files reads as NULL there)."""
    rows = con.execute(f"SELECT name, count(DISTINCT file_name) FROM parquet_schema({_listing(files)}) "
                       "GROUP BY name").fetchall()
    return dict(rows)


def default_field_map() -> dict[str, str]:
    """Canonical -> raw names from the netflow_v1 contract, for the canonical fields the PoC uses."""
    raw_by_name = {c.name: c.raw for c in load_contract().columns}
    return {f.name: raw_by_name[f.name] for f in FIELDS}


def build_mapping(schema: dict[str, str], per_file: dict[str, int], n_files: int, field_map: dict[str, str],
                  epoch_unit: dict[str, EpochUnit]) -> MappingReport:
    unknown = sorted((set(field_map) | set(epoch_unit)) - set(FIELD_BY_NAME))
    if unknown:
        raise SourceError(f"unknown canonical field(s) in field_map/epoch_unit: {unknown}; "
                          f"known: {list(FIELD_BY_NAME)}")
    mapping = {**default_field_map(), **field_map}
    fields = [_map_field(spec, mapping[spec.name], schema, per_file.get(mapping[spec.name], 0), n_files,
                         epoch_unit.get(spec.name)) for spec in FIELDS]
    used = {f.source for f in fields if f.status == "ok"}
    report = MappingReport(fields, sorted(c for c in schema if c not in used), n_files)
    report.assumptions = _assumptions(report)
    bad_required = [f for f in fields if FIELD_BY_NAME[f.name].required and f.status != "ok"]
    if bad_required:
        details = "; ".join(f"{f.name} <- {f.source}: {f.status} {f.note}" for f in bad_required)
        raise SourceError(f"required field(s) unusable: {details}. Set input.field_map in the config.")
    return report


def _map_field(spec: FieldSpec, source: str, schema: dict[str, str], files_present: int, n_files: int,
               unit: EpochUnit | None) -> FieldMapping:
    typ = schema.get(source)
    if typ is None:
        return FieldMapping(spec.name, source, None, "missing", note="column not in any input file")
    m = FieldMapping(spec.name, source, typ, "ok", files_present=files_present)
    if files_present < n_files:
        m.note = f"present in {files_present} of {n_files} files; NULL elsewhere"
    if spec.kind == "timestamp":
        return _map_timestamp(m, typ, unit)
    if spec.kind == "ip":
        if typ != "VARCHAR":
            return _incompatible(m, f"{typ}: expected text addresses (integer-encoded IPs are not converted)")
        m.conversion = "text; IPv4 dotted quads recognised for the internal/external share"
    elif spec.kind == "integer":
        if typ not in INTEGER_TYPES:
            return _incompatible(m, f"{typ}: expected an integer type")
        m.conversion = "integer code (never used as a magnitude)"
    elif typ not in INTEGER_TYPES | FLOAT_TYPES and not typ.startswith("DECIMAL"):
        return _incompatible(m, f"{typ}: expected a numeric type")
    else:
        m.conversion = "numeric; negative values treated as NULL"
    return m


def _map_timestamp(m: FieldMapping, typ: str, unit: EpochUnit | None) -> FieldMapping:
    if typ in INTEGER_TYPES:
        if unit is None:
            return _incompatible(m, "integer column: set input.epoch_unit for this field (s, ms, us or ns)")
        m.conversion = f"integer epoch in {unit} since 1970-01-01 UTC"
        return m
    kind = timestamps.source_kind(typ)
    if kind is timestamps.SourceKind.UNSUPPORTED:
        return _incompatible(m, f"{typ} is not a timestamp, text or integer epoch")
    m.conversion = {timestamps.SourceKind.AWARE: "UTC instant",
                    timestamps.SourceKind.NAIVE: "timestamp without zone, ASSUMED UTC (ADR-015)",
                    timestamps.SourceKind.TEXT: "ISO-8601 text; values without offset ASSUMED UTC"}[kind]
    return m


def _incompatible(m: FieldMapping, note: str) -> FieldMapping:
    m.status, m.note = "incompatible", (f"{m.note}; " if m.note else "") + note
    return m


def _assumptions(report: MappingReport) -> list[str]:
    out = [("No field meaning is validated against collector documentation (contract: 0/42 validated); "
           "the mapping is a configuration choice, not a verified semantic.")]
    usable = report.usable
    for name, f in usable.items():
        if "ASSUMED UTC" in f.conversion:
            out.append(f"{name} <- {f.source}: {f.conversion}; a local-time exporter would shift every window.")
    if "bytes" in usable or "packets" in usable:
        out.append("bytes/packets are assumed per flow, one direction (octetDeltaCount / packetDeltaCount).")
    if "flow_end" in usable:
        out.append("flow_end - flow_start is assumed to be the flow duration; exporter active/idle timeouts split "
                   "long flows, so durations are capped by the exporter configuration.")
    if "dst_ip" in usable:
        out.append("internal share uses RFC 1918 IPv4 ranges only; IPv6 or non-dotted values count as unknown.")
    for f in report.fields:
        if f.name not in usable:
            out.append(f"{f.name} is not available ({f.status}); features that need it are dropped.")
    return out


# --- flow relation -----------------------------------------------------------------------------------------------

def _quote(column: str) -> str:
    return '"' + column.replace('"', '""') + '"'


def _timestamp_parts(f: FieldMapping, unit: EpochUnit | None) -> tuple[str | None, str, str]:
    """(helper match column or None, value expr, status expr) for a timestamp field."""
    raw = _quote(f.source)
    if f.source_type in INTEGER_TYPES:
        value = f"timezone('UTC', make_timestamp(CAST({raw} AS BIGINT) {EPOCH_TO_MICROS[unit or 'us']}))"
        return None, value, f"CASE WHEN {raw} IS NULL THEN NULL ELSE 'ok' END"
    ts = timestamps.timestamp_sql(raw, f.source_type or "", f"{f.name}__parsed")
    match = None if ts.match == "NULL" else f"{ts.match} AS {ts.match_column}"
    return match, f"struct_extract({ts.parsed}, 'value')", f"struct_extract({ts.parsed}, 'status')"


def flows_sql(files: list[SourceFile], report: MappingReport, epoch_unit: dict[str, EpochUnit]) -> str:
    """Canonical flow relation over the source files. Unusable optional fields are NULL columns.

    Columns: flow_start, flow_end (TIMESTAMPTZ, UTC) with *_status (ok / no_offset / invalid), src_ip, dst_ip,
    dst_port, protocol, bytes, packets, duration_s, dst_is_private (NULL unless dst_ip is a dotted IPv4),
    source_file, source_row_index (0-based).
    """
    usable = report.usable
    helpers, cols = [], []
    for spec in FIELDS:
        f = usable.get(spec.name)
        if f is None:
            cols.append(f"CAST(NULL AS {NULL_TYPES[spec.kind]}) AS {spec.name}")
            if spec.kind == "timestamp":
                cols.append(f"CAST(NULL AS VARCHAR) AS {spec.name}_status")
            continue
        raw = _quote(f.source)
        if spec.kind == "timestamp":
            match, value, status = _timestamp_parts(f, epoch_unit.get(spec.name))
            if match:
                helpers.append(match)
            cols += [f"{value} AS {spec.name}", f"{status} AS {spec.name}_status"]
        elif spec.kind == "ip":
            cols.append(f"trim(CAST({raw} AS VARCHAR)) AS {spec.name}")
        elif spec.kind == "integer":
            cols.append(f"CAST({raw} AS BIGINT) AS {spec.name}")
        else:
            cols.append(f"CASE WHEN {raw} >= 0 THEN CAST({raw} AS DOUBLE) END AS {spec.name}")
    helper_sql = (", " + ", ".join(helpers)) if helpers else ""
    return f"""
SELECT *,
  CASE WHEN flow_end >= flow_start THEN epoch(flow_end - flow_start) END AS duration_s,
  CASE WHEN regexp_full_match(dst_ip, {sql_literal(IPV4_PATTERN)})
       THEN regexp_matches(dst_ip, {sql_literal(PRIVATE_IP_REGEX)}) END AS dst_is_private
FROM (
  SELECT {", ".join(cols)}, filename AS source_file, file_row_number AS source_row_index
  FROM (SELECT *{helper_sql} FROM {read_parquet_sql(files)})
)"""


@dataclass
class Source:
    """Everything a stage needs to read the flows."""

    files: list[SourceFile]
    mapping: MappingReport
    epoch_unit: dict[str, EpochUnit]
    fingerprint: str

    def flows_sql(self) -> str:
        return flows_sql(self.files, self.mapping, self.epoch_unit)


def open_source(con: duckdb.DuckDBPyConnection, patterns: list[str], field_map: dict[str, str],
                epoch_unit: dict[str, EpochUnit]) -> Source:
    files = resolve_files(patterns)
    schema = source_schema(con, files)
    report = build_mapping(schema, columns_per_file(con, files), len(files), field_map, epoch_unit)
    return Source(files, report, dict(epoch_unit), fingerprint(files))
