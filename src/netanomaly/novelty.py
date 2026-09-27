"""New-destination and new-port rates from a persistent seen set (V2-3): `new_dst_ip_rate`, `new_dst_port_rate`.

Definitions (see ARCHITECTURE.md → Host novelty, ADR-019):

- **Window**: the V0 host-window grid, `W = time_bucket(window_minutes, flow_start)` per `src_ip`. Event order comes
  from `flow_start` only; exporter sequence numbers and file row order are never used (ADR-016).
- **History of a window**: every flow of the same `src_ip` whose window starts strictly before `W`, i.e.
  `flow_start < window_start`. Flows inside `W` are never each other's history, so rows with tied `flow_start` (always
  in the same window) or any order within the window cannot change a result. There is no lookback limit: the seen set
  holds every earlier window in the lake.
- **New**: a destination `dst_ip` is new in `W` when `src_ip` has no flow to it in its history, which is the same as
  `W` being the pair's first window: `first_window(src_ip, dst_ip) = W`. Likewise for `dst_port` (as a label, not a
  magnitude); flows with NULL `dst_port` are ignored for ports.
- **Rates**: `new_dst_ip_rate = new_dst_ip / uniq_dst_ip` over the window's distinct destinations, and
  `new_dst_port_rate = new_dst_port / uniq_dst_port` (NULL when the window has no non-NULL port). In a host's first
  window every destination is new (rate 1): `host_first_seen` and `history_days` tell a consumer how much history
  stood behind a rate. Rates are ranking signals, not probabilities.

State (`features/novelty_state/`): the seen set, append-only and partitioned by the UTC day an entry was first seen:
`seen_src_ip`, `seen_dst_ip`, `seen_dst_port` / `first_date=D/part-0.parquet` with the key columns and `first_window`.
`manifest.json` records the state version, `window_minutes` and a fingerprint (file names + sizes) of every lake day
folded in. A day D is computed from the lake partition of D plus the state partitions of days before D only.

Reruns: the earliest lake day whose fingerprint differs from the manifest (a new, changed or removed day, or a day
whose output/state files are missing) is the restart day. Output and state partitions at or after it are deleted and
recomputed in day order; earlier days are kept untouched, because they depend only on data before them. A rerun with
no lake change recomputes nothing. A changed `window_minutes`, state version or `--rebuild` recomputes everything.
The manifest is rewritten after each finished day, so an interrupted run resumes at the first unfinished day.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from netanomaly.db import sql_literal
from netanomaly.feature_registry import Registry, require_usable
from netanomaly.schema import Contract

NOVELTY_FEATURES = ("new_dst_ip_rate", "new_dst_port_rate")
STATE_VERSION = 1
MINUTES_PER_DAY = 24 * 60
MANIFEST = "manifest.json"
PART = "part-0.parquet"
# seen-set table -> key columns; each row is one key with the first window it occurred in
SEEN_TABLES: dict[str, tuple[str, ...]] = {
    "seen_src_ip": ("src_ip",),
    "seen_dst_ip": ("src_ip", "dst_ip"),
    "seen_dst_port": ("src_ip", "dst_port"),
}


@dataclass(frozen=True)
class NoveltyRun:
    days: int                     # lake days covered by the output
    recomputed: tuple[date, ...]  # days computed by this run (empty when the lake did not change)
    rows: int                     # output rows in total


def require_usable_inputs(registry: Registry, contract: Contract) -> None:
    """Refuse to build a novelty feature the registry does not rate usable against the contract."""
    require_usable(registry, contract, NOVELTY_FEATURES)


def lake_days(lake: Path) -> dict[date, list[Path]]:
    """Lake flow files per UTC day partition; days without files are skipped."""
    days: dict[date, list[Path]] = {}
    for part in sorted((lake / "flows").glob("flow_date=*")):
        if files := sorted(part.glob("*.parquet")):
            days[date.fromisoformat(part.name.removeprefix("flow_date="))] = files
    return days


def fingerprint(files: list[Path]) -> str:
    """Identity of a lake day: its file names and sizes (ingest names files after the source file's hash)."""
    text = "\n".join(f"{p.name}:{p.stat().st_size}" for p in files)
    return hashlib.sha256(text.encode()).hexdigest()[:32]


def _partition(root: Path, key: str, day: date) -> Path:
    return root / f"{key}={day.isoformat()}"


def _partition_day(part: Path, key: str) -> date:
    return date.fromisoformat(part.name.removeprefix(f"{key}="))


def _day_files(out_dir: Path, state_dir: Path, day: date) -> list[Path]:
    return [_partition(out_dir, "flow_date", day) / PART,
            *(_partition(state_dir / t, "first_date", day) / PART for t in SEEN_TABLES)]


def _read_manifest(state_dir: Path, window_minutes: int) -> dict[date, str]:
    """Days already folded into the state, or {} when the state is missing or was built differently."""
    try:
        m = json.loads((state_dir / MANIFEST).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if m.get("state_version") != STATE_VERSION or m.get("window_minutes") != window_minutes:
        return {}
    return {date.fromisoformat(d): fp for d, fp in m.get("days", {}).items()}


def _write_manifest(state_dir: Path, window_minutes: int, days: dict[date, str]) -> None:
    body = {"state_version": STATE_VERSION, "window_minutes": window_minutes,
            "days": {d.isoformat(): fp for d, fp in sorted(days.items())}}
    tmp = state_dir / (MANIFEST + ".tmp")
    tmp.write_text(json.dumps(body, indent=1), encoding="utf-8")
    os.replace(tmp, state_dir / MANIFEST)


def restart_day(current: dict[date, str], done: dict[date, str], out_dir: Path, state_dir: Path) -> date | None:
    """Earliest day whose lake content differs from what the state was built from; None if nothing changed."""
    for day in sorted(current.keys() | done.keys()):
        if current.get(day) != done.get(day) or not all(p.exists() for p in _day_files(out_dir, state_dir, day)):
            return day
    return None


def _drop_from(root: Path, key: str, day: date) -> None:
    for part in root.glob(f"{key}=*"):
        if _partition_day(part, key) >= day:
            shutil.rmtree(part)


def _prior(state_dir: Path, table: str, day: date) -> str:
    """The seen-set entries first seen on days before `day`, as a SQL relation (empty when there are none)."""
    files = [p for part in sorted((state_dir / table).glob("first_date=*")) if _partition_day(part, "first_date") < day
             for p in sorted(part.glob("*.parquet"))]
    if not files:
        return f"(SELECT {', '.join(SEEN_TABLES[table])}, window_start AS first_window FROM nov_day WHERE false)"
    return f"read_parquet([{', '.join(sql_literal(p) for p in files)}])"


def features_sql(history_days: int) -> str:
    """Novelty per host-window of the current day, from `nov_day` and the day's first-seen entries."""
    new_ip = "count(DISTINCT d.dst_ip) FILTER (WHERE ni.first_window = d.window_start)"
    new_port = "count(DISTINCT d.dst_port) FILTER (WHERE np.first_window = d.window_start)"
    return f"""
WITH host AS (
  SELECT src_ip, first_window AS host_first_seen FROM nov_prior_seen_src_ip
  UNION ALL
  SELECT src_ip, first_window FROM nov_new_seen_src_ip
)
SELECT
  d.src_ip,
  d.window_start,
  count(DISTINCT d.dst_ip) AS uniq_dst_ip,
  {new_ip} AS new_dst_ip,
  {new_ip}::DOUBLE / nullif(count(DISTINCT d.dst_ip), 0) AS new_dst_ip_rate,
  count(DISTINCT d.dst_port) AS uniq_dst_port,
  {new_port} AS new_dst_port,
  {new_port}::DOUBLE / nullif(count(DISTINCT d.dst_port), 0) AS new_dst_port_rate,
  any_value(h.host_first_seen) AS host_first_seen,
  {int(history_days)} AS history_days
FROM nov_day d
LEFT JOIN nov_new_seen_dst_ip ni USING (src_ip, dst_ip)
LEFT JOIN nov_new_seen_dst_port np USING (src_ip, dst_port)
LEFT JOIN host h USING (src_ip)
GROUP BY d.src_ip, d.window_start
ORDER BY d.src_ip, d.window_start
"""


def compute_day(con: duckdb.DuckDBPyConnection, files: list[Path], day: date, history_days: int,
                out_dir: Path, state_dir: Path, window_minutes: int) -> None:
    """Novelty for one lake day, then append the day's first-seen entries to the seen set."""
    src = f"read_parquet([{', '.join(sql_literal(p) for p in files)}])"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nov_day AS
        SELECT src_ip, time_bucket(INTERVAL '{int(window_minutes)} minutes', flow_start) AS window_start,
               dst_ip, dst_port
        FROM {src}""")
    for table, keys in SEEN_TABLES.items():
        cols = ", ".join(keys)
        not_null = " AND ".join(f"{k} IS NOT NULL" for k in keys)
        con.execute(f"CREATE OR REPLACE TEMP TABLE nov_prior_{table} AS SELECT * FROM {_prior(state_dir, table, day)}")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE nov_new_{table} AS
            SELECT d.* FROM (SELECT {cols}, min(window_start) AS first_window FROM nov_day WHERE {not_null}
                             GROUP BY {cols}) d
            ANTI JOIN nov_prior_{table} p USING ({cols})""")
    part = _partition(out_dir, "flow_date", day)
    part.mkdir(parents=True)
    con.execute(f"COPY ({features_sql(history_days)}) TO {sql_literal(part / PART)} (FORMAT parquet, COMPRESSION zstd)")
    for table, keys in SEEN_TABLES.items():
        part = _partition(state_dir / table, "first_date", day)
        part.mkdir(parents=True)
        con.execute(f"COPY (SELECT * FROM nov_new_{table} ORDER BY {', '.join(keys)}) TO {sql_literal(part / PART)} "
                    "(FORMAT parquet, COMPRESSION zstd)")
    for table in SEEN_TABLES:
        con.execute(f"DROP TABLE nov_prior_{table}")
        con.execute(f"DROP TABLE nov_new_{table}")
    con.execute("DROP TABLE nov_day")


def build_host_novelty(con: duckdb.DuckDBPyConnection, lake: Path, out_dir: Path, state_dir: Path,
                       window_minutes: int, rebuild: bool = False) -> NoveltyRun:
    """Bring `out_dir/flow_date=D/part-0.parquet` and the seen set in `state_dir` up to date with the lake.

    Memory per query: one lake day plus the seen-set entries of earlier days (DuckDB spills beyond memory_limit).
    """
    if MINUTES_PER_DAY % window_minutes:
        raise ValueError(f"window_minutes={window_minutes} must divide a day, so no window spans two UTC days")
    lake_files = lake_days(lake)
    current = {d: fingerprint(files) for d, files in lake_files.items()}
    done = {} if rebuild else _read_manifest(state_dir, window_minutes)
    if not done:  # no trustworthy state: start from an empty seen set
        for d in (out_dir, state_dir):
            shutil.rmtree(d, ignore_errors=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    recomputed: list[date] = []
    if (restart := restart_day(current, done, out_dir, state_dir)) is not None:
        kept = {d: fp for d, fp in done.items() if d < restart}
        _write_manifest(state_dir, window_minutes, kept)  # first, so a crash below never trusts dropped days
        _drop_from(out_dir, "flow_date", restart)
        for table in SEEN_TABLES:
            _drop_from(state_dir / table, "first_date", restart)
        days = sorted(current)
        for i, day in enumerate(days):
            if day < restart:
                continue
            compute_day(con, lake_files[day], day, i, out_dir, state_dir, window_minutes)
            kept[day] = current[day]
            _write_manifest(state_dir, window_minutes, kept)
            recomputed.append(day)
    rows = 0
    if current:
        rows = con.execute(f"SELECT count(*) FROM read_parquet({sql_literal(out_dir / '**' / '*.parquet')})").fetchone()[0]
    return NoveltyRun(days=len(current), recomputed=tuple(recomputed), rows=rows)
