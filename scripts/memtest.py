"""V1-3 memory test: generate synthetic NetFlow, ingest it under the configured DuckDB memory_limit,
and record peak process memory.

Run the two steps as separate processes so the generator's memory never counts against ingestion:

    uv run python scripts/memtest.py generate --root <TEMP_DIR> --days 31 --hosts 3000 [--single-file] [--format csv]
    uv run python scripts/memtest.py ingest   --root <TEMP_DIR>

<TEMP_DIR> must be outside the repository (ADR-009/ADR-012: never commit generated bulk data).
`ingest` writes <TEMP_DIR>/memtest_result.json and prints it. Synthetic data only; no attacks injected.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import platform
import sys
import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from netanomaly.config import Paths, load_settings
from netanomaly.db import connect, sql_literal
from netanomaly.ingest import Status, ingest_directory
from netanomaly.schema import load_contract
from netanomaly.synth import generate

RESULT_FILE = "memtest_result.json"
SPILL_SAMPLE_SECONDS = 0.5


class _ProcessMemoryCounters(ctypes.Structure):  # PROCESS_MEMORY_COUNTERS (psapi.h)
    _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]


class _MemoryStatusEx(ctypes.Structure):  # MEMORYSTATUSEX (sysinfoapi.h)
    _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]


def peak_memory() -> dict[str, int | None]:
    """Peak memory of this process so far, in bytes.

    Windows: peak working set (resident) and peak commit charge (private memory requested, which also counts
    pages the OS trimmed to the page file). Elsewhere: ru_maxrss only.
    """
    if sys.platform == "win32":
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32, psapi = ctypes.WinDLL("kernel32"), ctypes.WinDLL("psapi")
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p  # HANDLE: 64-bit on x64, not the default c_int
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(_ProcessMemoryCounters), ctypes.c_ulong]
        if not psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            raise OSError("GetProcessMemoryInfo failed")
        return {"peak_rss_bytes": counters.PeakWorkingSetSize, "peak_commit_bytes": counters.PeakPagefileUsage}
    import resource

    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {"peak_rss_bytes": maxrss if sys.platform == "darwin" else maxrss * 1024, "peak_commit_bytes": None}


def system_memory() -> dict[str, int | None]:
    if sys.platform != "win32":
        pages, size = os.sysconf("SC_PHYS_PAGES"), os.sysconf("SC_PAGE_SIZE")
        avail = os.sysconf("SC_AVPHYS_PAGES") if "SC_AVPHYS_PAGES" in os.sysconf_names else None
        return {"total_ram_bytes": pages * size, "available_ram_bytes": avail * size if avail else None}
    status = _MemoryStatusEx()
    status.dwLength = ctypes.sizeof(status)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
    return {"total_ram_bytes": status.ullTotalPhys, "available_ram_bytes": status.ullAvailPhys}


def dir_bytes(path: Path) -> int:
    total = 0
    for p in path.rglob("*") if path.exists() else ():
        try:
            total += p.stat().st_size if p.is_file() else 0
        except FileNotFoundError:  # spill files vanish while we walk
            pass
    return total


class SpillSampler(threading.Thread):
    """Samples the DuckDB spill directory size; DuckDB deletes spill files, so only sampling sees the peak."""

    def __init__(self, path: Path) -> None:
        super().__init__(daemon=True)
        self.path, self.peak, self._stop_event = path, 0, threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(SPILL_SAMPLE_SECONDS):
            self.peak = max(self.peak, dir_bytes(self.path))

    def stop(self) -> int:
        self._stop_event.set()
        self.join()
        return self.peak


def _check_outside_repo(root: Path) -> None:
    repo = Path(__file__).resolve().parent.parent
    if root.resolve().is_relative_to(repo):
        raise SystemExit(f"--root must be outside the repository ({repo}); generated data is never committed")


def _merge_parts(parts: Path, out: Path, fmt: str, config: Path) -> None:
    """Stream daily parts into one file with DuckDB (bounded memory), then drop the parts."""
    s = load_settings(config)
    con = connect(s.duckdb.model_copy(update={"temp_directory": parts.parent / "tmp" / "merge"}))
    con.execute("SET preserve_insertion_order = true")
    options = "FORMAT parquet, COMPRESSION zstd" if fmt == "parquet" else "FORMAT csv, HEADER"
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM read_parquet({sql_literal(parts / '*.parquet')})) "
                f"TO {sql_literal(out)} ({options})")
    con.close()
    for part in parts.glob("*.parquet"):
        part.unlink()
    parts.rmdir()


def cmd_generate(args: argparse.Namespace) -> None:
    root = Path(args.root)
    _check_outside_repo(root)
    started = time.perf_counter()
    # One file per UTC day by default; --single-file merges them to stress one large ingest.
    target = root / "parts" if args.single_file else root / "raw"
    generate(target, root / "truth", days=args.days, n_hosts=args.hosts, seed=args.seed,
             start=date.fromisoformat(args.start), fmt="parquet" if args.single_file else args.format, injector=None)
    if args.single_file:
        _merge_parts(target, root / "raw" / f"synth_all.{args.format}", args.format, Path(args.config))
    print(json.dumps({"generate_seconds": round(time.perf_counter() - started, 1),
                      "raw_bytes": dir_bytes(root / "raw"), **peak_memory()}))


def cmd_ingest(args: argparse.Namespace) -> None:
    root = Path(args.root)
    _check_outside_repo(root)
    s = load_settings(Path(args.config))
    # Keep the configured memory_limit/threads; put every path (incl. spill) in the temporary root.
    duck = s.duckdb.model_copy(update={"temp_directory": root / "tmp" / "duckdb"})
    paths = Paths(**{k: root / k for k in Paths.model_fields})
    system_before = system_memory()
    sampler = SpillSampler(duck.temp_directory)
    sampler.start()
    started = time.perf_counter()
    con = connect(duck)
    entries = ingest_directory(con, paths.raw, load_contract(), paths.lake, duck.temp_directory / "stage",
                               s.ingest.max_reject_fraction)
    elapsed = time.perf_counter() - started
    peak_spill = sampler.stop()
    memory = peak_memory()  # before the verification count below, which is not part of ingestion
    lake_rows = con.execute(
        f"SELECT count(*) FROM read_parquet({sql_literal(paths.lake / 'flows' / '**' / '*.parquet')})"
    ).fetchone()[0]
    result = {
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "files": len(entries),
        "statuses": sorted({e.status for e in entries}),
        "rows_ingested": sum(e.rows for e in entries),
        "rows_rejected": sum(e.rejected_rows for e in entries),
        "lake_rows": lake_rows,
        "completed": bool(entries) and all(e.status == Status.INGESTED for e in entries),
        "ingest_seconds": round(elapsed, 1),
        **memory,
        "peak_spill_bytes": peak_spill,
        "raw_bytes": dir_bytes(paths.raw),
        "lake_bytes": dir_bytes(paths.lake),
        "memory_limit": duck.memory_limit,
        "threads": duck.threads,
        **{f"system_{k}_at_start": v for k, v in system_before.items()},
        "duckdb_version": duckdb.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
    }
    (root / RESULT_FILE).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if not result["completed"]:
        raise SystemExit("ingestion did not complete for every file")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate", help="write synthetic raw files (one per UTC day) to ROOT/raw")
    g.add_argument("--root", required=True)
    g.add_argument("--days", type=int, default=31)
    g.add_argument("--hosts", type=int, default=3000)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--start", default="2026-09-01")
    g.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    g.add_argument("--single-file", action="store_true", help="merge all days into one raw file")
    g.add_argument("--config", default="config.yaml", help="DuckDB settings used for --single-file merging")
    g.set_defaults(func=cmd_generate)
    i = sub.add_parser("ingest", help="ingest ROOT/raw under the configured memory_limit and record peak memory")
    i.add_argument("--root", required=True)
    i.add_argument("--config", default="config.yaml")
    i.set_defaults(func=cmd_ingest)
    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
