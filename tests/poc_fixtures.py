"""Tiny deterministic Parquet fixtures for the PoC tests (test code only; not a data generator for the pipeline).

Columns use the netflow_v1 contract's raw names so the default field mapping applies. Every value is derived from
the row's position, so tests can compute expected aggregates by hand.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

START = datetime(2026, 1, 5, tzinfo=UTC)  # a Monday


def flow_table(rows: list[dict]) -> pa.Table:
    """rows: dicts with src, dst, port, proto, bytes, packets, start (datetime), dur_s."""
    return pa.table({
        "src_id_addr": pa.array([r["src"] for r in rows], pa.string()),
        "dist_id_addr": pa.array([r["dst"] for r in rows], pa.string()),
        "dist_port": pa.array([r["port"] for r in rows], pa.int64()),
        "ip_protocol_id": pa.array([r["proto"] for r in rows], pa.int64()),
        "num_bytes": pa.array([r["bytes"] for r in rows], pa.int64()),
        "num_packets": pa.array([r["packets"] for r in rows], pa.int64()),
        "flow_start_time": pa.array([r["start"] for r in rows], pa.timestamp("us", tz="UTC")),
        "flow_end_time": pa.array([r["start"] + timedelta(seconds=r["dur_s"]) for r in rows],
                                  pa.timestamp("us", tz="UTC")),
    })


def regular_rows(day: int, hosts: int = 8) -> list[dict]:
    """Ordinary-looking traffic: each host sends 2-7 flows per hour to a few servers on common ports, with varied
    sizes and durations (varied enough that no model input is constant on the training days)."""
    out = []
    base = START + timedelta(days=day)
    for h in range(hosts):
        for hour in range(24):
            for k in range(2 + (h * 7 + hour * 3 + day) % 6):
                i = h * 1000 + hour * 10 + k + day * 7
                out.append({"src": f"10.0.0.{h + 1}", "dst": f"10.0.1.{i % 5 + 1}",
                            "port": (443, 80, 53, 22)[i % 4], "proto": 17 if i % 4 == 2 else 6,
                            "bytes": 300 + (i * 379) % 5000, "packets": 3 + (i * 7) % 20,
                            "start": base + timedelta(hours=hour, minutes=(5 + 9 * k) % 60), "dur_s": (i * 13) % 30})
    return out


def scan_rows(day: int, hour: int, host: str = "10.0.0.99", ports: int = 200) -> list[dict]:
    """A loud port sweep: `ports` tiny flows to distinct ports in one hour."""
    base = START + timedelta(days=day, hours=hour)
    return [{"src": host, "dst": "10.0.2.50", "port": 1000 + p, "proto": 6, "bytes": 60, "packets": 1,
             "start": base + timedelta(seconds=10 * p), "dur_s": 0} for p in range(ports)]


def write_days(directory: Path, days: range, extra: dict[int, list[dict]] | None = None) -> Path:
    """One Parquet file per day with regular traffic plus optional extra rows for that day."""
    directory.mkdir(parents=True, exist_ok=True)
    for d in days:
        rows = regular_rows(d) + (extra or {}).get(d, [])
        pq.write_table(flow_table(rows), directory / f"flows_{d:02d}.parquet")
    return directory


def write_config(path: Path, data_dir: Path, work_dir: Path, *, annotations: Path | None = None,
                 extra: str = "") -> Path:
    ann = f"annotations: {annotations.as_posix()}\n" if annotations else ""
    path.write_text(f"""name: fixture
input:
  paths: ["{data_dir.as_posix()}"]
work_dir: {work_dir.as_posix()}
{ann}annotation_buffer_hours: 12
window_minutes: 60
split:
  train: {{start: 2026-01-05, end: 2026-01-09}}
  validation: {{start: 2026-01-09, end: 2026-01-11}}
  test: {{start: 2026-01-11, end: 2026-01-13}}
models:
  iforest: {{n_estimators: 50, max_train_rows: 2000}}
  ocsvm: {{max_train_rows: 400}}
  n_jobs: 1
diagnostics: {{seed_repeats: 1, top_n: 20, top_k_per_day: 5, buffer_hours_sensitivity: [0, 24]}}
report: {{top_candidates: 5, heatmap_entities: 5, zoom_periods: 1, trace_top_n: 5}}
{extra}""", encoding="utf-8")
    return path
