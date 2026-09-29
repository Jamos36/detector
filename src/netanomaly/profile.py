"""`profile`: what is in the external Parquet, computed as DuckDB aggregates (no rows enter Python).

Reports files, row counts (footers), union schema with per-file presence, the field mapping, null/invalid rates of
the mapped fields, the UTC date span, rows per day, approximate cardinalities and the approximate number of
host-window feature rows for the configured window, so memory can be estimated before building features.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from netanomaly.source import FIELDS, Source, _listing, columns_per_file, source_schema

PROFILE_JSON, PROFILE_MD = "profile.json", "profile.md"


def build_profile(con: duckdb.DuckDBPyConnection, source: Source, window_minutes: int) -> dict:
    files = source.files
    footer = con.execute(f"SELECT file_name, num_rows FROM parquet_file_metadata({_listing(files)})").fetchall()
    schema = source_schema(con, files)
    per_file = columns_per_file(con, files)
    mapped = {f.source: f.name for f in source.mapping.fields if f.status == "ok"}
    nulls = ", ".join(f"count(*) - count({f.name}) AS null_{f.name}" for f in FIELDS)
    w = int(window_minutes)
    agg = con.execute(f"""
SELECT count(*) AS rows, {nulls},
  count(*) FILTER (WHERE flow_start_status = 'invalid') AS flow_start_invalid,
  count(*) FILTER (WHERE flow_start_status = 'no_offset') AS flow_start_no_offset,
  count(*) FILTER (WHERE flow_end < flow_start) AS end_before_start,
  count(*) FILTER (WHERE dst_ip IS NOT NULL AND dst_is_private IS NULL) AS dst_not_ipv4,
  min(flow_start)::VARCHAR AS first_flow, max(flow_start)::VARCHAR AS last_flow,
  approx_count_distinct(src_ip) AS approx_src_ip, approx_count_distinct(dst_ip) AS approx_dst_ip,
  approx_count_distinct(dst_port) AS approx_dst_port,
  approx_count_distinct(hash(src_ip, time_bucket(INTERVAL '{w} minutes', flow_start))) AS approx_host_windows
FROM ({source.flows_sql()})""")
    summary = dict(zip([d[0] for d in agg.description], agg.fetchone(), strict=True))
    daily = con.execute(f"""
SELECT CAST(flow_start AS DATE)::VARCHAR AS day, count(*) AS flows, approx_count_distinct(src_ip) AS hosts
FROM ({source.flows_sql()}) WHERE flow_start IS NOT NULL GROUP BY 1 ORDER BY 1""").fetchall()
    rows = summary["rows"] or 1
    return {
        "files": {"count": len(files), "bytes": sum(f.size for f in files),
                  "footer_rows": sum(n for _, n in footer), "min_rows": min(n for _, n in footer),
                  "max_rows": max(n for _, n in footer)},
        "columns": [{"name": c, "type": t, "files_present": per_file.get(c, 0), "mapped_as": mapped.get(c)}
                    for c, t in schema.items()],
        "mapping": source.mapping.to_dict(),
        "summary": summary,
        "null_rates": {f.name: round(summary[f"null_{f.name}"] / rows, 6) for f in FIELDS},
        "daily": [{"day": d, "flows": n, "approx_hosts": h} for d, n, h in daily],
        "window_minutes": w,
        "estimated_feature_matrix_mb_per_feature": round(summary["approx_host_windows"] * 8 / 2**20, 1),
    }


def write_profile(profile: dict, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / PROFILE_JSON).write_text(json.dumps(profile, indent=2, default=str), encoding="utf-8")
    md = out_dir / PROFILE_MD
    md.write_text(profile_markdown(profile), encoding="utf-8")
    return md


def profile_markdown(p: dict) -> str:
    s, f = p["summary"], p["files"]
    days = p["daily"]
    unmapped = ", ".join(f"`{c}`" for c in p["mapping"]["unmapped_columns"]) or "none"
    lines = [
        "# Input profile", "",
        f"- Files: {f['count']} Parquet, {f['bytes'] / 2**20:.1f} MiB, {f['footer_rows']:,} rows (footers).",
        f"- Flow start span (UTC): {s['first_flow']} .. {s['last_flow']} over {len(days)} days with data.",
        (f"- Approx. distinct: src_ip {s['approx_src_ip']:,}, dst_ip {s['approx_dst_ip']:,}, "
        f"dst_port {s['approx_dst_port']:,}."),
        (f"- Approx. host x {p['window_minutes']}-min windows (feature rows): {s['approx_host_windows']:,} "
        f"(~{p['estimated_feature_matrix_mb_per_feature']} MiB per float64 feature column if all were loaded)."),
        (f"- flow_start invalid: {s['flow_start_invalid']:,}; without offset (assumed UTC): "
        f"{s['flow_start_no_offset']:,}; flow_end before flow_start: {s['end_before_start']:,}; "
        f"dst_ip not dotted IPv4: {s['dst_not_ipv4']:,}."),
        "", "## Field mapping (canonical <- source)", "",
        "| canonical | source | type | status | conversion | note |", "|---|---|---|---|---|---|",
        *[f"| {m['name']} | `{m['source']}` | {m['source_type']} | {m['status']} | {m['conversion']} | {m['note']} |"
          for m in p["mapping"]["fields"]],
        "", f"Unmapped source columns (never modelled): {unmapped}",
        "", "### Assumptions", "", *[f"- {a}" for a in p["mapping"]["assumptions"]],
        "", "## Null rates of canonical fields", "", "| field | null share |", "|---|---|",
        *[f"| {k} | {v:.4f} |" for k, v in p["null_rates"].items()],
        "", "## Rows per UTC day", "", "| day | flows | approx hosts |", "|---|---|---|",
        *[f"| {d['day']} | {d['flows']:,} | {d['approx_hosts']:,} |" for d in days], "",
    ]
    return "\n".join(lines)
