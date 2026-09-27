"""Synthetic NetFlow generator with PERSISTENT hosts.

The supplied mock data draws every row independently (58k distinct source IPs
in 60k flows), so it cannot exercise host-behaviour features. This generator
creates a fixed population of hosts with roles, daily cycles and stable
favourite destinations, and emits the exact 42-column raw schema.

Semantics are kept internally consistent (unlike the mock data):
SYN-only flows have 1-3 packets, FIN/RST flows end with end_of_flow,
long flows end with active_timeout, flow_length == duration in ms.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from netanomaly.schema import load_contract

US = 1_000_000
EXTERNAL_NETS = ("192.0.2", "198.51.100", "203.0.113")  # RFC 5737 documentation ranges
EXTERNAL_AS = {"192.0.2": 64496, "198.51.100": 64497, "203.0.113": 64498}
RESERVED_EXTERNAL = 40  # tail of the external pool never used by benign traffic
N_EXPORTERS = 4
TIME_CODES = np.array(["UTC", "FLOW_END", "INGEST", "EXPORT"])

# service: (dst_port, proto, dst_kind, pkt_median, pkt_sigma, bytes_per_pkt, dur_median_s, dur_sigma)
SERVICES: dict[str, tuple[int, int, str, float, float, float, float, float]] = {
    "https": (443, 6, "external", 20, 1.2, 700, 3.0, 1.3),
    "http": (80, 6, "external", 12, 1.0, 600, 1.5, 1.0),
    "dns": (53, 17, "dns", 1, 0.2, 90, 0.03, 0.5),
    "ntp": (123, 17, "external", 1, 0.1, 76, 0.02, 0.3),
    "smb": (445, 6, "file", 40, 1.3, 900, 5.0, 1.2),
    "db": (3306, 6, "db", 25, 1.0, 400, 2.0, 1.0),
    "ssh": (22, 6, "server", 150, 1.2, 200, 300.0, 1.0),
    "rdp": (3389, 6, "server", 400, 1.0, 500, 600.0, 1.0),
}
ROLE_MIX = {  # service probabilities per role
    "workstation": {"https": 0.55, "http": 0.08, "dns": 0.25, "ntp": 0.02, "smb": 0.10},
    "server": {"https": 0.25, "dns": 0.30, "ntp": 0.05, "db": 0.40},
    "admin": {"https": 0.40, "dns": 0.25, "smb": 0.10, "ssh": 0.15, "rdp": 0.10},
}
ROLE_RATE = {"workstation": 25.0, "server": 18.0, "admin": 20.0}  # flows/hour at peak
ROLE_OS_TTL = {"workstation": 128, "server": 64, "admin": 128}

Core = dict[str, np.ndarray]
Injector = Callable[[np.random.Generator, "Topology", date, Core], Core]


@dataclass(frozen=True)
class Topology:
    ip: np.ndarray        # host IPs
    role: np.ndarray      # workstation / server / admin
    subnet: np.ndarray    # "10.10.3.0/24"
    favorites: np.ndarray  # (n_hosts, k) indices into external pool
    external: np.ndarray  # external IP pool
    dns: np.ndarray       # internal server IPs by kind
    file: np.ndarray
    db: np.ndarray
    server: np.ndarray

    def targets(self, kind: str) -> np.ndarray:
        return {"dns": self.dns, "file": self.file, "db": self.db, "server": self.server}[kind]


def build_topology(rng: np.random.Generator, n_hosts: int = 300, n_favorites: int = 25) -> Topology:
    n_servers = max(8, n_hosts // 10)
    n_admins = max(2, n_hosts // 30)
    roles = np.array(["server"] * n_servers + ["admin"] * n_admins + ["workstation"] * (n_hosts - n_servers - n_admins))
    third = np.where(roles == "server", 20 + np.arange(n_hosts) // 250, 1 + np.arange(n_hosts) // 60)
    fourth = 10 + np.arange(n_hosts) % 240
    ips = np.array([f"10.10.{t}.{f}" for t, f in zip(third, fourth, strict=True)])
    servers = ips[roles == "server"]
    external = np.array([f"{net}.{i}" for net in EXTERNAL_NETS for i in range(1, 255)])
    n_benign = len(external) - RESERVED_EXTERNAL
    popularity = 1.0 / np.arange(1, n_benign + 1) ** 0.9
    popularity /= popularity.sum()
    favorites = np.stack([rng.choice(n_benign, n_favorites, replace=False, p=popularity) for _ in ips])
    return Topology(
        ip=ips, role=roles, subnet=np.array([f"10.10.{t}.0/24" for t in third]),
        favorites=favorites, external=external,
        dns=servers[:2], file=servers[2:4], db=servers[4:7], server=servers,
    )


def diurnal(role: str, hours: np.ndarray) -> np.ndarray:
    """Activity multiplier by UTC hour: workstations peak mid-day, servers are flat."""
    if role == "server":
        return np.full(hours.shape, 1.0)
    return 0.08 + 0.92 * np.exp(-(((hours - 14) / 3.5) ** 2))


def day_start_us(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * US)


def normal_day(rng: np.random.Generator, topo: Topology, day: date) -> Core:
    """Benign traffic for one UTC day as core columns (see to_raw_table)."""
    hours = np.arange(24)
    lam = np.stack([ROLE_RATE[r] * diurnal(r, hours) for r in topo.role])  # (hosts, 24)
    counts = rng.poisson(lam).ravel()
    host_idx = np.repeat(np.repeat(np.arange(len(topo.ip)), 24), counts)
    hour = np.repeat(np.tile(hours, len(topo.ip)), counts)
    n = len(host_idx)
    start_us = day_start_us(day) + hour * 3600 * US + rng.integers(0, 3600 * US, n)

    service = np.empty(n, dtype=object)
    for role, mix in ROLE_MIX.items():
        mask = topo.role[host_idx] == role
        service[mask] = rng.choice(list(mix), mask.sum(), p=list(mix.values()))

    dst = np.empty(n, dtype=object)
    n_benign = len(topo.external) - RESERVED_EXTERNAL
    for name, (_, _, kind, *_rest) in SERVICES.items():
        mask = service == name
        if not mask.any():
            continue
        if kind == "external":
            fav = topo.favorites[host_idx[mask], rng.integers(0, topo.favorites.shape[1], mask.sum())]
            novel = rng.random(mask.sum()) < 0.05  # occasional first-seen destination
            fav[novel] = rng.integers(0, n_benign, novel.sum())
            dst[mask] = topo.external[fav]
        else:
            dst[mask] = rng.choice(topo.targets(kind), mask.sum())

    core = service_shape(rng, service)
    ttl = np.array([ROLE_OS_TTL[r] for r in topo.role])
    core.update(src_ip=topo.ip[host_idx], dst_ip=dst.astype(str), start_us=start_us, ttl_init=ttl[host_idx])
    return core


def service_shape(rng: np.random.Generator, service: np.ndarray) -> Core:
    """Packet/byte/duration/flag profile for each flow's service."""
    n = len(service)
    params = np.array([SERVICES[s] for s in service], dtype=object).reshape(n, 8)
    dst_port = params[:, 0].astype(np.int64)
    proto = params[:, 1].astype(np.int64)
    pk_med, pk_sig, bpp, dur_med, dur_sig = (params[:, i].astype(float) for i in (3, 4, 5, 6, 7))
    packets = np.maximum(1, np.round(pk_med * rng.lognormal(0, pk_sig))).astype(np.int64)
    bytes_ = (packets * np.clip(bpp * rng.lognormal(0, 0.3, n), 40, 1500)).astype(np.int64)
    dur_us = np.maximum(1_000, dur_med * rng.lognormal(0, dur_sig) * US).astype(np.int64)

    outcome = rng.random(n)
    tcp = proto == 6
    flag = np.where(outcome < 0.75, "ACK-PSH-FIN", "FIN-ACK").astype(object)
    reason = np.full(n, "end_of_flow", dtype=object)
    long_lived = tcp & (dur_us > 120 * US)
    flag[long_lived], reason[long_lived] = "PSH-ACK", "active_timeout"
    reset = tcp & (outcome > 0.98)
    flag[reset], reason[reset] = "RST", "end_of_flow"
    failed = tcp & (outcome > 0.995)  # occasional benign failed connection
    flag[failed], reason[failed] = "SYN", "idle_timeout"
    packets[failed] = rng.integers(1, 4, failed.sum())
    bytes_[failed] = packets[failed] * 60
    flag[~tcp], reason[~tcp] = None, "idle_timeout"
    return {"dst_port": dst_port, "proto": proto, "tcp_flag": flag, "packets": packets, "bytes": bytes_,
            "dur_us": dur_us, "end_reason": reason}


def concat_core(*parts: Core) -> Core:
    return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}


def _asn(nets: np.ndarray) -> np.ndarray:
    return np.array([EXTERNAL_AS.get(x, 64512 + int(x.split(".")[2]) % 5) for x in nets])


def _arrow(v: np.ndarray | pa.Array) -> pa.Array:
    if isinstance(v, pa.Array):
        return v
    return pa.array(v.tolist()) if v.dtype.kind in "OU" else pa.array(v)


def to_raw_table(core: Core, rng: np.random.Generator, seq_start: int) -> pa.Table:
    """Expand core columns into the exact 42-column raw schema, sorted by start time."""
    order = np.argsort(core["start_us"], kind="stable")
    c = {k: v[order] for k, v in core.items()}
    n = len(order)
    src, dst = c["src_ip"].astype(str), c["dst_ip"].astype(str)
    src_net = np.array([s.rsplit(".", 1)[0] for s in src])
    dst_net = np.array([d.rsplit(".", 1)[0] for d in dst])
    src_port = np.where(c["proto"] == 1, 0, rng.integers(49152, 65536, n))
    dst_port = np.where(c["proto"] == 1, 0, c["dst_port"])
    net_idx = np.unique(src_net, return_inverse=True)[1]
    exporter = net_idx % N_EXPORTERS
    vlan = 10 * (1 + net_idx % 30)
    ttl_min = c["ttl_init"] - rng.integers(1, 6, n)
    start, end = c["start_us"], c["start_us"] + c["dur_us"]
    proto_name = {1: "ICMP", 6: "TCP", 17: "UDP"}
    proto_full = {1: "Internet Control Message Protocol", 6: "Transmission Control Protocol",
                  17: "User Datagram Protocol"}

    def ts(us: np.ndarray) -> pa.Array:
        return pa.array(us, pa.timestamp("us", tz="UTC"))

    cols = {
        "src_id_addr": src, "src_subnet": np.char.add(src_net, ".0/24"), "src_port": src_port,
        "src_mask": np.full(n, 24), "src_as": _asn(src_net), "dist_id_addr": dst,
        "dist_subnet": np.char.add(dst_net, ".0/24"), "dist_port": dst_port, "dist_mask": np.full(n, 24),
        "dist_as": _asn(dst_net),
        "pair_socket": np.array([f"{a}:{b}->{x}:{y}" for a, b, x, y in zip(src, src_port, dst, dst_port, strict=True)]),
        "pair_ip": np.char.add(np.char.add(src, "->"), dst),
        "device_ip_addr": np.char.add("10.255.0.", (exporter + 1).astype(str)),
        "ip_version": np.full(n, 4), "ip_version_name": np.full(n, "IPv4"), "ip_version_id": np.full(n, 4),
        "ip_class_of_service": rng.choice([0, 0, 0, 8, 46], n),
        "ip_protocol_id": c["proto"], "ip_protocol_full_name": np.array([proto_full[p] for p in c["proto"]]),
        "ip_protocol_name": np.array([proto_name[p] for p in c["proto"]]),
        "next_hop_id_addr": np.char.add("10.254.0.", (exporter + 1).astype(str)),
        "observation_domain_id": exporter + 1, "packet_length": np.maximum(1, c["bytes"] // c["packets"]),
        "icmp_type_id": np.where(c["proto"] == 1, 8, -1), "ingress_id": 1 + net_idx % 30,
        "egress_id": np.full(n, 64), "tcp_flag": c["tcp_flag"], "num_bytes": c["bytes"],
        "num_packets": c["packets"], "ttl_min": ttl_min, "ttl_max": ttl_min + rng.integers(0, 2, n),
        "flow_start_time": ts(start), "flow_end_time": ts(end), "flow_end_reason": c["end_reason"],
        "flow_sequence": np.arange(seq_start, seq_start + n), "flow_set_id": 256 + exporter,
        "flow_length": c["dur_us"] // 1000, "vlad_id": vlan, "vlad_id_dot": vlan.astype(float),
        "vlad_id_customer": rng.integers(1000, 4095, n),
        "time_stamp": ts(end + rng.integers(0, 2 * US, n)), "time_code": rng.choice(TIME_CODES, n),
    }
    table = pa.table({k: _arrow(v) for k, v in cols.items()})
    if table.column_names != load_contract().raw_names:
        raise RuntimeError("generator drifted from schema contract")
    return table


def write_table(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        pq.write_table(table, path, compression="zstd")
    else:
        pacsv.write_csv(table, path)


def generate(out_dir: Path, truth_dir: Path, days: int = 6, n_hosts: int = 300, seed: int = 7,
             start: date = date(2026, 9, 1), fmt: str = "parquet", injector: Injector | None = None) -> Topology:
    """Write one raw file per UTC day to out_dir; host roles go to truth_dir (never ingested)."""
    rng = np.random.default_rng(seed)
    topo = build_topology(rng, n_hosts)
    truth_dir.mkdir(parents=True, exist_ok=True)
    pacsv.write_csv(pa.table({"ip": topo.ip, "role": topo.role, "subnet": topo.subnet}), truth_dir / "hosts.csv")
    seq = 1
    truth_seq: list[np.ndarray] = []
    truth_ids: list[np.ndarray] = []
    for d in range(days):
        day = start + timedelta(days=d)
        core = normal_day(rng, topo, day)
        if injector is not None:
            core = injector(rng, topo, day, core)
        table = to_raw_table(core, rng, seq)
        if "injection_id" in core:  # map injected flows to the flow_sequence they received
            ids = core["injection_id"][np.argsort(core["start_us"], kind="stable")]
            hit = ids != ""
            truth_seq.append(seq + np.flatnonzero(hit))
            truth_ids.append(ids[hit])
        seq += table.num_rows
        write_table(table, out_dir / f"synth_netflow_{day:%Y%m%d}.{fmt}")
    if truth_seq:
        pacsv.write_csv(pa.table({"flow_sequence": np.concatenate(truth_seq),
                                  "injection_id": np.concatenate(truth_ids).tolist()}),
                        truth_dir / "injected_flows.csv")
    return topo
