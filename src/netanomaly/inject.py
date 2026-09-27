"""Known-answer attack injection for label-free evaluation (roadmap V4).

Each attack is generated as extra flows from a real host in the topology, so
it must be found against that host's own normal behaviour. Every injected flow
is tagged with an injection_id; the tag never reaches the raw files, only the
separate truth files.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date

import numpy as np

from netanomaly.synth import RESERVED_EXTERNAL, US, Core, Topology, day_start_us


@dataclass
class InjectionLog:
    records: list[dict] = field(default_factory=list)


def _flows(n: int, src: str, dst: np.ndarray, dst_port: np.ndarray, start_us: np.ndarray, *, packets: np.ndarray,
           bytes_: np.ndarray, dur_us: np.ndarray, flag: np.ndarray, reason: np.ndarray, injection_id: str,
           proto: int = 6, ttl: int = 128) -> Core:
    return {
        "src_ip": np.full(n, src), "dst_ip": np.asarray(dst, dtype=str), "dst_port": np.asarray(dst_port, dtype=np.int64),
        "proto": np.full(n, proto), "tcp_flag": np.asarray(flag, dtype=object), "packets": packets.astype(np.int64),
        "bytes": bytes_.astype(np.int64), "dur_us": dur_us.astype(np.int64), "end_reason": np.asarray(reason, dtype=object),
        "start_us": start_us.astype(np.int64), "ttl_init": np.full(n, ttl), "injection_id": np.full(n, injection_id),
    }


def _probe_shape(rng: np.random.Generator, n: int) -> dict[str, np.ndarray]:
    """Failed/refused connection attempts: 1-2 packets, SYN or RST."""
    packets = rng.integers(1, 3, n)
    refused = rng.random(n) < 0.3
    return {"packets": packets, "bytes_": packets * 60, "dur_us": rng.integers(1_000, 3 * US, n),
            "flag": np.where(refused, "RST", "SYN"), "reason": np.where(refused, "end_of_flow", "idle_timeout")}


def vertical_scan(rng, src, target, t0_us, iid, n_ports=300, span_s=600) -> Core:
    ports = rng.choice(np.arange(1, 65536), n_ports, replace=False)
    return _flows(n_ports, src, np.full(n_ports, target), ports, t0_us + np.sort(rng.integers(0, span_s * US, n_ports)),
                  injection_id=iid, **_probe_shape(rng, n_ports))


def horizontal_scan(rng, src, targets, t0_us, iid, port=445, span_s=900) -> Core:
    n = len(targets)
    return _flows(n, src, targets, np.full(n, port), t0_us + np.sort(rng.integers(0, span_s * US, n)),
                  injection_id=iid, **_probe_shape(rng, n))


def beaconing(rng, src, c2_ip, t0_us, iid, interval_s=60, jitter=0.1, hours=6) -> Core:
    n = int(hours * 3600 / interval_s)
    gaps = interval_s * US * (1 + rng.uniform(-jitter, jitter, n))
    packets = rng.integers(4, 7, n)
    return _flows(n, src, np.full(n, c2_ip), np.full(n, 443), t0_us + np.cumsum(gaps).astype(np.int64),
                  packets=packets, bytes_=packets * rng.integers(180, 260, n), dur_us=rng.integers(50_000, 400_000, n),
                  flag=np.full(n, "ACK-PSH-FIN"), reason=np.full(n, "end_of_flow"), injection_id=iid)


def exfil_burst(rng, src, dest_ip, t0_us, iid, total_mb=500, n_flows=20, span_s=1800) -> Core:
    bytes_ = rng.dirichlet(np.ones(n_flows)) * total_mb * 1_000_000
    return _flows(n_flows, src, np.full(n_flows, dest_ip), np.full(n_flows, 443),
                  t0_us + np.sort(rng.integers(0, span_s * US, n_flows)), packets=np.maximum(1, bytes_ // 1400),
                  bytes_=bytes_, dur_us=rng.integers(20 * US, 90 * US, n_flows), flag=np.full(n_flows, "ACK-PSH-FIN"),
                  reason=np.full(n_flows, "end_of_flow"), injection_id=iid)


def brute_force(rng, src, target, t0_us, iid, port=22, attempts=300, span_s=900) -> Core:
    packets = rng.integers(10, 15, attempts)
    failed = rng.random(attempts) < 0.5
    return _flows(attempts, src, np.full(attempts, target), np.full(attempts, port),
                  t0_us + np.sort(rng.integers(0, span_s * US, attempts)), packets=packets,
                  bytes_=packets * rng.integers(120, 220, attempts), dur_us=rng.integers(500_000, 3 * US, attempts),
                  flag=np.where(failed, "RST", "FIN-ACK"), reason=np.full(attempts, "end_of_flow"), injection_id=iid)


ATTACKS = ("vertical_scan", "horizontal_scan", "beaconing", "exfil_burst", "brute_force")
HSCAN_TARGETS = 120


def make_injector(log: InjectionLog, attack_days: set[date], per_type: int = 1):
    """Injector for synth.generate: on each attack day, inject `per_type` of each attack."""

    def inject(rng: np.random.Generator, topo: Topology, day: date, core: Core) -> Core:
        core = {**core, "injection_id": np.full(len(core["src_ip"]), "")}
        if day not in attack_days:
            return core
        parts = [core]
        workstations = topo.ip[topo.role == "workstation"]
        reserved = topo.external[-RESERVED_EXTERNAL:]
        for kind in ATTACKS:
            for k in range(per_type):
                iid = f"{day:%Y%m%d}-{kind}-{k}"
                src = str(rng.choice(workstations))
                t0 = day_start_us(day) + int(rng.integers(1, 20)) * 3600 * US
                if kind == "vertical_scan":
                    dst = str(rng.choice(topo.server))
                    flows = vertical_scan(rng, src, dst, t0, iid)
                elif kind == "horizontal_scan":
                    others = topo.ip[topo.ip != src]
                    targets = rng.choice(others, min(HSCAN_TARGETS, len(others)), replace=False)
                    dst = f"{len(targets)} internal hosts"
                    flows = horizontal_scan(rng, src, targets, t0, iid)
                elif kind == "beaconing":
                    dst = str(rng.choice(reserved))
                    flows = beaconing(rng, src, dst, t0, iid)
                elif kind == "exfil_burst":
                    dst = str(rng.choice(reserved))
                    flows = exfil_burst(rng, src, dst, t0, iid)
                else:
                    dst = str(rng.choice(topo.server))
                    flows = brute_force(rng, src, dst, t0, iid)
                parts.append(flows)
                log.records.append({
                    "injection_id": iid, "attack_type": kind, "src_ip": src, "dst": dst,
                    "start_us": int(flows["start_us"].min()), "end_us": int((flows["start_us"] + flows["dur_us"]).max()),
                    "n_flows": len(flows["src_ip"]), "params": json.dumps({"day": str(day)}),
                })
        return {k: np.concatenate([p[k] for p in parts]) for k in core}

    return inject
