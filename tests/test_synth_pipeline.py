from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from netanomaly import alerts, features, iforest
from netanomaly.cli import main
from netanomaly.config import ModelSettings
from netanomaly.ingest import ingest_directory
from netanomaly.inject import ATTACKS, InjectionLog, make_injector
from netanomaly.synth import generate

START = date(2026, 9, 1)


@pytest.fixture(scope="module")
def synth_root(tmp_path_factory) -> Path:
    """Two small days (one clean, one with every attack type), run through the full CLI."""
    root = tmp_path_factory.mktemp("synth")
    base = ["--root", str(root)]
    main([*base, "generate", "--days", "2", "--clean-days", "1", "--hosts", "60", "--seed", "3"])
    main([*base, "run"])
    return root


def test_generator_matches_raw_schema_contract(tmp_path, contract):
    generate(tmp_path / "raw", tmp_path / "truth", days=1, n_hosts=40, seed=1)
    (f,) = (tmp_path / "raw").glob("*.parquet")
    assert pq.read_schema(f).names == contract.raw_names


def test_generator_is_deterministic_for_a_seed(tmp_path):
    for name in ("a", "b"):
        generate(tmp_path / name, tmp_path / f"{name}_truth", days=1, n_hosts=40, seed=5)
    a, b = (pq.read_table(next((tmp_path / n).glob("*.parquet"))) for n in ("a", "b"))
    assert a.equals(b)


def test_hosts_persist_and_flags_are_consistent(synth_root, con):
    hosts, flows_per_host, max_syn, inconsistent = con.sql(f"""
        SELECT count(DISTINCT src_ip),
               count(*) / count(DISTINCT src_ip),
               max(packets) FILTER (WHERE tcp_flags = 'SYN'),
               count(*) FILTER (WHERE tcp_flags IN ('FIN-ACK', 'ACK-PSH-FIN', 'RST')
                                AND flow_end_reason <> 'end_of_flow')
        FROM read_parquet('{features.flows_glob(synth_root / 'lake')}')""").fetchone()
    assert hosts <= 60
    assert flows_per_host > 100  # the whole point: hosts recur
    assert max_syn <= 3
    assert inconsistent == 0


def test_injection_truth_points_at_injected_flows(tmp_path, con, contract):
    log = InjectionLog()
    generate(tmp_path / "raw", tmp_path / "truth", days=1, n_hosts=40, seed=2,
             injector=make_injector(log, {START}))
    assert sorted({r["attack_type"] for r in log.records}) == sorted(ATTACKS)
    ingest_directory(con, tmp_path / "raw", contract, tmp_path / "lake", tmp_path / "stage")
    rows = con.sql(f"""
        SELECT t.injection_id, f.src_ip
        FROM read_csv('{(tmp_path / 'truth' / 'injected_flows.csv').as_posix()}') t
        JOIN read_parquet('{features.flows_glob(tmp_path / 'lake')}') f USING (flow_sequence)""").fetchall()
    expected_src = {r["injection_id"]: r["src_ip"] for r in log.records}
    assert len(rows) == sum(r["n_flows"] for r in log.records)
    assert all(expected_src[iid] == src for iid, src in rows)


def test_batched_scoring_equals_single_batch(synth_root, con, tmp_path):
    hw = synth_root / "features" / "host_window"
    model, manifest, _ = iforest.train(con, hw, list(features.HOST_WINDOW_FEATURES),
                                       ModelSettings(train_sample_rows=5_000, n_estimators=20), tmp_path / "m")
    n_small = iforest.score(con, hw, model, manifest, tmp_path / "small.parquet", batch_rows=1_000)
    n_big = iforest.score(con, hw, model, manifest, tmp_path / "big.parquet", batch_rows=10_000_000)
    q = "SELECT src_ip, window_start, anomaly_score FROM read_parquet('{}') ORDER BY 1, 2"
    assert n_small == n_big
    assert con.sql(q.format((tmp_path / "small.parquet").as_posix())).fetchall() == \
        con.sql(q.format((tmp_path / "big.parquet").as_posix())).fetchall()


def test_alerts_respect_budget_and_trace_to_lake_flows(synth_root, con):
    rows = list(csv.DictReader((synth_root / "outputs" / "top_alerts.csv").open(encoding="utf-8")))
    per_day: dict[str, int] = {}
    for r in rows:
        per_day[r["flow_date"]] = per_day.get(r["flow_date"], 0) + 1
    assert rows and max(per_day.values()) <= 100
    first_ids = {r["sample_flow_ids"].strip("[]").split(", ")[0] for r in rows[:10]}
    known = {x for (x,) in con.sql(
        f"SELECT flow_id FROM read_parquet('{features.flows_glob(synth_root / 'lake')}')").fetchall()}
    assert first_ids <= known


def test_recall_reports_every_attack_type(synth_root, con):
    result = alerts.recall_at_k(con, synth_root / "outputs" / "scores.parquet", synth_root / "lake",
                                synth_root / "truth", k=100, window_minutes=5)
    assert sorted(r[0] for r in result) == sorted(ATTACKS)
    assert all(r[2] == 1 for r in result)  # one injection of each type on the attack day
