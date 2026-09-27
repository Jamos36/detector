from __future__ import annotations

import csv
import inspect
import shutil
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from netanomaly import alerts, features, iforest
from netanomaly.cli import main
from netanomaly.config import ModelSettings
from netanomaly.feature_registry import load_registry
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


def test_registry_inputs_exist_in_the_lake(synth_root, con):
    registry = load_registry()
    lake_columns = {r[0] for r in con.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{features.flows_glob(synth_root / 'lake')}', hive_partitioning = true)"
    ).fetchall()}
    inputs = {i for f in registry.features for i in f.inputs}
    assert inputs <= lake_columns
    assert {d.name for d in registry.derived_columns} <= lake_columns
    host_window = pq.read_schema(next((synth_root / "features" / "host_window").rglob("*.parquet"))).names
    assert set(features.HOST_WINDOW_FEATURES) <= set(host_window)


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


# --- flow_sequence is a synthetic evaluation key only (V1-5) -------------------

def _recall(root: Path, lake: Path, truth: Path, con) -> list[tuple]:
    return alerts.recall_at_k(con, root / "outputs" / "scores.parquet", lake, truth, k=100, window_minutes=5)


def test_recall_refuses_a_lake_where_a_truth_flow_sequence_repeats(synth_root, con, tmp_path):
    lake = tmp_path / "lake"
    shutil.copytree(synth_root / "lake", lake)
    truth_csv = (synth_root / "truth" / "injected_flows.csv").as_posix()
    # a second file with the same flow_sequence, e.g. another exporter or an earlier `generate` into the same root
    src, seq = con.sql(f"""
        SELECT filename, flow_sequence FROM read_parquet('{features.flows_glob(lake)}', filename = true)
        WHERE flow_sequence = (SELECT min(flow_sequence) FROM read_csv('{truth_csv}'))""").fetchone()
    dup = Path(src).parent / "src_duplicate_0.parquet"
    con.execute(f"COPY (SELECT * FROM read_parquet('{Path(src).as_posix()}') WHERE flow_sequence = {seq}) "
                f"TO '{dup.as_posix()}' (FORMAT parquet)")
    with pytest.raises(ValueError, match="1 truth flow_sequence value.* more than one lake flow"):
        _recall(synth_root, lake, synth_root / "truth", con)


def test_recall_refuses_truth_that_points_at_flows_missing_from_the_lake(synth_root, con, tmp_path):
    truth = tmp_path / "truth"
    shutil.copytree(synth_root / "truth", truth)
    with (truth / "injected_flows.csv").open("a", newline="", encoding="utf-8") as fh:
        fh.write('999999999999,"20260902-brute_force-0"\n')
    with pytest.raises(ValueError, match="1 truth flow_sequence value.* no lake flow"):
        _recall(synth_root, synth_root / "lake", truth, con)


def test_production_modules_do_not_use_flow_sequence():
    """Ingest, features, scoring, alerting and DQ must not key on flow_sequence: its uniqueness in real exports
    is unverified. flow_id (source file hash + row number) is the traceability key."""
    src = Path(alerts.__file__).parent
    allowed = {"synth.py", "alerts.py"}  # generator and synthetic recall@K only
    offenders = [p.name for p in src.glob("*.py") if p.name not in allowed and "flow_sequence" in p.read_text("utf-8")]
    assert offenders == []
    assert "flow_sequence" not in inspect.getsource(alerts.write_top_alerts)


# --- host baselines (V2-2) ---------------------------------------------------

def test_baselines_cover_every_host_window_without_changing_v0_features(synth_root, con):
    hw = synth_root / "features" / "host_window"
    before = sorted(p.read_bytes() for p in hw.rglob("*.parquet"))
    main(["--root", str(synth_root), "baselines"])
    assert sorted(p.read_bytes() for p in hw.rglob("*.parquet")) == before  # V0 features untouched
    base = (synth_root / "features" / "host_baseline" / "**" / "*.parquet").as_posix()
    rows, mismatched, by_day = con.sql(f"""
        SELECT count(*), count(*) FILTER (WHERE h.bytes_out IS DISTINCT FROM b.bytes_out),
               list(DISTINCT b.flow_date || ':' || b.baseline_quality ORDER BY b.flow_date || ':' || b.baseline_quality)
        FROM read_parquet('{base}', hive_partitioning = true) b
        FULL JOIN read_parquet('{(hw / '**' / '*.parquet').as_posix()}', hive_partitioning = true) h
          USING (src_ip, window_start)""").fetchone()
    assert rows > 0 and mismatched == 0
    # two synthetic days: day 1 has no earlier data; day 2 has one earlier day < min_days (2)
    assert by_day == ["2026-09-01:none", "2026-09-02:none"]
