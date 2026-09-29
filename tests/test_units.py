"""Unit tests for the Parquet-only PoC (src/netanomaly/poc): config, source mapping, repo boundary, features,
splits, annotations, models and bands. Fixtures are tiny hand-built Parquet files (tests/poc_fixtures.py)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from netanomaly import annotations as ann
from netanomaly import bands, featureset, models, source, splits
from netanomaly.config import BandSettings, DuckDBSettings, PocConfig, SplitSettings, load_poc_config
from netanomaly.db import connect
from tests.fixtures import START, flow_table, regular_rows, scan_rows


@pytest.fixture
def con(tmp_path):
    return connect(DuckDBSettings(temp_directory=tmp_path / "duck"))


def _source(con, data: Path, **kw) -> source.Source:
    return source.open_source(con, [str(data)], kw.get("field_map", {}), kw.get("epoch_unit", {}))


def _defaults(tmp_path: Path) -> PocConfig:
    return PocConfig(input={"paths": ["x"]}, work_dir=tmp_path)


# --- config ------------------------------------------------------------------------------------------------------

def test_config_expands_environment_variables_and_refuses_unresolved(tmp_path, monkeypatch):
    cfg = tmp_path / "poc.yaml"
    cfg.write_text("input: {paths: ['${POC_TEST_DATA}/x']}\nwork_dir: ${POC_TEST_WORK}\n", encoding="utf-8")
    monkeypatch.setenv("POC_TEST_DATA", str(tmp_path / "data"))
    monkeypatch.setenv("POC_TEST_WORK", str(tmp_path / "work"))
    loaded = load_poc_config(cfg)
    assert Path(loaded.input.paths[0]) == tmp_path / "data" / "x"
    assert loaded.work_dir == tmp_path / "work"
    assert loaded.duckdb.temp_directory == tmp_path / "work" / "tmp" / "duckdb"
    monkeypatch.delenv("POC_TEST_WORK")
    with pytest.raises(ValueError, match="unresolved environment variable"):
        load_poc_config(cfg)


ROOT = Path(__file__).parents[1]


def test_shipped_config_is_valid_and_points_at_the_bundled_demo_data():
    cfg = load_poc_config(ROOT / "config.yaml")
    assert cfg.allow_inside_repo and cfg.split.train is not None
    assert all(Path(p).is_dir() for p in cfg.input.paths)
    assert cfg.work_dir == ROOT / "outputs"  # git-ignored
    assert [a.name for a in ann.load_annotations(cfg.annotations)] == ["demo-injected-attack-days"]


def test_memory_gb_derives_memory_settings_unless_they_are_set_explicitly(tmp_path):
    small = PocConfig(input={"paths": ["x"]}, work_dir=tmp_path, memory_gb=1, threads=2)
    big = PocConfig(input={"paths": ["x"]}, work_dir=tmp_path, memory_gb=16)
    assert small.duckdb.memory_limit == "0.5GB" and big.duckdb.memory_limit == "8GB"
    assert small.duckdb.threads == small.models.n_jobs == 2
    assert small.batch_rows < big.batch_rows and small.models.max_matrix_mb < big.models.max_matrix_mb
    assert small.models.iforest.max_train_rows < big.models.iforest.max_train_rows
    explicit = PocConfig(input={"paths": ["x"]}, work_dir=tmp_path, memory_gb=1, batch_rows=123_456,
                         duckdb={"memory_limit": "3GB"}, models={"ocsvm": {"max_train_rows": 777}})
    assert explicit.batch_rows == 123_456 and explicit.duckdb.memory_limit == "3GB"
    assert explicit.models.ocsvm.max_train_rows == 777 and explicit.models.iforest.max_train_rows == 50_000


def test_generated_docs_are_current():
    from netanomaly.featureset import features_document
    from netanomaly.schema import load_contract

    assert (ROOT / "FEATURES.md").read_text(encoding="utf-8") == features_document(), "run: uv run netanomaly docs"
    assert (ROOT / "SCHEMA.md").read_text(encoding="utf-8") == load_contract().to_markdown(), \
        "run: uv run netanomaly docs"


def test_config_rejects_windows_that_do_not_divide_a_day_and_unordered_bands(tmp_path):
    with pytest.raises(ValueError, match="divide 1440"):
        PocConfig(input={"paths": ["x"]}, work_dir=tmp_path, window_minutes=7)
    with pytest.raises(ValueError, match="strictly ordered"):
        BandSettings(quantiles={"Critical": 0.99, "High": 0.995, "Medium": 0.98, "Low": 0.9})
    with pytest.raises(ValueError, match="differ"):
        BandSettings(below_label="Low")


# --- source mapping and boundary ---------------------------------------------------------------------------------

def test_default_mapping_uses_contract_names_and_reports_unmapped(con, tmp_path):
    t = flow_table(regular_rows(0))
    (tmp_path / "d").mkdir()
    pq.write_table(t.append_column("extra_col", pa.array([1] * t.num_rows)), tmp_path / "d" / "a.parquet")
    s = _source(con, tmp_path / "d")
    usable = s.mapping.usable
    assert usable["src_ip"].source == "src_id_addr" and usable["flow_start"].conversion == "UTC instant"
    assert s.mapping.unmapped_columns == ["extra_col"]
    assert any("0/42" in a for a in s.mapping.assumptions)


def test_non_parquet_input_is_refused(tmp_path):
    bad = tmp_path / "d" / "flows.parquet"
    bad.parent.mkdir()
    bad.write_text("src_id_addr,flow_start_time\n1.2.3.4,2026-01-01\n", encoding="utf-8")
    with pytest.raises(source.SourceError, match="Parquet only"):
        source.resolve_files([str(bad.parent)])


def test_missing_required_field_and_integer_ips_are_reported(con, tmp_path):
    t = flow_table(regular_rows(0)).drop_columns(["src_id_addr"])
    t = t.append_column("src_int", pa.array(range(t.num_rows), pa.int64()))
    (tmp_path / "d").mkdir()
    pq.write_table(t, tmp_path / "d" / "a.parquet")
    with pytest.raises(source.SourceError, match="src_ip <- src_id_addr: missing"):
        _source(con, tmp_path / "d")
    with pytest.raises(source.SourceError, match="integer-encoded IPs are not converted"):
        _source(con, tmp_path / "d", field_map={"src_ip": "src_int"})
    with pytest.raises(source.SourceError, match="unknown canonical field"):
        _source(con, tmp_path / "d", field_map={"source_ip": "src_int"})


def test_integer_epoch_needs_a_unit_and_converts_to_utc(con, tmp_path):
    epoch_ms = int(datetime(2026, 1, 5, 10, 30, tzinfo=UTC).timestamp() * 1000)
    (tmp_path / "d").mkdir()
    pq.write_table(pa.table({"src_id_addr": ["10.0.0.1"], "ts": pa.array([epoch_ms], pa.int64())}),
                   tmp_path / "d" / "a.parquet")
    with pytest.raises(source.SourceError, match="epoch_unit"):
        _source(con, tmp_path / "d", field_map={"flow_start": "ts"})
    s = _source(con, tmp_path / "d", field_map={"flow_start": "ts"}, epoch_unit={"flow_start": "ms"})
    got = con.execute(f"SELECT flow_start, dst_ip, bytes FROM ({s.flows_sql()})").fetchone()
    assert got[0] == datetime(2026, 1, 5, 10, 30, tzinfo=UTC)
    assert got[1] is None and got[2] is None  # unmapped optional fields are NULL columns


def test_naive_timestamps_are_assumed_utc_and_the_assumption_is_recorded(con, tmp_path):
    (tmp_path / "d").mkdir()
    pq.write_table(pa.table({"src_id_addr": ["10.0.0.1"],
                             "flow_start_time": pa.array([datetime(2026, 1, 5, 10)], pa.timestamp("us"))}),  # noqa: DTZ001 (naive on purpose)
                   tmp_path / "d" / "a.parquet")
    s = _source(con, tmp_path / "d")
    assert "ASSUMED UTC" in s.mapping.usable["flow_start"].conversion
    assert any("ASSUMED UTC" in a for a in s.mapping.assumptions)
    got = con.execute(f"SELECT flow_start FROM ({s.flows_sql()})").fetchone()[0]
    assert got == datetime(2026, 1, 5, 10, tzinfo=UTC)


def test_repository_boundary_refuses_paths_inside_the_checkout(tmp_path):
    root = source.repo_root()
    assert root is not None
    with pytest.raises(source.SourceError, match="ADR-012"):
        source.check_outside_repo([root / "data"], "input data", allow=False)
    source.check_outside_repo([root / "data"], "input data", allow=True)  # explicit mock/synthetic assertion
    source.check_outside_repo([tmp_path], "work_dir", allow=False)


# --- features ----------------------------------------------------------------------------------------------------

def _rows(src, starts, **kw):
    return [{"src": src, "dst": kw.get("dst", "10.0.1.1"), "port": kw.get("port", 443), "proto": kw.get("proto", 6),
             "bytes": kw.get("bytes", 100), "packets": kw.get("packets", 2), "start": s, "dur_s": kw.get("dur", 2)}
            for s in starts]


def _table(con, tmp_path, rows, name="d") -> featureset.FeatureTable:
    d = tmp_path / name
    d.mkdir()
    pq.write_table(flow_table(rows), d / "a.parquet")
    return featureset.build_feature_table(con, _source(con, d), 60, tmp_path / f"cache_{name}")


def test_features_aggregate_one_host_window_exactly(con, tmp_path):
    t0 = START + timedelta(hours=10)
    rows = (_rows("10.0.0.1", [t0, t0 + timedelta(minutes=1)], dst="10.0.1.1", port=443, bytes=100, packets=2, dur=2)
            + _rows("10.0.0.1", [t0 + timedelta(minutes=2)], dst="8.8.8.8", port=53, proto=17, bytes=-5,
                    packets=1, dur=4))
    table = _table(con, tmp_path, rows)
    [r] = con.execute(f"SELECT * FROM {table.relation()}").to_arrow_table().to_pylist()
    assert r["window_start"] == t0 and r["window_end"] == t0 + timedelta(hours=1)
    assert r["flows"] == 3 and r["bytes_total"] == 200  # negative bytes are NULL, not subtracted
    assert r["packets_total"] == 5 and r["max_flow_bytes"] == 100 and r["bytes_per_packet"] == 40
    assert r["uniq_dst_ip"] == 2 and r["uniq_dst_port"] == 2
    assert r["internal_share"] == pytest.approx(2 / 3) and r["udp_share"] == pytest.approx(1 / 3)
    assert r["tcp_share"] == pytest.approx(2 / 3) and r["icmp_share"] == 0
    assert r["mean_duration_s"] == pytest.approx(8 / 3)
    assert r["n_source_files"] == 1


def test_window_boundaries_are_half_open(con, tmp_path):
    edge = START + timedelta(hours=10)
    rows = _rows("10.0.0.1", [edge - timedelta(microseconds=1), edge, edge + timedelta(minutes=59, seconds=59)])
    table = _table(con, tmp_path, rows)
    got = dict(con.execute(f"SELECT window_start, flows FROM {table.relation()}").fetchall())
    assert got == {edge - timedelta(hours=1): 1, edge: 2}


def test_later_rows_cannot_change_earlier_feature_rows(con, tmp_path):
    early = regular_rows(0)
    table_a = _table(con, tmp_path, early, "a")
    table_b = _table(con, tmp_path, early + regular_rows(1) + scan_rows(1, 3), "b")
    q = "SELECT * EXCLUDE (flow_date) FROM {} WHERE window_start < TIMESTAMPTZ '2026-01-06' ORDER BY ALL"
    assert con.execute(q.format(table_a.relation())).fetchall() == con.execute(q.format(table_b.relation())).fetchall()


def test_feature_selection_rejects_unknown_and_unavailable_names():
    with pytest.raises(featureset.FeatureError, match="unknown"):
        featureset.select_features(["flows"], ["flowz"], [])
    with pytest.raises(featureset.FeatureError, match="not mapped"):
        featureset.select_features(["flows"], ["bytes_total"], [])
    assert featureset.select_features(["flows", "bytes_total"], None, ["flows"]) == ["bytes_total"]


def test_identifiers_are_never_model_inputs():
    assert not set(featureset.TRACE_COLUMNS) & set(featureset.FEATURE_BY_NAME)
    for f in featureset.FEATURES:  # identifiers only ever appear inside a distinct count
        sql = f.sql.replace("count(DISTINCT dst_ip)", "").replace("count(DISTINCT dst_port)", "")
        assert not any(k in sql for k in ("src_ip", "dst_ip", "dst_port", "source_file", "source_row_index")), f.name


# --- splits and annotations --------------------------------------------------------------------------------------

def test_explicit_split_must_be_chronological_and_non_overlapping():
    days = [date(2026, 1, 5) + timedelta(days=i) for i in range(8)]
    bad = SplitSettings(train={"start": "2026-01-05", "end": "2026-01-09"},
                        validation={"start": "2026-01-08", "end": "2026-01-10"})
    with pytest.raises(splits.SplitError, match="chronological"):
        splits.resolve_split(bad, days)
    with pytest.raises(splits.SplitError, match="contains no data"):
        splits.resolve_split(SplitSettings(train={"start": "2025-01-01", "end": "2025-02-01"}), days)


def test_fraction_split_is_by_whole_days_in_time_order():
    days = [date(2026, 1, 5) + timedelta(days=i) for i in range(10)]
    got = splits.resolve_split(SplitSettings(fractions=(0.6, 0.2, 0.2)), list(reversed(days)))
    assert [(p.name, p.start, p.end) for p in got] == [
        ("train", date(2026, 1, 5), date(2026, 1, 11)), ("validation", date(2026, 1, 11), date(2026, 1, 13)),
        ("test", date(2026, 1, 13), date(2026, 1, 15))]


def test_annotation_dates_are_inclusive_days_and_categories_include_a_buffer(tmp_path, con):
    f = tmp_path / "a.yaml"
    f.write_text("intervals:\n  - {name: pt1, start: 2026-01-07, end: 2026-01-07, source: letter, confidence: low}\n",
                 encoding="utf-8")
    [a] = ann.load_annotations(f)
    assert (a.start, a.end) == (datetime(2026, 1, 7, tzinfo=UTC), datetime(2026, 1, 8, tzinfo=UTC))
    cat = ann.category_sql("ws", "ws + INTERVAL 1 HOUR", [a], 6)
    probe = {"2026-01-07 12:00:00+00": "inside", "2026-01-06 20:00:00+00": "buffer", "2026-01-08 05:00:00+00": "buffer",
             "2026-01-06 10:00:00+00": "outside", "2026-01-08 06:00:00+00": "outside"}
    for ts, expected in probe.items():
        assert con.execute(f"SELECT {cat} FROM (SELECT TIMESTAMPTZ '{ts}' AS ws)").fetchone()[0] == expected, ts
    assert ann.overlaps([a], a.start - timedelta(days=2), a.start - timedelta(hours=1), 6) == [
        {"name": "pt1", "kind": "buffer"}]


def test_annotation_end_before_start_is_rejected(tmp_path):
    f = tmp_path / "a.yaml"
    f.write_text("intervals:\n  - {name: x, start: 2026-01-07T10:00:00Z, end: 2026-01-07T09:00:00Z}\n",
                 encoding="utf-8")
    with pytest.raises(ValueError, match="end is not after start"):
        ann.load_annotations(f)


# --- models ------------------------------------------------------------------------------------------------------

TRAIN = "window_start < TIMESTAMPTZ '2026-01-08'"


FEATS = ["flows", "bytes_total", "uniq_dst_ip", "uniq_dst_port", "mean_duration_s"]


@pytest.fixture
def scan_table(con, tmp_path):
    return _table(con, tmp_path, [r for d in range(4) for r in regular_rows(d)] + scan_rows(3, 12))


def _fit_and_score(con, table, tmp_path, kind: str, where: str = TRAIN) -> Path:
    spec = next(s for s in models.specs_from_config(_defaults(tmp_path).models) if s.kind == kind)
    fitted = models.fit_model(con, table, FEATS, where, spec.with_overrides({"max_train_rows": 1000}), n_jobs=1,
                              max_matrix_mb=64)
    out = tmp_path / f"{kind}.parquet"
    models.score_to_parquet(con, table, [fitted], out, 1000)
    return out


@pytest.mark.parametrize("kind", models.KINDS)
def test_higher_score_means_more_anomalous_for_both_models(con, tmp_path, kind):
    # one isolated scan window inside the training period: both models must give it the highest raw score
    table = _table(con, tmp_path, [r for d in range(4) for r in regular_rows(d)] + scan_rows(1, 12))
    out = _fit_and_score(con, table, tmp_path, kind)
    top = con.execute(f"SELECT src_ip, window_start FROM read_parquet('{out.as_posix()}') "
                      f"ORDER BY {kind}_raw DESC LIMIT 1").fetchone()
    assert top == ("10.0.0.99", START + timedelta(days=1, hours=12))


def test_ocsvm_extrapolates_but_isolation_forest_cannot(con, scan_table, tmp_path):
    """A scan far beyond the training range (held out): the RBF One-Class SVM ranks it first, while the Isolation
    Forest scores it like the most extreme *training* values (thresholds lie inside the training range). This is why
    scores.parquet carries `beyond_train_range` and the report shows both models."""
    key = ("10.0.0.99", START + timedelta(days=3, hours=12))
    ranks = {}
    for kind in models.KINDS:
        out = _fit_and_score(con, scan_table, tmp_path, kind)
        ranks[kind] = con.execute(f"SELECT r FROM (SELECT src_ip, window_start, rank() OVER (ORDER BY {kind}_raw "
                                  f"DESC) AS r FROM read_parquet('{out.as_posix()}')) WHERE src_ip = ? AND "
                                  "window_start = ?", list(key)).fetchone()[0]
    assert ranks["ocsvm"] == 1
    assert ranks["iforest"] > 1  # documents the limitation; if sklearn changes this, revisit the docs
    from netanomaly.outputs import beyond_range_sql
    stats, beyond = beyond_range_sql(FEATS)
    flags = con.execute(f"WITH tr AS (SELECT {stats} FROM {scan_table.relation()} WHERE {TRAIN}) SELECT {beyond} "
                        f"FROM {scan_table.relation()} f CROSS JOIN tr WHERE f.src_ip = ? AND f.window_start = ?",
                        list(key)).fetchone()[0]
    assert {"flows", "uniq_dst_port"} <= set(flags)


def test_learned_transforms_see_only_training_rows(con, scan_table, tmp_path):
    spec = models.ModelSpec("ocsvm", _defaults(tmp_path).models.ocsvm.model_dump(), 42)
    fitted = models.fit_model(con, scan_table, ["flows", "uniq_dst_port"], TRAIN, spec, n_jobs=1, max_matrix_mb=64)
    train = con.execute(f"SELECT ln(1 + flows) AS a, ln(1 + uniq_dst_port) AS b FROM {scan_table.relation()} "
                        f"WHERE {TRAIN}").fetchnumpy()
    assert fitted.pipeline.named_steps["scale"].mean_ == pytest.approx([train["a"].mean(), train["b"].mean()])
    assert fitted.train_rows == fitted.train_rows_available == len(train["a"])


def test_training_sample_is_deterministic_and_ignores_later_rows(con, tmp_path):
    base = [r for d in range(3) for r in regular_rows(d)]
    table_a = _table(con, tmp_path, base, "a")
    table_b = _table(con, tmp_path, base + regular_rows(3) + scan_rows(3, 1), "b")
    q = [con.execute(models.training_sample_sql(t, ["flows"], TRAIN, 50, 3, 7)).fetchall() for t in (table_a, table_b)]
    assert q[0] == q[1] and len(q[0]) == 50
    per_day = con.execute(f"SELECT count(*) FROM ({models.training_sample_sql(table_a, ['flows'], TRAIN, 50, 3, 7)})"
                          " GROUP BY CAST(window_start AS DATE)").fetchall()
    assert max(n for (n,) in per_day) <= 17  # ceil(50 / 3): spread over the training days


def test_ocsvm_refuses_unbounded_training_and_memory_guard_triggers(con, scan_table, tmp_path):
    cfg = _defaults(tmp_path).models
    spec = models.ModelSpec("ocsvm", {**cfg.ocsvm.model_dump(), "max_train_rows": 700, "hard_max_train_rows": 500}, 1)
    with pytest.raises(models.ModelError, match="hard_max_train_rows"):
        models.fit_model(con, scan_table, ["flows"], TRAIN, spec, n_jobs=1, max_matrix_mb=64)
    big = models.ModelSpec("iforest", cfg.iforest.model_dump(), 1)
    with pytest.raises(models.ModelError, match="max_matrix_mb"):
        models.fit_model(con, scan_table, ["flows"] * 2000, TRAIN, big, n_jobs=1, max_matrix_mb=1)


def test_saved_models_round_trip_and_tampered_artifacts_are_refused(con, scan_table, tmp_path):
    spec = models.ModelSpec("iforest", _defaults(tmp_path).models.iforest.model_dump(), 3)
    fitted = models.fit_model(con, scan_table, FEATS, TRAIN, spec, n_jobs=1, max_matrix_mb=64)
    path = models.save_model(fitted, tmp_path / "m")
    loaded = models.load_model(tmp_path / "m", "iforest")
    x = np.array([[5, 3000, 2, 2, 10], [200, 12000, 1, 200, 0]], dtype=float)
    assert models.raw_scores(loaded.pipeline, x) == pytest.approx(models.raw_scores(fitted.pipeline, x))
    path.write_bytes(path.read_bytes() + b"tampered")
    with pytest.raises(models.ModelError, match="sha256"):
        models.load_model(tmp_path / "m", "iforest")


# --- bands -------------------------------------------------------------------------------------------------------

def test_quantile_bands_are_deterministic_and_non_probabilistic(con):
    ref = np.arange(1000, dtype=float)
    c = bands.calibrate("m", ref, 100.0, BandSettings(), "validation")
    assert c.thresholds == {"Critical": 999.0, "High": 995.0, "Medium": 990.0, "Low": 975.0}
    sql = bands.band_sql("x", c)
    got = [con.execute(f"SELECT {sql} FROM (SELECT {v}::DOUBLE AS x)").fetchone()[0] for v in (999, 996, 990, 975, 974)]
    assert got == ["Critical", "High", "Medium", "Low", "Benign"]
    assert bands.calibrate("m", ref[::-1].copy(), 100.0, BandSettings(), "validation").thresholds == c.thresholds


def test_budget_bands_translate_daily_budgets_to_reference_quantiles():
    s = BandSettings(mode="budget", budget_per_day={"Critical": 1, "High": 5, "Medium": 10, "Low": 20})
    assert bands.band_quantiles(s, 200.0) == pytest.approx({"Critical": 0.995, "High": 0.975, "Medium": 0.95,
                                                            "Low": 0.9})
    with pytest.raises(bands.BandError, match="reference windows per day"):
        bands.band_quantiles(s, 15.0)


def test_percentile_grid_is_monotone_and_exact_at_the_top():
    grid = bands.percentile_grid(np.arange(10_000, dtype=float))
    scores, shares = zip(*grid, strict=True)
    assert list(scores) == sorted(scores) and list(shares) == sorted(shares)
    assert shares[-1] == 1.0 and dict(grid)[9989.0] == pytest.approx(0.999)
