"""Unit tests for the optional source -> destination analysis (relationships.py) on tiny hand-built Parquet files:
novelty (never seen / recently unseen, exact lookback boundaries, warm-up), frequency change (increase, decrease,
zero history, insufficient support), ties, input order, past-only history (leakage) and carried history state."""

from __future__ import annotations

import math
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from netanomaly import relationships as rel
from netanomaly import source
from netanomaly.config import DuckDBSettings
from netanomaly.db import connect
from tests.fixtures import START, flow_table

H = timedelta(hours=1)
P = rel.RelParams(window_minutes=60, recent_lookback_days=1, baseline_lookback_days=1, min_support_windows=2,
                  warmup_days=0, change_log2_threshold=1.0, group_by_port_protocol=False)


@pytest.fixture
def con(tmp_path):
    return connect(DuckDBSettings(temp_directory=tmp_path / "duck"))


def flows(src: str, dst: str, at, n: int = 1, port: int = 443) -> list[dict]:
    return [{"src": src, "dst": dst, "port": port, "proto": 6, "bytes": 100, "packets": 2, "start": at,
             "dur_s": 1} for _ in range(n)]


def _write(directory: Path, rows: list[dict], files: int = 1) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for i in range(files):
        pq.write_table(flow_table(rows[i::files]), directory / f"part_{i}.parquet")
    return directory


def _analyze(con, tmp_path: Path, name: str, rows: list[dict], p: rel.RelParams = P, *, files: int = 1,
             **kw) -> rel.RelationshipResult:
    src = source.open_source(con, [str(_write(tmp_path / f"data_{name}", rows, files))], {}, {})
    return rel.analyze(con, src, p, tmp_path / f"out_{name}", **kw)


def _pairs(con, result: rel.RelationshipResult, where: str = "TRUE") -> list[dict]:
    cur = con.execute(f"SELECT * FROM read_parquet('{result.path(rel.PAIR_WINDOWS).as_posix()}') WHERE {where} "
                      "ORDER BY window_start, src_ip, dst_ip")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def _status(rows: list[dict], dst: str, at) -> dict:
    return next(r for r in rows if r["dst_ip"] == dst and r["window_start"] == at)


def test_never_seen_and_recently_unseen_are_separate_with_an_inclusive_lookback_boundary(con, tmp_path):
    t0 = START + 10 * H
    rows = (flows("a", "b", t0) + flows("a", "b", t0 + 24 * H)  # exactly the lookback later: seen recently
            + flows("a", "b", t0 + 49 * H)  # 25 h after the last contact: recently unseen
            + flows("a", "c", t0 + 50 * H))  # a destination never contacted before
    got = _pairs(con, _analyze(con, tmp_path, "nov", rows))
    assert _status(got, "b", t0)["novelty_status"] == "never_seen"
    at_boundary = _status(got, "b", t0 + 24 * H)
    assert at_boundary["novelty_status"] == "seen_recently" and at_boundary["days_since_last_seen"] == 1.0
    gap = _status(got, "b", t0 + 49 * H)
    assert gap["novelty_status"] == "recently_unseen" and gap["first_seen"] == t0
    assert gap["last_seen_before"] == t0 + 24 * H
    assert _status(got, "c", t0 + 50 * H)["novelty_status"] == "never_seen"


def test_never_seen_is_not_judged_during_warm_up(con, tmp_path):
    t0 = START
    rows = flows("a", "b", t0) + flows("a", "c", t0 + 23 * H) + flows("a", "d", t0 + 24 * H)
    result = _analyze(con, tmp_path, "warm", rows, rel.RelParams(**{**P.to_dict(), "warmup_days": 1}))
    got = _pairs(con, result)
    assert [r["novelty_status"] for r in got] == ["warmup", "warmup", "never_seen"]  # warm-up ends at t0 + 24 h
    hosts = con.execute(f"SELECT window_start, rel_new_dst, rel_warmup FROM read_parquet("
                        f"'{result.path(rel.HOST_WINDOWS).as_posix()}') ORDER BY 1").fetchall()
    assert [(n, w) for _, n, w in hosts] == [(None, True), (None, True), (1, False)]


def test_frequency_increase_decrease_stable_and_insufficient_history(con, tmp_path):
    rows = []
    for h in range(6):
        rows += flows("a", "b", START + h * H, 2) + flows("a", "c", START + h * H, 20)
    rows += flows("a", "b", START + 6 * H, 20)  # 10x its usual intensity
    rows += flows("a", "c", START + 6 * H, 1)  # a twentieth of its usual intensity
    rows += flows("a", "b", START + 7 * H, 2)  # back to normal (the spike does not move the median)
    got = _pairs(con, _analyze(con, tmp_path, "freq", rows))
    first, second = _status(got, "b", START), _status(got, "b", START + H)
    assert first["change_status"] == second["change_status"] == "insufficient_history"  # support 0, then 1 < 2
    assert first["log2_change"] is None and first["baseline_flows_median"] is None  # zero history: no value
    up = _status(got, "b", START + 6 * H)
    assert up["change_status"] == "increase" and up["baseline_flows_median"] == 2 and up["baseline_support"] == 6
    assert up["log2_change"] == pytest.approx(math.log2(21 / 3))
    assert _status(got, "c", START + 6 * H)["change_status"] == "decrease"
    assert _status(got, "b", START + 7 * H)["change_status"] == "stable"
    assert all(r["log2_change"] is None or math.isfinite(r["log2_change"]) for r in got)


def test_baseline_uses_only_the_lookback_period(con, tmp_path):
    rows = flows("a", "b", START, 50) + flows("a", "b", START + H, 50)  # old, heavy
    rows += [f for h in range(30, 33) for f in flows("a", "b", START + h * H, 2)]  # recent, light
    got = _pairs(con, _analyze(con, tmp_path, "look", rows, rel.RelParams(**{**P.to_dict(),
                                                                                "baseline_lookback_days": 0.5})))
    last = _status(got, "b", START + 32 * H)
    assert last["baseline_support"] == 2 and last["baseline_flows_median"] == 2  # the 50-flow windows are too old


def test_tied_timestamps_aggregate_into_one_pair_window(con, tmp_path):
    rows = flows("a", "b", START + H, 3) + flows("a", "b", START + H + timedelta(minutes=59, seconds=59))
    got = _pairs(con, _analyze(con, tmp_path, "tie", rows))
    assert len(got) == 1 and got[0]["flows"] == 4 and got[0]["bytes"] == 400


def _mixed_rows() -> list[dict]:
    rows = []
    for h in range(40):
        rows += flows("a", f"d{h % 3}", START + h * H, 1 + h % 4) + flows("b", "d0", START + h * H, 2)
    return rows + flows("a", "new", START + 39 * H, 9)


def test_input_row_and_file_order_do_not_change_results(con, tmp_path):
    rows = _mixed_rows()
    a = _pairs(con, _analyze(con, tmp_path, "order_a", rows))
    b = _pairs(con, _analyze(con, tmp_path, "order_b", list(reversed(rows)), files=3))
    assert a == b


def test_future_rows_cannot_change_earlier_results(con, tmp_path):
    rows = _mixed_rows()
    cut = START + 30 * H
    future = [f for h in range(30, 60) for f in flows("a", "d0", START + h * H, 40)] + flows("z", "d9", cut)
    base = _analyze(con, tmp_path, "past", rows)
    more = _analyze(con, tmp_path, "future", rows + future)
    where = f"window_start < TIMESTAMPTZ '{cut.isoformat()}'"
    assert _pairs(con, base, where) == _pairs(con, more, where)


def test_carried_history_state_reproduces_a_single_pass_and_refuses_overlap(con, tmp_path):
    rows = _mixed_rows()
    cut = START + 24 * H
    whole = _analyze(con, tmp_path, "whole", rows)
    data = _write(tmp_path / "data_split", rows)
    src = source.open_source(con, [str(data)], {}, {})
    first = rel.analyze(con, src, P, tmp_path / "out_first", end=cut)
    history = rel.load_history(first.state, P)
    assert history.end == cut and history.history_start == START
    second = rel.analyze(con, src, P, tmp_path / "out_second", start=cut, history=history)
    later = f"window_start >= TIMESTAMPTZ '{cut.isoformat()}'"
    assert _pairs(con, second) == _pairs(con, whole, later)
    with pytest.raises(rel.RelationshipError, match="overlaps or precedes"):
        rel.analyze(con, src, P, tmp_path / "out_overlap", history=history)  # all data, incl. before the cut
    other = rel.RelParams(**{**P.to_dict(), "recent_lookback_days": 3})
    with pytest.raises(rel.RelationshipError, match="other settings"):
        rel.load_history(first.state, other)


def test_port_protocol_grouping_splits_pairs(con, tmp_path):
    rows = flows("a", "b", START, port=443) + flows("a", "b", START, port=22)
    assert len(_pairs(con, _analyze(con, tmp_path, "pp0", rows))) == 1
    grouped = rel.RelParams(**{**P.to_dict(), "group_by_port_protocol": True})
    got = _pairs(con, _analyze(con, tmp_path, "pp1", rows, grouped))
    assert sorted(r["dst_port"] for r in got) == [22, 443]


def test_report_aggregates_long_spans_per_week_and_lists_traceable_pairs(con, tmp_path):
    from netanomaly import relationship_report
    from netanomaly.config import PocConfig

    rows = [f for d in range(0, 210, 3) for f in flows("a", f"d{d % 7}", START + timedelta(days=d), 1 + d % 5)]
    result = _analyze(con, tmp_path, "long", rows)
    cfg = PocConfig(input={"paths": ["x"]}, work_dir=tmp_path, relationship_analysis={"enabled": True, "top_pairs": 3})
    text = relationship_report.section("## R", con, result, cfg, [], tmp_path / "charts", model_features=[])
    assert "per UTC week" in text and (tmp_path / "charts" / "relationship_trends.svg").exists()
    assert "| source | destination |" in text and "| a | d" in text and "report-only" in text
    weeks = con.execute(f"SELECT count(DISTINCT date_trunc('week', window_start)) FROM read_parquet("
                        f"'{result.path(rel.PAIR_WINDOWS).as_posix()}')").fetchone()[0]
    assert weeks <= 31  # ~210 days -> at most 31 plotted points, whatever the number of pair-windows


def test_host_summary_and_evidence_keep_keys_and_reasons(con, tmp_path):
    result = _analyze(con, tmp_path, "sum", _mixed_rows())
    host = con.execute(f"SELECT rel_new_dst, rel_pairs FROM read_parquet('{result.path(rel.HOST_WINDOWS).as_posix()}')"
                       f" WHERE src_ip = 'a' AND window_start = TIMESTAMPTZ '{(START + 39 * H).isoformat()}'"
                       ).fetchone()
    assert host == (1, 2)  # 'new' is never seen; d0 was seen earlier
    ev = con.execute(f"SELECT src_ip, dst_ip, reason FROM read_parquet('{result.path(rel.EVIDENCE).as_posix()}') "
                     "WHERE dst_ip = 'new'").fetchall()
    assert ev == [("a", "new", ev[0][2])] and ev[0][2].startswith("never seen before")
    columns = {d[0] for d in con.execute(f"SELECT * FROM read_parquet("
                                         f"'{result.path(rel.HOST_WINDOWS).as_posix()}') LIMIT 0").description}
    assert set(rel.MODEL_COLUMNS) <= columns
