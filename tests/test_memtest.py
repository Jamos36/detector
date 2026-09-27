"""Smoke tests for scripts/memtest.py at tiny scale (the 20M-row run is manual; see PROJECT_STATUS.md)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "memtest.py"
CONFIG = SCRIPT.parent.parent / "config.yaml"


@pytest.fixture(scope="module")
def memtest():
    spec = importlib.util.spec_from_file_location("memtest", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(memtest, root: Path, *generate_args: str) -> dict:
    memtest.main(["generate", "--root", str(root), "--days", "2", "--hosts", "10", "--config", str(CONFIG),
                  *generate_args])
    memtest.main(["ingest", "--root", str(root), "--config", str(CONFIG)])
    return json.loads((root / memtest.RESULT_FILE).read_text(encoding="utf-8"))


def test_ingest_records_completion_and_peak_memory(memtest, tmp_path):
    result = _run(memtest, tmp_path / "daily")
    assert result["completed"] and result["files"] == 2
    assert result["rows_ingested"] == result["lake_rows"] > 0 and result["rows_rejected"] == 0
    assert result["peak_rss_bytes"] > 0 and result["memory_limit"] == "2GB"


def test_single_file_keeps_every_generated_row(memtest, tmp_path):
    daily = _run(memtest, tmp_path / "daily")
    single = _run(memtest, tmp_path / "single", "--single-file", "--format", "csv")
    assert [p.name for p in (tmp_path / "single" / "raw").iterdir()] == ["synth_all.csv"]
    assert single["completed"] and single["files"] == 1
    assert single["lake_rows"] == daily["lake_rows"]  # same seed: merging loses nothing


def test_refuses_to_write_inside_the_repository(memtest):
    with pytest.raises(SystemExit, match="outside the repository"):
        memtest.main(["generate", "--root", str(SCRIPT.parent / "tmp_memtest"), "--days", "1", "--hosts", "5"])
