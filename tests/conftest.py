from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from netanomaly.config import DuckDBSettings
from netanomaly.db import connect
from netanomaly.schema import Contract, load_contract

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_csv() -> Path:
    return FIXTURES / "sample.csv"


@pytest.fixture
def contract() -> Contract:
    return load_contract()


@pytest.fixture
def con(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    c = connect(DuckDBSettings(memory_limit="512MB", threads=2, temp_directory=tmp_path / "duck"))
    yield c
    c.close()
