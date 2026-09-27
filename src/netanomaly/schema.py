"""Schema contract: raw column names/types, canonical names, and modelling rules."""

from __future__ import annotations

from enum import StrEnum
from functools import cache
from importlib import resources

import yaml
from pydantic import BaseModel, model_validator


class ModelUse(StrEnum):
    ENTITY = "entity"
    FEATURE = "feature"
    DERIVE = "derive"
    PROVENANCE = "provenance"
    EXCLUDE = "exclude"


class Confidence(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class Column(BaseModel):
    raw: str
    name: str
    type: str
    model_use: ModelUse
    confidence: Confidence
    meaning: str

    @model_validator(mode="after")
    def _uncertain_columns_are_not_features(self) -> Column:
        if self.model_use is ModelUse.FEATURE and self.confidence in (Confidence.LOW, Confidence.UNKNOWN):
            raise ValueError(f"{self.raw}: {self.confidence} confidence columns cannot be model features")
        return self


class Contract(BaseModel):
    schema_version: int
    name: str
    columns: list[Column]

    @property
    def raw_names(self) -> list[str]:
        return [c.raw for c in self.columns]

    def duckdb_types(self) -> dict[str, str]:
        """Explicit raw-column types for read_csv, so no per-file type inference."""
        return {c.raw: c.type for c in self.columns}

    def rename_select(self) -> str:
        """SQL select list that casts and renames raw columns to canonical names."""
        return ",\n  ".join(f'CAST("{c.raw}" AS {c.type}) AS "{c.name}"' for c in self.columns)

    def check_columns(self, found: list[str]) -> tuple[list[str], list[str]]:
        """Return (missing, unexpected) raw columns for a file's header."""
        expected = set(self.raw_names)
        present = set(found)
        return sorted(expected - present), sorted(present - expected)


@cache
def load_contract(name: str = "netflow_v1") -> Contract:
    text = resources.files("netanomaly.contracts").joinpath(f"{name}.yaml").read_text(encoding="utf-8")
    contract = Contract.model_validate(yaml.safe_load(text))
    names = [c.name for c in contract.columns]
    if len(set(names)) != len(names) or len(set(contract.raw_names)) != len(contract.raw_names):
        raise ValueError("schema contract has duplicate column names")
    return contract
