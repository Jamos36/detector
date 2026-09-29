"""User-supplied pentest date ranges as weak temporal annotations.

They are context for charts and descriptive comparisons only: a broad range contains clean traffic, and testing may
have happened outside it. Nothing here produces a label; the models never see an annotation (the only optional use
in training is to *exclude* annotated windows from the baseline, `split.exclude_annotated_from_train`).

File format (YAML):

    intervals:
      - name: pentest-2025-q1
        start: 2025-02-10          # date = 00:00 UTC; or an ISO datetime (no offset = UTC, noted)
        end: 2025-02-14            # a date end is inclusive (the whole day); a datetime end is exclusive
        source: "engagement letter"
        notes: "external + internal scope"
        confidence: medium         # optional free text: how well the dates are known
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import yaml

from netanomaly.db import sql_literal

INSIDE, BUFFER, OUTSIDE = "inside", "buffer", "outside"
CATEGORY_LABELS = {INSIDE: "inside known pentest window", BUFFER: "buffer / uncertain",
                   OUTSIDE: "outside supplied windows"}


@dataclass(frozen=True)
class Annotation:
    name: str
    start: datetime  # UTC, inclusive
    end: datetime  # UTC, exclusive
    source: str = ""
    notes: str = ""
    confidence: str | None = None
    assumed_utc: bool = False

    def to_dict(self) -> dict:
        return {"name": self.name, "start": self.start.isoformat(), "end": self.end.isoformat(),
                "source": self.source, "notes": self.notes, "confidence": self.confidence,
                "assumed_utc": self.assumed_utc}


def _instant(value: object, *, is_end: bool) -> tuple[datetime, bool]:
    if isinstance(value, datetime):
        naive = value.tzinfo is None
        return (value.replace(tzinfo=UTC) if naive else value.astimezone(UTC)), naive
    if isinstance(value, date):
        day = value + timedelta(days=1) if is_end else value
        return datetime.combine(day, time(), tzinfo=UTC), False
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10:
            return _instant(date.fromisoformat(text), is_end=is_end)
        return _instant(datetime.fromisoformat(text), is_end=is_end)
    raise ValueError(f"not a date or datetime: {value!r}")


def load_annotations(path: Path | None) -> list[Annotation]:
    if path is None:
        return []
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    out = []
    for i, item in enumerate(data.get("intervals") or []):
        try:
            start, naive_s = _instant(item["start"], is_end=False)
            end, naive_e = _instant(item["end"], is_end=True)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"{path}: interval {i}: {exc}") from exc
        if end <= start:
            raise ValueError(f"{path}: interval {i} ({item.get('name')}): end is not after start")
        out.append(Annotation(str(item.get("name") or f"interval-{i + 1}"), start, end, str(item.get("source", "")),
                              str(item.get("notes", "")), item.get("confidence"), naive_s or naive_e))
    names = [a.name for a in out]
    if len(set(names)) != len(names):
        raise ValueError(f"{path}: interval names must be unique")
    return sorted(out, key=lambda a: a.start)


def _ts(value: datetime) -> str:
    return f"TIMESTAMPTZ {sql_literal(value.isoformat())}"


def _overlap(start_col: str, end_col: str, a: Annotation, buffer: timedelta) -> str:
    return f"({start_col} < {_ts(a.end + buffer)} AND {end_col} > {_ts(a.start - buffer)})"


def category_sql(start_col: str, end_col: str, annotations: list[Annotation], buffer_hours: float) -> str:
    """SQL for a window's category: inside (overlaps an interval), buffer (only its +-buffer), else outside."""
    if not annotations:
        return f"'{OUTSIDE}'"
    inside = " OR ".join(_overlap(start_col, end_col, a, timedelta()) for a in annotations)
    buffered = " OR ".join(_overlap(start_col, end_col, a, timedelta(hours=buffer_hours)) for a in annotations)
    return f"CASE WHEN {inside} THEN '{INSIDE}' WHEN {buffered} THEN '{BUFFER}' ELSE '{OUTSIDE}' END"


def names_sql(start_col: str, end_col: str, annotations: list[Annotation], buffer_hours: float) -> str:
    """Comma-separated names of intervals whose buffered range overlaps the window (NULL when none)."""
    if not annotations:
        return "CAST(NULL AS VARCHAR)"
    parts = ", ".join(f"CASE WHEN {_overlap(start_col, end_col, a, timedelta(hours=buffer_hours))} "
                      f"THEN {sql_literal(a.name)} END" for a in annotations)
    return f"nullif(concat_ws(',', {parts}), '')"


def overlaps(annotations: list[Annotation], start: datetime, end: datetime, buffer_hours: float) -> list[dict]:
    """Intervals touching [start, end): kind 'inside' (the interval itself) or 'buffer' (only its buffer)."""
    out, pad = [], timedelta(hours=buffer_hours)
    for a in annotations:
        if start < a.end and end > a.start:
            out.append({"name": a.name, "kind": INSIDE})
        elif start < a.end + pad and end > a.start - pad:
            out.append({"name": a.name, "kind": BUFFER})
    return out
