"""Parquet-only proof of concept: rank unusual host-windows in about a year of NetFlow Parquet and compare them
with broad, user-supplied pentest date ranges (weak temporal annotations, never labels).

Stages (each a plain function, also exposed as `netanomaly poc <stage>`):
profile -> features -> train (Isolation Forest, One-Class SVM) -> score -> report. `experiment` runs them all.
Raw flows are read directly from external Parquet with DuckDB; nothing is copied into this checkout (ADR-012).
"""
