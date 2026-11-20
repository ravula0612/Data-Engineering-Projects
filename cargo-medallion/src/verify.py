# Databricks notebook source
import re

catalog, schema = (dbutils.widgets.get(name) for name in ("catalog", "schema"))
if any(re.fullmatch(r"[a-z][a-z0-9_]*", name) is None for name in (catalog, schema)):
    raise ValueError("Catalog and schema identifiers must use lowercase letters, digits or underscores")


def table(name):
    return spark.read.table(f"`{catalog}`.`{schema}`.`{name}`")


counts = {
    "bronze_cargo_raw": 13,
    "silver_cargo_quarantine": 7,
    "gold_cargo_route_daily": 4,
}
for name, expected in counts.items():
    actual = table(name).count()
    if actual != expected:
        raise RuntimeError(f"{name}: expected {expected} rows, got {actual}")

events = sorted(row.event_id for row in table("silver_cargo_events").select("event_id").collect())
if events != ["E01", "E02", "E03", "E04", "E08"]:
    raise RuntimeError(f"Unexpected accepted events: {events}")

quality = {row.reject_reason: row.rejected_rows for row in table("gold_cargo_quality").collect()}
expected_quality = {
    "CONFLICTING_EVENT_ID": 2,
    "INVALID_EVENT_TYPE": 1,
    "INVALID_ROUTE": 1,
    "INVALID_TIMESTAMP": 1,
    "MALFORMED_JSON": 1,
    "MISSING_IDENTIFIER": 1,
}
if quality != expected_quality:
    raise RuntimeError(f"Unexpected quarantine totals: {quality}")

print("Cargo Medallion smoke checks passed")
