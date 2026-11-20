# Databricks notebook source
import re
from pathlib import Path


def identifier(name):
    value = dbutils.widgets.get(name)
    if re.fullmatch(r"[a-z][a-z0-9_]*", value) is None:
        raise ValueError(f"{name} must use lowercase letters, digits or underscores")
    return value


catalog, schema, volume = (identifier(name) for name in ("catalog", "schema", "volume"))
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
landing = f"/Volumes/{catalog}/{schema}/{volume}/events"
dbutils.fs.mkdirs(landing)
existing = {entry.name for entry in dbutils.fs.ls(landing)}

fixture_dir = Path(dbutils.widgets.get("fixture_dir"))
fixtures = sorted(fixture_dir.glob("batch-*.ndjson"))
if not fixtures:
    raise FileNotFoundError(f"No cargo fixtures found in {fixture_dir}")
for fixture in fixtures:
    target = f"{landing}/{fixture.name}"
    payload = fixture.read_text(encoding="utf-8")
    if fixture.name in existing:
        if dbutils.fs.head(target, len(payload.encode("utf-8")) + 1) != payload:
            raise RuntimeError(f"Landing file differs from bundled fixture: {target}")
    elif not dbutils.fs.put(target, payload, overwrite=False):
        raise RuntimeError(f"Could not write fixture: {target}")
