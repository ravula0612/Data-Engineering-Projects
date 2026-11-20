from pathlib import Path
import runpy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyspark
from pyspark.sql import SparkSession

PROJECT = Path(__file__).resolve().parents[1]


def register_only(**_settings):
    return lambda function: function


class CargoPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]").appName("cargo-medallion-test")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        decorators = SimpleNamespace(
            table=register_only, temporary_view=register_only, materialized_view=register_only,
        )
        with patch.object(pyspark, "pipelines", decorators, create=True):
            cls.pipeline = runpy.run_path(
                str(PROJECT / "src" / "pipeline.py"), init_globals={"spark": cls.spark},
            )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def load_batches(self, *names):
        rows = [
            (line, name)
            for name in names
            for line in (PROJECT / "fixtures" / name).read_text(encoding="utf-8").splitlines()
        ]
        raw = self.spark.createDataFrame(rows, ["raw_payload", "source_file"])
        raw.createOrReplaceTempView("bronze_cargo_raw")
        self.pipeline["cargo_classified"]().createOrReplaceTempView("cargo_classified")
        self.pipeline["cargo_conflicts"]().createOrReplaceTempView("cargo_conflicts")
        self.pipeline["silver_cargo_events"]().createOrReplaceTempView("silver_cargo_events")
        self.pipeline["silver_cargo_quarantine"]().createOrReplaceTempView("silver_cargo_quarantine")

    def event_ids(self):
        return sorted(row.event_id for row in self.spark.table("silver_cargo_events").collect())

    def test_first_batch_then_duplicate_late_event_and_conflict(self):
        self.load_batches("batch-001.ndjson")
        self.assertEqual(self.event_ids(), ["E01", "E02", "E03", "E04", "E05"])

        self.load_batches("batch-001.ndjson", "batch-002.ndjson")
        self.assertEqual(self.spark.table("bronze_cargo_raw").count(), 13)
        self.assertEqual(self.event_ids(), ["E01", "E02", "E03", "E04", "E08"])

        quarantine = self.spark.table("silver_cargo_quarantine").collect()
        disputed = [row for row in quarantine if row.reject_reason == "CONFLICTING_EVENT_ID"]
        self.assertCountEqual(
            [(row.event_id, row.source_file) for row in disputed],
            [("E05", "batch-001.ndjson"), ("E05", "batch-002.ndjson")],
        )
        self.assertEqual(len(quarantine), 7)
        quality = {row.reject_reason: row.rejected_rows
                   for row in self.pipeline["gold_cargo_quality"]().collect()}
        self.assertEqual(quality, {
            "CONFLICTING_EVENT_ID": 2, "INVALID_EVENT_TYPE": 1, "INVALID_ROUTE": 1,
            "INVALID_TIMESTAMP": 1, "MALFORMED_JSON": 1, "MISSING_IDENTIFIER": 1,
        })

        routes = {
            (str(row.event_date_utc), row.origin, row.destination):
                (row.events, row.distinct_shipments, row.departed_events, row.delivered_events)
            for row in self.pipeline["gold_cargo_route_daily"]().collect()
        }
        self.assertEqual(routes, {
            ("2026-05-02", "HYD", "SIN"): (1, 1, 1, 0),
            ("2026-05-03", "DFW", "LHR"): (2, 1, 1, 0),
            ("2026-05-03", "HYD", "SIN"): (1, 1, 0, 0),
            ("2026-05-04", "HYD", "SIN"): (1, 1, 0, 1),
        })


if __name__ == "__main__":
    unittest.main()
