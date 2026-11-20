# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

EVENT_COLUMNS = (
    "event_id", "shipment_id", "event_ts", "origin", "destination", "event_type",
)
EVENT_TYPES = ("BOOKED", "DEPARTED", "ARRIVED", "DELIVERED")
PAYLOAD_SCHEMA = StructType([
    StructField(name, StringType())
    for name in (*EVENT_COLUMNS, "_corrupt_record")
])


def classify_events(raw):
    payload = F.from_json("raw_payload", PAYLOAD_SCHEMA, {
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
    })
    parsed = (
        raw.withColumn("payload", payload)
        .select("raw_payload", "source_file", "payload.*")
        .withColumn("event_id", F.trim("event_id"))
        .withColumn("shipment_id", F.trim("shipment_id"))
        .withColumn("event_ts_raw", F.trim("event_ts"))
        .withColumn("event_ts", F.try_to_timestamp("event_ts_raw"))
        .withColumn("origin", F.upper(F.trim("origin")))
        .withColumn("destination", F.upper(F.trim("destination")))
        .withColumn("event_type", F.upper(F.trim("event_type")))
    )

    missing_id = (
        F.coalesce(F.col("event_id"), F.lit("")) == ""
    ) | (
        F.coalesce(F.col("shipment_id"), F.lit("")) == ""
    )
    timestamp_has_zone = F.col("event_ts_raw").rlike(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$"
    )
    route_is_valid = F.coalesce(
        F.col("origin").rlike("^[A-Z]{3}$")
        & F.col("destination").rlike("^[A-Z]{3}$")
        & (F.col("origin") != F.col("destination")),
        F.lit(False),
    )
    reason = (
        F.when(F.col("_corrupt_record").isNotNull() | (F.trim("raw_payload") == "null"),
               "MALFORMED_JSON")
        .when(missing_id, "MISSING_IDENTIFIER")
        .when(F.col("event_ts").isNull() | ~timestamp_has_zone, "INVALID_TIMESTAMP")
        .when(~route_is_valid, "INVALID_ROUTE")
        .when(~F.coalesce(F.col("event_type").isin(*EVENT_TYPES), F.lit(False)),
              "INVALID_EVENT_TYPE")
    )
    return parsed.select(*EVENT_COLUMNS, "source_file", "raw_payload",
                         reason.alias("reject_reason"))


def conflicting_ids(classified):
    business_fields = F.struct(*EVENT_COLUMNS[1:])
    return (
        classified.where("reject_reason IS NULL")
        .groupBy("event_id")
        .agg(F.countDistinct(business_fields).alias("payload_versions"))
        .where("payload_versions > 1")
        .select("event_id")
    )


def clean_events(classified, conflicts):
    return (
        classified.where("reject_reason IS NULL")
        .join(conflicts, "event_id", "left_anti")
        .select(*EVENT_COLUMNS)
        .distinct()
    )


def quarantine_events(classified, conflicts):
    rejects = classified.where("reject_reason IS NOT NULL")
    disputed = (
        classified.where("reject_reason IS NULL")
        .join(conflicts, "event_id", "inner")
        .withColumn("reject_reason", F.lit("CONFLICTING_EVENT_ID"))
    )
    columns = ("event_id", "shipment_id", "source_file", "raw_payload", "reject_reason")
    return rejects.select(*columns).unionByName(disputed.select(*columns))


def route_daily(events):
    return (
        events.withColumn("event_date_utc", F.to_date("event_ts"))
        .groupBy("event_date_utc", "origin", "destination")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("shipment_id").alias("distinct_shipments"),
            F.count_if(F.col("event_type") == "DEPARTED").alias("departed_events"),
            F.count_if(F.col("event_type") == "DELIVERED").alias("delivered_events"),
        )
    )


@dp.table(name="bronze_cargo_raw", comment="Raw NDJSON lines with source file metadata.")
def bronze_cargo_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(spark.conf.get("cargo.landing_path"))
        .selectExpr("value AS raw_payload", "_metadata.file_path AS source_file")
    )


@dp.temporary_view(name="cargo_classified")
def cargo_classified():
    return classify_events(spark.read.table("bronze_cargo_raw"))


@dp.temporary_view(name="cargo_conflicts")
def cargo_conflicts():
    return conflicting_ids(spark.read.table("cargo_classified"))


@dp.materialized_view(name="silver_cargo_events")
def silver_cargo_events():
    return clean_events(spark.read.table("cargo_classified"), spark.read.table("cargo_conflicts"))


@dp.materialized_view(name="silver_cargo_quarantine")
def silver_cargo_quarantine():
    return quarantine_events(
        spark.read.table("cargo_classified"), spark.read.table("cargo_conflicts"),
    )


@dp.materialized_view(name="gold_cargo_route_daily")
def gold_cargo_route_daily():
    return route_daily(spark.read.table("silver_cargo_events"))


@dp.materialized_view(name="gold_cargo_quality")
def gold_cargo_quality():
    return (
        spark.read.table("silver_cargo_quarantine")
        .groupBy("reject_reason")
        .agg(F.count("*").alias("rejected_rows"))
    )
