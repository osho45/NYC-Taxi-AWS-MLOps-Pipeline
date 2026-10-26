from __future__ import annotations

import json
import logging
import sys
import boto3
from datetime import datetime, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    date_trunc,
    lit,
)

LOG = logging.getLogger("gold_mart_build")
LOG.setLevel(logging.INFO)

if not LOG.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    LOG.addHandler(handler)


ML_ZONE_START = 2
ML_ZONE_END = 263

FORBIDDEN_ML_FIELDS = {
    "dropoff_datetime",
    "tip_amount",
    "fare_amount",
    "passenger_count",
    "trip_distance",
}


def validate_year_month(year: int, month: int) -> None:
    if not 2000 <= year <= 2100:
        raise ValueError(f"Invalid year: {year}")

    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}")


def month_bounds(
    year: int,
    month: int,
) -> tuple[datetime, datetime]:
    validate_year_month(year, month)

    start = datetime(year, month, 1)

    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)

    return start, end


def create_tables(
    spark,
    *,
    audit_database: str,
    gold_database: str,
    lake_bucket: str,
) -> None:
    ml_table = f"glue_catalog.{gold_database}.hourly_ml_observations"

    bi_table = f"glue_catalog.{gold_database}.hourly_fleet_utilization"

    ledger_table = f"glue_catalog.{audit_database}.gold_mart_build_ledger"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ml_table} (
            target_year INT,
            target_month INT,
            pickup_location_id INT,
            pickup_hour_ts TIMESTAMP,
            ride_count INT
        )
        USING iceberg
        PARTITIONED BY (target_year, target_month)
        LOCATION
          's3://{lake_bucket}/warehouse/gold/hourly_ml_observations/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bi_table} (
            target_year INT,
            target_month INT,
            pickup_hour_ts TIMESTAMP,
            pickup_location_id INT,
            trip_count BIGINT,
            avg_distance DOUBLE
        )
        USING iceberg
        PARTITIONED BY (target_year, target_month)
        LOCATION
          's3://{lake_bucket}/warehouse/gold/hourly_fleet_utilization/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ledger_table} (
            source_year INT,
            source_month INT,
            source_checksum STRING,
            silver_row_count BIGINT,
            ml_observation_row_count BIGINT,
            bi_row_count BIGINT,
            gold_run_id STRING,
            status STRING,
            finished_at TIMESTAMP
        )
        USING iceberg
        LOCATION
          's3://{lake_bucket}/warehouse/audit/gold_mart_build_ledger/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)


def append_build_ledger(
    spark,
    ledger_table: str,
    *,
    source_year: int,
    source_month: int,
    source_checksum: str,
    silver_row_count: int,
    ml_observation_row_count: int,
    bi_row_count: int,
    gold_run_id: str,
) -> None:
    ledger_row = Row(
        source_year=source_year,
        source_month=source_month,
        source_checksum=source_checksum,
        silver_row_count=silver_row_count,
        ml_observation_row_count=ml_observation_row_count,
        bi_row_count=bi_row_count,
        gold_run_id=gold_run_id,
        status="SUCCESS",
    )

    (
        spark.createDataFrame([ledger_row])
        .withColumn("finished_at", current_timestamp())
        .writeTo(ledger_table)
        .append()
    )

def write_ml_history_to_dynamodb(
    ml_df,
    table_name: str,
) -> None:
    """
    Publish Gold ML observations to the DynamoDB serving table.
    """

    def write_partition(rows):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        with table.batch_writer(overwrite_by_pkeys=[
            "pickup_location_id",
            "pickup_hour_ts",
        ]) as batch:
            for row in rows:
                batch.put_item(
                    Item={
                        "pickup_location_id": int(row.pickup_location_id),
                        "pickup_hour_ts": row.pickup_hour_ts.isoformat(),
                        "ride_count": int(row.ride_count),
                    }
                )

    (
        ml_df
        .select(
            "pickup_location_id",
            "pickup_hour_ts",
            "ride_count",
        )
        .foreachPartition(write_partition)
    )

    LOG.info(
        "DYNAMODB_ML_HISTORY_PUBLISHED: table=%s",
        table_name,
    )

def update_prediction_range_metadata(
    table_name: str,
    start_datetime: datetime,
    end_datetime: datetime,
) -> None:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    metadata_key = {
        "pickup_location_id": 0,
        "pickup_hour_ts": "METADATA",
    }

    response = table.get_item(Key=metadata_key)
    existing = response.get("Item")

    data_start = start_datetime
    data_end = end_datetime

    if existing:
        existing_start = datetime.fromisoformat(
            existing["data_start_time"]
        )
        existing_end = datetime.fromisoformat(
            existing["data_end_time"]
        )

        data_start = min(data_start, existing_start)
        data_end = max(data_end, existing_end)

    earliest_prediction = data_start + timedelta(hours=168)
    latest_prediction = data_end

    table.put_item(
        Item={
            **metadata_key,
            "record_type": "prediction_range",
            "data_start_time": data_start.isoformat(),
            "data_end_time": data_end.isoformat(),
            "earliest_prediction_time": earliest_prediction.isoformat(),
            "latest_prediction_time": latest_prediction.isoformat(),
        }
    )

    LOG.info(
        "PREDICTION_RANGE_METADATA_UPDATED: earliest=%s latest=%s",
        earliest_prediction.isoformat(),
        latest_prediction.isoformat(),
    )

def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "RUN_ID",
            "LAKE_BUCKET",
            "AUDIT_DATABASE",
            "SILVER_DATABASE",
            "GOLD_DATABASE",
            "DEMAND_HISTORY_TABLE",
        ],
    )

    source_year = int(args["SOURCE_YEAR"])
    source_month = int(args["SOURCE_MONTH"])
    gold_run_id = args["RUN_ID"]

    start_datetime, end_datetime = month_bounds(
        source_year,
        source_month,
    )

    start_text = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_text = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    hours_in_month = int((end_datetime - start_datetime).total_seconds() / 3600)

    zone_count = ML_ZONE_END - ML_ZONE_START + 1
    expected_ml_rows = hours_in_month * zone_count

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    audit_database = args["AUDIT_DATABASE"]
    silver_database = args["SILVER_DATABASE"]
    gold_database = args["GOLD_DATABASE"]
    lake_bucket = args["LAKE_BUCKET"]
    demand_history_table = args["DEMAND_HISTORY_TABLE"]

    silver_table = f"glue_catalog.{silver_database}.yellow_taxi"

    silver_publish_ledger = f"glue_catalog.{audit_database}.silver_publish_ledger"

    ml_table = f"glue_catalog.{gold_database}.hourly_ml_observations"

    bi_table = f"glue_catalog.{gold_database}.hourly_fleet_utilization"

    gold_ledger = f"glue_catalog.{audit_database}.gold_mart_build_ledger"

    create_tables(
        spark,
        audit_database=audit_database,
        gold_database=gold_database,
        lake_bucket=lake_bucket,
    )

    latest_silver_publish = (
        spark.table(silver_publish_ledger)
        .where(
            (col("source_year") == source_year)
            & (col("source_month") == source_month)
            & (col("status") == "SUCCESS")
        )
        .orderBy(col("finished_at").desc())
        .select(
            "source_checksum",
            "valid_row_count",
            "silver_run_id",
        )
        .first()
    )

    if latest_silver_publish is None:
        raise ValueError(
            "No successful Silver publish exists for "
            f"{source_year:04d}-{source_month:02d}."
        )

    source_checksum = latest_silver_publish["source_checksum"]

    expected_silver_count = int(latest_silver_publish["valid_row_count"])

    LOG.info(
        "GOLD_SOURCE_SELECTED: %s",
        json.dumps(
            {
                "source_year": source_year,
                "source_month": source_month,
                "source_checksum": source_checksum,
                "expected_silver_count": expected_silver_count,
                "hours_in_month": hours_in_month,
                "ml_zone_count": zone_count,
                "expected_ml_rows": expected_ml_rows,
                "gold_run_id": gold_run_id,
            }
        ),
    )

    silver_df = (
        spark.table(silver_table)
        .where(
            (col("source_year") == source_year)
            & (col("source_month") == source_month)
            & (col("source_checksum") == source_checksum)
            & (col("pickup_datetime") >= lit(start_datetime))
            & (col("pickup_datetime") < lit(end_datetime))
        )
        .cache()
    )

    silver_row_count = silver_df.count()

    if silver_row_count == 0:
        raise ValueError("The selected Silver month contains no rows.")

    if silver_row_count != expected_silver_count:
        raise ValueError(
            "Silver ledger reconciliation failed: "
            f"ledger={expected_silver_count}, "
            f"actual={silver_row_count}"
        )

    # -----------------------------------------------------------------
    # ML MART
    #
    # Dense grain:
    # one row for every zone from 2 through 263
    # for every hour in the requested month.
    # -----------------------------------------------------------------

    time_skeleton_df = spark.sql(f"""
        SELECT explode(
            sequence(
                timestamp('{start_text}'),
                timestamp('{end_text}') - interval 1 hour,
                interval 1 hour
            )
        ) AS pickup_hour_ts
        """)

    zone_skeleton_df = spark.sql(f"""
        SELECT explode(
            sequence({ML_ZONE_START}, {ML_ZONE_END})
        ) AS pickup_location_id
        """)

    dense_grid_df = zone_skeleton_df.crossJoin(time_skeleton_df)

    actual_rides_df = silver_df.groupBy(
        col("pickup_location_id"),
        date_trunc(
            "hour",
            col("pickup_datetime"),
        ).alias("pickup_hour_ts"),
    ).agg(count(lit(1)).cast("int").alias("actual_rides"))

    ml_df = (
        dense_grid_df.alias("grid")
        .join(
            actual_rides_df.alias("actual"),
            on=[
                col("grid.pickup_location_id") == col("actual.pickup_location_id"),
                col("grid.pickup_hour_ts") == col("actual.pickup_hour_ts"),
            ],
            how="left",
        )
        .select(
            lit(source_year).cast("int").alias("target_year"),
            lit(source_month).cast("int").alias("target_month"),
            col("grid.pickup_location_id").cast("int").alias("pickup_location_id"),
            col("grid.pickup_hour_ts").cast("timestamp").alias("pickup_hour_ts"),
            coalesce(
                col("actual.actual_rides"),
                lit(0),
            )
            .cast("int")
            .alias("ride_count"),
        )
        .cache()
    )

    ml_row_count = ml_df.count()

    if ml_row_count != expected_ml_rows:
        raise ValueError(
            "Dense ML grid count is incorrect: "
            f"expected={expected_ml_rows}, "
            f"actual={ml_row_count}"
        )

    leaked_fields = sorted(FORBIDDEN_ML_FIELDS.intersection(ml_df.columns))

    if leaked_fields:
        raise ValueError(
            "Target leakage fields detected in ML mart: " + ", ".join(leaked_fields)
        )

    # -----------------------------------------------------------------
    # BI MART
    #
    # Sparse grain:
    # one row for each observed pickup-zone and hour combination.
    # -----------------------------------------------------------------

    bi_df = (
        silver_df.groupBy(
            date_trunc(
                "hour",
                col("pickup_datetime"),
            ).alias("pickup_hour_ts"),
            col("pickup_location_id"),
        )
        .agg(
            count(lit(1)).cast("long").alias("trip_count"),
            avg(col("trip_distance")).cast("double").alias("avg_distance"),
        )
        .select(
            lit(source_year).cast("int").alias("target_year"),
            lit(source_month).cast("int").alias("target_month"),
            col("pickup_hour_ts").cast("timestamp").alias("pickup_hour_ts"),
            col("pickup_location_id").cast("int").alias("pickup_location_id"),
            col("trip_count").cast("long").alias("trip_count"),
            col("avg_distance").cast("double").alias("avg_distance"),
        )
        .cache()
    )

    bi_row_count = bi_df.count()

    if bi_row_count == 0:
        raise ValueError("The BI mart contains no rows.")

    LOG.info(
        "GOLD_MART_BUILD_SUMMARY: %s",
        json.dumps(
            {
                "silver_row_count": silver_row_count,
                "ml_observation_row_count": ml_row_count,
                "expected_ml_observation_row_count": (expected_ml_rows),
                "bi_row_count": bi_row_count,
                "ml_leakage_check": "PASS",
            }
        ),
    )

    # Dynamic partition overwrite replaces only the month represented
    # in each output DataFrame.
    ml_df.writeTo(ml_table).overwritePartitions()

    bi_df.writeTo(bi_table).overwritePartitions()

    published_ml_count = (
        spark.table(ml_table)
        .where(
            (col("target_year") == source_year) & (col("target_month") == source_month)
        )
        .count()
    )

    published_bi_count = (
        spark.table(bi_table)
        .where(
            (col("target_year") == source_year) & (col("target_month") == source_month)
        )
        .count()
    )

    if published_ml_count != expected_ml_rows:
        raise ValueError(
            "Published ML mart count is incorrect: "
            f"expected={expected_ml_rows}, "
            f"published={published_ml_count}"
        )

    if published_bi_count != bi_row_count:
        raise ValueError(
            "Published BI mart count is incorrect: "
            f"expected={bi_row_count}, "
            f"published={published_bi_count}"
        )

    write_ml_history_to_dynamodb(
            ml_df,
            demand_history_table,
        )
    update_prediction_range_metadata(
            demand_history_table,
            start_datetime,
            end_datetime,
        )

    append_build_ledger(
        spark,
        gold_ledger,
        source_year=source_year,
        source_month=source_month,
        source_checksum=source_checksum,
        silver_row_count=silver_row_count,
        ml_observation_row_count=published_ml_count,
        bi_row_count=published_bi_count,
        gold_run_id=gold_run_id,
    )

    silver_df.unpersist()
    ml_df.unpersist()
    bi_df.unpersist()

    LOG.info(
        "GOLD_BUILD_COMPLETE: %s",
        json.dumps(
            {
                "source_year": source_year,
                "source_month": source_month,
                "source_checksum": source_checksum,
                "silver_row_count": silver_row_count,
                "ml_observation_row_count": published_ml_count,
                "bi_row_count": published_bi_count,
                "gold_run_id": gold_run_id,
            }
        ),
    )

    job.commit()


if __name__ == "__main__":
    main()
