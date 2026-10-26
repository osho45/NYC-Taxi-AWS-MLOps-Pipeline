from __future__ import annotations

import json
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import (
    array,
    col,
    current_timestamp,
    filter as array_filter,
    hour,
    lit,
    month as spark_month,
    size,
    to_date,
    unix_timestamp,
    when,
    year as spark_year,
)

LOG = logging.getLogger("silver_process")
LOG.setLevel(logging.INFO)

if not LOG.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    LOG.addHandler(handler)


SILVER_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "trip_duration_minutes",
    "pickup_date",
    "pickup_hour",
    "source_filename",
    "source_checksum",
    "source_year",
    "source_month",
    "bronze_ingestion_run_id",
    "bronze_ingested_at_utc",
    "silver_run_id",
    "silver_processed_at_utc",
]


def validate_year_month(source_year: int, source_month: int) -> None:
    if not 2000 <= source_year <= 2100:
        raise ValueError(f"Invalid source year: {source_year}")

    if not 1 <= source_month <= 12:
        raise ValueError(f"Invalid source month: {source_month}")


def create_tables(
    spark,
    *,
    audit_database: str,
    silver_database: str,
    quarantine_database: str,
    lake_bucket: str,
) -> None:
    publish_ledger = f"glue_catalog.{audit_database}.silver_publish_ledger"

    candidate_table = f"glue_catalog.{silver_database}.yellow_taxi_candidate"

    silver_table = f"glue_catalog.{silver_database}.yellow_taxi"

    quarantine_table = f"glue_catalog.{quarantine_database}.yellow_taxi_rejected"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {publish_ledger} (
            source_year INT,
            source_month INT,
            source_filename STRING,
            source_checksum STRING,
            bronze_row_count BIGINT,
            valid_row_count BIGINT,
            quarantined_row_count BIGINT,
            silver_run_id STRING,
            status STRING,
            finished_at TIMESTAMP
        )
        USING iceberg
        LOCATION
          's3://{lake_bucket}/warehouse/audit/silver_publish_ledger/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    silver_schema = """
        vendor_id INT,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        rate_code_id DOUBLE,
        store_and_fwd_flag STRING,
        pickup_location_id INT,
        dropoff_location_id INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        airport_fee DOUBLE,
        trip_duration_minutes DOUBLE,
        pickup_date DATE,
        pickup_hour INT,
        source_filename STRING,
        source_checksum STRING,
        source_year INT,
        source_month INT,
        bronze_ingestion_run_id STRING,
        bronze_ingested_at_utc TIMESTAMP,
        silver_run_id STRING,
        silver_processed_at_utc TIMESTAMP
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {candidate_table} (
            {silver_schema}
        )
        USING iceberg
        PARTITIONED BY (source_year, source_month)
        LOCATION
          's3://{lake_bucket}/warehouse/silver/yellow_taxi_candidate/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            {silver_schema}
        )
        USING iceberg
        PARTITIONED BY (source_year, source_month)
        LOCATION
          's3://{lake_bucket}/warehouse/silver/yellow_taxi/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table} (
            {silver_schema},
            validation_errors ARRAY<STRING>,
            quarantined_at_utc TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_year, source_month)
        LOCATION
          's3://{lake_bucket}/warehouse/quarantine/yellow_taxi_rejected/'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)


def append_publish_ledger(
    spark,
    ledger_table: str,
    *,
    source_year: int,
    source_month: int,
    source_filename: str,
    source_checksum: str,
    bronze_row_count: int,
    valid_row_count: int,
    quarantined_row_count: int,
    silver_run_id: str,
) -> None:
    ledger_row = Row(
        source_year=source_year,
        source_month=source_month,
        source_filename=source_filename,
        source_checksum=source_checksum,
        bronze_row_count=bronze_row_count,
        valid_row_count=valid_row_count,
        quarantined_row_count=quarantined_row_count,
        silver_run_id=silver_run_id,
        status="SUCCESS",
    )

    (
        spark.createDataFrame([ledger_row])
        .withColumn("finished_at", current_timestamp())
        .writeTo(ledger_table)
        .append()
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
            "BRONZE_DATABASE",
            "SILVER_DATABASE",
            "QUARANTINE_DATABASE",
        ],
    )

    source_year = int(args["SOURCE_YEAR"])
    source_month = int(args["SOURCE_MONTH"])
    silver_run_id = args["RUN_ID"]

    validate_year_month(source_year, source_month)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    audit_database = args["AUDIT_DATABASE"]
    bronze_database = args["BRONZE_DATABASE"]
    silver_database = args["SILVER_DATABASE"]
    quarantine_database = args["QUARANTINE_DATABASE"]
    lake_bucket = args["LAKE_BUCKET"]

    ingestion_ledger = f"glue_catalog.{audit_database}.ingestion_ledger"

    publish_ledger = f"glue_catalog.{audit_database}.silver_publish_ledger"

    bronze_table = f"glue_catalog.{bronze_database}.yellow_taxi_raw"

    candidate_table = f"glue_catalog.{silver_database}.yellow_taxi_candidate"

    silver_table = f"glue_catalog.{silver_database}.yellow_taxi"

    quarantine_table = f"glue_catalog.{quarantine_database}.yellow_taxi_rejected"

    create_tables(
        spark,
        audit_database=audit_database,
        silver_database=silver_database,
        quarantine_database=quarantine_database,
        lake_bucket=lake_bucket,
    )

    latest_ingestion = (
        spark.table(ingestion_ledger)
        .where(
            (col("source_year") == source_year)
            & (col("source_month") == source_month)
            & (col("status") == "SUCCESS")
        )
        .orderBy(col("finished_at").desc())
        .select(
            "source_filename",
            "file_checksum",
            "ingestion_run_id",
            "finished_at",
        )
        .first()
    )

    if latest_ingestion is None:
        raise ValueError(
            "No successful Bronze ingestion ledger record exists for "
            f"{source_year:04d}-{source_month:02d}."
        )

    source_filename = latest_ingestion["source_filename"]
    source_checksum = latest_ingestion["file_checksum"]

    LOG.info(
        "SILVER_SOURCE_SELECTED: %s",
        json.dumps(
            {
                "source_year": source_year,
                "source_month": source_month,
                "source_filename": source_filename,
                "source_checksum": source_checksum,
                "silver_run_id": silver_run_id,
            }
        ),
    )

    already_published = (
        spark.table(publish_ledger)
        .where(
            (col("source_checksum") == source_checksum) & (col("status") == "SUCCESS")
        )
        .limit(1)
        .count()
        > 0
    )

    if already_published:
        LOG.info(
            "SILVER_IDEMPOTENCY_SKIP: checksum=%s",
            source_checksum,
        )

        job.commit()
        return

    bronze_df = spark.table(bronze_table).where(
        (col("source_year") == source_year)
        & (col("source_month") == source_month)
        & (col("source_checksum") == source_checksum)
    )

    bronze_row_count = bronze_df.count()

    if bronze_row_count == 0:
        raise ValueError(
            "The selected Bronze source contains no rows: "
            f"{source_year:04d}-{source_month:02d}, "
            f"checksum={source_checksum}"
        )

    normalized_df = bronze_df.select(
        col("VendorID").cast("int").alias("vendor_id"),
        col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("passenger_count").cast("double").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("RatecodeID").cast("double").alias("rate_code_id"),
        col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        col("PULocationID").cast("int").alias("pickup_location_id"),
        col("DOLocationID").cast("int").alias("dropoff_location_id"),
        col("payment_type").cast("int").alias("payment_type"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("extra").cast("double").alias("extra"),
        col("mta_tax").cast("double").alias("mta_tax"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("tolls_amount").cast("double").alias("tolls_amount"),
        col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        col("total_amount").cast("double").alias("total_amount"),
        col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col("airport_fee").cast("double").alias("airport_fee"),
        (
            (
                unix_timestamp(col("tpep_dropoff_datetime"))
                - unix_timestamp(col("tpep_pickup_datetime"))
            )
            / lit(60.0)
        )
        .cast("double")
        .alias("trip_duration_minutes"),
        to_date(col("tpep_pickup_datetime")).alias("pickup_date"),
        hour(col("tpep_pickup_datetime")).cast("int").alias("pickup_hour"),
        col("source_filename").cast("string").alias("source_filename"),
        col("source_checksum").cast("string").alias("source_checksum"),
        col("source_year").cast("int").alias("source_year"),
        col("source_month").cast("int").alias("source_month"),
        col("ingestion_run_id").cast("string").alias("bronze_ingestion_run_id"),
        col("ingested_at_utc").cast("timestamp").alias("bronze_ingested_at_utc"),
        lit(silver_run_id).cast("string").alias("silver_run_id"),
        current_timestamp().alias("silver_processed_at_utc"),
    ).cache()

    validation_errors = array_filter(
        array(
            when(
                col("pickup_datetime").isNull(),
                lit("MISSING_PICKUP_DATETIME"),
            ),
            when(
                col("dropoff_datetime").isNull(),
                lit("MISSING_DROPOFF_DATETIME"),
            ),
            when(
                col("pickup_datetime").isNotNull()
                & col("dropoff_datetime").isNotNull()
                & (col("dropoff_datetime") <= col("pickup_datetime")),
                lit("NON_POSITIVE_TRIP_DURATION"),
            ),
            when(
                col("trip_duration_minutes") > lit(1440.0),
                lit("TRIP_DURATION_OVER_24_HOURS"),
            ),
            when(
                col("trip_distance").isNull(),
                lit("MISSING_TRIP_DISTANCE"),
            ),
            when(
                col("trip_distance") < lit(0.0),
                lit("NEGATIVE_TRIP_DISTANCE"),
            ),
            when(
                col("trip_distance") > lit(200.0),
                lit("TRIP_DISTANCE_OVER_200_MILES"),
            ),
            when(
                col("fare_amount").isNull(),
                lit("MISSING_FARE_AMOUNT"),
            ),
            when(
                col("fare_amount") < lit(0.0),
                lit("NEGATIVE_FARE_AMOUNT"),
            ),
            when(
                col("total_amount").isNull(),
                lit("MISSING_TOTAL_AMOUNT"),
            ),
            when(
                col("total_amount") < lit(0.0),
                lit("NEGATIVE_TOTAL_AMOUNT"),
            ),
            when(
                col("pickup_location_id").isNull()
                | (col("pickup_location_id") <= lit(0)),
                lit("INVALID_PICKUP_LOCATION"),
            ),
            when(
                col("dropoff_location_id").isNull()
                | (col("dropoff_location_id") <= lit(0)),
                lit("INVALID_DROPOFF_LOCATION"),
            ),
            when(
                col("passenger_count").isNotNull()
                & (
                    (col("passenger_count") < lit(0.0))
                    | (col("passenger_count") > lit(9.0))
                ),
                lit("INVALID_PASSENGER_COUNT"),
            ),
            when(
                col("pickup_datetime").isNotNull()
                & (
                    (spark_year(col("pickup_datetime")) != source_year)
                    | (spark_month(col("pickup_datetime")) != source_month)
                ),
                lit("PICKUP_OUTSIDE_SOURCE_MONTH"),
            ),
        ),
        lambda error: error.isNotNull(),
    )

    evaluated_df = normalized_df.withColumn(
        "validation_errors",
        validation_errors,
    ).cache()

    valid_df = (
        evaluated_df.where(size(col("validation_errors")) == 0)
        .drop("validation_errors")
        .select(*SILVER_COLUMNS)
        .cache()
    )

    rejected_df = (
        evaluated_df.where(size(col("validation_errors")) > 0)
        .withColumn("quarantined_at_utc", current_timestamp())
        .select(
            *SILVER_COLUMNS,
            "validation_errors",
            "quarantined_at_utc",
        )
        .cache()
    )

    valid_row_count = valid_df.count()
    quarantined_row_count = rejected_df.count()

    if bronze_row_count != valid_row_count + quarantined_row_count:
        raise ValueError(
            "Silver count reconciliation failed: "
            f"bronze={bronze_row_count}, "
            f"valid={valid_row_count}, "
            f"quarantined={quarantined_row_count}"
        )

    if valid_row_count == 0:
        raise ValueError("Silver quality gate failed because no valid rows remain.")

    LOG.info(
        "SILVER_VALIDATION_SUMMARY: %s",
        json.dumps(
            {
                "bronze_row_count": bronze_row_count,
                "valid_row_count": valid_row_count,
                "quarantined_row_count": quarantined_row_count,
            }
        ),
    )

    # WRITE: stage valid rows in the candidate table.
    valid_df.writeTo(candidate_table).overwritePartitions()

    # WRITE: replace this month's quarantine partition.
    if quarantined_row_count > 0:
        rejected_df.writeTo(quarantine_table).overwritePartitions()
    else:
        spark.sql(f"""
            DELETE FROM {quarantine_table}
            WHERE source_year = {source_year}
              AND source_month = {source_month}
            """)

    # AUDIT: read the staged candidate back from Iceberg.
    staged_df = (
        spark.table(candidate_table)
        .where(
            (col("source_year") == source_year)
            & (col("source_month") == source_month)
            & (col("source_checksum") == source_checksum)
            & (col("silver_run_id") == silver_run_id)
        )
        .select(*SILVER_COLUMNS)
        .cache()
    )

    staged_row_count = staged_df.count()

    if staged_row_count != valid_row_count:
        raise ValueError(
            "Silver candidate audit failed: "
            f"expected={valid_row_count}, "
            f"staged={staged_row_count}"
        )

    staged_checksum_count = staged_df.select("source_checksum").distinct().count()

    if staged_checksum_count != 1:
        raise ValueError(
            "Silver candidate contains an unexpected number "
            f"of source checksums: {staged_checksum_count}"
        )

    # PUBLISH: replace only the affected monthly Silver partition.
    staged_df.writeTo(silver_table).overwritePartitions()

    published_month_df = spark.table(silver_table).where(
        (col("source_year") == source_year) & (col("source_month") == source_month)
    )

    published_row_count = published_month_df.count()

    if published_row_count != valid_row_count:
        raise ValueError(
            "Silver publish verification failed: "
            f"expected={valid_row_count}, "
            f"published={published_row_count}"
        )

    wrong_checksum_count = (
        published_month_df.where(col("source_checksum") != source_checksum)
        .limit(1)
        .count()
    )

    if wrong_checksum_count > 0:
        raise ValueError(
            "Published Silver partition contains rows from "
            "an unexpected source checksum."
        )

    append_publish_ledger(
        spark,
        publish_ledger,
        source_year=source_year,
        source_month=source_month,
        source_filename=source_filename,
        source_checksum=source_checksum,
        bronze_row_count=bronze_row_count,
        valid_row_count=valid_row_count,
        quarantined_row_count=quarantined_row_count,
        silver_run_id=silver_run_id,
    )

    normalized_df.unpersist()
    evaluated_df.unpersist()
    valid_df.unpersist()
    rejected_df.unpersist()
    staged_df.unpersist()

    LOG.info(
        "SILVER_PUBLISH_COMPLETE: %s",
        json.dumps(
            {
                "source_year": source_year,
                "source_month": source_month,
                "source_checksum": source_checksum,
                "bronze_row_count": bronze_row_count,
                "valid_row_count": valid_row_count,
                "quarantined_row_count": quarantined_row_count,
                "silver_run_id": silver_run_id,
            }
        ),
    )

    job.commit()


if __name__ == "__main__":
    main()
