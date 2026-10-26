from __future__ import annotations

import json
import logging
import sys
from typing import Any
from urllib.parse import unquote, urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, lit

SOURCE_COLUMN_TYPES = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "RatecodeID": "double",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def validate_year_month(year: int, month: int) -> None:
    if not 2000 <= year <= 2100:
        raise ValueError(f"Invalid source year: {year}")

    if not 1 <= month <= 12:
        raise ValueError(f"Invalid source month: {month}")


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)

    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI '{s3_uri}'. Expected s3://bucket/key.")

    return parsed.netloc, unquote(parsed.path.lstrip("/"))


def read_source_metadata(
    s3_client: Any,
    source_s3_uri: str,
) -> dict[str, Any]:
    bucket, key = parse_s3_uri(source_s3_uri)

    response = s3_client.head_object(
        Bucket=bucket,
        Key=key,
    )

    metadata = response.get("Metadata", {})
    checksum = metadata.get("sha256")

    if not checksum:
        raise ValueError(
            "The source S3 object does not contain the required "
            "'sha256' metadata value."
        )

    return {
        "bucket": bucket,
        "key": key,
        "checksum": checksum,
        "content_length": int(response["ContentLength"]),
        "metadata": metadata,
    }


def append_ledger_record(
    spark,
    ledger_table: str,
    *,
    source_filename: str,
    source_s3_uri: str,
    checksum: str,
    source_year: int,
    source_month: int,
    run_id: str,
    row_count: int,
) -> None:
    ledger_row = Row(
        source_filename=source_filename,
        source_s3_uri=source_s3_uri,
        file_checksum=checksum,
        source_year=source_year,
        source_month=source_month,
        ingestion_run_id=run_id,
        row_count=row_count,
        status="SUCCESS",
    )

    ledger_df = spark.createDataFrame([ledger_row]).withColumn(
        "finished_at", current_timestamp()
    )

    ledger_df.writeTo(ledger_table).append()


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SOURCE_S3_URI",
            "SOURCE_FILENAME",
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "RUN_ID",
            "LAKE_BUCKET",
            "AUDIT_DATABASE",
            "BRONZE_DATABASE",
        ],
    )

    source_year = int(args["SOURCE_YEAR"])
    source_month = int(args["SOURCE_MONTH"])

    validate_year_month(source_year, source_month)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    # Initialize standard Python stdout logger
    logger = logging.getLogger("bronze_ingest")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source_s3_uri = args["SOURCE_S3_URI"]
    source_filename = args["SOURCE_FILENAME"]
    run_id = args["RUN_ID"]
    lake_bucket = args["LAKE_BUCKET"]
    audit_database = args["AUDIT_DATABASE"]
    bronze_database = args["BRONZE_DATABASE"]

    s3_client = boto3.client("s3")
    source = read_source_metadata(s3_client, source_s3_uri)

    checksum = source["checksum"]
    object_size = source["content_length"]
    object_metadata = source["metadata"]

    expected_year = object_metadata.get("source-year")
    expected_month = object_metadata.get("source-month")

    if expected_year and int(expected_year) != source_year:
        raise ValueError(
            f"Source-year metadata is {expected_year}, "
            f"but the job argument is {source_year}."
        )

    if expected_month and int(expected_month) != source_month:
        raise ValueError(
            f"Source-month metadata is {expected_month}, "
            f"but the job argument is {source_month}."
        )

    ledger_table = f"glue_catalog.{audit_database}.ingestion_ledger"
    bronze_table = f"glue_catalog.{bronze_database}.yellow_taxi_raw"
    ledger_location = f"s3://{lake_bucket}/warehouse/audit/ingestion_ledger/"
    bronze_location = f"s3://{lake_bucket}/warehouse/bronze/yellow_taxi_raw/"

    logger.info(
        "Starting Bronze ingestion: "
        + json.dumps(
            {
                "source_s3_uri": source_s3_uri,
                "source_filename": source_filename,
                "source_year": source_year,
                "source_month": source_month,
                "source_checksum": checksum,
                "source_object_size": object_size,
                "run_id": run_id,
            }
        )
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ledger_table} (
            source_filename STRING,
            source_s3_uri STRING,
            file_checksum STRING,
            source_year INT,
            source_month INT,
            ingestion_run_id STRING,
            row_count BIGINT,
            status STRING,
            finished_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{ledger_location}'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table} (
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            store_and_fwd_flag STRING,
            PULocationID INT,
            DOLocationID INT,
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
            source_filename STRING,
            source_checksum STRING,
            source_year INT,
            source_month INT,
            ingestion_run_id STRING,
            ingested_at_utc TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_year, source_month)
        LOCATION '{bronze_location}'
        TBLPROPERTIES (
            'format-version' = '2'
        )
        """)

    ledger_success_exists = (
        spark.table(ledger_table)
        .where((col("file_checksum") == checksum) & (col("status") == "SUCCESS"))
        .limit(1)
        .count()
        > 0
    )

    if ledger_success_exists:
        logger.info(
            f"BRONZE_IDEMPOTENCY_SKIP: checksum already has "
            f"a SUCCESS ledger record: {checksum}"
        )
        job.commit()
        return

    bronze_checksum_exists = (
        spark.table(bronze_table)
        .where(col("source_checksum") == checksum)
        .limit(1)
        .count()
        > 0
    )

    if bronze_checksum_exists:
        existing_row_count = (
            spark.table(bronze_table).where(col("source_checksum") == checksum).count()
        )

        logger.warning(
            "Checksum exists in Bronze but not in the ledger. "
            "Repairing the ledger without appending rows."
        )

        append_ledger_record(
            spark,
            ledger_table,
            source_filename=source_filename,
            source_s3_uri=source_s3_uri,
            checksum=checksum,
            source_year=source_year,
            source_month=source_month,
            run_id=run_id,
            row_count=existing_row_count,
        )

        job.commit()
        return

    df_raw = spark.read.parquet(source_s3_uri)

    for required_column, spark_type in SOURCE_COLUMN_TYPES.items():
        if required_column not in df_raw.columns:
            logger.warning(
                f"Source column '{required_column}' is missing. Adding it as NULL."
            )

            df_raw = df_raw.withColumn(
                required_column,
                lit(None).cast(spark_type),
            )

    df_bronze = df_raw.select(
        col("VendorID").cast("int").alias("VendorID"),
        col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
        col("passenger_count").cast("double").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("RatecodeID").cast("double").alias("RatecodeID"),
        col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        col("PULocationID").cast("int").alias("PULocationID"),
        col("DOLocationID").cast("int").alias("DOLocationID"),
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
        lit(source_filename).cast("string").alias("source_filename"),
        lit(checksum).cast("string").alias("source_checksum"),
        lit(source_year).cast("int").alias("source_year"),
        lit(source_month).cast("int").alias("source_month"),
        lit(run_id).cast("string").alias("ingestion_run_id"),
        current_timestamp().alias("ingested_at_utc"),
    ).cache()

    source_row_count = df_bronze.count()

    if source_row_count == 0:
        raise ValueError(f"The source file contains no rows: {source_s3_uri}")

    logger.info(f"Appending {source_row_count:,d} rows to {bronze_table}")

    df_bronze.writeTo(bronze_table).append()

    append_ledger_record(
        spark,
        ledger_table,
        source_filename=source_filename,
        source_s3_uri=source_s3_uri,
        checksum=checksum,
        source_year=source_year,
        source_month=source_month,
        run_id=run_id,
        row_count=source_row_count,
    )

    df_bronze.unpersist()

    logger.info(
        f"BRONZE_INGESTION_COMPLETE: year={source_year:04d} month={source_month:02d} "
        f"rows={source_row_count} checksum={checksum}"
    )

    job.commit()


if __name__ == "__main__":
    main()
