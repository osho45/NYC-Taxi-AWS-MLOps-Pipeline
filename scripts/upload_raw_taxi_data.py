#!/usr/bin/env python3

"""
Download NYC Yellow Taxi Parquet files and upload them to Amazon S3.

Default date range:
    January 2025 through May 2026

S3 object layout:
    yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet

The year= and month= portions follow Hive-style partition naming.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from boto3.s3.transfer import TransferConfig
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_START = "2025-01"
DEFAULT_END = "2026-05"
DEFAULT_REGION = "us-east-1"

LOG = logging.getLogger("upload_raw_taxi_data")


@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> "YearMonth":
        try:
            year_text, month_text = value.split("-", maxsplit=1)
            year = int(year_text)
            month = int(month_text)
        except (ValueError, AttributeError) as exc:
            raise argparse.ArgumentTypeError(
                f"Invalid year-month '{value}'. Use YYYY-MM."
            ) from exc

        if not 1 <= month <= 12:
            raise argparse.ArgumentTypeError(
                f"Invalid month in '{value}'. Month must be 01 through 12."
            )

        return cls(year=year, month=month)

    def next_month(self) -> "YearMonth":
        if self.month == 12:
            return YearMonth(self.year + 1, 1)

        return YearMonth(self.year, self.month + 1)

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def iter_months(start: YearMonth, end: YearMonth) -> Iterator[YearMonth]:
    current = start

    while current.year < end.year or (
        current.year == end.year and current.month <= end.month
    ):
        yield current
        current = current.next_month()


def build_http_session() -> requests.Session:
    retry_policy = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )

    adapter = HTTPAdapter(
        max_retries=retry_policy,
        pool_connections=2,
        pool_maxsize=2,
    )

    session = requests.Session()
    session.mount("https://", adapter)

    return session


def terraform_raw_bucket() -> str:
    """
    Read raw_bucket_name from the Terraform output.

    The script is located under:
        aws/scripts/

    Terraform is located under:
        aws/terraform/
    """
    terraform_directory = Path(__file__).resolve().parents[1] / "terraform"

    if not terraform_directory.exists():
        raise RuntimeError(f"Terraform directory not found: {terraform_directory}")

    command = [
        "terraform",
        f"-chdir={terraform_directory}",
        "output",
        "-raw",
        "raw_bucket_name",
    ]

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Terraform was not found. Install Terraform or pass --bucket."
        ) from exc
    except subprocess.CalledProcessError as exc:
        message = exc.stderr.strip() or exc.stdout.strip()

        raise RuntimeError(
            "Could not read raw_bucket_name from Terraform output. "
            f"Terraform said: {message}"
        ) from exc

    bucket = result.stdout.strip()

    if not bucket:
        raise RuntimeError("Terraform returned an empty raw bucket name.")

    return bucket


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True

    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))

        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False

        raise


def download_to_temporary_file(
    http_session: requests.Session,
    url: str,
) -> tuple[Path, str, int]:
    """
    Download one file while calculating SHA-256.

    Only one monthly file is held on local disk at a time.
    """
    response = http_session.get(
        url,
        stream=True,
        timeout=(30, 300),
    )

    if response.status_code == 404:
        response.close()
        raise FileNotFoundError(f"Source file is not available: {url}")

    response.raise_for_status()

    sha256 = hashlib.sha256()
    bytes_downloaded = 0

    temporary_file = tempfile.NamedTemporaryFile(
        prefix="yellow_taxi_",
        suffix=".parquet",
        delete=False,
    )

    temporary_path = Path(temporary_file.name)

    try:
        with temporary_file:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if not chunk:
                    continue

                temporary_file.write(chunk)
                sha256.update(chunk)
                bytes_downloaded += len(chunk)

    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    finally:
        response.close()

    if bytes_downloaded == 0:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded file was empty: {url}")

    return temporary_path, sha256.hexdigest(), bytes_downloaded


def upload_month(
    *,
    http_session: requests.Session,
    s3_client,
    bucket: str,
    year_month: YearMonth,
    overwrite: bool,
) -> str:
    filename = f"yellow_tripdata_{year_month}.parquet"

    source_url = f"{BASE_URL}/{filename}"

    s3_key = (
        f"yellow/"
        f"year={year_month.year:04d}/"
        f"month={year_month.month:02d}/"
        f"{filename}"
    )

    s3_uri = f"s3://{bucket}/{s3_key}"

    if not overwrite and object_exists(s3_client, bucket, s3_key):
        LOG.info("SKIP: Object already exists: %s", s3_uri)
        return "skipped"

    LOG.info("Downloading %s", source_url)

    temporary_path, sha256, file_size = download_to_temporary_file(
        http_session,
        source_url,
    )

    LOG.info(
        "Downloaded %s bytes; SHA-256=%s",
        f"{file_size:,}",
        sha256,
    )

    transfer_config = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True,
    )

    metadata = {
        "dataset": "yellow-taxi",
        "source-year": f"{year_month.year:04d}",
        "source-month": f"{year_month.month:02d}",
        "source-url": source_url,
        "sha256": sha256,
    }

    LOG.info("Uploading to %s", s3_uri)

    try:
        s3_client.upload_file(
            Filename=str(temporary_path),
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/vnd.apache.parquet",
                "Metadata": metadata,
            },
            Config=transfer_config,
        )

        uploaded_object = s3_client.head_object(
            Bucket=bucket,
            Key=s3_key,
        )

        uploaded_size = uploaded_object["ContentLength"]

        if uploaded_size != file_size:
            raise RuntimeError(
                "Uploaded object size does not match downloaded file size. "
                f"Downloaded={file_size}, uploaded={uploaded_size}"
            )

        LOG.info("SUCCESS: %s", s3_uri)
        return "uploaded"

    finally:
        temporary_path.unlink(missing_ok=True)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download NYC Yellow Taxi Parquet files and upload them "
            "to the Terraform-created raw S3 bucket."
        )
    )

    parser.add_argument(
        "--start",
        type=YearMonth.parse,
        default=YearMonth.parse(DEFAULT_START),
        help=f"First month in YYYY-MM format. Default: {DEFAULT_START}",
    )

    parser.add_argument(
        "--end",
        type=YearMonth.parse,
        default=YearMonth.parse(DEFAULT_END),
        help=f"Last month in YYYY-MM format. Default: {DEFAULT_END}",
    )

    parser.add_argument(
        "--bucket",
        help=(
            "Raw S3 bucket name. When omitted, the script reads "
            "raw_bucket_name from Terraform output."
        ),
    )

    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"AWS Region. Default: {DEFAULT_REGION}",
    )

    parser.add_argument(
        "--profile",
        help=(
            "Optional AWS CLI profile. When omitted, Boto3 uses the "
            "normal AWS credential chain."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an S3 object when its key already exists.",
    )

    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with later months when one month fails.",
    )

    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_arguments()

    if (args.start.year, args.start.month) > (args.end.year, args.end.month):
        LOG.error("--start must not be later than --end.")
        return 2

    bucket = args.bucket or terraform_raw_bucket()

    LOG.info("AWS Region: %s", args.region)
    LOG.info("Raw bucket: %s", bucket)
    LOG.info("Date range: %s through %s", args.start, args.end)

    boto_session = boto3.Session(
        profile_name=args.profile,
        region_name=args.region,
    )

    s3_client = boto_session.client("s3")
    http_session = build_http_session()

    counts = {
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
    }

    for year_month in iter_months(args.start, args.end):
        try:
            result = upload_month(
                http_session=http_session,
                s3_client=s3_client,
                bucket=bucket,
                year_month=year_month,
                overwrite=args.overwrite,
            )

            counts[result] += 1

        except FileNotFoundError as exc:
            counts["failed"] += 1
            LOG.error("%s", exc)

            if not args.continue_on_error:
                return 1

        except (
            ClientError,
            NoCredentialsError,
            requests.RequestException,
            RuntimeError,
        ) as exc:
            counts["failed"] += 1
            LOG.exception("FAILED for %s: %s", year_month, exc)

            if not args.continue_on_error:
                return 1

    LOG.info(
        "Finished: uploaded=%d skipped=%d failed=%d",
        counts["uploaded"],
        counts["skipped"],
        counts["failed"],
    )

    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
