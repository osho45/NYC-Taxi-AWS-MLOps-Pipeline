from __future__ import annotations

import hashlib
import json
import os
import re
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


EXPECTED_BUCKET = os.environ["EXPECTED_BUCKET"]
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
ORCHESTRATION_ENABLED = os.environ.get("ORCHESTRATION_ENABLED", "false").lower() == "true"

step_functions = boto3.client("stepfunctions")
s3 = boto3.client("s3")


TAXI_KEY_PATTERN = re.compile(
    r"^yellow/year=(?P<year>\d{4})/"
    r"month=(?P<month>\d{2})/"
    r"yellow_tripdata_(?P=year)-(?P=month)\.parquet$"
)


def parse_eventbridge_event(event: dict) -> tuple[str, str]:
    detail = event["detail"]

    bucket = detail["bucket"]["name"]
    key = unquote_plus(detail["object"]["key"])

    return bucket, key


def validate_and_extract(bucket: str, key: str) -> dict:
    if bucket != EXPECTED_BUCKET:
        raise ValueError(
            f"Unexpected bucket: {bucket}. "
            f"Expected: {EXPECTED_BUCKET}"
        )

    match = TAXI_KEY_PATTERN.match(key)

    if not match:
        raise ValueError(
            f"Object key does not match expected taxi pattern: {key}"
        )

    year = match.group("year")
    month = match.group("month")

    if not 1 <= int(month) <= 12:
        raise ValueError(f"Invalid source month in object key: {month}")

    return {
        "source_bucket": bucket,
        "source_key": key,
        "source_filename": key.split("/")[-1],
        "source_year": year,
        "source_month": month,
    }


def build_run_id(bucket: str, key: str, version_id: str | None = None) -> str:
    raw_value = f"{bucket}:{key}:{version_id or ''}"
    return hashlib.sha256(raw_value.encode("utf-8")).hexdigest()[:32]


def process_eventbridge_event(event: dict) -> dict:
    bucket, key = parse_eventbridge_event(event)

    source = validate_and_extract(bucket, key)

    head = s3.head_object(
        Bucket=bucket,
        Key=key,
    )

    version_id = head.get("VersionId")

    run_id = build_run_id(
        bucket=bucket,
        key=key,
        version_id=version_id,
    )

    workflow_params = {
        **source,
        "run_id": run_id,
    }

    if not ORCHESTRATION_ENABLED:
        raise RuntimeError(
            "Orchestration is disabled; retain the SQS message for retry."
        )

    if not STATE_MACHINE_ARN:
        raise ValueError(
            "ORCHESTRATION_ENABLED is true but STATE_MACHINE_ARN is empty"
        )

    execution_arn = STATE_MACHINE_ARN.replace(":stateMachine:", ":execution:") + f":{run_id}"
    try:
        response = step_functions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=run_id,
            input=json.dumps(workflow_params, sort_keys=True, separators=(",", ":")),
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "ExecutionAlreadyExists":
            raise
        existing = step_functions.describe_execution(executionArn=execution_arn)
        if existing["status"] not in {"RUNNING", "SUCCEEDED"}:
            raise RuntimeError(
                f"Existing execution {execution_arn} ended {existing['status']}; investigate or redrive it."
            ) from error
        return {
            "status": "execution_already_exists",
            "execution_arn": execution_arn,
            "execution_status": existing["status"],
            **workflow_params,
        }

    return {
        "status": "execution_started",
        "state_machine_arn": STATE_MACHINE_ARN,
        "execution_arn": response["executionArn"],
        **workflow_params,
    }


def lambda_handler(event, context):
    results = []

    # Normal production path:
    # SQS -> Lambda
    if "Records" in event:
        for record in event["Records"]:
            if record.get("eventSource") != "aws:sqs":
                continue

            body = json.loads(record["body"])

            result = process_eventbridge_event(body)
            results.append(result)

    # Allows direct EventBridge-shaped test events
    elif event.get("source") == "aws.s3":
        result = process_eventbridge_event(event)
        results.append(result)

    else:
        raise ValueError("Unsupported event format")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "processed": len(results),
                "results": results,
            }
        ),
    }
