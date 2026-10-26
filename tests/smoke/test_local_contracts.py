"""Small, credential-free checks of the event and prediction contracts."""

from __future__ import annotations

import importlib.util
import json
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError


AWS_ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, AWS_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        os.environ,
        {
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "EXPECTED_BUCKET": "taxi-raw-test",
        },
    ):
        spec.loader.exec_module(module)
    return module


class LocalContracts(unittest.TestCase):
    def test_sqs_event_fails_when_workflow_disabled(self):
        module = load_module("data_received_test", "lambda/data_received/lambda_function.py")
        module.s3 = Mock()
        module.step_functions = Mock()
        module.s3.head_object.return_value = {"VersionId": "v1"}
        event = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(
                        {
                            "detail": {
                                "bucket": {"name": "taxi-raw-test"},
                                "object": {
                                    "key": "yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet"
                                },
                            }
                        }
                    ),
                }
            ]
        }
        with self.assertRaisesRegex(RuntimeError, "Orchestration is disabled"):
            module.lambda_handler(event, None)
        module.step_functions.start_execution.assert_not_called()

    def test_enabled_event_starts_monthly_workflow(self):
        module = load_module("data_received_enabled_test", "lambda/data_received/lambda_function.py")
        module.ORCHESTRATION_ENABLED = True
        module.STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:test"
        module.s3 = Mock()
        module.s3.head_object.return_value = {"VersionId": "v1"}
        module.step_functions = Mock()
        module.step_functions.start_execution.return_value = {"executionArn": "arn:aws:states:us-east-1:123456789012:execution:test:run-1"}
        result = module.process_eventbridge_event(
            {
                "detail": {
                    "bucket": {"name": "taxi-raw-test"},
                    "object": {
                        "key": "yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet"
                    },
                }
            }
        )
        self.assertEqual(result["status"], "execution_started")
        self.assertEqual(result["execution_arn"], "arn:aws:states:us-east-1:123456789012:execution:test:run-1")
        self.assertEqual(
            json.loads(module.step_functions.start_execution.call_args.kwargs["input"])["source_month"],
            "01",
        )
        self.assertEqual(module.step_functions.start_execution.call_args.kwargs["name"], result["run_id"])
        with self.assertRaisesRegex(ValueError, "Invalid source month"):
            module.validate_and_extract(
                "taxi-raw-test",
                "yellow/year=2025/month=13/yellow_tripdata_2025-13.parquet",
            )

    def test_duplicate_execution_is_acknowledged_only_if_running_or_succeeded(self):
        module = load_module("data_received_duplicate_test", "lambda/data_received/lambda_function.py")
        module.ORCHESTRATION_ENABLED = True
        module.STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:test"
        module.s3 = Mock()
        module.s3.head_object.return_value = {"VersionId": "v1"}
        module.step_functions = Mock()
        module.step_functions.start_execution.side_effect = ClientError(
            {"Error": {"Code": "ExecutionAlreadyExists", "Message": "duplicate"}}, "StartExecution"
        )
        event = {"detail": {"bucket": {"name": "taxi-raw-test"}, "object": {
            "key": "yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet"}}}
        module.step_functions.describe_execution.return_value = {"status": "SUCCEEDED"}
        self.assertEqual(module.process_eventbridge_event(event)["status"], "execution_already_exists")
        module.step_functions.describe_execution.return_value = {"status": "FAILED"}
        with self.assertRaisesRegex(RuntimeError, "investigate or redrive"):
            module.process_eventbridge_event(event)

    def test_prediction_uses_training_feature_names(self):
        module = load_module("inference_test", "ml/lambda_api/lambda_function.py")
        module.get_historical_features = Mock(
            return_value={
                "lag_1h": 1,
                "lag_24h": 2,
                "lag_168h": 3,
                "rolling_mean_3h": 4,
                "rolling_mean_24h": 5,
            }
        )
        model = Mock()
        model.predict.return_value = [12.5]
        module.load_model = Mock(return_value=model)

        response = module.lambda_handler(
            {
                "routeKey": "POST /predict",
                "body": json.dumps(
                    {
                        "pickup_location_id": 161,
                        "prediction_time": "2025-03-20T17:00:00Z",
                    }
                ),
            },
            None,
        )
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(json.loads(response["body"])["predicted_rides"], 12.5)
        row = model.predict.call_args.args[0]
        self.assertEqual(list(row.columns), module.FEATURES)
        self.assertEqual(row.iloc[0]["lag_168h"], 3)

    def test_streamlit_starts_with_default_zone(self):
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_file(str(AWS_ROOT / "ml/frontend/app.py")).run(timeout=30)
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(len(app.button), 1)
        self.assertEqual(app.selectbox[0].value, "Manhattan - Midtown Center")


if __name__ == "__main__":
    unittest.main()
