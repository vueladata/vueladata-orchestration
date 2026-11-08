"""Validate application errors in the invocation task so retries invoke again."""
import json

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


class CheckedLambdaInvokeFunctionOperator(LambdaInvokeFunctionOperator):
    def execute(self, context):
        response = super().execute(context)
        try:
            payload = json.loads(response)
            status = int(payload["statusCode"])
        except (TypeError, ValueError, KeyError) as exc:
            raise AirflowException("Lambda returned an invalid statusCode response") from exc
        if status == 429:
            # Do not spend more anonymous credits. Lambda must honor upstream
            # X-Rate-Limit-Retry-After-Seconds across EventBridge and Airflow.
            raise AirflowFailException("Upstream quota exhausted; respect retry-after before a new run")
        if status != 200:
            raise AirflowException(f"Lambda ingestion failed with status {status}")
        return {"statusCode": status}
