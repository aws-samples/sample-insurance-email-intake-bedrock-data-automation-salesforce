"""
Lambda 2: Check BDA Status

Lightweight function called in a loop by Step Functions.
Returns normalized status for the Choice state.
"""

import json
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── AWS Clients ───
bedrock = boto3.client("bedrock-data-automation-runtime")


def lambda_handler(event, context):
    """Check BDA invocation status."""
    logger.info("Event: %s", json.dumps(event, default=str))

    invocation_arn = event["bda"]["invocation_arn"]

    status_response = bedrock.get_data_automation_status(
        invocationArn=invocation_arn
    )

    status = status_response["status"]
    logger.info("BDA status: %s for invocation: %s", status, invocation_arn)

    # Normalize status values
    if status in ("COMPLETED", "SUCCESS", "Success"):
        normalized_status = "COMPLETED"
    elif status in ("FAILED", "ERROR", "SERVICE_ERROR"):
        normalized_status = "FAILED"
    else:
        normalized_status = "IN_PROGRESS"

    # Build output prefix from invocation ARN
    invocation_id = invocation_arn.split("/")[-1]
    output_prefix = f"bda-output//{invocation_id}"

    return {
        "status": normalized_status,
        "output_prefix": output_prefix,
    }
