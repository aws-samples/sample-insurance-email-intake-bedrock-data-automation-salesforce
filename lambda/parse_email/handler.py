"""
Lambda 1: Start Processing

Triggered by S3 event when a new email arrives.
- Parses MIME email & checks for duplicates
- Extracts attachments to S3
- Invokes BDA async (if PDFs found)
- Starts Step Functions execution to wait for BDA + finalize
"""

import json
import hashlib
import time
import re
import email as email_lib
from email import policy
import os
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── AWS Clients ───
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sfn_client = boto3.client("stepfunctions")
bedrock = boto3.client("bedrock-data-automation-runtime")

# ─── Configuration ───
BUCKET_NAME = os.environ["BUCKET_NAME"]
DEDUP_TABLE_NAME = os.environ.get("DEDUP_TABLE_NAME", "EmailDedup")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
BDA_PROJECT_ARN = os.environ.get("BDA_PROJECT_ARN", "")

dedup_table = dynamodb.Table(DEDUP_TABLE_NAME)


def lambda_handler(event, context):
    """Entry point — triggered by S3 ObjectCreated event."""
    logger.info("Event received: %s", json.dumps(event, default=str))

    s3_record = event["Records"][0]["s3"]
    bucket = s3_record["bucket"]["name"]
    s3_key = s3_record["object"]["key"]

    logger.info("Processing email: s3://%s/%s", bucket, s3_key)

    # ── Step 1: Parse MIME email ──
    raw_email = s3.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
    msg = email_lib.message_from_bytes(raw_email, policy=policy.default)

    email_data = parse_email_metadata(msg)
    logger.info("Parsed email from=%s subject=%s", email_data["from"], email_data["subject"])

    # ── Step 2: Duplicate check ──
    email_hash = compute_hash(email_data, msg)

    if is_duplicate(email_hash):
        logger.info("Duplicate detected (hash=%s). Skipping.", email_hash)
        return {"status": "skipped", "reason": "duplicate", "hash": email_hash}

    # ── Step 3: Extract attachments ──
    email_id = hashlib.md5(s3_key.encode(), usedforsecurity=False).hexdigest()[:12]
    attachments = extract_attachments(msg, email_id, bucket)

    if not attachments:
        logger.info("No attachments found. Marking processed and skipping.")
        mark_processed(email_hash, s3_key)
        return {"status": "skipped", "reason": "no_attachments"}

    logger.info("Extracted %d attachment(s)", len(attachments))

    # ── Step 4: Invoke BDA (if PDFs present) ──
    bda_result = invoke_bda(attachments, bucket)

    # ── Step 5: Start Step Functions execution ──
    execution_input = {
        "bucket": bucket,
        "s3_key": s3_key,
        "email_id": email_id,
        "email_hash": email_hash,
        "email_data": email_data,
        "attachments": attachments,
        "bda": bda_result,
    }

    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=f"email-{email_id}-{int(time.time())}",
        input=json.dumps(execution_input),
    )

    logger.info("Step Functions execution started: %s", response["executionArn"])

    return {
        "status": "processing",
        "execution_arn": response["executionArn"],
        "email_id": email_id,
    }


# ═══════════════════════════════════════════════════════════════════
# Email Parsing
# ═══════════════════════════════════════════════════════════════════

def parse_email_metadata(msg):
    """Extract metadata from parsed MIME message."""
    body = ""
    body_part = msg.get_body(preferencelist=("plain", "html"))
    if body_part:
        try:
            body = body_part.get_content()
        except Exception:
            body = str(body_part)

    return {
        "message_id": msg.get("Message-ID", ""),
        "from": msg.get("From", ""),
        "subject": msg.get("Subject", ""),
        "date": msg.get("Date", ""),
        "body": body[:30000],
    }


# ═══════════════════════════════════════════════════════════════════
# Deduplication
# ═══════════════════════════════════════════════════════════════════

def compute_hash(email_data, msg):
    """Compute a dedup hash from email metadata + attachment checksums."""
    hash_input = (
        f"{email_data['message_id']}|{email_data['from']}|"
        f"{email_data['subject']}|{email_data['date']}"
    )

    for part in msg.iter_attachments():
        try:
            content = part.get_content_bytes()
            hash_input += f"|{hashlib.sha256(content).hexdigest()}"
        except Exception:
            pass

    return hashlib.sha256(hash_input.encode()).hexdigest()


def is_duplicate(email_hash):
    """Check if this email hash already exists in DynamoDB."""
    response = dedup_table.get_item(Key={"email_hash": email_hash})
    return "Item" in response


def mark_processed(email_hash, s3_key):
    """Record this email as processed in DynamoDB with 30-day TTL."""
    dedup_table.put_item(
        Item={
            "email_hash": email_hash,
            "s3_key": s3_key,
            "processed_at": int(time.time()),
            "ttl": int(time.time()) + (30 * 24 * 60 * 60),
        }
    )


# ═══════════════════════════════════════════════════════════════════
# Attachment Extraction
# ═══════════════════════════════════════════════════════════════════

def extract_attachments(msg, email_id, bucket):
    """Extract attachments from MIME message and store in S3."""
    attachments = []

    for part in msg.iter_attachments():
        raw_filename = part.get_filename() or f"attachment_{len(attachments)}.bin"
        filename = sanitize_filename(raw_filename)
        try:
            content = part.get_content_bytes()
        except Exception:
            content = part.get_payload(decode=True) or b""

        attachment_s3_key = f"attachments/{email_id}/{filename}"
        s3.put_object(Bucket=bucket, Key=attachment_s3_key, Body=content)

        attachments.append({
            "filename": filename,
            "s3_key": attachment_s3_key,
            "content_type": part.get_content_type(),
            "size": len(content),
        })

    return attachments


def sanitize_filename(filename):
    """Remove path traversal characters and unsafe chars from a filename."""
    # Strip any directory components (path traversal defense)
    filename = os.path.basename(filename)
    # Remove any characters that aren't alphanumeric, dots, hyphens, or underscores
    filename = re.sub(r"[^\w.\-]", "_", filename)
    # Prevent empty filenames or hidden files
    filename = filename.lstrip(".")
    if not filename:
        filename = "unnamed_attachment"
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200 - len(ext)] + ext
    return filename


# ═══════════════════════════════════════════════════════════════════
# BDA Invocation
# ═══════════════════════════════════════════════════════════════════

def invoke_bda(attachments, bucket):
    """Start BDA async job for PDF attachments. Returns invocation metadata."""
    pdf_attachments = [a for a in attachments if a["content_type"] == "application/pdf"]

    if not pdf_attachments:
        return {"skipped": True, "reason": "no_pdf_attachments"}

    if not BDA_PROJECT_ARN:
        logger.warning("BDA_PROJECT_ARN not set. Skipping document splitting.")
        return {"skipped": True, "reason": "bda_not_configured"}

    # Get region and account for profile ARN
    region = os.environ.get("AWS_REGION", "us-east-1")
    account_id = os.environ.get("AWS_ACCOUNT_ID", "")
    if not account_id:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
    profile_arn = f"arn:aws:bedrock:{region}:{account_id}:data-automation-profile/us.data-automation-v1"

    attachment = pdf_attachments[0]
    logger.info("Invoking BDA: project=%s, input=s3://%s/%s",
                BDA_PROJECT_ARN, bucket, attachment["s3_key"])

    response = bedrock.invoke_data_automation_async(
        inputConfiguration={
            "s3Uri": f"s3://{bucket}/{attachment['s3_key']}"
        },
        dataAutomationConfiguration={
            "dataAutomationProjectArn": BDA_PROJECT_ARN,
            "stage": "LIVE",
        },
        outputConfiguration={
            "s3Uri": f"s3://{bucket}/bda-output/"
        },
        dataAutomationProfileArn=profile_arn,
    )

    invocation_arn = response["invocationArn"]
    logger.info("BDA invocation started: %s", invocation_arn)

    return {
        "skipped": False,
        "invocation_arn": invocation_arn,
        "input_attachment": attachment,
    }
