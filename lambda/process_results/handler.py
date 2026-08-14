"""
Lambda 3: Process Results

Called after BDA completes (or is skipped). Handles:
- Parsing BDA output metadata
- Physically splitting PDF by page boundaries
- Creating Salesforce Case with Composite API
- Marking the email as processed in DynamoDB
"""

import json
import io
import base64
import time
import os
import logging

import boto3
import jwt
import requests
from pypdf import PdfReader, PdfWriter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── AWS Clients ───
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
secrets_client = boto3.client("secretsmanager")

# ─── Configuration ───
DEDUP_TABLE_NAME = os.environ.get("DEDUP_TABLE_NAME", "EmailDedup")
SF_SECRET_ID = os.environ.get("SF_SECRET_ID", "salesforce/integration")
SF_LOGIN_URL = os.environ.get("SF_LOGIN_URL", "https://login.salesforce.com")

dedup_table = dynamodb.Table(DEDUP_TABLE_NAME)


def lambda_handler(event, context):
    """Process BDA results, split PDF, create Salesforce Case."""
    logger.info("Event: %s", json.dumps(event, default=str))

    bucket = event["bucket"]
    email_data = event["email_data"]
    email_hash = event["email_hash"]
    s3_key = event["s3_key"]
    attachments = event["attachments"]
    bda = event["bda"]
    bda_status = event.get("bda_status", {})

    # Determine documents to upload
    if bda.get("skipped"):
        # BDA was skipped — use attachments as-is
        documents = [
            {
                "s3_key": a["s3_key"],
                "classification": "Unsplit Document",
                "filename": a["filename"],
            }
            for a in attachments
        ]
    else:
        # BDA completed — parse output and split PDF
        output_prefix = bda_status.get("Payload", {}).get("output_prefix", "")
        if not output_prefix:
            # Fallback: derive from invocation ARN
            invocation_id = bda["invocation_arn"].split("/")[-1]
            output_prefix = f"bda-output//{invocation_id}"

        documents = parse_bda_output(output_prefix, bucket)

        # If BDA output parsing failed, fall back to unsplit attachments
        if not documents:
            logger.warning("BDA output parsing returned no documents. Using originals.")
            documents = [
                {
                    "s3_key": a["s3_key"],
                    "classification": "Unsplit Document",
                    "filename": a["filename"],
                }
                for a in attachments
            ]

    # Add non-PDF attachments as supporting documents
    non_pdf = [
        {
            "s3_key": a["s3_key"],
            "classification": "Supporting Document",
            "filename": a["filename"],
        }
        for a in attachments
        if a["content_type"] != "application/pdf"
    ]
    documents.extend(non_pdf)

    logger.info("Total documents for Salesforce: %d", len(documents))

    # Create Salesforce Case
    case_id = create_salesforce_case_with_documents(email_data, documents, bucket)
    logger.info("Salesforce Case created: %s", case_id)

    # Mark as processed
    mark_processed(email_hash, s3_key)

    return {
        "status": "success",
        "case_id": case_id,
        "documents_uploaded": len(documents),
    }


# ═══════════════════════════════════════════════════════════════════
# BDA Output Parsing & PDF Splitting
# ═══════════════════════════════════════════════════════════════════

def parse_bda_output(output_prefix, bucket):
    """Parse BDA output using job_metadata.json, then split PDF by pages."""
    logger.info("Parsing BDA output from prefix: %s", output_prefix)

    try:
        metadata_key = f"{output_prefix}/job_metadata.json"
        result = s3.get_object(Bucket=bucket, Key=metadata_key)
        metadata = json.loads(result["Body"].read())

        split_docs = []

        for asset in metadata.get("output_metadata", []):
            input_s3_key = asset.get("asset_input_path", {}).get("s3_key", "")

            # Download the original PDF
            original_pdf_obj = s3.get_object(Bucket=bucket, Key=input_s3_key)
            original_pdf_bytes = original_pdf_obj["Body"].read()

            for i, segment in enumerate(asset.get("segment_metadata", [])):
                custom_output_path = segment.get("custom_output_path", "")
                if not custom_output_path:
                    continue

                custom_key = custom_output_path.replace(f"s3://{bucket}/", "")
                try:
                    custom_result = s3.get_object(Bucket=bucket, Key=custom_key)
                    custom_data = json.loads(custom_result["Body"].read())

                    doc_type = custom_data.get("document_class", {}).get("type", "Unknown Document")
                    blueprint_name = custom_data.get("matched_blueprint", {}).get("name", "Unknown")
                    page_indices = custom_data.get("split_document", {}).get("page_indices", [])
                    classification = doc_type or blueprint_name

                    # Physically split the PDF
                    split_pdf_bytes = extract_pdf_pages(original_pdf_bytes, page_indices)

                    if split_pdf_bytes:
                        split_filename = f"{classification}_pages_{'_'.join(str(p + 1) for p in page_indices)}.pdf"
                        split_s3_key = f"split-docs/{output_prefix.split('/')[-1]}/{split_filename}"
                        s3.put_object(Bucket=bucket, Key=split_s3_key, Body=split_pdf_bytes)

                        split_docs.append({
                            "s3_key": split_s3_key,
                            "classification": classification,
                            "filename": split_filename,
                        })
                        logger.info("Split doc %d: type=%s, pages=%s", i, classification, page_indices)

                except Exception as e:
                    logger.error("Error processing segment %d: %s", i, str(e))

        return split_docs

    except Exception as e:
        logger.error("Error parsing BDA output: %s", str(e))
        return []


def extract_pdf_pages(pdf_bytes, page_indices):
    """Extract specific pages from a PDF and return as bytes."""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        writer = PdfWriter()

        for page_idx in page_indices:
            if page_idx < len(reader.pages):
                writer.add_page(reader.pages[page_idx])

        output = io.BytesIO()
        writer.write(output)
        return output.getvalue()

    except Exception as e:
        logger.error("PDF page extraction error: %s", str(e))
        return None


# ═══════════════════════════════════════════════════════════════════
# Salesforce Case Creation
# ═══════════════════════════════════════════════════════════════════

def create_salesforce_case_with_documents(email_data, documents, bucket):
    """Create Salesforce Case + upload documents via Composite API."""
    sf_token, sf_instance_url = get_salesforce_token()

    # Download and base64 encode all documents
    doc_payloads = []
    for doc in documents:
        obj = s3.get_object(Bucket=bucket, Key=doc["s3_key"])
        doc_payloads.append({
            "classification": doc["classification"],
            "filename": doc["filename"],
            "data_base64": base64.b64encode(obj["Body"].read()).decode("utf-8"),
        })

    # Build Composite API request
    composite_request = {"allOrNone": True, "compositeRequest": []}

    composite_request["compositeRequest"].append({
        "method": "POST",
        "url": "/services/data/v61.0/sobjects/Case",
        "referenceId": "newCase",
        "body": {
            "Subject": email_data["subject"],
            "Description": email_data["body"][:32000],
            "Origin": "Email",
            "SuppliedEmail": email_data["from"],
            "Type": "Policy Application",
            "Priority": "Normal",
        },
    })

    for i, doc in enumerate(doc_payloads):
        composite_request["compositeRequest"].append({
            "method": "POST",
            "url": "/services/data/v61.0/sobjects/ContentVersion",
            "referenceId": f"doc_{i}",
            "body": {
                "Title": doc["classification"],
                "PathOnClient": doc["filename"],
                "VersionData": doc["data_base64"],
                "ContentLocation": "S",
                "FirstPublishLocationId": "@{newCase.id}",
            },
        })

    logger.info("Calling Salesforce Composite API with %d sub-requests",
                len(composite_request["compositeRequest"]))

    response = requests.post(
        f"{sf_instance_url}/services/data/v61.0/composite",
        headers={
            "Authorization": f"Bearer {sf_token}",
            "Content-Type": "application/json",
        },
        json=composite_request,
        timeout=30,
    )

    if response.status_code != 200:
        raise Exception(
            f"Salesforce Composite API error ({response.status_code}): {response.text}"
        )

    result = response.json()
    for sub_response in result.get("compositeResponse", []):
        if sub_response["httpStatusCode"] >= 400:
            raise Exception(f"Salesforce sub-request failed: {json.dumps(sub_response)}")

    return result["compositeResponse"][0]["body"]["id"]


def get_salesforce_token():
    """Authenticate to Salesforce using JWT Bearer Flow."""
    creds = json.loads(
        secrets_client.get_secret_value(SecretId=SF_SECRET_ID)["SecretString"]
    )

    # Private key is retrieved from AWS Secrets Manager at runtime (not hardcoded)
    private_key = creds["private_key"]  # nosec — loaded from Secrets Manager

    claim = {
        "iss": creds["consumer_key"],
        "sub": creds["service_user_email"],
        "aud": SF_LOGIN_URL,
        "exp": int(time.time()) + 300,
    }

    assertion = jwt.encode(claim, private_key, algorithm="RS256")

    token_response = requests.post(
        f"{SF_LOGIN_URL}/services/oauth2/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        },
        timeout=15,
    )

    if token_response.status_code != 200:
        raise Exception(
            f"Salesforce auth failed ({token_response.status_code}): {token_response.text}"
        )

    token_data = token_response.json()
    return token_data["access_token"], token_data["instance_url"]


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
