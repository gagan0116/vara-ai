import json
import base64
import os
from datetime import datetime, timezone
from typing import Dict, Any

from google.cloud import storage
from google.cloud import tasks_v2

BUCKET_NAME = "refunds_bucket"

# Cloud Tasks Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "vara-483300")
LOCATION = os.getenv("CLOUD_RUN_REGION", "northamerica-northeast1")
QUEUE_NAME = os.getenv("MCP_QUEUE_NAME", "mcp-processing-queue")
# We expect this env var to be set in Cloud Run. 
# Defaulting to a likely URL pattern, but the hash is unknown until deployment.
MCP_PROCESSOR_URL = os.getenv("MCP_PROCESSOR_URL") 
SERVICE_ACCOUNT_EMAIL = "mcp-runtime@vara-483300.iam.gserviceaccount.com"

def _serialize_for_json(obj: Any):
    """
    JSON serializer that converts raw bytes to base64.
    """
    if isinstance(obj, (bytes, bytearray)):
        return {
            "__type__": "bytes",
            "encoding": "base64",
            "data": base64.b64encode(obj).decode("ascii"),
        }

    raise TypeError(f"Type {type(obj)} is not JSON serializable")


def _normalize_timestamp(received_at) -> datetime:
    """
    Accepts datetime | ISO string | epoch
    Returns UTC datetime
    """
    if isinstance(received_at, datetime):
        return received_at.astimezone(timezone.utc)

    if isinstance(received_at, str):
        return datetime.fromisoformat(received_at).astimezone(timezone.utc)

    if isinstance(received_at, (int, float)):
        return datetime.fromtimestamp(received_at, tz=timezone.utc)

    # Fallback
    return datetime.now(timezone.utc)


def _enqueue_mcp_task(bucket_name: str, blob_path: str):
    """Enqueue a Cloud Task to process this email via MCP."""
    if not MCP_PROCESSOR_URL:
        print("⚠️ MCP_PROCESSOR_URL not set. Skipping Cloud Task enqueue.")
        return

    try:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
        
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": MCP_PROCESSOR_URL,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "bucket": bucket_name,
                    "blob_path": blob_path
                }).encode(),
                "oidc_token": {
                    "service_account_email": SERVICE_ACCOUNT_EMAIL
                }
            }
        }
        
        response = client.create_task(request={"parent": parent, "task": task})
        print(f"📬 Enqueued task {response.name}")
        
    except Exception as e:
        print(f"❌ Failed to enqueue task: {e}")


def store_email_result(result: Dict):
    """
    Stores processed email result in Cloud Storage and triggers MCP processing.
    """

    user_id = result.get("user_id")
    received_at = result.get("received_at")

    if not user_id:
        raise ValueError("user_id is required to store email")

    timestamp = _normalize_timestamp(received_at)

    ts = timestamp.strftime("%Y%m%dT%H%M%SZ")
    safe_user = user_id.replace("@", "_at_").replace(".", "_")

    blob_path = f"{safe_user}/{safe_user}_{ts}.json"

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        json.dumps(
            result,
            indent=2,
            default=_serialize_for_json,
        ),
        content_type="application/json",
    )

    print(f"📦 Stored email in gs://{BUCKET_NAME}/{blob_path}")
    
    # Enqueue for MCP processing
    _enqueue_mcp_task(BUCKET_NAME, blob_path)
