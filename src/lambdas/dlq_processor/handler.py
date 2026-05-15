"""
DLQ processor Lambda.

Triggered by the Lambda Dead Letter Queue (SQS). Each invocation:

1. Reads failed records from the SQS event.
2. Persists the raw payload + failure metadata to an S3 "dead letter" bucket
   partitioned by ``dt=YYYY-MM-DD/hh=HH``.
3. Emits a CloudWatch log line per record so on-call can grep quickly.

Environment variables:

* ``DLQ_S3_BUCKET`` - destination bucket (required).
* ``DLQ_S3_PREFIX`` - key prefix (default ``streaming-dlq``).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment]

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def _s3_client():
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 not available")
    return boto3.client("s3")


def _partition_prefix(prefix: str, now: datetime) -> str:
    return f"{prefix.rstrip('/')}/" f"dt={now:%Y-%m-%d}/hh={now:%H}/"


def _build_envelope(record: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap an SQS record with normalized failure metadata."""
    body = record.get("body", "")
    try:
        parsed_body: Any = json.loads(body)
    except (TypeError, ValueError):
        parsed_body = body

    attributes = record.get("attributes", {}) or {}
    return {
        "dlq_record_id": str(uuid.uuid4()),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "source_message_id": record.get("messageId"),
        "approximate_receive_count": attributes.get("ApproximateReceiveCount"),
        "sent_timestamp": attributes.get("SentTimestamp"),
        "event_source_arn": record.get("eventSourceARN"),
        "body": parsed_body,
        "message_attributes": record.get("messageAttributes", {}),
    }


def process_event(
    event: Dict[str, Any],
    context: Any = None,
    s3_client: Any = None,
) -> Dict[str, Any]:
    bucket = os.getenv("DLQ_S3_BUCKET")
    if not bucket:
        raise RuntimeError("DLQ_S3_BUCKET env var is required")
    prefix = os.getenv("DLQ_S3_PREFIX", "streaming-dlq")
    s3 = s3_client or _s3_client()

    records: List[Dict[str, Any]] = event.get("Records", []) or []
    if not records:
        return {"archived": 0}

    now = datetime.now(timezone.utc)
    partition = _partition_prefix(prefix, now)
    archived = 0
    failures: List[str] = []

    for rec in records:
        envelope = _build_envelope(rec)
        key = f"{partition}{envelope['dlq_record_id']}.json"
        body = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
        except ClientError as exc:
            logger.error("Failed to archive DLQ record to s3://%s/%s: %s", bucket, key, exc)
            failures.append(rec.get("messageId", "unknown"))
            continue
        archived += 1
        logger.info(
            "Archived DLQ record id=%s source_msg=%s key=s3://%s/%s",
            envelope["dlq_record_id"],
            envelope["source_message_id"],
            bucket,
            key,
        )

    return {"archived": archived, "failed": failures}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return process_event(event, context)
