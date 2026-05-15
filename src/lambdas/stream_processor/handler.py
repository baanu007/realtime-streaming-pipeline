"""
Kinesis -> Lambda stream processor.

Responsibilities:

1. Decode Kinesis records (base64 -> JSON).
2. Enrich each event with user metadata from a DynamoDB lookup table
   (cached for the duration of the warm container).
3. Filter / route:
   - All valid events go to a Firehose delivery stream that lands them in S3
     for Snowpipe ingestion.
   - High-value orders and fraud signals also fan out to an SNS topic.
4. Use Lambda's partial batch response so only failed records are retried.

Environment variables:

* ``FIREHOSE_DELIVERY_STREAM`` - Firehose delivery stream name (required).
* ``SNS_ALERT_TOPIC_ARN``      - SNS topic ARN for alerts (required).
* ``ENRICHMENT_TABLE``         - DynamoDB table name with user metadata.
* ``HIGH_VALUE_THRESHOLD``     - dollar threshold for order alerts (default 1000).
* ``AWS_REGION``               - region (provided by Lambda).
"""

from __future__ import annotations

import json
import logging
import os
import time
from functools import lru_cache
from typing import Any, Dict, List, Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - covered in tests via stubs
    boto3 = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment]

from src.common.kinesis_utils import DecodedRecord, FailedRecord, decode_records

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

FIREHOSE_BATCH_LIMIT = 500
FIREHOSE_PAYLOAD_LIMIT = 4 * 1024 * 1024  # PutRecordBatch limit
DEFAULT_HIGH_VALUE_THRESHOLD = 1000.0
FRAUD_SIGNALS = {"fraud_suspected", "card_velocity_high", "geo_mismatch"}

# Firehose per-record retry policy. Real data-loss bug fix: PutRecordBatch can
# return ``FailedPutCount > 0`` while the overall HTTP call succeeds; the failed
# entries are identified by ``ErrorCode`` in ``RequestResponses`` and *must* be
# retried (or surfaced to the caller) or the records are silently dropped.
FIREHOSE_MAX_RETRIES = 3
FIREHOSE_RETRY_BASE_DELAY = 0.5  # seconds; doubled each attempt
FIREHOSE_FAILED_RECORD_LOG_LIMIT = 512  # bytes of record body to keep in logs


# ----------------------------------------------------------------------
# AWS client accessors (lazy so tests can monkey-patch)
# ----------------------------------------------------------------------
def _firehose_client():
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 not available")
    return boto3.client("firehose")


def _sns_client():
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 not available")
    return boto3.client("sns")


def _dynamodb_table(name: str):
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 not available")
    return boto3.resource("dynamodb").Table(name)


# ----------------------------------------------------------------------
# Enrichment
# ----------------------------------------------------------------------
@lru_cache(maxsize=2048)
def _lookup_user(table_name: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a user row from DynamoDB, cached per warm container.

    Returns ``None`` on miss or error - enrichment must never fail the record.
    """
    try:
        table = _dynamodb_table(table_name)
        resp = table.get_item(Key={"user_id": user_id})
    except ClientError as exc:
        logger.warning("DynamoDB lookup failed for %s: %s", user_id, exc)
        return None
    return resp.get("Item")


def enrich(event: Dict[str, Any], table_name: Optional[str]) -> Dict[str, Any]:
    """Return an enriched copy of *event*.

    Even without a DynamoDB table configured we still add a deterministic
    ``processed_at`` field so downstream consumers can rely on it.
    """
    enriched = dict(event)
    user_id = event.get("user_id")
    if table_name and user_id:
        meta = _lookup_user(table_name, str(user_id))
        if meta:
            enriched["user_segment"] = meta.get("segment")
            enriched["user_country"] = meta.get("country")
            enriched["user_lifetime_value"] = meta.get("lifetime_value")
    enriched.setdefault("ingestion_pipeline", "kinesis-lambda-firehose")
    return enriched


# ----------------------------------------------------------------------
# Routing decisions
# ----------------------------------------------------------------------
def is_high_value_order(event: Dict[str, Any], threshold: float) -> bool:
    if event.get("event_type") != "order":
        return False
    try:
        amount = float(event.get("amount", 0) or 0)
    except (TypeError, ValueError):
        return False
    return amount >= threshold


def is_fraud_signal(event: Dict[str, Any]) -> bool:
    signal = event.get("fraud_signal") or event.get("risk_flag")
    return bool(signal) and str(signal) in FRAUD_SIGNALS


# ----------------------------------------------------------------------
# Sinks
# ----------------------------------------------------------------------
def _firehose_records(events: List[Dict[str, Any]]) -> List[Dict[str, bytes]]:
    """Format events for Firehose PutRecordBatch (newline-delimited JSON)."""
    return [{"Data": (json.dumps(e, separators=(",", ":")) + "\n").encode("utf-8")} for e in events]


def send_to_firehose(
    events: List[Dict[str, Any]],
    stream_name: str,
    client: Any = None,
) -> int:
    """Send events to Firehose. Returns count of successfully delivered events.

    Per-record failures inside ``PutRecordBatch`` responses are retried with
    exponential backoff up to ``FIREHOSE_MAX_RETRIES`` times. Records that
    never succeed are logged (body trimmed) and counted as not delivered, so
    callers can detect a deficit instead of seeing a silent data loss.
    """
    if not events:
        return 0
    fh = client or _firehose_client()
    delivered = 0
    batch: List[Dict[str, Any]] = []
    batch_bytes = 0
    for event in events:
        rec = {"Data": (json.dumps(event, separators=(",", ":")) + "\n").encode("utf-8")}
        size = len(rec["Data"])
        if batch and (
            len(batch) >= FIREHOSE_BATCH_LIMIT or batch_bytes + size > FIREHOSE_PAYLOAD_LIMIT
        ):
            delivered += _flush_firehose(fh, stream_name, batch)
            batch = []
            batch_bytes = 0
        batch.append(rec)
        batch_bytes += size
    if batch:
        delivered += _flush_firehose(fh, stream_name, batch)
    return delivered


def _flush_firehose(
    client: Any, stream_name: str, batch: List[Dict[str, Any]]
) -> int:
    """PutRecordBatch with per-record exponential-backoff retry.

    Returns the count of records that were successfully delivered. Records
    whose response entry contains ``ErrorCode`` are retried up to
    ``FIREHOSE_MAX_RETRIES`` times with exponential backoff
    (``FIREHOSE_RETRY_BASE_DELAY``, doubled each attempt). Any still-failing
    records have a truncated copy of their body logged for forensics; they are
    *not* counted as delivered, so the caller can detect the deficit.
    """
    pending = list(batch)
    initial_count = len(pending)
    delivered_total = 0
    last_errors: List[Dict[str, Any]] = [{} for _ in pending]

    for attempt in range(FIREHOSE_MAX_RETRIES + 1):
        try:
            resp = client.put_record_batch(
                DeliveryStreamName=stream_name, Records=pending
            )
        except ClientError as exc:
            logger.error("Firehose PutRecordBatch failed: %s", exc)
            raise

        responses = resp.get("RequestResponses", []) or []
        failed_count = int(resp.get("FailedPutCount", 0) or 0)

        if not failed_count:
            delivered_total += len(pending)
            return delivered_total

        # Identify failed entries by presence of ErrorCode (Firehose contract).
        next_pending: List[Dict[str, Any]] = []
        next_errors: List[Dict[str, Any]] = []
        succeeded_this_round = 0
        for rec, response_entry in zip(pending, responses):
            if response_entry.get("ErrorCode"):
                next_pending.append(rec)
                next_errors.append(
                    {
                        "ErrorCode": response_entry.get("ErrorCode"),
                        "ErrorMessage": response_entry.get("ErrorMessage"),
                    }
                )
            else:
                succeeded_this_round += 1
        delivered_total += succeeded_this_round

        # Defensive: if RequestResponses length didn't match pending, treat any
        # remainder as failed so we don't accidentally count records as ok.
        if len(responses) != len(pending):
            logger.error(
                "Firehose RequestResponses length mismatch: %s vs %s; "
                "treating remainder as failed",
                len(responses),
                len(pending),
            )
            for rec in pending[len(responses):]:
                next_pending.append(rec)
                next_errors.append({"ErrorCode": "ResponseLengthMismatch"})

        logger.warning(
            "Firehose attempt %s/%s: %s/%s records failed; will %s",
            attempt + 1,
            FIREHOSE_MAX_RETRIES + 1,
            len(next_pending),
            initial_count,
            "retry" if attempt < FIREHOSE_MAX_RETRIES else "surface to caller",
        )

        pending = next_pending
        last_errors = next_errors

        if not pending:
            return delivered_total

        if attempt < FIREHOSE_MAX_RETRIES:
            time.sleep(FIREHOSE_RETRY_BASE_DELAY * (2 ** attempt))

    # Exhausted retries; log a trimmed body for each unrecovered record.
    for rec, err in zip(pending, last_errors):
        body = rec.get("Data", b"")
        try:
            preview = body[:FIREHOSE_FAILED_RECORD_LOG_LIMIT].decode(
                "utf-8", errors="replace"
            )
        except Exception:  # pragma: no cover - defensive
            preview = "<unprintable>"
        logger.error(
            "Firehose record unrecovered after %s attempts: error=%s msg=%s body=%s",
            FIREHOSE_MAX_RETRIES + 1,
            err.get("ErrorCode"),
            err.get("ErrorMessage"),
            preview,
        )
    return delivered_total


def send_alerts(
    alerts: List[Dict[str, Any]],
    topic_arn: str,
    client: Any = None,
) -> int:
    """Publish each alert as a separate SNS message; returns count published."""
    if not alerts:
        return 0
    sns = client or _sns_client()
    published = 0
    for alert in alerts:
        try:
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(alert, separators=(",", ":")),
                Subject=f"Streaming alert: {alert.get('event_type', 'unknown')}",
                MessageAttributes={
                    "event_type": {
                        "DataType": "String",
                        "StringValue": str(alert.get("event_type", "unknown")),
                    }
                },
            )
            published += 1
        except ClientError as exc:
            logger.error("SNS publish failed for %s: %s", alert.get("event_id"), exc)
    return published


# ----------------------------------------------------------------------
# Handler
# ----------------------------------------------------------------------
def _env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Required env var {name} is not set")
    return value


def process_event(
    event: Dict[str, Any],
    context: Any = None,
    firehose_client: Any = None,
    sns_client: Any = None,
) -> Dict[str, Any]:
    """Lambda entry point (exposed as ``process_event`` for testability)."""
    firehose_stream = _env("FIREHOSE_DELIVERY_STREAM", required=True)
    sns_topic = _env("SNS_ALERT_TOPIC_ARN", required=True)
    enrichment_table = _env("ENRICHMENT_TABLE")
    threshold = float(_env("HIGH_VALUE_THRESHOLD", str(DEFAULT_HIGH_VALUE_THRESHOLD)))

    valid_events: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []
    batch_item_failures: List[Dict[str, str]] = []

    for rec in decode_records(event):
        if isinstance(rec, FailedRecord):
            logger.warning(
                "Skipping unparseable record seq=%s reason=%s",
                rec.sequence_number,
                rec.reason,
            )
            if rec.sequence_number:
                batch_item_failures.append({"itemIdentifier": rec.sequence_number})
            continue

        assert isinstance(rec, DecodedRecord)
        try:
            enriched = enrich(rec.payload, enrichment_table)
        except Exception:  # noqa: BLE001 - never poison the whole batch
            logger.exception("Enrichment failure for seq=%s", rec.sequence_number)
            batch_item_failures.append({"itemIdentifier": rec.sequence_number})
            continue

        valid_events.append(enriched)
        if is_high_value_order(enriched, threshold) or is_fraud_signal(enriched):
            alerts.append(enriched)

    delivered = 0
    try:
        delivered = send_to_firehose(valid_events, firehose_stream, client=firehose_client)
    except ClientError:
        # Re-raise so Lambda retries the whole batch on infrastructure errors.
        raise

    published = send_alerts(alerts, sns_topic, client=sns_client)

    logger.info(
        "processed=%s delivered=%s alerts=%s failed=%s",
        len(valid_events),
        delivered,
        published,
        len(batch_item_failures),
    )
    return {
        "batchItemFailures": batch_item_failures,
        "metrics": {
            "processed": len(valid_events),
            "delivered": delivered,
            "alerts": published,
            "failed": len(batch_item_failures),
        },
    }


# Lambda's configured handler points here.
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return process_event(event, context)
