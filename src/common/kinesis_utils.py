"""
Helpers for decoding Kinesis records inside Lambda handlers.

Kinesis records are base64-encoded on the Lambda event boundary; this module
keeps that decoding logic in one place so it can be tested in isolation and
reused by both the stream processor and the DLQ processor.
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class DecodedRecord:
    """A successfully decoded Kinesis record."""

    sequence_number: str
    partition_key: str
    payload: Dict[str, Any]
    approximate_arrival_timestamp: Optional[float] = None


@dataclass
class FailedRecord:
    """A record that could not be decoded (will be sent to the DLQ)."""

    sequence_number: Optional[str]
    raw: str
    reason: str


def decode_records(event: Dict[str, Any]) -> Iterable[Any]:
    """Yield :class:`DecodedRecord` or :class:`FailedRecord` for each entry."""
    records: List[Dict[str, Any]] = event.get("Records", []) or []
    for rec in records:
        kinesis = rec.get("kinesis", {}) or {}
        seq = kinesis.get("sequenceNumber")
        b64_data = kinesis.get("data")
        if not b64_data:
            yield FailedRecord(seq, "", "missing kinesis.data")
            continue
        try:
            raw_bytes = base64.b64decode(b64_data)
        except (ValueError, TypeError) as exc:
            yield FailedRecord(seq, str(b64_data), f"base64 error: {exc}")
            continue
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            yield FailedRecord(
                seq,
                raw_bytes[:1024].decode("utf-8", errors="replace"),
                f"json error: {exc}",
            )
            continue
        if not isinstance(payload, dict):
            yield FailedRecord(
                seq,
                str(payload)[:1024],
                "payload is not a JSON object",
            )
            continue
        yield DecodedRecord(
            sequence_number=seq or "",
            partition_key=kinesis.get("partitionKey", ""),
            payload=payload,
            approximate_arrival_timestamp=kinesis.get("approximateArrivalTimestamp"),
        )
