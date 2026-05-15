"""Shared pytest fixtures."""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Make the project root importable as a package root (`src.*`).
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch):
    """Default-safe AWS env so boto3 stubs/moto are happy and we never hit real AWS."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    yield


def _encode(payload: Dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode(
        "ascii"
    )


@pytest.fixture
def kinesis_event_factory():
    """Build a Kinesis Lambda event from a list of payload dicts."""

    def _make(payloads: List[Dict[str, Any]], partition_key: str = "u-1") -> Dict[str, Any]:
        return {
            "Records": [
                {
                    "kinesis": {
                        "sequenceNumber": f"seq-{i}",
                        "partitionKey": partition_key,
                        "data": _encode(p),
                        "approximateArrivalTimestamp": 1_700_000_000 + i,
                    },
                    "eventSource": "aws:kinesis",
                }
                for i, p in enumerate(payloads)
            ]
        }

    return _make
