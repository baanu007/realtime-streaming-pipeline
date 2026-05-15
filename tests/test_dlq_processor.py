"""Tests for the DLQ processor Lambda."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from src.lambdas.dlq_processor import handler as dlq_handler


class _FakeS3:
    def __init__(self):
        self.objects: List[Dict[str, Any]] = []

    def put_object(self, **kwargs):
        self.objects.append(kwargs)
        return {"ETag": "fake"}


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DLQ_S3_BUCKET", "dlq-bucket")
    monkeypatch.setenv("DLQ_S3_PREFIX", "streaming-dlq")


def _sqs_event(bodies: List[Any]) -> Dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": f"msg-{i}",
                "body": body if isinstance(body, str) else json.dumps(body),
                "attributes": {
                    "ApproximateReceiveCount": "3",
                    "SentTimestamp": "1700000000000",
                },
                "messageAttributes": {},
                "eventSourceARN": "arn:aws:sqs:us-east-1:0:dlq",
            }
            for i, body in enumerate(bodies)
        ]
    }


def test_dlq_archives_each_record_to_partitioned_s3_key(env):
    s3 = _FakeS3()
    event = _sqs_event(
        [
            {"event_id": "1", "broken": True},
            "non-json body",
        ]
    )

    result = dlq_handler.process_event(event, s3_client=s3)

    assert result["archived"] == 2
    assert len(s3.objects) == 2
    for obj in s3.objects:
        assert obj["Bucket"] == "dlq-bucket"
        assert obj["Key"].startswith("streaming-dlq/dt=")
        assert "/hh=" in obj["Key"]
        envelope = json.loads(obj["Body"])
        assert "dlq_record_id" in envelope
        assert envelope["approximate_receive_count"] == "3"


def test_dlq_no_records_returns_zero(env):
    result = dlq_handler.process_event({"Records": []}, s3_client=_FakeS3())
    assert result == {"archived": 0}


def test_dlq_requires_bucket_env(monkeypatch):
    monkeypatch.delenv("DLQ_S3_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="DLQ_S3_BUCKET"):
        dlq_handler.process_event(_sqs_event([{"a": 1}]), s3_client=_FakeS3())
