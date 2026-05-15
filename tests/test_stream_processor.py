"""Tests for the Kinesis stream processor Lambda handler."""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, List

import pytest

from src.lambdas.stream_processor import handler as stream_handler


# ---------------------------------------------------------------------------
# Fakes for Firehose and SNS
# ---------------------------------------------------------------------------
class _FakeFirehose:
    def __init__(self, failed_count: int = 0):
        self.calls: List[Dict[str, Any]] = []
        self._failed_count = failed_count

    def put_record_batch(self, *, DeliveryStreamName, Records):  # noqa: N803
        self.calls.append({"stream": DeliveryStreamName, "records": list(Records)})
        return {
            "FailedPutCount": self._failed_count,
            "RequestResponses": [{"RecordId": f"r-{i}"} for i in range(len(Records))],
        }


class _FakeSNS:
    def __init__(self):
        self.published: List[Dict[str, Any]] = []

    def publish(self, **kwargs):
        self.published.append(kwargs)
        return {"MessageId": f"m-{len(self.published)}"}


# ---------------------------------------------------------------------------
# Routing helpers
# ---------------------------------------------------------------------------
def test_is_high_value_order_true_above_threshold():
    e = {"event_type": "order", "amount": 1500.0}
    assert stream_handler.is_high_value_order(e, threshold=1000.0) is True


def test_is_high_value_order_false_below_threshold():
    assert (
        stream_handler.is_high_value_order({"event_type": "order", "amount": 50}, threshold=1000)
        is False
    )


def test_is_high_value_order_ignores_non_orders():
    assert (
        stream_handler.is_high_value_order(
            {"event_type": "page_view", "amount": 99999}, threshold=1000
        )
        is False
    )


def test_is_fraud_signal_recognizes_known_flags():
    assert stream_handler.is_fraud_signal({"fraud_signal": "fraud_suspected"}) is True
    assert stream_handler.is_fraud_signal({"risk_flag": "geo_mismatch"}) is True


def test_is_fraud_signal_ignores_unknown_values():
    assert stream_handler.is_fraud_signal({"fraud_signal": "definitely_fine"}) is False
    assert stream_handler.is_fraud_signal({}) is False


# ---------------------------------------------------------------------------
# End-to-end handler behaviour
# ---------------------------------------------------------------------------
@pytest.fixture
def env_setup(monkeypatch):
    monkeypatch.setenv("FIREHOSE_DELIVERY_STREAM", "test-firehose")
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:alerts")
    monkeypatch.delenv("ENRICHMENT_TABLE", raising=False)
    monkeypatch.setenv("HIGH_VALUE_THRESHOLD", "500")
    # bust the lru_cache so user lookups don't leak between tests
    stream_handler._lookup_user.cache_clear()


def test_handler_routes_orders_and_alerts(env_setup, kinesis_event_factory):
    payloads = [
        {"event_id": "1", "event_type": "page_view", "user_id": "u1", "page": "/"},
        {"event_id": "2", "event_type": "order", "user_id": "u2", "amount": 1500},
        {"event_id": "3", "event_type": "order", "user_id": "u3", "amount": 10},
        {
            "event_id": "4",
            "event_type": "page_view",
            "user_id": "u4",
            "fraud_signal": "geo_mismatch",
        },
    ]
    event = kinesis_event_factory(payloads)
    firehose = _FakeFirehose()
    sns = _FakeSNS()

    result = stream_handler.process_event(
        event, context=None, firehose_client=firehose, sns_client=sns
    )

    # All 4 records should be delivered to Firehose.
    assert result["metrics"]["delivered"] == 4
    assert result["metrics"]["failed"] == 0
    assert result["batchItemFailures"] == []

    # Two alerts: the high-value order and the fraud-flagged page view.
    assert result["metrics"]["alerts"] == 2
    alert_event_ids = {json.loads(call["Message"])["event_id"] for call in sns.published}
    assert alert_event_ids == {"2", "4"}


def test_handler_records_partial_decode_failures(env_setup):
    bad_b64 = base64.b64encode(b"not-json").decode("ascii")
    event = {
        "Records": [
            {
                "kinesis": {
                    "sequenceNumber": "seq-bad",
                    "partitionKey": "p",
                    "data": bad_b64,
                }
            },
            {
                "kinesis": {
                    "sequenceNumber": "seq-good",
                    "partitionKey": "p",
                    "data": base64.b64encode(
                        json.dumps({"event_id": "g", "event_type": "page_view"}).encode()
                    ).decode("ascii"),
                }
            },
        ]
    }
    firehose = _FakeFirehose()
    sns = _FakeSNS()

    result = stream_handler.process_event(event, firehose_client=firehose, sns_client=sns)

    assert result["metrics"]["delivered"] == 1
    assert result["batchItemFailures"] == [{"itemIdentifier": "seq-bad"}]


def test_handler_enriches_when_dynamodb_table_set(env_setup, monkeypatch, kinesis_event_factory):
    monkeypatch.setenv("ENRICHMENT_TABLE", "users")

    # Patch the DynamoDB lookup at the function boundary - keeps the test free
    # of moto/boto wiring while still exercising the real enrichment path.
    def _fake_lookup(table_name, user_id):
        assert table_name == "users"
        return {"segment": "vip", "country": "US", "lifetime_value": 9999}

    monkeypatch.setattr(stream_handler, "_lookup_user", _fake_lookup)

    event = kinesis_event_factory(
        [{"event_id": "1", "event_type": "page_view", "user_id": "u1", "page": "/"}]
    )
    firehose = _FakeFirehose()
    sns = _FakeSNS()

    stream_handler.process_event(event, firehose_client=firehose, sns_client=sns)

    sent = json.loads(firehose.calls[0]["records"][0]["Data"].decode("utf-8").rstrip("\n"))
    assert sent["user_segment"] == "vip"
    assert sent["user_country"] == "US"
    assert sent["user_lifetime_value"] == 9999
    assert sent["ingestion_pipeline"] == "kinesis-lambda-firehose"


def test_handler_requires_required_env(monkeypatch, kinesis_event_factory):
    monkeypatch.delenv("FIREHOSE_DELIVERY_STREAM", raising=False)
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:0:alerts")
    event = kinesis_event_factory([{"event_id": "1", "event_type": "page_view"}])
    with pytest.raises(RuntimeError, match="FIREHOSE_DELIVERY_STREAM"):
        stream_handler.process_event(event, firehose_client=_FakeFirehose(), sns_client=_FakeSNS())


def test_handler_handles_empty_batch(env_setup):
    firehose = _FakeFirehose()
    sns = _FakeSNS()
    result = stream_handler.process_event({"Records": []}, firehose_client=firehose, sns_client=sns)
    assert result["metrics"] == {"processed": 0, "delivered": 0, "alerts": 0, "failed": 0}
    assert firehose.calls == []
    assert sns.published == []
