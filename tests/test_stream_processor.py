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
        responses = []
        for i in range(len(Records)):
            if i < self._failed_count:
                responses.append(
                    {
                        "ErrorCode": "ServiceUnavailableException",
                        "ErrorMessage": "throttled",
                    }
                )
            else:
                responses.append({"RecordId": f"r-{i}"})
        return {
            "FailedPutCount": self._failed_count,
            "RequestResponses": responses,
        }


class _ScriptedFirehose:
    """Firehose fake whose responses follow a scripted sequence.

    Each script entry is a list with one boolean per record in the call:
    ``True`` = succeed, ``False`` = fail with a retryable ErrorCode. The fake
    only sees the *currently pending* records on each retry attempt, so the
    script reflects what Firehose would return on each successive call.
    """

    def __init__(self, script):
        self.script = list(script)
        self.calls: List[Dict[str, Any]] = []

    def put_record_batch(self, *, DeliveryStreamName, Records):  # noqa: N803
        self.calls.append({"stream": DeliveryStreamName, "records": list(Records)})
        if not self.script:
            raise AssertionError("_ScriptedFirehose script exhausted")
        plan = self.script.pop(0)
        assert len(plan) == len(Records), (
            f"script entry has {len(plan)} outcomes but call had {len(Records)} records"
        )
        responses = []
        failed = 0
        for i, ok in enumerate(plan):
            if ok:
                responses.append({"RecordId": f"r-{i}"})
            else:
                failed += 1
                responses.append(
                    {
                        "ErrorCode": "ServiceUnavailableException",
                        "ErrorMessage": "throttled",
                    }
                )
        return {"FailedPutCount": failed, "RequestResponses": responses}


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


# ---------------------------------------------------------------------------
# Firehose retry behaviour (real data-loss bug fix)
# ---------------------------------------------------------------------------
def test_send_to_firehose_retries_failed_records_no_silent_drop(monkeypatch):
    """Firehose returns FailedPutCount=2 then everything succeeds on retry.

    The function must retry the failed records (not silently drop them) and
    report ``delivered == len(events)``.
    """
    # Speed up the test - no real sleeping.
    monkeypatch.setattr(stream_handler.time, "sleep", lambda *_a, **_k: None)

    events = [{"event_id": f"e{i}", "event_type": "page_view"} for i in range(5)]
    # First call: records 0 and 1 fail, 2-4 succeed.
    # Second call (retry of just records 0 and 1): both succeed.
    firehose = _ScriptedFirehose(
        [
            [False, False, True, True, True],
            [True, True],
        ]
    )

    delivered = stream_handler.send_to_firehose(
        events, "test-firehose", client=firehose
    )

    assert delivered == 5, "all records must be delivered after retry"
    assert len(firehose.calls) == 2, "retry should have produced a second call"
    # First call had all 5; second call should only contain the 2 that failed.
    assert len(firehose.calls[0]["records"]) == 5
    assert len(firehose.calls[1]["records"]) == 2


def test_send_to_firehose_exhausts_retries_and_reports_deficit(monkeypatch):
    """After exhausting retries, unrecovered records must not be counted as delivered."""
    monkeypatch.setattr(stream_handler.time, "sleep", lambda *_a, **_k: None)

    events = [{"event_id": "a"}, {"event_id": "b"}]
    # Every attempt fails record 0; record 1 fails first call then succeeds.
    firehose = _ScriptedFirehose(
        [
            [False, False],  # initial call
            [False, True],   # retry 1 (both still pending)
            [False],         # retry 2 (only record 0 still pending)
            [False],         # retry 3 (final)
        ]
    )

    delivered = stream_handler.send_to_firehose(
        events, "test-firehose", client=firehose
    )

    # 1 record succeeded; 1 unrecovered should NOT be counted as delivered.
    assert delivered == 1
    # 1 initial + 3 retries.
    assert len(firehose.calls) == 1 + stream_handler.FIREHOSE_MAX_RETRIES
