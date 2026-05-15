"""Tests for the producer-side event simulator."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from src.producers.event_simulator import (
    KinesisEventSimulator,
    SimulatorConfig,
    build_record,
    chunk_records,
)
from src.producers.synthetic_data import GeneratorConfig, SyntheticEventGenerator


# ---------------------------------------------------------------------------
# Schema / shape tests on the generator
# ---------------------------------------------------------------------------
def test_generator_produces_known_event_types():
    gen = SyntheticEventGenerator(GeneratorConfig(seed=42))
    events = gen.generate_batch(200)
    types = {e["event_type"] for e in events}
    assert types.issubset({"page_view", "order", "signup"})
    # Required common fields
    for e in events:
        assert "event_id" in e
        assert "event_time" in e
        assert "user_id" in e
        assert e["event_type"] in {"page_view", "order", "signup"}


def test_generator_orders_have_amount_and_high_value_flag():
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=7,
            page_view_weight=0.0,
            order_weight=1.0,
            signup_weight=0.0,
        )
    )
    events = gen.generate_batch(50)
    assert events, "expected non-empty batch"
    for e in events:
        assert e["event_type"] == "order"
        assert "amount" in e and isinstance(e["amount"], float)
        assert isinstance(e["is_high_value"], bool)
        # amount must equal quantity * unit_price (rounded)
        assert round(e["quantity"] * e["unit_price"], 2) == e["amount"]


def test_generator_batch_negative_raises():
    gen = SyntheticEventGenerator(GeneratorConfig(seed=1))
    with pytest.raises(ValueError):
        gen.generate_batch(-1)


# ---------------------------------------------------------------------------
# Record building / chunking
# ---------------------------------------------------------------------------
def test_build_record_uses_user_id_as_partition_key():
    rec = build_record({"user_id": "U-123", "event_id": "E-1", "x": 1})
    assert rec["PartitionKey"] == "U-123"
    payload = json.loads(rec["Data"])
    assert payload["x"] == 1


def test_build_record_falls_back_to_event_id():
    rec = build_record({"event_id": "E-2"})
    assert rec["PartitionKey"] == "E-2"


def test_chunk_records_respects_max_records():
    records = [{"Data": b"x", "PartitionKey": "k"} for _ in range(1200)]
    chunks = list(chunk_records(records, max_records=500))
    assert [len(c) for c in chunks] == [500, 500, 200]


def test_chunk_records_respects_max_bytes():
    big = {"Data": b"x" * 1024, "PartitionKey": "k"}
    chunks = list(chunk_records([big] * 10, max_records=500, max_bytes=4096))
    # Each record is ~1025 bytes incl. partition key, so chunks of ~3-4.
    assert all(sum(len(r["Data"]) + len(r["PartitionKey"]) for r in c) <= 4096 for c in chunks)
    assert sum(len(c) for c in chunks) == 10


# ---------------------------------------------------------------------------
# Simulator with a stubbed client
# ---------------------------------------------------------------------------
class _FakeKinesis:
    """Records every PutRecords call; can be configured to partial-fail once."""

    def __init__(self, fail_first: bool = False):
        self.calls: List[Dict[str, Any]] = []
        self._fail_first = fail_first

    def put_records(self, *, StreamName: str, Records: List[Dict[str, Any]]):  # noqa: N803
        self.calls.append({"stream": StreamName, "records": list(Records)})
        if self._fail_first:
            self._fail_first = False
            # First record fails, rest succeed
            results = [{"ErrorCode": "InternalFailure"}] + [
                {"SequenceNumber": f"s-{i}", "ShardId": "shardId-0"} for i in range(1, len(Records))
            ]
            return {"FailedRecordCount": 1, "Records": results}
        return {
            "FailedRecordCount": 0,
            "Records": [
                {"SequenceNumber": f"s-{i}", "ShardId": "shardId-0"} for i in range(len(Records))
            ],
        }


def test_simulator_put_batch_happy_path():
    cfg = SimulatorConfig(stream_name="test-stream", rate=10, duration=1, batch_size=5)
    fake = _FakeKinesis()
    sim = KinesisEventSimulator(cfg, kinesis_client=fake)
    records = [build_record({"user_id": f"u-{i}", "event_id": f"e-{i}"}) for i in range(5)]

    stats = sim.put_batch(records)

    assert stats == {"sent": 5, "failed": 0}
    assert len(fake.calls) == 1
    assert fake.calls[0]["stream"] == "test-stream"


def test_simulator_put_batch_retries_partial_failures():
    cfg = SimulatorConfig(
        stream_name="test-stream", rate=10, duration=1, batch_size=5, max_retries=2
    )
    fake = _FakeKinesis(fail_first=True)
    sim = KinesisEventSimulator(cfg, kinesis_client=fake)
    records = [build_record({"user_id": f"u-{i}"}) for i in range(3)]

    stats = sim.put_batch(records)

    # 3 records: first call fails 1, retry succeeds -> 3 sent total.
    assert stats == {"sent": 3, "failed": 0}
    assert len(fake.calls) == 2  # original + retry
    assert len(fake.calls[1]["records"]) == 1  # only the failed one was retried


def test_simulator_run_bounded_duration_sends_events(monkeypatch):
    cfg = SimulatorConfig(
        stream_name="test-stream",
        rate=1000,  # fast
        duration=1,  # short
        batch_size=10,
    )
    fake = _FakeKinesis()
    sim = KinesisEventSimulator(cfg, kinesis_client=fake)

    # Avoid actual sleeping in the loop so the test stays fast.
    monkeypatch.setattr("src.producers.event_simulator.time.sleep", lambda *_: None)

    stats = sim.run()

    assert stats["sent"] > 0
    assert stats["failed"] == 0
    assert fake.calls, "expected at least one PutRecords call"


def test_simulator_config_validation():
    with pytest.raises(ValueError):
        SimulatorConfig(stream_name="x", rate=0)
    with pytest.raises(ValueError):
        SimulatorConfig(stream_name="x", rate=10, batch_size=0)
    with pytest.raises(ValueError):
        SimulatorConfig(stream_name="x", rate=10, batch_size=10, duration=0)
