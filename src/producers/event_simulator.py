"""
Kinesis event simulator.

Streams synthetic events into an Amazon Kinesis Data Stream at a configurable
rate using ``PutRecords`` for batching. Partition keys are derived from
``user_id`` so events for the same user land on the same shard and preserve
ordering.

Usage::

    python -m src.producers.event_simulator \\
        --stream-name events-stream \\
        --rate 200 \\
        --duration 60

The script is intentionally defensive: PutRecords can partially fail, so
failed records are retried with exponential backoff up to a small number of
attempts before being logged and dropped.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

try:  # boto3 is only required at runtime, not for import (tests can stub it).
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - exercised only when boto3 missing
    boto3 = None  # type: ignore[assignment]
    BotoConfig = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment]

from .synthetic_data import GeneratorConfig, SyntheticEventGenerator

logger = logging.getLogger(__name__)

# Kinesis PutRecords hard limit is 500 records / 5 MiB per call.
MAX_RECORDS_PER_PUT = 500
MAX_PAYLOAD_BYTES = 5 * 1024 * 1024
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_RETRIES = 3


@dataclass
class SimulatorConfig:
    """Runtime configuration for the simulator."""

    stream_name: str
    rate: int = 100  # events / second
    duration: Optional[int] = None  # seconds; None means run forever
    batch_size: int = DEFAULT_BATCH_SIZE
    region: Optional[str] = None
    max_retries: int = DEFAULT_MAX_RETRIES

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate must be > 0")
        if self.batch_size <= 0 or self.batch_size > MAX_RECORDS_PER_PUT:
            raise ValueError(f"batch_size must be in (0, {MAX_RECORDS_PER_PUT}]")
        if self.duration is not None and self.duration <= 0:
            raise ValueError("duration must be > 0 or None")


def build_record(event: Dict[str, Any]) -> Dict[str, Any]:
    """Format an event dict as a Kinesis PutRecords entry.

    ``user_id`` is used as partition key so a user's events go to the same
    shard. If user_id is missing, we fall back to event_id so the call still
    succeeds.
    """
    partition_key = str(event.get("user_id") or event.get("event_id") or "unknown")
    return {
        "Data": json.dumps(event, separators=(",", ":")).encode("utf-8"),
        "PartitionKey": partition_key,
    }


def chunk_records(
    records: List[Dict[str, Any]],
    max_records: int = MAX_RECORDS_PER_PUT,
    max_bytes: int = MAX_PAYLOAD_BYTES,
) -> Iterable[List[Dict[str, Any]]]:
    """Yield record chunks that satisfy Kinesis PutRecords limits."""
    chunk: List[Dict[str, Any]] = []
    chunk_bytes = 0
    for r in records:
        rec_bytes = len(r["Data"]) + len(r["PartitionKey"].encode("utf-8"))
        if chunk and (len(chunk) >= max_records or chunk_bytes + rec_bytes > max_bytes):
            yield chunk
            chunk = []
            chunk_bytes = 0
        chunk.append(r)
        chunk_bytes += rec_bytes
    if chunk:
        yield chunk


class KinesisEventSimulator:
    """Drives the generator and ships events to Kinesis."""

    def __init__(
        self,
        config: SimulatorConfig,
        generator: Optional[SyntheticEventGenerator] = None,
        kinesis_client: Any = None,
    ):
        self.config = config
        self.generator = generator or SyntheticEventGenerator(GeneratorConfig())
        if kinesis_client is not None:
            self.client = kinesis_client
        else:
            if boto3 is None:  # pragma: no cover
                raise RuntimeError(
                    "boto3 is required to talk to Kinesis. Install requirements.txt."
                )
            boto_cfg = BotoConfig(retries={"max_attempts": 5, "mode": "standard"})
            self.client = boto3.client(
                "kinesis",
                region_name=config.region or os.getenv("AWS_REGION", "us-east-1"),
                config=boto_cfg,
            )

    # ------------------------------------------------------------------
    def put_batch(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Send a batch of records, retrying on partial failures.

        Returns a small stats dict: ``{"sent": int, "failed": int}``.
        """
        sent = 0
        failed = 0
        for sub_batch in chunk_records(records):
            pending = sub_batch
            for attempt in range(self.config.max_retries + 1):
                try:
                    resp = self.client.put_records(
                        StreamName=self.config.stream_name,
                        Records=pending,
                    )
                except ClientError as exc:  # network / throttle
                    logger.warning(
                        "PutRecords ClientError (attempt %s/%s): %s",
                        attempt + 1,
                        self.config.max_retries + 1,
                        exc,
                    )
                    if attempt == self.config.max_retries:
                        failed += len(pending)
                        break
                    time.sleep(min(2**attempt, 10))
                    continue

                failed_count = int(resp.get("FailedRecordCount", 0) or 0)
                if failed_count == 0:
                    sent += len(pending)
                    pending = []
                    break

                # Retry only the records that failed.
                next_pending: List[Dict[str, Any]] = []
                for rec, result in zip(pending, resp.get("Records", [])):
                    if result.get("ErrorCode"):
                        next_pending.append(rec)
                    else:
                        sent += 1
                pending = next_pending
                if attempt == self.config.max_retries:
                    failed += len(pending)
                    break
                time.sleep(min(2**attempt, 10))

        return {"sent": sent, "failed": failed}

    # ------------------------------------------------------------------
    def run(self) -> Dict[str, int]:
        """Run the simulator loop. Returns aggregate stats."""
        interval = 1.0 / self.config.rate
        start = time.monotonic()
        next_log = start + 5.0
        total_sent = 0
        total_failed = 0
        buffer: List[Dict[str, Any]] = []

        logger.info(
            "Starting simulator: stream=%s rate=%s/s batch=%s duration=%s",
            self.config.stream_name,
            self.config.rate,
            self.config.batch_size,
            self.config.duration,
        )

        try:
            while True:
                if self.config.duration is not None:
                    if time.monotonic() - start >= self.config.duration:
                        break

                event = self.generator.generate()
                buffer.append(build_record(event))

                if len(buffer) >= self.config.batch_size:
                    stats = self.put_batch(buffer)
                    total_sent += stats["sent"]
                    total_failed += stats["failed"]
                    buffer.clear()

                if time.monotonic() >= next_log:
                    elapsed = time.monotonic() - start
                    logger.info(
                        "Progress: sent=%s failed=%s elapsed=%.1fs",
                        total_sent,
                        total_failed,
                        elapsed,
                    )
                    next_log = time.monotonic() + 5.0

                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Interrupted, flushing remaining buffer...")
        finally:
            if buffer:
                stats = self.put_batch(buffer)
                total_sent += stats["sent"]
                total_failed += stats["failed"]

        logger.info("Done. sent=%s failed=%s", total_sent, total_failed)
        return {"sent": total_sent, "failed": total_failed}


# ----------------------------------------------------------------------
# CLI entrypoint
# ----------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> SimulatorConfig:
    parser = argparse.ArgumentParser(description="Kinesis event simulator")
    parser.add_argument("--stream-name", required=True, help="Kinesis stream name")
    parser.add_argument("--rate", type=int, default=100, help="Events per second")
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="How long to run in seconds (omit for unbounded)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Records per PutRecords call (1-500)",
    )
    parser.add_argument("--region", default=None, help="AWS region override")
    args = parser.parse_args(argv)
    return SimulatorConfig(
        stream_name=args.stream_name,
        rate=args.rate,
        duration=args.duration,
        batch_size=args.batch_size,
        region=args.region,
    )


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = _parse_args(argv)
    simulator = KinesisEventSimulator(cfg)
    stats = simulator.run()
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
