#!/usr/bin/env python3
"""Azure Storage Queue load generator with results-queue completion tracking.

Sends messages to an input queue at a configurable rate.
A single background consumer drains a results queue and resolves each
waiting coroutine via an asyncio.Future keyed on correlation_id.

Expected message format sent to --input-queue:
    {"correlation_id": "<uuid4>", ...extra payload fields...}

Expected completion message format in --results-queue:
    {"correlation_id": "<uuid4>"}

Dependencies:
    uv pip install azure-storage-queue

Environment:
    AZURE_STORAGE_CONNECTION_STRING  — Azure Storage connection string

Usage example:
    python azure/queue_load_tester.py \\
        --input-queue   jobs \\
        --results-queue job-results \\
        --rate 30 \\
        --count 60 \\
        --payload '{"type": "process"}' \\
        --output results.csv
"""

import asyncio
import argparse
import json
import os
import statistics
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from azure.storage.queue.aio import QueueServiceClient


@dataclass
class Result:
    seq: int
    correlation_id: str
    submitted_at: float
    completed_at: Optional[float] = None
    error: Optional[str] = None

    @property
    def latency(self) -> Optional[float]:
        if self.completed_at is not None:
            return self.completed_at - self.submitted_at
        return None


async def results_consumer(
    conn_str: str,
    queue_name: str,
    pending: dict[str, "asyncio.Future[float]"],
    poll_interval: float,
    stop_event: asyncio.Event,
) -> None:
    """Single background task: drains the results queue and resolves futures."""
    async with QueueServiceClient.from_connection_string(conn_str) as svc:
        client = svc.get_queue_client(queue_name)
        while not stop_event.is_set() or pending:
            try:
                messages = client.receive_messages(max_messages=32)
                async for msg in messages:
                    try:
                        data = json.loads(msg.content)
                        corr_id = data.get("correlation_id")
                        if corr_id and corr_id in pending:
                            future = pending.pop(corr_id)
                            if not future.done():
                                future.set_result(time.monotonic())
                            await client.delete_message(msg)
                    except (json.JSONDecodeError, Exception):
                        pass  # leave unrecognized messages in the queue
            except Exception as exc:
                print(f"\n[consumer error] {exc}", file=sys.stderr)

            if pending:
                await asyncio.sleep(poll_interval)


async def send_one(
    conn_str: str,
    input_queue_name: str,
    pending: dict[str, "asyncio.Future[float]"],
    args: argparse.Namespace,
    seq: int,
) -> Result:
    corr_id = str(uuid.uuid4())
    submitted_at = time.monotonic()
    future: asyncio.Future[float] = asyncio.get_running_loop().create_future()
    pending[corr_id] = future

    result = Result(seq=seq, correlation_id=corr_id, submitted_at=submitted_at)

    try:
        async with QueueServiceClient.from_connection_string(conn_str) as svc:
            client = svc.get_queue_client(input_queue_name)
            payload = {**args.payload, "correlation_id": corr_id}
            await client.send_message(json.dumps(payload))
    except Exception as exc:
        pending.pop(corr_id, None)
        if not future.done():
            future.cancel()
        result.error = f"send failed: {exc}"
        return result

    try:
        completed_at = await asyncio.wait_for(
            asyncio.shield(future), timeout=args.poll_timeout
        )
        result.completed_at = completed_at
    except asyncio.TimeoutError:
        pending.pop(corr_id, None)
        result.error = f"timed out after {args.poll_timeout}s"

    return result


def _percentile(sorted_data: list[float], p: float) -> float:
    idx = int(len(sorted_data) * p)
    return sorted_data[min(idx, len(sorted_data) - 1)]


async def amain(args: argparse.Namespace) -> None:
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        sys.exit("AZURE_STORAGE_CONNECTION_STRING environment variable not set")

    interval = 60.0 / args.rate
    pending: dict[str, asyncio.Future[float]] = {}
    stop_event = asyncio.Event()

    consumer = asyncio.create_task(
        results_consumer(conn_str, args.results_queue, pending, args.poll_interval, stop_event)
    )

    tasks: list[asyncio.Task] = []
    for seq in range(args.count):
        task = asyncio.create_task(
            send_one(conn_str, args.input_queue, pending, args, seq)
        )
        tasks.append(task)
        print(f"  submitted {seq + 1}/{args.count}", end="\r", flush=True)
        if seq < args.count - 1:
            await asyncio.sleep(interval)

    print(f"\nAll {args.count} messages sent — waiting for completions...")
    results: list[Result] = await asyncio.gather(*tasks)

    stop_event.set()
    await consumer

    completed = [r for r in results if r.latency is not None]
    errors    = [r for r in results if r.error]
    latencies = sorted(r.latency for r in completed)  # type: ignore[arg-type]

    print(f"\n{'='*52}")
    print(f"Total: {args.count}  Completed: {len(completed)}  Errors: {len(errors)}")
    print(f"Target rate: {args.rate} msg/min  Actual interval: {interval:.2f}s")

    if latencies:
        print("\nEnd-to-end latency (seconds):")
        print(f"  min   {min(latencies):.3f}")
        print(f"  p50   {_percentile(latencies, 0.50):.3f}")
        print(f"  p90   {_percentile(latencies, 0.90):.3f}")
        print(f"  p95   {_percentile(latencies, 0.95):.3f}")
        print(f"  p99   {_percentile(latencies, 0.99):.3f}")
        print(f"  max   {max(latencies):.3f}")
        if len(latencies) > 1:
            print(f"  mean  {statistics.mean(latencies):.3f}")
            print(f"  stdev {statistics.stdev(latencies):.3f}")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for r in errors:
            print(f"  seq={r.seq} corr={r.correlation_id[:8]}…: {r.error}")

    if args.output:
        import csv
        with open(args.output, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["seq", "correlation_id", "submitted_at", "completed_at", "latency_s", "error"])
            for r in results:
                writer.writerow([r.seq, r.correlation_id, r.submitted_at, r.completed_at, r.latency, r.error])
        print(f"\nPer-message data written to {args.output}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Azure Storage Queue load generator with results-queue completion tracking",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input-queue", required=True, metavar="NAME",
                        help="Queue to send work messages to")
    parser.add_argument("--results-queue", required=True, metavar="NAME",
                        help="Queue where the processor posts completion messages")
    parser.add_argument("--rate", type=float, required=True, metavar="N",
                        help="Messages per minute to send")
    parser.add_argument("--count", type=int, required=True, metavar="N",
                        help="Total number of messages to send")
    parser.add_argument("--payload", type=json.loads, default={}, metavar="JSON",
                        help='Extra fields merged into each message, e.g. \'{"type": "job"}\'')
    parser.add_argument("--poll-interval", type=float, default=1.0, metavar="SECS",
                        help="Seconds between results queue drain attempts")
    parser.add_argument("--poll-timeout", type=float, default=300.0, metavar="SECS",
                        help="Max seconds to wait per message before recording a timeout")
    parser.add_argument("--output", metavar="FILE",
                        help="Optional CSV file for per-message timing rows")
    args = parser.parse_args()

    if args.rate <= 0:
        sys.exit("--rate must be > 0")
    if args.count <= 0:
        sys.exit("--count must be > 0")

    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
