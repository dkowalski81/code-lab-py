#!/usr/bin/env python3
"""Async POST load generator with GET status polling for performance measurement.

Dependency: aiohttp
    uv pip install aiohttp
    # or: pip install aiohttp

Usage example:
    python load_tester.py \
        --post-url  http://localhost:8000/jobs \
        --status-url http://localhost:8000/jobs/{id} \
        --rate 30 \
        --count 60 \
        --payload '{"input": "hello"}' \
        --output results.csv
"""

import asyncio
import aiohttp
import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class Result:
    seq: int
    job_id: Optional[str]
    submitted_at: float       # monotonic
    completed_at: Optional[float] = None
    error: Optional[str] = None

    @property
    def latency(self) -> Optional[float]:
        if self.completed_at is not None:
            return self.completed_at - self.submitted_at
        return None


async def poll_until_done(
    session: aiohttp.ClientSession,
    url: str,
    status_field: str,
    done_value: str,
    poll_interval: float,
    timeout: float,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await asyncio.sleep(poll_interval)
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if str(data.get(status_field, "")) == done_value:
                return True
    return False


async def run_one(
    session: aiohttp.ClientSession,
    args: argparse.Namespace,
    seq: int,
) -> Result:
    result = Result(seq=seq, job_id=None, submitted_at=time.monotonic())
    try:
        async with session.post(args.post_url, json=args.payload) as resp:
            resp.raise_for_status()
            data = await resp.json()

        job_id = data.get(args.id_field)
        if job_id is None:
            result.error = f"field '{args.id_field}' missing from POST response: {data}"
            return result
        result.job_id = str(job_id)

        status_url = args.status_url.format(id=job_id)
        done = await poll_until_done(
            session, status_url,
            args.status_field, args.done_value,
            args.poll_interval, args.poll_timeout,
        )
        if done:
            result.completed_at = time.monotonic()
        else:
            result.error = f"timed out after {args.poll_timeout}s"
    except Exception as exc:
        result.error = str(exc)
    return result


def _percentile(sorted_data: list[float], p: float) -> float:
    idx = int(len(sorted_data) * p)
    return sorted_data[min(idx, len(sorted_data) - 1)]


async def amain(args: argparse.Namespace) -> None:
    interval = 60.0 / args.rate  # seconds between submissions
    tasks: list[asyncio.Task] = []

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        for seq in range(args.count):
            task = asyncio.create_task(run_one(session, args, seq))
            tasks.append(task)
            print(f"  submitted {seq + 1}/{args.count}", end="\r", flush=True)
            if seq < args.count - 1:
                await asyncio.sleep(interval)

        print(f"\nAll {args.count} requests sent — waiting for completions...")
        results: list[Result] = await asyncio.gather(*tasks)

    completed = [r for r in results if r.latency is not None]
    errors    = [r for r in results if r.error]
    latencies = sorted(r.latency for r in completed)  # type: ignore[arg-type]

    print(f"\n{'='*52}")
    print(f"Total: {args.count}  Completed: {len(completed)}  Errors: {len(errors)}")
    print(f"Target rate: {args.rate} req/min  "
          f"Actual interval: {interval:.2f}s")

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
            print(f"  seq={r.seq} job={r.job_id}: {r.error}")

    if args.output:
        import csv
        with open(args.output, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["seq", "job_id", "submitted_at", "completed_at", "latency_s", "error"])
            for r in results:
                writer.writerow([r.seq, r.job_id, r.submitted_at, r.completed_at, r.latency, r.error])
        print(f"\nPer-request data written to {args.output}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Async POST load generator with GET status polling",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--post-url", required=True, metavar="URL",
                        help="Endpoint to POST jobs to")
    parser.add_argument("--status-url", required=True, metavar="URL_TEMPLATE",
                        help="GET status URL template containing {id}, e.g. http://host/jobs/{id}")
    parser.add_argument("--rate", type=float, required=True, metavar="N",
                        help="Requests per minute to send")
    parser.add_argument("--count", type=int, required=True, metavar="N",
                        help="Total number of POST requests to send")
    parser.add_argument("--payload", type=json.loads, default={}, metavar="JSON",
                        help='POST body as a JSON string, e.g. \'{"key": "value"}\'')
    parser.add_argument("--id-field", default="id",
                        help="Field in POST response that carries the job ID")
    parser.add_argument("--status-field", default="status",
                        help="Field in GET response to check for completion")
    parser.add_argument("--done-value", default="completed",
                        help="Value of --status-field that signals the job is done")
    parser.add_argument("--poll-interval", type=float, default=1.0, metavar="SECS",
                        help="Seconds between status poll attempts")
    parser.add_argument("--poll-timeout", type=float, default=300.0, metavar="SECS",
                        help="Max seconds to wait per job before recording a timeout error")
    parser.add_argument("--concurrency", type=int, default=200,
                        help="Max simultaneous open TCP connections")
    parser.add_argument("--output", metavar="FILE",
                        help="Optional CSV file to write per-request timing rows to")
    args = parser.parse_args()

    if args.rate <= 0:
        sys.exit("--rate must be > 0")
    if args.count <= 0:
        sys.exit("--count must be > 0")

    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
