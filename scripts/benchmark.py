#!/usr/bin/env python3
"""Small dependency-free local load generator. Results depend on local hardware."""

from __future__ import annotations

import argparse
import concurrent.futures
import statistics
import time
import urllib.request


def operation(base: str, api_key: str, index: int) -> float:
    key = f"benchmark:{index % 1000}"
    request = urllib.request.Request(
        f"{base}/v1/kv/{key}",
        data=f"value-{index}".encode(),
        method="PUT",
        headers={
            "Authorization": f"Bearer {api_key}",
            "X-Client-Id": f"benchmark-{index % 32}",
        },
    )
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()
        if response.status != 201:
            raise RuntimeError(f"unexpected status {response.status}")
    return (time.perf_counter() - started) * 1000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--api-key", default="dev-secret")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=16)
    args = parser.parse_args()

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        latencies = list(
            pool.map(
                lambda i: operation(args.base, args.api_key, i),
                range(args.requests),
            )
        )
    duration = time.perf_counter() - started
    ordered = sorted(latencies)

    def percentile(p: float) -> float:
        index = min(len(ordered) - 1, int(len(ordered) * p))
        return ordered[index]

    print(f"requests:    {args.requests}")
    print(f"concurrency: {args.concurrency}")
    print(f"throughput:  {args.requests / duration:.1f} req/s")
    print(f"mean:        {statistics.mean(latencies):.2f} ms")
    print(f"p50:         {percentile(0.50):.2f} ms")
    print(f"p95:         {percentile(0.95):.2f} ms")
    print(f"p99:         {percentile(0.99):.2f} ms")


if __name__ == "__main__":
    main()
