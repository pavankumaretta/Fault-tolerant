#!/usr/bin/env python3
"""End-to-end HTTP test for auth, CRUD, failover, quorum loss, recovery, and metrics."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

API_KEY = "integration-secret"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request(
    base: str,
    method: str,
    path: str,
    body: bytes | None = None,
    authorized: bool = True,
) -> tuple[int, str, dict[str, str]]:
    headers = {"X-Client-Id": "integration-test"}
    if authorized:
        headers["Authorization"] = f"Bearer {API_KEY}"
    req = urllib.request.Request(base + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as response:
            return response.status, response.read().decode(), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(), dict(exc.headers)


def parse_json(body: str) -> dict[str, Any]:
    return json.loads(body)


def wait_until_ready(base: str) -> None:
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            status, _, _ = request(base, "GET", "/healthz", authorized=False)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("server did not become ready")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="./build/kvstore_server")
    args = parser.parse_args()

    binary = str(Path(args.binary).resolve())
    port = free_port()
    base = f"http://127.0.0.1:{port}"

    with tempfile.TemporaryDirectory(prefix="kvstore-integration-") as data_dir:
        process = subprocess.Popen(
            [
                binary,
                "--bind", "127.0.0.1",
                "--port", str(port),
                "--data-dir", data_dir,
                "--shards", "2",
                "--replicas", "3",
                "--api-key", API_KEY,
                "--rate", "100",
                "--burst", "100",
                "--retries", "2",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_until_ready(base)

            status, _, _ = request(base, "GET", "/v1/kv/account", authorized=False)
            assert status == 401, status

            status, body, _ = request(base, "PUT", "/v1/kv/account", b"gold")
            assert status == 201, (status, body)
            created = parse_json(body)
            shard_id = int(created["shard"])
            first_leader = str(created["leader"])

            status, body, _ = request(base, "GET", "/v1/kv/account")
            assert status == 200, (status, body)
            assert parse_json(body)["value"] == "gold"

            # Take down the leader. A new leader should be elected automatically.
            status, body, _ = request(base, "POST", f"/admin/fail/{shard_id}/{first_leader}")
            assert status == 200, (status, body)

            status, body, _ = request(base, "GET", "/v1/kv/account")
            assert status == 200, (status, body)
            second_leader = str(parse_json(body)["leader"])
            assert second_leader != first_leader

            status, body, _ = request(base, "GET", "/admin/cluster")
            assert status == 200, (status, body)
            cluster = parse_json(body)
            replicas = cluster["shards"][shard_id]["replicas"]
            online_ids = [r["id"] for r in replicas if r["online"]]

            # Remove one more node, leaving no majority for this three-replica shard.
            victim = next(replica for replica in online_ids if replica != second_leader)
            status, body, _ = request(base, "POST", f"/admin/fail/{shard_id}/{victim}")
            assert status == 200, (status, body)

            status, body, _ = request(base, "PUT", "/v1/kv/account", b"platinum")
            assert status == 503, (status, body)

            # Recover both nodes, wait for the breaker cooldown, and verify writes resume.
            for replica_id in (first_leader, victim):
                status, body, _ = request(base, "POST", f"/admin/recover/{shard_id}/{replica_id}")
                assert status == 200, (status, body)
            time.sleep(5.2)

            status, body, _ = request(base, "PUT", "/v1/kv/account", b"platinum")
            assert status == 201, (status, body)
            status, body, _ = request(base, "GET", "/v1/kv/account")
            assert status == 200
            assert parse_json(body)["value"] == "platinum"

            status, metrics, _ = request(base, "GET", "/metrics", authorized=False)
            assert status == 200
            assert "kv_raft_elections_total" in metrics
            assert "kv_gateway_retries_total" in metrics

            print("Integration test passed: auth, CRUD, failover, quorum, recovery, metrics.")
            return 0
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
            stdout, stderr = process.communicate()
            if process.returncode not in (0, -15):
                print("SERVER STDOUT:\n", stdout)
                print("SERVER STDERR:\n", stderr)


if __name__ == "__main__":
    raise SystemExit(main())
