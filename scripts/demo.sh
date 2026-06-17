#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${KV_DEMO_PORT:-8090}"
API_KEY="demo-secret"
DATA_DIR="$ROOT_DIR/demo-data"
SERVER="$ROOT_DIR/build/kvstore_server"

if [[ ! -x "$SERVER" ]]; then
  cmake -S "$ROOT_DIR" -B "$ROOT_DIR/build" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$ROOT_DIR/build" -j
fi

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

"$SERVER" \
  --bind 127.0.0.1 \
  --port "$PORT" \
  --data-dir "$DATA_DIR" \
  --shards 2 \
  --replicas 3 \
  --api-key "$API_KEY" \
  --rate 100 \
  --burst 100 \
  --retries 0 \
  >"$DATA_DIR/server.out" 2>"$DATA_DIR/server.log" &
SERVER_PID=$!

cleanup() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

BASE="http://127.0.0.1:$PORT"
AUTH=(-H "Authorization: Bearer $API_KEY" -H "X-Client-Id: demo")

for _ in {1..50}; do
  if curl -fsS "$BASE/healthz" >/dev/null; then
    break
  fi
  sleep 0.1
done

echo "== 1. Store and read a key =="
PUT_RESPONSE=$(curl -fsS -X PUT "$BASE/v1/kv/account:42" "${AUTH[@]}" --data-binary 'gold')
echo "$PUT_RESPONSE" | python3 -m json.tool
curl -fsS "$BASE/v1/kv/account:42" "${AUTH[@]}" | python3 -m json.tool

SHARD=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["shard"])' <<<"$PUT_RESPONSE")
LEADER=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["leader"])' <<<"$PUT_RESPONSE")

echo
echo "== 2. Fail current leader $LEADER on shard $SHARD =="
curl -fsS -X POST "$BASE/admin/fail/$SHARD/$LEADER" "${AUTH[@]}" | python3 -m json.tool

echo
echo "== 3. Read succeeds after automatic leader election =="
GET_AFTER_FAIL=$(curl -fsS "$BASE/v1/kv/account:42" "${AUTH[@]}")
echo "$GET_AFTER_FAIL" | python3 -m json.tool
NEW_LEADER=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["leader"])' <<<"$GET_AFTER_FAIL")

echo
echo "== 4. Remove another replica and demonstrate quorum protection =="
CLUSTER=$(curl -fsS "$BASE/admin/cluster" "${AUTH[@]}")
VICTIM=$(python3 -c 'import json,sys; shard=int(sys.argv[1]); leader=sys.argv[2]; data=json.load(sys.stdin); print(next(r["id"] for r in data["shards"][shard]["replicas"] if r["online"] and r["id"] != leader))' "$SHARD" "$NEW_LEADER" <<<"$CLUSTER")
curl -fsS -X POST "$BASE/admin/fail/$SHARD/$VICTIM" "${AUTH[@]}" | python3 -m json.tool

HTTP_CODE=$(curl -sS -o "$DATA_DIR/rejected.json" -w '%{http_code}' \
  -X PUT "$BASE/v1/kv/account:42" "${AUTH[@]}" --data-binary 'platinum')
echo "HTTP $HTTP_CODE"
cat "$DATA_DIR/rejected.json" | python3 -m json.tool

echo
echo "== 5. Recover replicas and resume writes =="
curl -fsS -X POST "$BASE/admin/recover/$SHARD/$LEADER" "${AUTH[@]}" | python3 -m json.tool
curl -fsS -X POST "$BASE/admin/recover/$SHARD/$VICTIM" "${AUTH[@]}" | python3 -m json.tool
curl -fsS -X PUT "$BASE/v1/kv/account:42" "${AUTH[@]}" --data-binary 'platinum' | python3 -m json.tool
curl -fsS "$BASE/v1/kv/account:42" "${AUTH[@]}" | python3 -m json.tool

echo
echo "== 6. Metrics sample =="
curl -fsS "$BASE/metrics" | grep -E 'requests_total|retries_total|elections_total|replication_failures_total'

echo
echo "Demo complete. Structured logs: $DATA_DIR/server.log"
