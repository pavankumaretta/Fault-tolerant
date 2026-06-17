# Fault-Tolerant Sharded Key-Value Store

[![CI](https://github.com/pavankumaretta/fault-tolerant-sharded-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/pavankumaretta/fault-tolerant-sharded-kv-store/actions/workflows/ci.yml)

A distributed-systems project built with **C++20, Raft consensus concepts, and SQLite**. The service partitions keys across shards, replicates every write to a majority of replicas, automatically elects a replacement leader after failure, and exposes the cluster through a resilient HTTP API gateway.

The gateway includes:

- Bearer-token authentication
- Deterministic request routing to shards
- Per-client token-bucket rate limiting
- Bounded retries with exponential backoff
- Per-shard circuit breakers
- Prometheus-compatible metrics
- Structured JSON logs
- Fault-injection and recovery endpoints

> This is a compact educational Raft implementation designed to demonstrate leader election, RequestVote, AppendEntries, majority commit, replicated logs, failover, and recovery. It is not a replacement for a production-hardened consensus library.

Each shard is an independent three-replica Raft group. A write is acknowledged only after a majority stores the log entry. Reads are served by the current shard leader. Each replica persists its key-value state, Raft log, term, vote, and commit index in its own SQLite database.

## What this repository proves

- **Sharding:** stable FNV-1a hashing routes a key to one shard.
- **Consensus:** leader election, voting, replicated logs, and majority commit.
- **Fault tolerance:** leader failure triggers automatic election without losing committed data.
- **Durability:** SQLite WAL persists data and Raft metadata across process restarts.
- **Traffic protection:** auth, rate limiting, retries, and circuit breakers protect the backend.
- **Observability:** request, retry, election, failure, and replication metrics are exported for Prometheus.
- **Testing:** C++ unit tests and a Python end-to-end chaos test run in CI.

## Quick start

### Prerequisites

- CMake 3.20+
- C++20 compiler
- SQLite3 development library
- Python 3.10+ for the integration test

Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libsqlite3-dev python3
```

macOS:

```bash
brew install cmake sqlite3
```

### Build and test

```bash
git clone https://github.com/pavankumaretta/fault-tolerant-sharded-kv-store.git
cd fault-tolerant-sharded-kv-store
make test
```

### Run locally

```bash
make run
```

The server starts at `http://localhost:8080` with development API key `dev-secret`.

## API examples

Set a key:

```bash
curl -i -X PUT http://localhost:8080/v1/kv/user:42 \
  -H 'Authorization: Bearer dev-secret' \
  -H 'X-Client-Id: local-demo' \
  --data-binary 'active'
```

Read a key:

```bash
curl -s http://localhost:8080/v1/kv/user:42 \
  -H 'Authorization: Bearer dev-secret' | python3 -m json.tool
```

Delete a key:

```bash
curl -i -X DELETE http://localhost:8080/v1/kv/user:42 \
  -H 'Authorization: Bearer dev-secret'
```

View cluster state:

```bash
curl -s http://localhost:8080/admin/cluster \
  -H 'Authorization: Bearer dev-secret' | python3 -m json.tool
```

View Prometheus metrics:

```bash
curl http://localhost:8080/metrics
```

## Fault-injection demo

Run the automated demonstration:

```bash
make demo
```

Or manually fail a leader. First inspect the cluster:

```bash
curl -s http://localhost:8080/admin/cluster \
  -H 'Authorization: Bearer dev-secret' | python3 -m json.tool
```

Then fail a replica:

```bash
curl -X POST http://localhost:8080/admin/fail/0/s0-r0 \
  -H 'Authorization: Bearer dev-secret'
```

The next operation on shard `0` triggers a new election. Recover the node with:

```bash
curl -X POST http://localhost:8080/admin/recover/0/s0-r0 \
  -H 'Authorization: Bearer dev-secret'
```

The recovered replica installs the leader's committed snapshot before rejoining.

## Configuration

Every option can be supplied as a command-line flag or environment variable.

| Setting | Flag | Environment variable | Default |
|---|---|---|---:|
| Bind address | `--bind` | `KV_BIND` | `0.0.0.0` |
| Port | `--port` | `KV_PORT` | `8080` |
| Data directory | `--data-dir` | `KV_DATA_DIR` | `./data` |
| Shards | `--shards` | `KV_SHARDS` | `4` |
| Replicas per shard | `--replicas` | `KV_REPLICAS` | `3` |
| API key | `--api-key` | `KV_API_KEY` | `dev-secret` |
| Requests/sec/client | `--rate` | `KV_RATE_PER_SECOND` | `20` |
| Burst capacity | `--burst` | `KV_BURST` | `40` |
| Retry count | `--retries` | `KV_RETRIES` | `2` |

Example:

```bash
KV_API_KEY='replace-me' \
KV_SHARDS=8 \
KV_REPLICAS=3 \
KV_DATA_DIR=./cluster-data \
./build/kvstore_server
```

## Docker

```bash
docker compose up --build
```

The SQLite files are persisted in the `kv-data` Docker volume.

## Tests

### Unit tests

The C++ unit suite validates:

- CRUD behavior
- deterministic sharding
- automatic leader failover
- rejection of writes without a majority
- recovery and synchronization
- SQLite persistence after restart

```bash
ctest --test-dir build --output-on-failure
```

### End-to-end test

The Python integration test starts the real HTTP server and validates:

1. authentication failure
2. PUT and GET
3. leader failure
4. automatic leader election
5. majority loss and write rejection
6. replica recovery
7. resumed writes
8. Prometheus metrics

```bash
python3 tests/integration_test.py --binary ./build/kvstore_server
```

## Repository structure

```text
.
├── include/kv/             # Public C++ interfaces
├── src/                    # Raft, SQLite, gateway, cluster, HTTP server
├── tests/                  # Unit and end-to-end tests
├── scripts/                # Demo and benchmark helpers
├── docs/                   # Architecture and failure semantics
├── api/openapi.yaml        # OpenAPI 3.0 contract
├── postman/                # Ready-to-import Postman collection
├── .github/workflows/      # GitHub Actions CI
├── CMakeLists.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Resume-ready project entry

**Fault-Tolerant Sharded Key-Value Store | C++, Raft, SQLite**  
Built a sharded key-value service with replicated Raft logs, majority-based commits, automatic leader failover, and SQLite WAL persistence; added an API gateway with authentication, rate limiting, retries, circuit breakers, fault injection, and Prometheus observability.

## Design notes and limitations

See [Architecture](docs/ARCHITECTURE.md) and [Failure Modes](docs/FAILURE_MODES.md).

Current scope intentionally keeps all Raft replicas inside one server process while giving each replica an independent durable SQLite database. This makes the consensus and failure behavior easy to run, test, and review in a single GitHub repository. A production extension would move each replica into its own process or container and transport RequestVote/AppendEntries over gRPC or TLS-enabled HTTP.

## License

MIT License. See [LICENSE](LICENSE).
