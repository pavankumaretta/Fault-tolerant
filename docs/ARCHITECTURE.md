# Architecture

## Request path

1. The HTTP server parses a request and forwards it to the API gateway.
2. The gateway verifies the bearer token.
3. A per-client token bucket enforces the configured request rate.
4. FNV-1a hashes the key and selects a shard.
5. The per-shard circuit breaker decides whether the backend call is allowed.
6. The shard leader appends the command to its durable SQLite Raft log.
7. The leader sends AppendEntries to followers.
8. The command is committed only when a majority acknowledges the entry.
9. Every acknowledging replica applies the committed command to its SQLite `kv` table.
10. The gateway records latency and operation metrics and returns the response.

## Components

### HTTP server

A dependency-light HTTP/1.1 server implemented with POSIX sockets. It supports one request per connection, bounded request sizes, read/write timeouts, and threaded request handling.

### API gateway

The gateway provides:

- static bearer-token authentication
- client identification through `X-Client-Id` or remote IP
- token-bucket rate limiting
- deterministic key routing
- bounded retry with exponential backoff
- circuit breaking after repeated shard failures
- JSON responses and security headers
- Prometheus-compatible metrics

### Sharding

The cluster uses `FNV1a(key) % shard_count`. This is deterministic and simple to inspect. A production version could use rendezvous hashing or a virtual-node consistent hash ring to reduce movement when shard membership changes.

### Raft group

Each shard owns a fixed-size replica group. A replica persists:

- current term
- voted-for candidate
- commit index
- last-applied index
- replicated log
- materialized key-value state

Election chooses the most up-to-date online candidate and asks every online node for a vote. A leader is accepted only after a majority votes for it.

Writes follow this sequence:

1. append locally on leader
2. replicate to followers
3. require majority acknowledgement
4. commit on leader
5. send commit heartbeat to followers
6. apply command to SQLite state machine

Uncommitted entries are discarded when a majority cannot be reached, preventing a failed client write from being applied later by this implementation.

### SQLite storage

Each replica has an independent SQLite database using WAL mode. Tables:

- `kv`: materialized key-value state
- `raft_log`: ordered replicated commands
- `meta`: persistent Raft term, vote, and commit metadata

### Recovery

When an offline replica recovers, it installs the current leader's committed log snapshot and rebuilds its local state machine before participating again.

## Concurrency model

- HTTP connections are handled in detached worker threads.
- Cluster, shard, replica, rate limiter, and circuit breaker state are protected with mutexes.
- Metrics use atomic counters.
- SQLite connections use `SQLITE_OPEN_FULLMUTEX` and a busy timeout.

## Production extensions

- Run each replica as an independent process/container.
- Replace snapshot copying with incremental log backfill and snapshot compaction.
- Add randomized election timeouts and periodic heartbeats.
- Use gRPC with mTLS for internal Raft RPCs.
- Add membership changes and consistent-hash virtual nodes.
- Add linearizable read-index or lease reads.
- Add OpenTelemetry traces and Grafana dashboards.
- Add encrypted values and API-key rotation.
