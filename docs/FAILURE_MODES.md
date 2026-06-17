# Failure Modes and Expected Behavior

| Failure | Expected behavior |
|---|---|
| One follower is offline | Reads and writes continue because two of three replicas form a majority. |
| Current leader is offline | The next request triggers a new election among the two online replicas. |
| Two replicas are offline | Reads/writes requiring a leader are rejected with HTTP 503 because no majority exists. |
| Replica returns after downtime | It installs the leader's committed snapshot before serving as a follower. |
| SQLite database survives restart | The replica reloads term, vote, commit index, log, and materialized state. |
| Repeated shard failures | The gateway opens that shard's circuit breaker for five seconds. |
| Temporary backend failure | The gateway retries up to the configured limit with exponential backoff. |
| Client exceeds request rate | The gateway returns HTTP 429 and `Retry-After: 1`. |
| Missing/incorrect token | The gateway returns HTTP 401 before touching cluster state. |

## Consistency model

Committed writes are replicated to a majority before acknowledgement. Reads are served from the elected leader's committed state. The implementation demonstrates the core safety path for a fixed membership group, but it does not implement every optimization and edge case from the complete Raft paper.

## Deliberate boundaries

- Replica communication is in-process rather than over a network.
- Elections are request-driven rather than timer-driven.
- Membership is static for the lifetime of the process.
- Snapshot recovery copies the committed log rather than using chunked snapshot transfer.
- Values are limited to 1 MiB by the gateway.
