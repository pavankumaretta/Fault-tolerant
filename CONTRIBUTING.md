# Contributing

1. Create a feature branch.
2. Keep changes focused and add tests for behavior changes.
3. Run `make test` before opening a pull request.
4. Compile with warnings enabled and avoid introducing third-party dependencies without justification.
5. Document protocol or consistency changes in `docs/`.

Suggested extensions include multi-process replicas, gRPC transport, log compaction, randomized election timeouts, OpenTelemetry, and consistent hashing.
