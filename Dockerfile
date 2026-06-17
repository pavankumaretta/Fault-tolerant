FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake libsqlite3-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)" \
    && ctest --test-dir build --output-on-failure

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home kvstore

COPY --from=builder /src/build/kvstore_server /usr/local/bin/kvstore_server
RUN mkdir -p /var/lib/kvstore && chown -R kvstore:kvstore /var/lib/kvstore

USER kvstore
EXPOSE 8080
VOLUME ["/var/lib/kvstore"]

ENV KV_BIND=0.0.0.0 \
    KV_PORT=8080 \
    KV_DATA_DIR=/var/lib/kvstore \
    KV_SHARDS=4 \
    KV_REPLICAS=3 \
    KV_RATE_PER_SECOND=20 \
    KV_BURST=40 \
    KV_RETRIES=2

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/kvstore_server"]
