#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace kv {

class Metrics {
public:
    void record_request(std::uint64_t latency_us);
    void inc_auth_failure();
    void inc_rate_limited();
    void inc_get();
    void inc_put();
    void inc_delete();
    void inc_retry();
    void inc_circuit_open();
    void inc_gateway_failure();
    void inc_election();
    void inc_replication_failure();

    std::string render_prometheus() const;

private:
    std::atomic<std::uint64_t> requests_{0};
    std::atomic<std::uint64_t> request_latency_us_{0};
    std::atomic<std::uint64_t> auth_failures_{0};
    std::atomic<std::uint64_t> rate_limited_{0};
    std::atomic<std::uint64_t> gets_{0};
    std::atomic<std::uint64_t> puts_{0};
    std::atomic<std::uint64_t> deletes_{0};
    std::atomic<std::uint64_t> retries_{0};
    std::atomic<std::uint64_t> circuit_open_{0};
    std::atomic<std::uint64_t> gateway_failures_{0};
    std::atomic<std::uint64_t> elections_{0};
    std::atomic<std::uint64_t> replication_failures_{0};
};

}  // namespace kv
