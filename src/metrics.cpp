#include "kv/metrics.hpp"

#include <sstream>

namespace kv {

void Metrics::record_request(std::uint64_t latency_us) {
    requests_.fetch_add(1, std::memory_order_relaxed);
    request_latency_us_.fetch_add(latency_us, std::memory_order_relaxed);
}
void Metrics::inc_auth_failure() { auth_failures_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_rate_limited() { rate_limited_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_get() { gets_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_put() { puts_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_delete() { deletes_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_retry() { retries_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_circuit_open() { circuit_open_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_gateway_failure() { gateway_failures_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_election() { elections_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::inc_replication_failure() { replication_failures_.fetch_add(1, std::memory_order_relaxed); }

std::string Metrics::render_prometheus() const {
    const auto requests = requests_.load(std::memory_order_relaxed);
    const auto latency = request_latency_us_.load(std::memory_order_relaxed);
    std::ostringstream out;
    out << "# HELP kv_gateway_requests_total Total HTTP requests.\n"
        << "# TYPE kv_gateway_requests_total counter\n"
        << "kv_gateway_requests_total " << requests << "\n"
        << "# HELP kv_gateway_request_latency_microseconds_sum Cumulative request latency.\n"
        << "# TYPE kv_gateway_request_latency_microseconds_sum counter\n"
        << "kv_gateway_request_latency_microseconds_sum " << latency << "\n"
        << "# HELP kv_gateway_request_latency_microseconds_avg Average request latency.\n"
        << "# TYPE kv_gateway_request_latency_microseconds_avg gauge\n"
        << "kv_gateway_request_latency_microseconds_avg "
        << (requests == 0 ? 0 : latency / requests) << "\n"
        << "kv_gateway_auth_failures_total " << auth_failures_.load() << "\n"
        << "kv_gateway_rate_limited_total " << rate_limited_.load() << "\n"
        << "kv_gateway_get_total " << gets_.load() << "\n"
        << "kv_gateway_put_total " << puts_.load() << "\n"
        << "kv_gateway_delete_total " << deletes_.load() << "\n"
        << "kv_gateway_retries_total " << retries_.load() << "\n"
        << "kv_gateway_circuit_open_total " << circuit_open_.load() << "\n"
        << "kv_gateway_failures_total " << gateway_failures_.load() << "\n"
        << "kv_raft_elections_total " << elections_.load() << "\n"
        << "kv_raft_replication_failures_total " << replication_failures_.load() << "\n";
    return out.str();
}

}  // namespace kv
