#pragma once

#include "kv/cluster.hpp"
#include "kv/http_server.hpp"
#include "kv/metrics.hpp"

#include <chrono>
#include <cstddef>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv {

class TokenBucket {
public:
    TokenBucket(double refill_per_second, double capacity);
    bool consume(double tokens = 1.0);

private:
    double refill_per_second_;
    double capacity_;
    double tokens_;
    std::chrono::steady_clock::time_point last_refill_;
};

enum class CircuitState { Closed, Open, HalfOpen };

class CircuitBreaker {
public:
    CircuitBreaker(std::size_t failure_threshold,
                   std::chrono::milliseconds open_duration);

    bool allow_request();
    void record_success();
    void record_failure();
    std::string state_string() const;

private:
    std::size_t failure_threshold_;
    std::chrono::milliseconds open_duration_;
    std::size_t consecutive_failures_{0};
    CircuitState state_{CircuitState::Closed};
    std::chrono::steady_clock::time_point opened_at_{};
};

class Gateway {
public:
    Gateway(Cluster& cluster,
            Metrics& metrics,
            std::string api_key,
            double rate_per_second,
            double burst,
            std::size_t max_retries);

    HttpResponse handle(const HttpRequest& request);

private:
    bool authorized(const HttpRequest& request) const;
    bool allowed_by_rate_limit(const std::string& client_id);
    CircuitBreaker& breaker_for(std::size_t shard_id);
    HttpResponse json_response(int status, const std::string& body) const;
    HttpResponse execute_with_resilience(const std::string& operation,
                                         const std::string& key,
                                         const std::string& value = "");

    Cluster& cluster_;
    Metrics& metrics_;
    std::string api_key_;
    double rate_per_second_;
    double burst_;
    std::size_t max_retries_;

    mutable std::mutex rate_mutex_;
    std::unordered_map<std::string, TokenBucket> buckets_;

    mutable std::mutex breaker_mutex_;
    std::vector<CircuitBreaker> breakers_;
};

}  // namespace kv
