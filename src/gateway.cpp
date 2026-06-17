#include "kv/gateway.hpp"

#include "kv/common.hpp"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

namespace kv {

TokenBucket::TokenBucket(double refill_per_second, double capacity)
    : refill_per_second_(refill_per_second),
      capacity_(capacity),
      tokens_(capacity),
      last_refill_(std::chrono::steady_clock::now()) {}

bool TokenBucket::consume(double tokens) {
    const auto now = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = now - last_refill_;
    tokens_ = std::min(capacity_, tokens_ + elapsed.count() * refill_per_second_);
    last_refill_ = now;
    if (tokens_ < tokens) {
        return false;
    }
    tokens_ -= tokens;
    return true;
}

CircuitBreaker::CircuitBreaker(std::size_t failure_threshold,
                               std::chrono::milliseconds open_duration)
    : failure_threshold_(failure_threshold), open_duration_(open_duration) {}

bool CircuitBreaker::allow_request() {
    if (state_ == CircuitState::Closed) {
        return true;
    }
    if (state_ == CircuitState::Open) {
        const auto elapsed = std::chrono::steady_clock::now() - opened_at_;
        if (elapsed >= open_duration_) {
            state_ = CircuitState::HalfOpen;
            return true;
        }
        return false;
    }
    return true;
}

void CircuitBreaker::record_success() {
    consecutive_failures_ = 0;
    state_ = CircuitState::Closed;
}

void CircuitBreaker::record_failure() {
    ++consecutive_failures_;
    if (state_ == CircuitState::HalfOpen || consecutive_failures_ >= failure_threshold_) {
        state_ = CircuitState::Open;
        opened_at_ = std::chrono::steady_clock::now();
    }
}

std::string CircuitBreaker::state_string() const {
    switch (state_) {
        case CircuitState::Closed: return "closed";
        case CircuitState::Open: return "open";
        case CircuitState::HalfOpen: return "half-open";
    }
    return "unknown";
}

Gateway::Gateway(Cluster& cluster,
                 Metrics& metrics,
                 std::string api_key,
                 double rate_per_second,
                 double burst,
                 std::size_t max_retries)
    : cluster_(cluster),
      metrics_(metrics),
      api_key_(std::move(api_key)),
      rate_per_second_(rate_per_second),
      burst_(burst),
      max_retries_(max_retries) {
    breakers_.reserve(cluster_.shard_count());
    for (std::size_t i = 0; i < cluster_.shard_count(); ++i) {
        breakers_.emplace_back(3, std::chrono::milliseconds(5000));
    }
}

HttpResponse Gateway::handle(const HttpRequest& request) {
    const auto started = std::chrono::steady_clock::now();
    auto finish = [this, started](HttpResponse response) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started);
        metrics_.record_request(static_cast<std::uint64_t>(elapsed.count()));
        response.headers["X-Content-Type-Options"] = "nosniff";
        response.headers["X-Request-Latency-Us"] = std::to_string(elapsed.count());
        return response;
    };

    if (request.path == "/healthz") {
        return finish(json_response(200, "{\"status\":\"ok\"}"));
    }
    if (request.path == "/metrics") {
        HttpResponse response;
        response.status = 200;
        response.content_type = "text/plain; version=0.0.4";
        response.body = metrics_.render_prometheus();
        return finish(std::move(response));
    }

    if (!authorized(request)) {
        metrics_.inc_auth_failure();
        return finish(json_response(401, "{\"error\":\"missing or invalid bearer token\"}"));
    }

    const std::string client_id = [&]() {
        const auto it = request.headers.find("x-client-id");
        return it == request.headers.end() ? request.remote_ip : it->second;
    }();

    if (!allowed_by_rate_limit(client_id)) {
        metrics_.inc_rate_limited();
        auto response = json_response(429, "{\"error\":\"rate limit exceeded\"}");
        response.headers["Retry-After"] = "1";
        return finish(std::move(response));
    }

    const auto parts = split_path(request.path);

    if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "kv") {
        const std::string& key = parts[2];
        if (key.empty() || key.size() > 512) {
            return finish(json_response(400, "{\"error\":\"key must be 1-512 characters\"}"));
        }

        if (request.method == "GET") {
            metrics_.inc_get();
            return finish(execute_with_resilience("GET", key));
        }
        if (request.method == "PUT") {
            if (request.body.size() > 1024 * 1024) {
                return finish(json_response(413, "{\"error\":\"value exceeds 1 MiB\"}"));
            }
            metrics_.inc_put();
            return finish(execute_with_resilience("PUT", key, request.body));
        }
        if (request.method == "DELETE") {
            metrics_.inc_delete();
            return finish(execute_with_resilience("DELETE", key));
        }
        return finish(json_response(405, "{\"error\":\"method not allowed\"}"));
    }

    if (request.path == "/admin/cluster" && request.method == "GET") {
        return finish(json_response(200, cluster_.status_json()));
    }

    if (parts.size() == 4 && parts[0] == "admin" &&
        (parts[1] == "fail" || parts[1] == "recover") && request.method == "POST") {
        try {
            const auto shard_id = static_cast<std::size_t>(std::stoull(parts[2]));
            const bool ok = parts[1] == "fail"
                ? cluster_.fail_replica(shard_id, parts[3])
                : cluster_.recover_replica(shard_id, parts[3]);
            if (!ok) {
                return finish(json_response(404, "{\"error\":\"replica or shard not found\"}"));
            }
            return finish(json_response(200,
                "{\"status\":\"ok\",\"action\":\"" + json_escape(parts[1]) +
                "\",\"replica\":\"" + json_escape(parts[3]) + "\"}"));
        } catch (...) {
            return finish(json_response(400, "{\"error\":\"invalid shard id\"}"));
        }
    }

    if (parts.size() == 3 && parts[0] == "admin" && parts[1] == "elect" &&
        request.method == "POST") {
        try {
            const auto shard_id = static_cast<std::size_t>(std::stoull(parts[2]));
            if (!cluster_.elect(shard_id)) {
                return finish(json_response(503, "{\"error\":\"leader election failed\"}"));
            }
            return finish(json_response(200, "{\"status\":\"leader elected\"}"));
        } catch (...) {
            return finish(json_response(400, "{\"error\":\"invalid shard id\"}"));
        }
    }

    return finish(json_response(404, "{\"error\":\"route not found\"}"));
}

bool Gateway::authorized(const HttpRequest& request) const {
    const auto it = request.headers.find("authorization");
    if (it == request.headers.end()) {
        return false;
    }
    return it->second == "Bearer " + api_key_;
}

bool Gateway::allowed_by_rate_limit(const std::string& client_id) {
    std::lock_guard lock(rate_mutex_);
    auto [it, inserted] = buckets_.try_emplace(client_id, rate_per_second_, burst_);
    return it->second.consume();
}

CircuitBreaker& Gateway::breaker_for(std::size_t shard_id) {
    return breakers_.at(shard_id);
}

HttpResponse Gateway::json_response(int status, const std::string& body) const {
    HttpResponse response;
    response.status = status;
    response.content_type = "application/json";
    response.body = body;
    return response;
}

HttpResponse Gateway::execute_with_resilience(const std::string& operation,
                                              const std::string& key,
                                              const std::string& value) {
    const auto shard_id = cluster_.shard_for_key(key);
    {
        std::lock_guard lock(breaker_mutex_);
        if (!breaker_for(shard_id).allow_request()) {
            metrics_.inc_circuit_open();
            return json_response(503,
                "{\"error\":\"circuit breaker is open\",\"shard\":" +
                std::to_string(shard_id) + "}");
        }
    }

    OperationResult result;
    for (std::size_t attempt = 0; attempt <= max_retries_; ++attempt) {
        if (operation == "GET") {
            result = cluster_.get(key);
        } else if (operation == "PUT") {
            result = cluster_.put(key, value);
        } else {
            result = cluster_.erase(key);
        }

        if (result.status < 500) {
            std::lock_guard lock(breaker_mutex_);
            breaker_for(shard_id).record_success();
            break;
        }

        {
            std::lock_guard lock(breaker_mutex_);
            breaker_for(shard_id).record_failure();
        }
        if (attempt < max_retries_) {
            metrics_.inc_retry();
            std::this_thread::sleep_for(std::chrono::milliseconds(20 * (1ULL << attempt)));
        }
    }

    if (result.status >= 500) {
        metrics_.inc_gateway_failure();
    }

    std::ostringstream body;
    if (result.ok) {
        body << "{\"status\":\"" << json_escape(result.message) << "\""
             << ",\"key\":\"" << json_escape(key) << "\""
             << ",\"shard\":" << result.shard
             << ",\"leader\":\"" << json_escape(result.leader) << "\"";
        if (operation == "GET") {
            body << ",\"value\":\"" << json_escape(result.value) << "\"";
        }
        body << "}";
    } else {
        body << "{\"error\":\"" << json_escape(result.message) << "\""
             << ",\"key\":\"" << json_escape(key) << "\""
             << ",\"shard\":" << result.shard;
        if (!result.leader.empty()) {
            body << ",\"leader\":\"" << json_escape(result.leader) << "\"";
        }
        body << "}";
    }
    return json_response(result.status, body.str());
}

}  // namespace kv
