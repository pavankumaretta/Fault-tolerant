#include "kv/cluster.hpp"
#include "kv/gateway.hpp"
#include "kv/http_server.hpp"
#include "kv/metrics.hpp"

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

kv::HttpServer* g_server = nullptr;

void handle_signal(int) {
    if (g_server != nullptr) {
        g_server->stop();
    }
}

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* value = std::getenv(name)) {
        return value;
    }
    return fallback;
}

std::size_t env_size_or(const char* name, std::size_t fallback) {
    if (const char* value = std::getenv(name)) {
        return static_cast<std::size_t>(std::stoull(value));
    }
    return fallback;
}

double env_double_or(const char* name, double fallback) {
    if (const char* value = std::getenv(name)) {
        return std::stod(value);
    }
    return fallback;
}

struct Config {
    std::string bind{"0.0.0.0"};
    std::uint16_t port{8080};
    std::filesystem::path data_dir{"./data"};
    std::size_t shards{4};
    std::size_t replicas{3};
    std::string api_key{"dev-secret"};
    double rate_per_second{20.0};
    double burst{40.0};
    std::size_t retries{2};
};

Config parse_config(int argc, char** argv) {
    Config config;
    config.bind = env_or("KV_BIND", config.bind);
    config.port = static_cast<std::uint16_t>(env_size_or("KV_PORT", config.port));
    config.data_dir = env_or("KV_DATA_DIR", config.data_dir.string());
    config.shards = env_size_or("KV_SHARDS", config.shards);
    config.replicas = env_size_or("KV_REPLICAS", config.replicas);
    config.api_key = env_or("KV_API_KEY", config.api_key);
    config.rate_per_second = env_double_or("KV_RATE_PER_SECOND", config.rate_per_second);
    config.burst = env_double_or("KV_BURST", config.burst);
    config.retries = env_size_or("KV_RETRIES", config.retries);

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                throw std::invalid_argument("missing value for " + arg);
            }
            return argv[++i];
        };

        if (arg == "--bind") config.bind = next();
        else if (arg == "--port") config.port = static_cast<std::uint16_t>(std::stoul(next()));
        else if (arg == "--data-dir") config.data_dir = next();
        else if (arg == "--shards") config.shards = std::stoull(next());
        else if (arg == "--replicas") config.replicas = std::stoull(next());
        else if (arg == "--api-key") config.api_key = next();
        else if (arg == "--rate") config.rate_per_second = std::stod(next());
        else if (arg == "--burst") config.burst = std::stod(next());
        else if (arg == "--retries") config.retries = std::stoull(next());
        else if (arg == "--help") {
            std::cout
                << "Fault-Tolerant Sharded KV Store\n\n"
                << "Options:\n"
                << "  --bind ADDRESS       Bind address (default 0.0.0.0)\n"
                << "  --port PORT          HTTP port (default 8080)\n"
                << "  --data-dir PATH      SQLite data directory (default ./data)\n"
                << "  --shards N           Number of shards (default 4)\n"
                << "  --replicas N         Replicas per shard (default 3)\n"
                << "  --api-key KEY        Bearer token (default dev-secret)\n"
                << "  --rate N             Requests/sec per client (default 20)\n"
                << "  --burst N            Token bucket capacity (default 40)\n"
                << "  --retries N          Gateway retries (default 2)\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " + arg);
        }
    }

    if (config.shards == 0 || config.replicas == 0) {
        throw std::invalid_argument("shards and replicas must be positive");
    }
    return config;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const auto config = parse_config(argc, argv);

        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
#ifdef SIGPIPE
        std::signal(SIGPIPE, SIG_IGN);
#endif

        kv::Metrics metrics;
        kv::Cluster cluster(config.shards, config.replicas, config.data_dir, metrics);
        kv::Gateway gateway(
            cluster,
            metrics,
            config.api_key,
            config.rate_per_second,
            config.burst,
            config.retries);

        kv::HttpServer server(
            config.bind,
            config.port,
            [&gateway](const kv::HttpRequest& request) {
                return gateway.handle(request);
            });
        g_server = &server;

        std::cout << "Fault-Tolerant Sharded KV Store\n"
                  << "  listening: http://" << config.bind << ':' << config.port << '\n'
                  << "  shards: " << config.shards << '\n'
                  << "  replicas/shard: " << config.replicas << '\n'
                  << "  data dir: " << config.data_dir << '\n'
                  << "  API key: configured" << '\n'
                  << std::flush;

        server.run();
        g_server = nullptr;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
