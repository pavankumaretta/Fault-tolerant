#include "kv/cluster.hpp"
#include "kv/metrics.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

std::filesystem::path temp_path(const std::string& name) {
    const auto path = std::filesystem::temp_directory_path() /
        ("kvstore-" + name + "-" + std::to_string(kv::now_ms()));
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

void test_put_get_delete_and_sharding() {
    const auto dir = temp_path("basic");
    kv::Metrics metrics;
    kv::Cluster cluster(4, 3, dir, metrics);

    const auto put = cluster.put("customer:42", "active");
    assert(put.ok);
    assert(put.status == 201);
    assert(put.shard == cluster.shard_for_key("customer:42"));

    const auto get = cluster.get("customer:42");
    assert(get.ok);
    assert(get.value == "active");

    const auto missing = cluster.get("does-not-exist");
    assert(!missing.ok);
    assert(missing.status == 404);

    const auto erased = cluster.erase("customer:42");
    assert(erased.ok);
    assert(cluster.get("customer:42").status == 404);

    std::filesystem::remove_all(dir);
}

void test_leader_failover_and_quorum() {
    const auto dir = temp_path("failover");
    kv::Metrics metrics;
    kv::Cluster cluster(1, 3, dir, metrics);

    assert(cluster.put("session", "v1").ok);

    // The deterministic initial election chooses r0 when all logs are equal.
    assert(cluster.fail_replica(0, "s0-r0"));
    const auto after_leader_failure = cluster.get("session");
    assert(after_leader_failure.ok);
    assert(after_leader_failure.value == "v1");
    assert(after_leader_failure.leader != "s0-r0");

    // With only one replica online, a majority write must be rejected.
    assert(cluster.fail_replica(0, "s0-r1"));
    const auto rejected = cluster.put("session", "v2");
    assert(!rejected.ok);
    assert(rejected.status == 503);

    // Recover a quorum; the recovered node is synchronized from the leader.
    assert(cluster.recover_replica(0, "s0-r0"));
    const auto accepted = cluster.put("session", "v3");
    assert(accepted.ok);
    assert(cluster.get("session").value == "v3");

    std::filesystem::remove_all(dir);
}

void test_sqlite_persistence_across_restart() {
    const auto dir = temp_path("persistence");
    {
        kv::Metrics metrics;
        kv::Cluster cluster(2, 3, dir, metrics);
        assert(cluster.put("persistent-key", "persistent-value").ok);
    }
    {
        kv::Metrics metrics;
        kv::Cluster cluster(2, 3, dir, metrics);
        const auto result = cluster.get("persistent-key");
        assert(result.ok);
        assert(result.value == "persistent-value");
    }
    std::filesystem::remove_all(dir);
}

}  // namespace

int main() {
    test_put_get_delete_and_sharding();
    test_leader_failover_and_quorum();
    test_sqlite_persistence_across_restart();
    std::cout << "All unit tests passed.\n";
    return 0;
}
