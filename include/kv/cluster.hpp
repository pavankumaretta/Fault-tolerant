#pragma once

#include "kv/common.hpp"
#include "kv/metrics.hpp"
#include "kv/raft.hpp"

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace kv {

class Shard {
public:
    Shard(std::size_t shard_id,
          std::size_t replica_count,
          const std::filesystem::path& data_dir,
          Metrics& metrics);

    OperationResult put(const std::string& key, const std::string& value);
    OperationResult erase(const std::string& key);
    OperationResult get(const std::string& key);

    bool fail_replica(const std::string& replica_id);
    bool recover_replica(const std::string& replica_id);
    bool elect_leader();

    std::string leader_id() const;
    std::size_t id() const;
    std::string status_json() const;

private:
    std::shared_ptr<RaftReplica> current_leader_locked() const;
    bool ensure_leader_locked();
    OperationResult replicate_command_locked(const std::string& command,
                                              const std::string& key,
                                              const std::string& value);
    void sync_follower_locked(const std::shared_ptr<RaftReplica>& follower,
                              const std::shared_ptr<RaftReplica>& leader);

    std::size_t shard_id_;
    std::vector<std::shared_ptr<RaftReplica>> replicas_;
    std::string leader_id_;
    Metrics& metrics_;
    mutable std::mutex mutex_;
};

class Cluster {
public:
    Cluster(std::size_t shard_count,
            std::size_t replicas_per_shard,
            const std::filesystem::path& data_dir,
            Metrics& metrics);

    std::size_t shard_for_key(const std::string& key) const;

    OperationResult put(const std::string& key, const std::string& value);
    OperationResult erase(const std::string& key);
    OperationResult get(const std::string& key);

    bool fail_replica(std::size_t shard_id, const std::string& replica_id);
    bool recover_replica(std::size_t shard_id, const std::string& replica_id);
    bool elect(std::size_t shard_id);

    std::string status_json() const;
    std::size_t shard_count() const;

private:
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace kv
