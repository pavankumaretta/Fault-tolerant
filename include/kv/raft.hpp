#pragma once

#include "kv/sqlite_store.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace kv {

enum class Role { Follower, Candidate, Leader };

std::string role_to_string(Role role);

struct ReplicaSnapshot {
    std::int64_t term{0};
    std::int64_t commit_index{0};
    std::vector<LogEntry> log;
};

class RaftReplica {
public:
    RaftReplica(std::string id, std::size_t shard_id, const std::string& db_path);

    const std::string& id() const;
    std::size_t shard_id() const;

    bool is_online() const;
    void set_online(bool online);

    Role role() const;
    void set_role(Role role);

    std::int64_t current_term() const;
    std::int64_t commit_index() const;
    std::int64_t last_log_index() const;
    std::int64_t last_log_term() const;
    std::int64_t term_at(std::int64_t index) const;
    std::vector<LogEntry> logs_from(std::int64_t index) const;

    bool request_vote(std::int64_t term,
                      const std::string& candidate_id,
                      std::int64_t candidate_last_index,
                      std::int64_t candidate_last_term);

    bool append_entries(std::int64_t term,
                        const std::string& leader_id,
                        std::int64_t prev_log_index,
                        std::int64_t prev_log_term,
                        const std::vector<LogEntry>& entries,
                        std::int64_t leader_commit);

    LogEntry append_local(const std::string& command,
                          const std::string& key,
                          const std::string& value);

    void commit_to(std::int64_t index);
    void discard_uncommitted();
    std::optional<std::string> get(const std::string& key) const;

    ReplicaSnapshot export_snapshot() const;
    void install_snapshot(const ReplicaSnapshot& snapshot);

    std::string status_json() const;

private:
    void persist_state();
    void load_state();
    void apply_entry(const LogEntry& entry);

    std::string id_;
    std::size_t shard_id_;
    std::unique_ptr<SqliteStore> store_;

    mutable std::mutex mutex_;
    Role role_{Role::Follower};
    bool online_{true};
    std::int64_t current_term_{0};
    std::string voted_for_;
    std::int64_t commit_index_{0};
    std::int64_t last_applied_{0};
};

}  // namespace kv
