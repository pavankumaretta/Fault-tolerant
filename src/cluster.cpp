#include "kv/cluster.hpp"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace kv {

Shard::Shard(std::size_t shard_id,
             std::size_t replica_count,
             const std::filesystem::path& data_dir,
             Metrics& metrics)
    : shard_id_(shard_id), metrics_(metrics) {
    if (replica_count < 1) {
        throw std::invalid_argument("replica_count must be at least 1");
    }
    for (std::size_t i = 0; i < replica_count; ++i) {
        const std::string replica_id = "s" + std::to_string(shard_id_) + "-r" + std::to_string(i);
        const auto db_path = data_dir / (replica_id + ".db");
        replicas_.push_back(std::make_shared<RaftReplica>(replica_id, shard_id_, db_path.string()));
    }
    elect_leader();
}

OperationResult Shard::put(const std::string& key, const std::string& value) {
    std::lock_guard lock(mutex_);
    return replicate_command_locked("PUT", key, value);
}

OperationResult Shard::erase(const std::string& key) {
    std::lock_guard lock(mutex_);
    return replicate_command_locked("DELETE", key, "");
}

OperationResult Shard::get(const std::string& key) {
    std::lock_guard lock(mutex_);
    OperationResult result;
    result.shard = shard_id_;
    if (!ensure_leader_locked()) {
        result.status = 503;
        result.message = "no quorum available for leader election";
        return result;
    }

    const auto leader = current_leader_locked();
    result.leader = leader ? leader->id() : "";
    if (!leader) {
        result.status = 503;
        result.message = "leader unavailable";
        return result;
    }

    const auto value = leader->get(key);
    if (!value) {
        result.status = 404;
        result.message = "key not found";
        return result;
    }

    result.ok = true;
    result.status = 200;
    result.message = "ok";
    result.value = *value;
    return result;
}

bool Shard::fail_replica(const std::string& replica_id) {
    std::lock_guard lock(mutex_);
    for (const auto& replica : replicas_) {
        if (replica->id() == replica_id) {
            replica->set_online(false);
            if (leader_id_ == replica_id) {
                leader_id_.clear();
            }
            log_event("WARN", "replica_failed", replica_id);
            return true;
        }
    }
    return false;
}

bool Shard::recover_replica(const std::string& replica_id) {
    std::lock_guard lock(mutex_);
    std::shared_ptr<RaftReplica> recovered;
    for (const auto& replica : replicas_) {
        if (replica->id() == replica_id) {
            recovered = replica;
            break;
        }
    }
    if (!recovered) {
        return false;
    }

    recovered->set_online(true);
    if (ensure_leader_locked()) {
        const auto leader = current_leader_locked();
        if (leader && leader->id() != recovered->id()) {
            sync_follower_locked(recovered, leader);
        }
    }
    log_event("INFO", "replica_recovered", replica_id);
    return true;
}

bool Shard::elect_leader() {
    std::lock_guard lock(mutex_);
    return ensure_leader_locked();
}

std::string Shard::leader_id() const {
    std::lock_guard lock(mutex_);
    return leader_id_;
}

std::size_t Shard::id() const { return shard_id_; }

std::string Shard::status_json() const {
    std::lock_guard lock(mutex_);
    std::ostringstream out;
    out << "{\"shard\":" << shard_id_
        << ",\"leader\":\"" << json_escape(leader_id_) << "\""
        << ",\"replicas\":[";
    for (std::size_t i = 0; i < replicas_.size(); ++i) {
        if (i > 0) out << ',';
        out << replicas_[i]->status_json();
    }
    out << "]}";
    return out.str();
}

std::shared_ptr<RaftReplica> Shard::current_leader_locked() const {
    for (const auto& replica : replicas_) {
        if (replica->id() == leader_id_ && replica->is_online()) {
            return replica;
        }
    }
    return nullptr;
}

bool Shard::ensure_leader_locked() {
    if (current_leader_locked()) {
        return true;
    }

    const std::size_t quorum = replicas_.size() / 2 + 1;
    std::vector<std::shared_ptr<RaftReplica>> online;
    for (const auto& replica : replicas_) {
        if (replica->is_online()) {
            online.push_back(replica);
        }
    }
    if (online.size() < quorum) {
        leader_id_.clear();
        return false;
    }

    auto candidate = *std::max_element(online.begin(), online.end(), [](const auto& a, const auto& b) {
        if (a->commit_index() != b->commit_index()) {
            return a->commit_index() < b->commit_index();
        }
        if (a->last_log_term() != b->last_log_term()) {
            return a->last_log_term() < b->last_log_term();
        }
        return a->last_log_index() < b->last_log_index();
    });

    std::int64_t next_term = 1;
    for (const auto& replica : replicas_) {
        next_term = std::max(next_term, replica->current_term() + 1);
    }

    candidate->set_role(Role::Candidate);
    const auto candidate_last_index = candidate->last_log_index();
    const auto candidate_last_term = candidate->last_log_term();

    std::size_t votes = 0;
    for (const auto& replica : online) {
        if (replica->request_vote(next_term,
                                  candidate->id(),
                                  candidate_last_index,
                                  candidate_last_term)) {
            ++votes;
        }
    }

    if (votes < quorum) {
        candidate->set_role(Role::Follower);
        leader_id_.clear();
        return false;
    }

    for (const auto& replica : replicas_) {
        replica->set_role(Role::Follower);
    }
    candidate->set_role(Role::Leader);
    leader_id_ = candidate->id();

    for (const auto& replica : online) {
        if (replica->id() != candidate->id()) {
            sync_follower_locked(replica, candidate);
        }
    }

    metrics_.inc_election();
    log_event("INFO", "leader_elected",
              "shard=" + std::to_string(shard_id_) + ",leader=" + leader_id_);
    return true;
}

OperationResult Shard::replicate_command_locked(const std::string& command,
                                                 const std::string& key,
                                                 const std::string& value) {
    OperationResult result;
    result.shard = shard_id_;

    if (!ensure_leader_locked()) {
        result.status = 503;
        result.message = "no quorum available";
        return result;
    }

    const auto leader = current_leader_locked();
    if (!leader) {
        result.status = 503;
        result.message = "leader unavailable";
        return result;
    }
    result.leader = leader->id();

    const std::size_t quorum = replicas_.size() / 2 + 1;
    const auto entry = leader->append_local(command, key, value);
    const auto prev_index = entry.index - 1;
    const auto prev_term = leader->term_at(prev_index);

    std::size_t acknowledgements = 1;
    std::vector<std::shared_ptr<RaftReplica>> acknowledged_followers;

    for (const auto& follower : replicas_) {
        if (follower->id() == leader->id() || !follower->is_online()) {
            continue;
        }

        if (follower->commit_index() != leader->commit_index() ||
            follower->last_log_index() != leader->commit_index()) {
            sync_follower_locked(follower, leader);
        }

        const bool accepted = follower->append_entries(
            leader->current_term(),
            leader->id(),
            prev_index,
            prev_term,
            std::vector<LogEntry>{entry},
            leader->commit_index());

        if (accepted) {
            ++acknowledgements;
            acknowledged_followers.push_back(follower);
        } else {
            metrics_.inc_replication_failure();
        }
    }

    if (acknowledgements < quorum) {
        leader->discard_uncommitted();
        for (const auto& follower : acknowledged_followers) {
            follower->discard_uncommitted();
        }
        result.status = 503;
        result.message = "write rejected because a majority could not acknowledge the log entry";
        return result;
    }

    leader->commit_to(entry.index);
    for (const auto& follower : acknowledged_followers) {
        follower->append_entries(
            leader->current_term(),
            leader->id(),
            entry.index,
            entry.term,
            {},
            entry.index);
    }

    result.ok = true;
    result.status = command == "DELETE" ? 200 : 201;
    result.message = command == "DELETE" ? "deleted" : "stored";
    return result;
}

void Shard::sync_follower_locked(const std::shared_ptr<RaftReplica>& follower,
                                  const std::shared_ptr<RaftReplica>& leader) {
    if (!follower || !leader || !follower->is_online()) {
        return;
    }
    follower->install_snapshot(leader->export_snapshot());
}

Cluster::Cluster(std::size_t shard_count,
                 std::size_t replicas_per_shard,
                 const std::filesystem::path& data_dir,
                 Metrics& metrics) {
    if (shard_count < 1) {
        throw std::invalid_argument("shard_count must be at least 1");
    }
    std::filesystem::create_directories(data_dir);
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Shard>(i, replicas_per_shard, data_dir, metrics));
    }
}

std::size_t Cluster::shard_for_key(const std::string& key) const {
    return static_cast<std::size_t>(fnv1a_64(key) % shards_.size());
}

OperationResult Cluster::put(const std::string& key, const std::string& value) {
    return shards_[shard_for_key(key)]->put(key, value);
}

OperationResult Cluster::erase(const std::string& key) {
    return shards_[shard_for_key(key)]->erase(key);
}

OperationResult Cluster::get(const std::string& key) {
    return shards_[shard_for_key(key)]->get(key);
}

bool Cluster::fail_replica(std::size_t shard_id, const std::string& replica_id) {
    return shard_id < shards_.size() && shards_[shard_id]->fail_replica(replica_id);
}

bool Cluster::recover_replica(std::size_t shard_id, const std::string& replica_id) {
    return shard_id < shards_.size() && shards_[shard_id]->recover_replica(replica_id);
}

bool Cluster::elect(std::size_t shard_id) {
    return shard_id < shards_.size() && shards_[shard_id]->elect_leader();
}

std::string Cluster::status_json() const {
    std::ostringstream out;
    out << "{\"shard_count\":" << shards_.size() << ",\"shards\":[";
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        if (i > 0) out << ',';
        out << shards_[i]->status_json();
    }
    out << "]}";
    return out.str();
}

std::size_t Cluster::shard_count() const { return shards_.size(); }

}  // namespace kv
