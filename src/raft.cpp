#include "kv/raft.hpp"

#include "kv/common.hpp"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace kv {

std::string role_to_string(Role role) {
    switch (role) {
        case Role::Follower: return "follower";
        case Role::Candidate: return "candidate";
        case Role::Leader: return "leader";
    }
    return "unknown";
}

RaftReplica::RaftReplica(std::string id,
                         std::size_t shard_id,
                         const std::string& db_path)
    : id_(std::move(id)),
      shard_id_(shard_id),
      store_(std::make_unique<SqliteStore>(db_path)) {
    load_state();
}

const std::string& RaftReplica::id() const { return id_; }
std::size_t RaftReplica::shard_id() const { return shard_id_; }

bool RaftReplica::is_online() const {
    std::lock_guard lock(mutex_);
    return online_;
}

void RaftReplica::set_online(bool online) {
    std::lock_guard lock(mutex_);
    online_ = online;
    if (!online_) {
        role_ = Role::Follower;
    }
}

Role RaftReplica::role() const {
    std::lock_guard lock(mutex_);
    return role_;
}

void RaftReplica::set_role(Role role) {
    std::lock_guard lock(mutex_);
    role_ = role;
}

std::int64_t RaftReplica::current_term() const {
    std::lock_guard lock(mutex_);
    return current_term_;
}

std::int64_t RaftReplica::commit_index() const {
    std::lock_guard lock(mutex_);
    return commit_index_;
}

std::int64_t RaftReplica::last_log_index() const {
    std::lock_guard lock(mutex_);
    return store_->last_log_index();
}

std::int64_t RaftReplica::last_log_term() const {
    std::lock_guard lock(mutex_);
    return store_->last_log_term();
}

std::int64_t RaftReplica::term_at(std::int64_t index) const {
    std::lock_guard lock(mutex_);
    if (index == 0) {
        return 0;
    }
    const auto entry = store_->log_at(index);
    return entry ? entry->term : -1;
}

std::vector<LogEntry> RaftReplica::logs_from(std::int64_t index) const {
    std::lock_guard lock(mutex_);
    return store_->logs_from(index);
}

bool RaftReplica::request_vote(std::int64_t term,
                               const std::string& candidate_id,
                               std::int64_t candidate_last_index,
                               std::int64_t candidate_last_term) {
    std::lock_guard lock(mutex_);
    if (!online_ || term < current_term_) {
        return false;
    }

    if (term > current_term_) {
        current_term_ = term;
        voted_for_.clear();
        role_ = Role::Follower;
    }

    const auto own_last_term = store_->last_log_term();
    const auto own_last_index = store_->last_log_index();
    const bool candidate_is_current =
        candidate_last_term > own_last_term ||
        (candidate_last_term == own_last_term && candidate_last_index >= own_last_index);

    if (candidate_is_current && (voted_for_.empty() || voted_for_ == candidate_id)) {
        voted_for_ = candidate_id;
        persist_state();
        return true;
    }
    persist_state();
    return false;
}

bool RaftReplica::append_entries(std::int64_t term,
                                 const std::string& leader_id,
                                 std::int64_t prev_log_index,
                                 std::int64_t prev_log_term,
                                 const std::vector<LogEntry>& entries,
                                 std::int64_t leader_commit) {
    std::lock_guard lock(mutex_);
    if (!online_ || term < current_term_) {
        return false;
    }

    if (term > current_term_) {
        current_term_ = term;
        voted_for_.clear();
    }
    role_ = Role::Follower;
    voted_for_ = leader_id;

    if (prev_log_index > 0) {
        const auto previous = store_->log_at(prev_log_index);
        if (!previous || previous->term != prev_log_term) {
            persist_state();
            return false;
        }
    }

    for (const auto& entry : entries) {
        const auto existing = store_->log_at(entry.index);
        if (existing && existing->term != entry.term) {
            store_->truncate_log_from(entry.index);
        }
        if (!store_->log_at(entry.index)) {
            store_->append_log(entry);
        }
    }

    const auto last_index = store_->last_log_index();
    const auto new_commit = std::min(leader_commit, last_index);
    if (new_commit > commit_index_) {
        for (std::int64_t index = last_applied_ + 1; index <= new_commit; ++index) {
            const auto entry = store_->log_at(index);
            if (!entry) {
                break;
            }
            apply_entry(*entry);
            last_applied_ = index;
        }
        commit_index_ = std::max(commit_index_, new_commit);
    }

    persist_state();
    return true;
}

LogEntry RaftReplica::append_local(const std::string& command,
                                   const std::string& key,
                                   const std::string& value) {
    std::lock_guard lock(mutex_);
    if (!online_ || role_ != Role::Leader) {
        throw std::runtime_error("append_local requires an online leader");
    }
    LogEntry entry;
    entry.index = store_->last_log_index() + 1;
    entry.term = current_term_;
    entry.command = command;
    entry.key = key;
    entry.value = value;
    store_->append_log(entry);
    return entry;
}

void RaftReplica::commit_to(std::int64_t index) {
    std::lock_guard lock(mutex_);
    if (!online_) {
        return;
    }
    const auto target = std::min(index, store_->last_log_index());
    for (std::int64_t i = last_applied_ + 1; i <= target; ++i) {
        const auto entry = store_->log_at(i);
        if (!entry) {
            break;
        }
        apply_entry(*entry);
        last_applied_ = i;
    }
    commit_index_ = std::max(commit_index_, target);
    persist_state();
}

void RaftReplica::discard_uncommitted() {
    std::lock_guard lock(mutex_);
    store_->truncate_log_from(commit_index_ + 1);
}

std::optional<std::string> RaftReplica::get(const std::string& key) const {
    std::lock_guard lock(mutex_);
    if (!online_) {
        return std::nullopt;
    }
    return store_->get(key);
}

ReplicaSnapshot RaftReplica::export_snapshot() const {
    std::lock_guard lock(mutex_);
    ReplicaSnapshot snapshot;
    snapshot.term = current_term_;
    snapshot.commit_index = commit_index_;
    snapshot.log = store_->logs_from(1);
    if (snapshot.log.size() > static_cast<std::size_t>(commit_index_)) {
        snapshot.log.resize(static_cast<std::size_t>(commit_index_));
    }
    return snapshot;
}

void RaftReplica::install_snapshot(const ReplicaSnapshot& snapshot) {
    std::lock_guard lock(mutex_);
    if (!online_) {
        return;
    }
    store_->clear_all();
    current_term_ = std::max(current_term_, snapshot.term);
    voted_for_.clear();
    role_ = Role::Follower;
    commit_index_ = 0;
    last_applied_ = 0;
    for (const auto& entry : snapshot.log) {
        store_->append_log(entry);
    }
    const auto target = std::min<std::int64_t>(snapshot.commit_index, store_->last_log_index());
    for (std::int64_t i = 1; i <= target; ++i) {
        const auto entry = store_->log_at(i);
        if (entry) {
            apply_entry(*entry);
            last_applied_ = i;
        }
    }
    commit_index_ = target;
    persist_state();
}

std::string RaftReplica::status_json() const {
    std::lock_guard lock(mutex_);
    std::ostringstream out;
    out << "{\"id\":\"" << json_escape(id_) << "\""
        << ",\"online\":" << (online_ ? "true" : "false")
        << ",\"role\":\"" << role_to_string(role_) << "\""
        << ",\"term\":" << current_term_
        << ",\"commit_index\":" << commit_index_
        << ",\"last_log_index\":" << store_->last_log_index()
        << "}";
    return out.str();
}

void RaftReplica::persist_state() {
    store_->set_meta("current_term", std::to_string(current_term_));
    store_->set_meta("voted_for", voted_for_);
    store_->set_meta("commit_index", std::to_string(commit_index_));
    store_->set_meta("last_applied", std::to_string(last_applied_));
}

void RaftReplica::load_state() {
    if (const auto value = store_->get_meta("current_term")) {
        current_term_ = std::stoll(*value);
    }
    if (const auto value = store_->get_meta("voted_for")) {
        voted_for_ = *value;
    }
    if (const auto value = store_->get_meta("commit_index")) {
        commit_index_ = std::stoll(*value);
    }
    if (const auto value = store_->get_meta("last_applied")) {
        last_applied_ = std::stoll(*value);
    }
    if (last_applied_ < commit_index_) {
        for (std::int64_t i = last_applied_ + 1; i <= commit_index_; ++i) {
            if (const auto entry = store_->log_at(i)) {
                apply_entry(*entry);
                last_applied_ = i;
            }
        }
        persist_state();
    }
}

void RaftReplica::apply_entry(const LogEntry& entry) {
    if (entry.command == "PUT") {
        store_->put(entry.key, entry.value);
    } else if (entry.command == "DELETE") {
        store_->erase(entry.key);
    } else {
        throw std::runtime_error("unknown raft command: " + entry.command);
    }
}

}  // namespace kv
