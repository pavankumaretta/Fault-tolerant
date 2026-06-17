#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

struct sqlite3;

namespace kv {

struct LogEntry {
    std::int64_t index{0};
    std::int64_t term{0};
    std::string command;
    std::string key;
    std::string value;
};

class SqliteStore {
public:
    explicit SqliteStore(const std::string& path);
    ~SqliteStore();

    SqliteStore(const SqliteStore&) = delete;
    SqliteStore& operator=(const SqliteStore&) = delete;

    void put(const std::string& key, const std::string& value);
    void erase(const std::string& key);
    std::optional<std::string> get(const std::string& key) const;
    std::vector<std::pair<std::string, std::string>> all_kv() const;

    void append_log(const LogEntry& entry);
    void truncate_log_from(std::int64_t index);
    std::optional<LogEntry> log_at(std::int64_t index) const;
    std::vector<LogEntry> logs_from(std::int64_t index) const;
    std::int64_t last_log_index() const;
    std::int64_t last_log_term() const;

    void set_meta(const std::string& key, const std::string& value);
    std::optional<std::string> get_meta(const std::string& key) const;

    void clear_all();

private:
    void exec(const std::string& sql) const;
    void initialize();

    sqlite3* db_{nullptr};
    std::string path_;
    mutable std::mutex mutex_;
};

}  // namespace kv
