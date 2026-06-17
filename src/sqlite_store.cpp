#include "kv/sqlite_store.hpp"

#include <sqlite3.h>

#include <filesystem>
#include <stdexcept>

namespace kv {
namespace {

class Statement {
public:
    Statement(sqlite3* db, const char* sql) : db_(db) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
    }

    ~Statement() {
        if (stmt_ != nullptr) {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt* get() const { return stmt_; }

private:
    sqlite3* db_{nullptr};
    sqlite3_stmt* stmt_{nullptr};
};

void check_step(sqlite3* db, int rc) {
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }
}

}  // namespace

SqliteStore::SqliteStore(const std::string& path) : path_(path) {
    const auto parent = std::filesystem::path(path).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }

    const int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
    if (sqlite3_open_v2(path.c_str(), &db_, flags, nullptr) != SQLITE_OK) {
        const std::string error = db_ ? sqlite3_errmsg(db_) : "unable to open sqlite database";
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
        throw std::runtime_error(error);
    }
    sqlite3_busy_timeout(db_, 5000);
    initialize();
}

SqliteStore::~SqliteStore() {
    if (db_ != nullptr) {
        sqlite3_close(db_);
    }
}

void SqliteStore::initialize() {
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA synchronous=NORMAL;");
    exec("PRAGMA foreign_keys=ON;");
    exec("CREATE TABLE IF NOT EXISTS kv ("
         "key TEXT PRIMARY KEY,"
         "value BLOB NOT NULL,"
         "updated_at INTEGER NOT NULL"
         ");");
    exec("CREATE TABLE IF NOT EXISTS raft_log ("
         "log_index INTEGER PRIMARY KEY,"
         "term INTEGER NOT NULL,"
         "command TEXT NOT NULL,"
         "key TEXT NOT NULL,"
         "value BLOB NOT NULL"
         ");");
    exec("CREATE TABLE IF NOT EXISTS meta ("
         "key TEXT PRIMARY KEY,"
         "value TEXT NOT NULL"
         ");");
}

void SqliteStore::exec(const std::string& sql) const {
    char* error = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        std::string message = error ? error : "sqlite execution failed";
        sqlite3_free(error);
        throw std::runtime_error(message);
    }
}

void SqliteStore::put(const std::string& key, const std::string& value) {
    std::lock_guard lock(mutex_);
    Statement stmt(db_,
        "INSERT INTO kv(key, value, updated_at) VALUES(?, ?, unixepoch()) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;");
    sqlite3_bind_text(stmt.get(), 1, key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt.get(), 2, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
    check_step(db_, sqlite3_step(stmt.get()));
}

void SqliteStore::erase(const std::string& key) {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "DELETE FROM kv WHERE key=?;");
    sqlite3_bind_text(stmt.get(), 1, key.c_str(), -1, SQLITE_TRANSIENT);
    check_step(db_, sqlite3_step(stmt.get()));
}

std::optional<std::string> SqliteStore::get(const std::string& key) const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "SELECT value FROM kv WHERE key=?;");
    sqlite3_bind_text(stmt.get(), 1, key.c_str(), -1, SQLITE_TRANSIENT);
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
        return std::nullopt;
    }
    check_step(db_, rc);
    const auto* data = static_cast<const char*>(sqlite3_column_blob(stmt.get(), 0));
    const int size = sqlite3_column_bytes(stmt.get(), 0);
    return std::string(data ? data : "", static_cast<std::size_t>(size));
}

std::vector<std::pair<std::string, std::string>> SqliteStore::all_kv() const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "SELECT key, value FROM kv ORDER BY key;");
    std::vector<std::pair<std::string, std::string>> result;
    while (true) {
        const int rc = sqlite3_step(stmt.get());
        if (rc == SQLITE_DONE) {
            break;
        }
        check_step(db_, rc);
        const auto* key = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 0));
        const auto* data = static_cast<const char*>(sqlite3_column_blob(stmt.get(), 1));
        const int size = sqlite3_column_bytes(stmt.get(), 1);
        result.emplace_back(key ? key : "", std::string(data ? data : "", static_cast<std::size_t>(size)));
    }
    return result;
}

void SqliteStore::append_log(const LogEntry& entry) {
    std::lock_guard lock(mutex_);
    Statement stmt(db_,
        "INSERT OR REPLACE INTO raft_log(log_index, term, command, key, value) VALUES(?, ?, ?, ?, ?);");
    sqlite3_bind_int64(stmt.get(), 1, entry.index);
    sqlite3_bind_int64(stmt.get(), 2, entry.term);
    sqlite3_bind_text(stmt.get(), 3, entry.command.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 4, entry.key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt.get(), 5, entry.value.data(), static_cast<int>(entry.value.size()), SQLITE_TRANSIENT);
    check_step(db_, sqlite3_step(stmt.get()));
}

void SqliteStore::truncate_log_from(std::int64_t index) {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "DELETE FROM raft_log WHERE log_index >= ?;");
    sqlite3_bind_int64(stmt.get(), 1, index);
    check_step(db_, sqlite3_step(stmt.get()));
}

std::optional<LogEntry> SqliteStore::log_at(std::int64_t index) const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_,
        "SELECT log_index, term, command, key, value FROM raft_log WHERE log_index=?;");
    sqlite3_bind_int64(stmt.get(), 1, index);
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
        return std::nullopt;
    }
    check_step(db_, rc);
    LogEntry entry;
    entry.index = sqlite3_column_int64(stmt.get(), 0);
    entry.term = sqlite3_column_int64(stmt.get(), 1);
    entry.command = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 2));
    entry.key = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 3));
    const auto* data = static_cast<const char*>(sqlite3_column_blob(stmt.get(), 4));
    const int size = sqlite3_column_bytes(stmt.get(), 4);
    entry.value.assign(data ? data : "", static_cast<std::size_t>(size));
    return entry;
}

std::vector<LogEntry> SqliteStore::logs_from(std::int64_t index) const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_,
        "SELECT log_index, term, command, key, value FROM raft_log "
        "WHERE log_index >= ? ORDER BY log_index;");
    sqlite3_bind_int64(stmt.get(), 1, index);
    std::vector<LogEntry> entries;
    while (true) {
        const int rc = sqlite3_step(stmt.get());
        if (rc == SQLITE_DONE) {
            break;
        }
        check_step(db_, rc);
        LogEntry entry;
        entry.index = sqlite3_column_int64(stmt.get(), 0);
        entry.term = sqlite3_column_int64(stmt.get(), 1);
        entry.command = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 2));
        entry.key = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 3));
        const auto* data = static_cast<const char*>(sqlite3_column_blob(stmt.get(), 4));
        const int size = sqlite3_column_bytes(stmt.get(), 4);
        entry.value.assign(data ? data : "", static_cast<std::size_t>(size));
        entries.push_back(std::move(entry));
    }
    return entries;
}

std::int64_t SqliteStore::last_log_index() const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "SELECT COALESCE(MAX(log_index), 0) FROM raft_log;");
    check_step(db_, sqlite3_step(stmt.get()));
    return sqlite3_column_int64(stmt.get(), 0);
}

std::int64_t SqliteStore::last_log_term() const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "SELECT term FROM raft_log ORDER BY log_index DESC LIMIT 1;");
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
        return 0;
    }
    check_step(db_, rc);
    return sqlite3_column_int64(stmt.get(), 0);
}

void SqliteStore::set_meta(const std::string& key, const std::string& value) {
    std::lock_guard lock(mutex_);
    Statement stmt(db_,
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value;");
    sqlite3_bind_text(stmt.get(), 1, key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 2, value.c_str(), -1, SQLITE_TRANSIENT);
    check_step(db_, sqlite3_step(stmt.get()));
}

std::optional<std::string> SqliteStore::get_meta(const std::string& key) const {
    std::lock_guard lock(mutex_);
    Statement stmt(db_, "SELECT value FROM meta WHERE key=?;");
    sqlite3_bind_text(stmt.get(), 1, key.c_str(), -1, SQLITE_TRANSIENT);
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
        return std::nullopt;
    }
    check_step(db_, rc);
    const auto* value = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 0));
    return std::string(value ? value : "");
}

void SqliteStore::clear_all() {
    std::lock_guard lock(mutex_);
    exec("BEGIN IMMEDIATE;");
    try {
        exec("DELETE FROM kv;");
        exec("DELETE FROM raft_log;");
        exec("DELETE FROM meta;");
        exec("COMMIT;");
    } catch (...) {
        exec("ROLLBACK;");
        throw;
    }
}

}  // namespace kv
