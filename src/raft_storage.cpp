#include "atlas/raft_storage.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

namespace atlas::raft {
namespace {

constexpr std::uint8_t kMagic[] = {'A', 'T', 'L', 'R', 'F', 'T', '0', '1'};
constexpr std::uint32_t kVersion = 1;
constexpr std::uint64_t kMaxFileBytes = 256ULL * 1024ULL * 1024ULL;
std::atomic<std::uint64_t> g_temp_sequence{1};

void put_u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<std::uint8_t>(value >> shift));
}
void put_u64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<std::uint8_t>(value >> shift));
}
std::uint32_t get_u32(const std::uint8_t* data) {
    return (static_cast<std::uint32_t>(data[0]) << 24) |
           (static_cast<std::uint32_t>(data[1]) << 16) |
           (static_cast<std::uint32_t>(data[2]) << 8) | data[3];
}
std::uint64_t get_u64(const std::uint8_t* data) {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) value = (value << 8) | data[i];
    return value;
}
std::uint64_t checksum(const std::uint8_t* data, std::size_t size) {
    std::uint64_t hash = 14695981039346656037ULL;
    for (std::size_t i = 0; i < size; ++i) { hash ^= data[i]; hash *= 1099511628211ULL; }
    return hash;
}
void sync_fd(int fd, const char* what) {
    while (::fsync(fd) != 0) {
        if (errno == EINTR) continue;
        throw RaftError(std::string(what) + ": " + std::strerror(errno));
    }
}
void write_all(int fd, const std::uint8_t* bytes, std::size_t size) {
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t result = ::write(fd, bytes + offset, size - offset);
        if (result < 0) { if (errno == EINTR) continue; throw RaftError("Raft storage write: " + std::string(std::strerror(errno))); }
        if (result == 0) throw RaftError("Raft storage write made no progress");
        offset += static_cast<std::size_t>(result);
    }
}

std::vector<std::uint8_t> serialize(const PersistentState& state, const std::vector<LogEntry>& log,
                                    LogIndex commit_index, const std::vector<RequestOutcome>& outcomes) {
    if (commit_index > log.size()) throw RaftError("commit index exceeds log length");
    std::vector<std::uint8_t> bytes;
    bytes.insert(bytes.end(), std::begin(kMagic), std::end(kMagic));
    put_u32(bytes, kVersion);
    put_u64(bytes, state.current_term);
    bytes.push_back(state.voted_for.has_value() ? 1 : 0);
    put_u64(bytes, state.voted_for.value_or(0));
    put_u64(bytes, commit_index);
    put_u64(bytes, log.size());
    for (const auto& entry : log) {
        if (entry.command.size() > std::numeric_limits<std::uint32_t>::max()) throw RaftError("Raft command too large");
        put_u64(bytes, entry.term);
        put_u32(bytes, static_cast<std::uint32_t>(entry.command.size()));
        bytes.insert(bytes.end(), entry.command.begin(), entry.command.end());
        if (bytes.size() > kMaxFileBytes - 8) throw RaftError("Raft storage file exceeds configured maximum");
    }
    put_u64(bytes, outcomes.size());
    for (const auto& outcome : outcomes) {
        if (outcome.request_fingerprint.size() > UINT16_MAX || outcome.encoded_response.size() > UINT16_MAX)
            throw RaftError("Raft request outcome too large");
        put_u64(bytes, outcome.client_id); put_u64(bytes, outcome.request_id);
        put_u32(bytes, static_cast<std::uint32_t>(outcome.request_fingerprint.size()));
        bytes.insert(bytes.end(), outcome.request_fingerprint.begin(), outcome.request_fingerprint.end());
        put_u32(bytes, static_cast<std::uint32_t>(outcome.encoded_response.size()));
        bytes.insert(bytes.end(), outcome.encoded_response.begin(), outcome.encoded_response.end());
    }
    put_u64(bytes, checksum(bytes.data(), bytes.size()));
    if (bytes.size() > kMaxFileBytes) throw RaftError("Raft storage file exceeds configured maximum");
    return bytes;
}

void sync_directory(const std::filesystem::path& directory) {
    const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) throw RaftError("open Raft storage directory: " + std::string(std::strerror(errno)));
    try { sync_fd(fd, "sync Raft storage directory"); }
    catch (...) { ::close(fd); throw; }
    if (::close(fd) != 0) throw RaftError("close Raft storage directory: " + std::string(std::strerror(errno)));
}

} // namespace

FilePersistentStorage::FilePersistentStorage(std::filesystem::path directory,
                                                                                         FileStorePublicationHook publication_hook)
        : directory_(std::move(directory)), file_path_(directory_ / "raft-state.bin"),
            publication_hook_(std::move(publication_hook)) {
    if (directory_.empty()) throw RaftError("Raft storage directory must be configured");
    std::filesystem::create_directories(directory_);
    std::lock_guard lock(mutex_);
    load_locked();
}

PersistentState FilePersistentStorage::load() { std::lock_guard lock(mutex_); return state_; }
std::vector<LogEntry> FilePersistentStorage::load_log() { std::lock_guard lock(mutex_); return log_; }
LogIndex FilePersistentStorage::load_commit_index() { std::lock_guard lock(mutex_); return commit_index_; }
std::vector<RequestOutcome> FilePersistentStorage::load_request_outcomes() { std::lock_guard lock(mutex_); return outcomes_; }

void FilePersistentStorage::save_request_outcome(const RequestOutcome& outcome) {
    std::lock_guard lock(mutex_);
    for (const auto& existing : outcomes_) {
        if (existing.client_id == outcome.client_id && existing.request_id == outcome.request_id) {
            if (existing != outcome) throw RaftError("request outcome key conflicts with durable record");
            return;
        }
    }
    outcomes_.push_back(outcome);
    try { publish_locked(); } catch (...) { outcomes_.pop_back(); throw; }
}

void FilePersistentStorage::save(const PersistentState& state) {
    std::lock_guard lock(mutex_);
    const auto previous = state_;
    state_ = state;
    try { publish_locked(); } catch (...) { state_ = previous; throw; }
}
void FilePersistentStorage::save_log(const std::vector<LogEntry>& entries) {
    std::lock_guard lock(mutex_);
    if (commit_index_ > entries.size()) throw RaftError("cannot truncate committed Raft log entries");
    auto previous = log_;
    log_ = entries;
    try { publish_locked(); } catch (...) { log_.swap(previous); throw; }
}
void FilePersistentStorage::save_commit_index(LogIndex commit_index) {
    std::lock_guard lock(mutex_);
    if (commit_index < commit_index_ || commit_index > log_.size())
        throw RaftError("invalid Raft commit-index update");
    const auto previous = commit_index_;
    commit_index_ = commit_index;
    try { publish_locked(); } catch (...) { commit_index_ = previous; throw; }
}

void FilePersistentStorage::load_locked() {
    if (!std::filesystem::exists(file_path_)) return;
    std::ifstream input(file_path_, std::ios::binary);
    if (!input) throw RaftError("cannot open Raft storage file");
    input.seekg(0, std::ios::end);
    const auto end = input.tellg();
    if (end < 0 || static_cast<std::uint64_t>(end) > kMaxFileBytes || end < 8 + 4 + 8 + 1 + 8 + 8 + 8 + 8)
        throw RaftError("invalid Raft storage file length");
    input.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(end));
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!input) throw RaftError("truncated Raft storage file");
    if (!std::equal(std::begin(kMagic), std::end(kMagic), bytes.begin()) || get_u32(bytes.data() + 8) != kVersion)
        throw RaftError("unsupported Raft storage format");
    if (get_u64(bytes.data() + bytes.size() - 8) != checksum(bytes.data(), bytes.size() - 8))
        throw RaftError("Raft storage checksum mismatch");
    std::size_t offset = 12;
    state_.current_term = get_u64(bytes.data() + offset); offset += 8;
    const auto has_vote = bytes[offset++];
    const auto voted_for = get_u64(bytes.data() + offset); offset += 8;
    if (has_vote > 1 || (has_vote == 0 && voted_for != 0) || (has_vote == 1 && voted_for == 0))
        throw RaftError("invalid persisted Raft vote");
    if (has_vote) state_.voted_for = voted_for;
    commit_index_ = get_u64(bytes.data() + offset); offset += 8;
    const std::uint64_t count = get_u64(bytes.data() + offset); offset += 8;
    if (count > (bytes.size() - offset - 8) / 12) throw RaftError("invalid Raft log entry count");
    log_.reserve(static_cast<std::size_t>(count));
    for (std::uint64_t i = 0; i < count; ++i) {
        if (offset + 12 > bytes.size() - 8) throw RaftError("truncated Raft log entry header");
        LogEntry entry;
        entry.term = get_u64(bytes.data() + offset); offset += 8;
        const auto length = get_u32(bytes.data() + offset); offset += 4;
        if (length > bytes.size() - 8 - offset) throw RaftError("truncated Raft command payload");
        entry.command.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                             bytes.begin() + static_cast<std::ptrdiff_t>(offset + length));
        offset += length;
        if (entry.term == 0 || entry.term > state_.current_term ||
            (!log_.empty() && entry.term < log_.back().term)) throw RaftError("invalid persisted Raft log term");
        log_.push_back(std::move(entry));
    }
    if (offset + 8 > bytes.size() - 8) throw RaftError("truncated Raft request outcomes");
    const std::uint64_t outcome_count = get_u64(bytes.data() + offset); offset += 8;
    if (outcome_count > (bytes.size() - 8 - offset) / 24) throw RaftError("invalid Raft request outcome count");
    for (std::uint64_t i = 0; i < outcome_count; ++i) {
        if (offset + 20 > bytes.size() - 8) throw RaftError("truncated Raft request outcome");
        RequestOutcome outcome;
        outcome.client_id = get_u64(bytes.data() + offset); offset += 8;
        outcome.request_id = get_u64(bytes.data() + offset); offset += 8;
        const auto fingerprint_length = get_u32(bytes.data() + offset); offset += 4;
        if (fingerprint_length > bytes.size() - 8 - offset) throw RaftError("truncated Raft request fingerprint");
        outcome.request_fingerprint.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                            bytes.begin() + static_cast<std::ptrdiff_t>(offset + fingerprint_length));
        offset += fingerprint_length;
        if (offset + 4 > bytes.size() - 8) throw RaftError("truncated Raft response length");
        const auto response_length = get_u32(bytes.data() + offset); offset += 4;
        if (response_length > bytes.size() - 8 - offset) throw RaftError("truncated Raft response");
        outcome.encoded_response.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                        bytes.begin() + static_cast<std::ptrdiff_t>(offset + response_length));
        offset += response_length;
        outcomes_.push_back(std::move(outcome));
    }
    if (offset != bytes.size() - 8 || commit_index_ > log_.size()) throw RaftError("Raft storage length/index mismatch");
}

void FilePersistentStorage::publish_locked() {
    const auto bytes = serialize(state_, log_, commit_index_, outcomes_);
    const auto temp = directory_ / ("raft-state.tmp." + std::to_string(static_cast<unsigned long long>(::getpid())) +
                                    "." + std::to_string(g_temp_sequence.fetch_add(1)));
    const int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) throw RaftError("create Raft storage temporary: " + std::string(std::strerror(errno)));
    bool exists = true;
    try {
        if (publication_hook_) publication_hook_(FileStorePublicationStage::TemporaryCreated);
        write_all(fd, bytes.data(), bytes.size());
        if (publication_hook_) publication_hook_(FileStorePublicationStage::BytesWritten);
        sync_fd(fd, "sync Raft state file");
        if (publication_hook_) publication_hook_(FileStorePublicationStage::FileSynced);
        if (::close(fd) != 0) throw RaftError("close Raft state file: " + std::string(std::strerror(errno)));
        if (publication_hook_) publication_hook_(FileStorePublicationStage::BeforeRename);
        std::error_code error;
        std::filesystem::rename(temp, file_path_, error);
        if (error) throw RaftError("publish Raft state: " + error.message());
        exists = false;
        if (publication_hook_) publication_hook_(FileStorePublicationStage::Renamed);
        sync_directory(directory_);
        if (publication_hook_) publication_hook_(FileStorePublicationStage::DirectorySynced);
    } catch (...) {
        if (exists) { ::close(fd); std::error_code ignored; std::filesystem::remove(temp, ignored); }
        throw;
    }
}

} // namespace atlas::raft
