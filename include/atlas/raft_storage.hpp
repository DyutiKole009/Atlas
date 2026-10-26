#pragma once

#include "atlas/raft.hpp"

#include <filesystem>
#include <functional>
#include <mutex>

namespace atlas::raft {

enum class FileStorePublicationStage { TemporaryCreated, BytesWritten, FileSynced, BeforeRename, Renamed, DirectorySynced };
using FileStorePublicationHook = std::function<void(FileStorePublicationStage)>;

// Single-process-owned, local POSIX file store. One process must own a given directory at a time.
// Every update atomically republishes term/vote, the complete log, and the commit index together.
class FilePersistentStorage final : public PersistentStorage {
public:
    explicit FilePersistentStorage(std::filesystem::path directory,
                                   FileStorePublicationHook publication_hook = {});

    [[nodiscard]] PersistentState load() override;
    void save(const PersistentState& state) override;
    [[nodiscard]] std::vector<LogEntry> load_log() override;
    void save_log(const std::vector<LogEntry>& entries) override;
    [[nodiscard]] LogIndex load_commit_index() override;
    void save_commit_index(LogIndex commit_index) override;
    [[nodiscard]] std::vector<RequestOutcome> load_request_outcomes() override;
    void save_request_outcome(const RequestOutcome& outcome) override;

private:
    void publish_locked();
    void load_locked();

    std::filesystem::path directory_;
    std::filesystem::path file_path_;
    FileStorePublicationHook publication_hook_;
    std::mutex mutex_;
    PersistentState state_{};
    std::vector<LogEntry> log_;
    LogIndex commit_index_{};
    std::vector<RequestOutcome> outcomes_;
};

} // namespace atlas::raft
