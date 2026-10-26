#pragma once

#include "atlas/order_book.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <vector>

namespace atlas {

enum class WalRecordType : std::uint8_t {
    Submit = 1,
    Cancel = 2,
};

enum class WalDurabilityMode {
    Buffered,
    Durable,
};

struct WalAppendResult {
    std::uint64_t record_index{};
    std::size_t bytes_written{};
    bool acknowledged{false};
};

struct ReplaySummary {
    std::size_t accepted_commands{};
    std::size_t canceled_commands{};
    std::vector<Trade> trades;
};

struct SnapshotInfo {
    std::uint64_t wal_offset{};
    std::size_t order_count{};
};

enum class SnapshotPublicationStage { TemporaryCreated, BytesWritten, FileSynced, BeforeRename, Renamed, DirectorySynced };
using SnapshotPublicationHook = std::function<void(SnapshotPublicationStage)>;

class WalError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::filesystem::path& path,
                           WalDurabilityMode mode = WalDurabilityMode::Buffered);
    ~WriteAheadLog();

    [[nodiscard]] WalAppendResult append_submit(const OrderRequest& request,
                                               EngineCommandSequence command_sequence);
    [[nodiscard]] WalAppendResult append_cancel(OrderId order_id,
                                               EngineCommandSequence command_sequence);
    void flush();
    [[nodiscard]] std::uint64_t synchronize_and_position();

    static ReplaySummary replay(const std::filesystem::path& path, OrderBook& book);
    static ReplaySummary replay_from(const std::filesystem::path& path,
                                     std::uint64_t byte_offset,
                                     OrderBook& book);

    static constexpr std::uint8_t kVersion = 1;
    static constexpr std::uint64_t kMaxRecordLength = 1ULL << 20;

private:
    friend class SnapshotStore;

    struct RecordHeader {
        std::uint8_t version{};
        std::uint8_t type{};
        EngineCommandSequence command_sequence{};
        std::uint32_t payload_length{};
    };

    static std::uint64_t fnv1a64(const std::vector<std::uint8_t>& bytes) noexcept;
    static void append_u64(std::vector<std::uint8_t>& bytes, std::uint64_t value);
    static void append_u32(std::vector<std::uint8_t>& bytes, std::uint32_t value);
    static void append_u8(std::vector<std::uint8_t>& bytes, std::uint8_t value);
    static std::uint64_t read_u64_le(const std::uint8_t* bytes) noexcept;
    static std::uint32_t read_u32_le(const std::uint8_t* bytes) noexcept;

    static std::vector<std::uint8_t> encode_submit_payload(const OrderRequest& request);
    static std::vector<std::uint8_t> encode_cancel_payload(OrderId order_id);
    static std::vector<std::uint8_t> encode_record(WalRecordType type,
                                                  EngineCommandSequence command_sequence,
                                                  const std::vector<std::uint8_t>& payload);

    static OrderRequest decode_submit_payload(const std::vector<std::uint8_t>& payload);
    static OrderId decode_cancel_payload(const std::vector<std::uint8_t>& payload);

    static void write_all(int fd, const void* data, std::size_t size);

    std::filesystem::path path_;
    WalDurabilityMode mode_{WalDurabilityMode::Buffered};
    int fd_{-1};
    std::uint64_t next_record_index_{1};
};

class SnapshotStore {
public:
    // WAL is fsynced before its byte offset is captured; snapshot publication is atomic and fsynced.
    static SnapshotInfo save(const std::filesystem::path& snapshot_path,
                             WriteAheadLog& wal,
                             const OrderBook& book,
                             SnapshotPublicationHook publication_hook = {});
    static SnapshotInfo load(const std::filesystem::path& snapshot_path, OrderBook& book);
    static ReplaySummary recover(const std::filesystem::path& snapshot_path,
                                 const std::filesystem::path& wal_path,
                                 OrderBook& book);

    static constexpr std::uint32_t kVersion = 1;
    static constexpr std::uint64_t kMaxSnapshotBytes = 256ULL * 1024ULL * 1024ULL;
};

} // namespace atlas
