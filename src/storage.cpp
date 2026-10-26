#include "atlas/storage.hpp"

#include <algorithm>
#include <cerrno>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <system_error>
#include <utility>
#include <unistd.h>

namespace atlas {
namespace {

constexpr std::uint8_t kSnapshotMagic[] = {'A', 'T', 'L', 'S', 'N', 'A', 'P', '1'};
constexpr std::size_t kSnapshotHeaderSize = 8 + 4 + 8 + 8;
constexpr std::size_t kSnapshotOrderSize = 83;
std::atomic<std::uint64_t> g_snapshot_temp_id{1};

std::uint32_t checked_u32(std::uint64_t value, const char* context) {
    if (value > std::numeric_limits<std::uint32_t>::max()) {
        throw WalError(std::string(context) + ": integer overflow");
    }
    return static_cast<std::uint32_t>(value);
}

void write_u64_le(std::vector<std::uint8_t>& out, std::uint64_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 32) & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 40) & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 48) & 0xFFULL));
    out.push_back(static_cast<std::uint8_t>((value >> 56) & 0xFFULL));
}

void write_u32_le(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xFFU));
}

void write_u8(std::vector<std::uint8_t>& out, std::uint8_t value) {
    out.push_back(value);
}

std::uint32_t read_u32_le(const std::uint8_t* bytes) noexcept {
    return static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8) |
           (static_cast<std::uint32_t>(bytes[2]) << 16) |
           (static_cast<std::uint32_t>(bytes[3]) << 24);
}

std::uint64_t read_u64_le(const std::uint8_t* bytes) noexcept {
    return static_cast<std::uint64_t>(bytes[0]) |
           (static_cast<std::uint64_t>(bytes[1]) << 8) |
           (static_cast<std::uint64_t>(bytes[2]) << 16) |
           (static_cast<std::uint64_t>(bytes[3]) << 24) |
           (static_cast<std::uint64_t>(bytes[4]) << 32) |
           (static_cast<std::uint64_t>(bytes[5]) << 40) |
           (static_cast<std::uint64_t>(bytes[6]) << 48) |
           (static_cast<std::uint64_t>(bytes[7]) << 56);
}

std::uint64_t checksum64(const std::uint8_t* bytes, std::size_t size) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;
    for (std::size_t index = 0; index < size; ++index) {
        hash ^= bytes[index];
        hash *= 1099511628211ULL;
    }
    return hash;
}

void sync_or_throw(int fd, const std::string& context) {
    while (::fsync(fd) != 0) {
        if (errno == EINTR) {
            continue;
        }
        throw WalError(context + ": " + std::strerror(errno));
    }
}

void write_all_fd(int fd, const std::uint8_t* bytes, std::size_t size,
                  const std::string& context) {
    std::size_t written_total = 0;
    while (written_total < size) {
        const ssize_t written = ::write(fd, bytes + written_total, size - written_total);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw WalError(context + ": " + std::strerror(errno));
        }
        if (written == 0) {
            throw WalError(context + ": write made no progress");
        }
        written_total += static_cast<std::size_t>(written);
    }
}

std::vector<std::uint8_t> encode_snapshot(const OrderBookState& state,
                                         std::uint64_t wal_offset) {
    if (state.orders.size() > (SnapshotStore::kMaxSnapshotBytes - kSnapshotHeaderSize - 8) /
                                  kSnapshotOrderSize) {
        throw WalError("snapshot order count exceeds size limit");
    }
    std::vector<std::uint8_t> payload;
    payload.reserve(8 + 8 + 1 + 8 + 8 + state.orders.size() * kSnapshotOrderSize);
    write_u64_le(payload, state.next_command_sequence);
    write_u64_le(payload, state.next_trade_sequence);
    write_u8(payload, state.instrument_id.has_value() ? 1 : 0);
    write_u64_le(payload, state.instrument_id.value_or(0));
    write_u64_le(payload, state.orders.size());
    for (const OrderState& order : state.orders) {
        write_u64_le(payload, order.request.id);
        write_u64_le(payload, order.request.client_id);
        write_u64_le(payload, order.request.request_id);
        write_u64_le(payload, order.request.instrument_id);
        write_u8(payload, static_cast<std::uint8_t>(order.request.side));
        write_u8(payload, static_cast<std::uint8_t>(order.request.type));
        write_u64_le(payload, static_cast<std::uint64_t>(order.request.limit_price));
        write_u64_le(payload, order.request.quantity);
        write_u64_le(payload, order.command_sequence);
        write_u64_le(payload, order.filled_quantity);
        write_u64_le(payload, order.leaves_quantity);
        write_u64_le(payload, order.canceled_quantity);
        write_u8(payload, static_cast<std::uint8_t>(order.status));
    }

    std::vector<std::uint8_t> encoded;
    encoded.reserve(kSnapshotHeaderSize + payload.size() + 8);
    encoded.insert(encoded.end(), std::begin(kSnapshotMagic), std::end(kSnapshotMagic));
    write_u32_le(encoded, SnapshotStore::kVersion);
    write_u64_le(encoded, payload.size());
    write_u64_le(encoded, wal_offset);
    encoded.insert(encoded.end(), payload.begin(), payload.end());
    write_u64_le(encoded, checksum64(encoded.data(), encoded.size()));
    if (encoded.size() > SnapshotStore::kMaxSnapshotBytes) {
        throw WalError("snapshot exceeds size limit");
    }
    return encoded;
}

std::pair<OrderBookState, SnapshotInfo> decode_snapshot(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < kSnapshotHeaderSize + 8 || bytes.size() > SnapshotStore::kMaxSnapshotBytes) {
        throw WalError("invalid snapshot file length");
    }
    if (!std::equal(std::begin(kSnapshotMagic), std::end(kSnapshotMagic), bytes.begin())) {
        throw WalError("invalid snapshot magic");
    }
    const std::uint32_t version = read_u32_le(bytes.data() + 8);
    if (version != SnapshotStore::kVersion) {
        throw WalError("unsupported snapshot version");
    }
    const std::uint64_t payload_length = read_u64_le(bytes.data() + 12);
    const std::uint64_t wal_offset = read_u64_le(bytes.data() + 20);
    if (payload_length != bytes.size() - kSnapshotHeaderSize - 8) {
        throw WalError("snapshot payload length mismatch");
    }
    const std::uint64_t stored_checksum = read_u64_le(bytes.data() + bytes.size() - 8);
    if (stored_checksum != checksum64(bytes.data(), bytes.size() - 8)) {
        throw WalError("snapshot checksum mismatch");
    }

    const std::uint8_t* payload = bytes.data() + kSnapshotHeaderSize;
    const std::size_t payload_size = static_cast<std::size_t>(payload_length);
    constexpr std::size_t fixed_payload_size = 8 + 8 + 1 + 8 + 8;
    if (payload_size < fixed_payload_size) {
        throw WalError("snapshot payload is too short");
    }
    OrderBookState state;
    std::size_t offset = 0;
    state.next_command_sequence = read_u64_le(payload + offset); offset += 8;
    state.next_trade_sequence = read_u64_le(payload + offset); offset += 8;
    const std::uint8_t has_instrument = payload[offset++];
    const InstrumentId instrument = read_u64_le(payload + offset); offset += 8;
    if (has_instrument > 1 || (has_instrument == 0 && instrument != 0) ||
        (has_instrument == 1 && instrument == 0)) {
        throw WalError("invalid snapshot instrument metadata");
    }
    if (has_instrument != 0) {
        state.instrument_id = instrument;
    }
    const std::uint64_t order_count = read_u64_le(payload + offset); offset += 8;
    if (order_count > (payload_size - fixed_payload_size) / kSnapshotOrderSize ||
        fixed_payload_size + order_count * kSnapshotOrderSize != payload_size) {
        throw WalError("snapshot order count does not match payload length");
    }
    state.orders.reserve(static_cast<std::size_t>(order_count));
    for (std::uint64_t index = 0; index < order_count; ++index) {
        OrderState order{};
        order.request.id = read_u64_le(payload + offset); offset += 8;
        order.request.client_id = read_u64_le(payload + offset); offset += 8;
        order.request.request_id = read_u64_le(payload + offset); offset += 8;
        order.request.instrument_id = read_u64_le(payload + offset); offset += 8;
        order.request.side = static_cast<Side>(payload[offset++]);
        order.request.type = static_cast<OrderType>(payload[offset++]);
        order.request.limit_price = static_cast<PriceTicks>(read_u64_le(payload + offset)); offset += 8;
        order.request.quantity = read_u64_le(payload + offset); offset += 8;
        order.command_sequence = read_u64_le(payload + offset); offset += 8;
        order.filled_quantity = read_u64_le(payload + offset); offset += 8;
        order.leaves_quantity = read_u64_le(payload + offset); offset += 8;
        order.canceled_quantity = read_u64_le(payload + offset); offset += 8;
        order.status = static_cast<OrderStatus>(payload[offset++]);
        state.orders.push_back(order);
    }
    return {std::move(state), SnapshotInfo{wal_offset, static_cast<std::size_t>(order_count)}};
}

void sync_directory_or_throw(const std::filesystem::path& directory) {
    const int dir_fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) {
        throw WalError("failed to open snapshot directory: " + std::string(std::strerror(errno)));
    }
    try {
        sync_or_throw(dir_fd, "failed to synchronize snapshot directory");
    } catch (...) {
        ::close(dir_fd);
        throw;
    }
    if (::close(dir_fd) != 0) {
        throw WalError("failed to close snapshot directory: " + std::string(std::strerror(errno)));
    }
}

std::vector<std::uint8_t> encode_submit_payload_impl(const OrderRequest& request) {
    std::vector<std::uint8_t> payload;
    write_u64_le(payload, request.id);
    write_u64_le(payload, request.client_id);
    write_u64_le(payload, request.request_id);
    write_u64_le(payload, request.instrument_id);
    write_u8(payload, static_cast<std::uint8_t>(request.side));
    write_u8(payload, static_cast<std::uint8_t>(request.type));
    write_u64_le(payload, static_cast<std::uint64_t>(request.limit_price));
    write_u64_le(payload, request.quantity);
    return payload;
}

OrderRequest decode_submit_payload_impl(const std::vector<std::uint8_t>& payload) {
    if (payload.size() != 8 * 4 + 2 + 8 + 8) {
        throw WalError("invalid submit payload length");
    }
    OrderRequest request{};
    std::size_t offset = 0;
    request.id = static_cast<OrderId>(read_u64_le(payload.data() + offset)); offset += 8;
    request.client_id = static_cast<ClientId>(read_u64_le(payload.data() + offset)); offset += 8;
    request.request_id = static_cast<RequestId>(read_u64_le(payload.data() + offset)); offset += 8;
    request.instrument_id = static_cast<InstrumentId>(read_u64_le(payload.data() + offset)); offset += 8;
    request.side = static_cast<Side>(payload[offset]); ++offset;
    request.type = static_cast<OrderType>(payload[offset]); ++offset;
    request.limit_price = static_cast<PriceTicks>(static_cast<std::int64_t>(read_u64_le(payload.data() + offset))); offset += 8;
    request.quantity = static_cast<Quantity>(read_u64_le(payload.data() + offset));
    return request;
}

std::vector<std::uint8_t> encode_cancel_payload_impl(OrderId order_id) {
    std::vector<std::uint8_t> payload;
    write_u64_le(payload, order_id);
    return payload;
}

} // namespace

WriteAheadLog::WriteAheadLog(const std::filesystem::path& path, WalDurabilityMode mode)
    : path_(path), mode_(mode) {
    const std::filesystem::path parent = path_.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }

    const bool existed = std::filesystem::exists(path_);
    const int flags = O_WRONLY | O_CREAT | O_APPEND;
    fd_ = ::open(path_.c_str(), flags, 0666);
    if (fd_ < 0) {
        throw WalError("failed to open WAL: " + std::string(std::strerror(errno)));
    }
    if (!existed && mode_ == WalDurabilityMode::Durable) {
        try {
            sync_directory_or_throw(parent.empty() ? std::filesystem::path(".") : parent);
        } catch (...) {
            ::close(fd_);
            fd_ = -1;
            throw;
        }
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

WalAppendResult WriteAheadLog::append_submit(const OrderRequest& request,
                                            EngineCommandSequence command_sequence) {
    const std::vector<std::uint8_t> payload = encode_submit_payload_impl(request);
    const std::vector<std::uint8_t> record = encode_record(WalRecordType::Submit, command_sequence, payload);
    write_all(fd_, record.data(), record.size());
    if (mode_ == WalDurabilityMode::Durable) {
        sync_or_throw(fd_, "failed to synchronize WAL append");
    }
    WalAppendResult result;
    result.record_index = next_record_index_++;
    result.bytes_written = record.size();
    result.acknowledged = true;
    return result;
}

WalAppendResult WriteAheadLog::append_cancel(OrderId order_id,
                                           EngineCommandSequence command_sequence) {
    const std::vector<std::uint8_t> payload = encode_cancel_payload_impl(order_id);
    const std::vector<std::uint8_t> record = encode_record(WalRecordType::Cancel, command_sequence, payload);
    write_all(fd_, record.data(), record.size());
    if (mode_ == WalDurabilityMode::Durable) {
        sync_or_throw(fd_, "failed to synchronize WAL append");
    }
    WalAppendResult result;
    result.record_index = next_record_index_++;
    result.bytes_written = record.size();
    result.acknowledged = true;
    return result;
}

void WriteAheadLog::flush() {
    if (fd_ >= 0 && mode_ == WalDurabilityMode::Durable) {
        sync_or_throw(fd_, "failed to synchronize WAL");
    }
}

std::uint64_t WriteAheadLog::synchronize_and_position() {
    if (fd_ < 0) {
        throw WalError("WAL is not open");
    }
    sync_or_throw(fd_, "failed to synchronize WAL checkpoint");
    const off_t position = ::lseek(fd_, 0, SEEK_END);
    if (position < 0) {
        throw WalError("failed to determine WAL checkpoint position: " +
                       std::string(std::strerror(errno)));
    }
    return static_cast<std::uint64_t>(position);
}

ReplaySummary WriteAheadLog::replay(const std::filesystem::path& path, OrderBook& book) {
    return replay_from(path, 0, book);
}

ReplaySummary WriteAheadLog::replay_from(const std::filesystem::path& path,
                                         std::uint64_t byte_offset,
                                         OrderBook& book) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        if (byte_offset == 0 && !std::filesystem::exists(path)) {
            return ReplaySummary{};
        }
        throw WalError("failed to open WAL for replay: " + path.string());
    }

    std::vector<std::uint8_t> file((std::istreambuf_iterator<char>(input)), {});
    if (byte_offset > file.size()) {
        throw WalError("snapshot WAL position is beyond end of log");
    }
    std::size_t offset = static_cast<std::size_t>(byte_offset);
    ReplaySummary summary;

    while (offset < file.size()) {
        const std::size_t remaining = file.size() - offset;
        const std::size_t header_size = 1 + 1 + 8 + 4;
        const std::size_t checksum_size = 8;
        if (remaining < header_size + checksum_size) {
            throw WalError("truncated record header at log offset " + std::to_string(offset));
        }

        const std::uint8_t version = file[offset];
        const std::uint8_t record_type = file[offset + 1];
        const std::uint64_t command_sequence = read_u64_le(file.data() + offset + 2);
        const std::uint32_t payload_length = read_u32_le(file.data() + offset + 10);
        const std::size_t record_size = header_size + static_cast<std::size_t>(payload_length) + checksum_size;

        if (payload_length > kMaxRecordLength || record_size > remaining) {
            throw WalError("truncated or malformed record at log offset " + std::to_string(offset));
        }

        const std::size_t payload_offset = offset + header_size;
        const std::size_t checksum_offset = payload_offset + payload_length;
        const std::vector<std::uint8_t> payload(file.begin() + static_cast<std::ptrdiff_t>(payload_offset),
                                              file.begin() + static_cast<std::ptrdiff_t>(checksum_offset));
        const std::uint64_t stored_checksum = read_u64_le(file.data() + checksum_offset);

        std::vector<std::uint8_t> header_and_payload(file.begin() + static_cast<std::ptrdiff_t>(offset),
                                                   file.begin() + static_cast<std::ptrdiff_t>(checksum_offset));
        const std::uint64_t expected_checksum = fnv1a64(header_and_payload);
        if (version != kVersion || stored_checksum != expected_checksum) {
            throw WalError("checksum mismatch or unsupported record version at log offset " + std::to_string(offset));
        }

        if (record_type == static_cast<std::uint8_t>(WalRecordType::Submit)) {
            const auto request = decode_submit_payload_impl(payload);
            const auto result = book.submit(request);
            if (!result.accepted() || !result.order.has_value() ||
                result.order->command_sequence != command_sequence) {
                throw WalError("replay rejected valid submit command at sequence " + std::to_string(command_sequence));
            }
            ++summary.accepted_commands;
            summary.trades.insert(summary.trades.end(), result.trades.begin(), result.trades.end());
        } else if (record_type == static_cast<std::uint8_t>(WalRecordType::Cancel)) {
            const auto order_id = decode_cancel_payload(payload);
            const auto result = book.cancel(order_id);
            if (!result.canceled() || result.command_sequence != command_sequence) {
                throw WalError("replay rejected valid cancel command for order " + std::to_string(order_id));
            }
            ++summary.canceled_commands;
        } else {
            throw WalError("unknown WAL record type at log offset " + std::to_string(offset));
        }

        offset += record_size;
    }

    return summary;
}

std::vector<std::uint8_t> WriteAheadLog::encode_record(WalRecordType type,
                                                      EngineCommandSequence command_sequence,
                                                      const std::vector<std::uint8_t>& payload) {
    if (payload.size() > kMaxRecordLength) {
        throw WalError("record payload too large");
    }

    std::vector<std::uint8_t> record;
    append_u8(record, kVersion);
    append_u8(record, static_cast<std::uint8_t>(type));
    append_u64(record, command_sequence);
    append_u32(record, checked_u32(payload.size(), "payload length"));
    record.insert(record.end(), payload.begin(), payload.end());
    const std::uint64_t checksum = fnv1a64(record);
    append_u64(record, checksum);
    return record;
}

std::vector<std::uint8_t> WriteAheadLog::encode_submit_payload(const OrderRequest& request) {
    return encode_submit_payload_impl(request);
}

std::vector<std::uint8_t> WriteAheadLog::encode_cancel_payload(OrderId order_id) {
    return encode_cancel_payload_impl(order_id);
}

OrderRequest WriteAheadLog::decode_submit_payload(const std::vector<std::uint8_t>& payload) {
    return decode_submit_payload_impl(payload);
}

OrderId WriteAheadLog::decode_cancel_payload(const std::vector<std::uint8_t>& payload) {
    if (payload.size() != 8) {
        throw WalError("invalid cancel payload length");
    }
    return static_cast<OrderId>(read_u64_le(payload.data()));
}

void WriteAheadLog::write_all(int fd, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    std::size_t total = 0;
    while (total < size) {
        const auto written = ::write(fd, bytes + total, size - total);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw WalError("failed to write WAL: " + std::string(std::strerror(errno)));
        }
        if (written == 0) {
            throw WalError("failed to write WAL: write made no progress");
        }
        total += static_cast<std::size_t>(written);
    }
}

std::uint64_t WriteAheadLog::fnv1a64(const std::vector<std::uint8_t>& bytes) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;
    for (std::uint8_t byte : bytes) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

void WriteAheadLog::append_u64(std::vector<std::uint8_t>& bytes, std::uint64_t value) {
    write_u64_le(bytes, value);
}

void WriteAheadLog::append_u32(std::vector<std::uint8_t>& bytes, std::uint32_t value) {
    write_u32_le(bytes, value);
}

void WriteAheadLog::append_u8(std::vector<std::uint8_t>& bytes, std::uint8_t value) {
    write_u8(bytes, value);
}

std::uint64_t WriteAheadLog::read_u64_le(const std::uint8_t* bytes) noexcept {
    return static_cast<std::uint64_t>(bytes[0]) |
           (static_cast<std::uint64_t>(bytes[1]) << 8) |
           (static_cast<std::uint64_t>(bytes[2]) << 16) |
           (static_cast<std::uint64_t>(bytes[3]) << 24) |
           (static_cast<std::uint64_t>(bytes[4]) << 32) |
           (static_cast<std::uint64_t>(bytes[5]) << 40) |
           (static_cast<std::uint64_t>(bytes[6]) << 48) |
           (static_cast<std::uint64_t>(bytes[7]) << 56);
}

std::uint32_t WriteAheadLog::read_u32_le(const std::uint8_t* bytes) noexcept {
    return static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8) |
           (static_cast<std::uint32_t>(bytes[2]) << 16) |
           (static_cast<std::uint32_t>(bytes[3]) << 24);
}

SnapshotInfo SnapshotStore::save(const std::filesystem::path& snapshot_path,
                                 WriteAheadLog& wal,
                                 const OrderBook& book,
                                 SnapshotPublicationHook publication_hook) {
    const std::uint64_t wal_offset = wal.synchronize_and_position();
    OrderBook replayed;
    (void)WriteAheadLog::replay(wal.path_, replayed);
    const OrderBookState state = book.export_state();
    if (encode_snapshot(state, 0) != encode_snapshot(replayed.export_state(), 0)) {
        throw WalError("snapshot state does not match the durable WAL");
    }
    const std::vector<std::uint8_t> bytes = encode_snapshot(state, wal_offset);
    std::filesystem::path directory = snapshot_path.parent_path();
    if (directory.empty()) {
        directory = ".";
    }
    std::filesystem::create_directories(directory);
    const std::string temp_name = snapshot_path.filename().string() + ".tmp." +
        std::to_string(static_cast<unsigned long long>(::getpid())) + "." +
        std::to_string(g_snapshot_temp_id.fetch_add(1));
    const std::filesystem::path temp_path = directory / temp_name;
    const int fd = ::open(temp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd < 0) {
        throw WalError("failed to create temporary snapshot: " + std::string(std::strerror(errno)));
    }
    bool temp_exists = true;
    try {
        if (publication_hook) publication_hook(SnapshotPublicationStage::TemporaryCreated);
        write_all_fd(fd, bytes.data(), bytes.size(), "failed to write temporary snapshot");
        if (publication_hook) publication_hook(SnapshotPublicationStage::BytesWritten);
        sync_or_throw(fd, "failed to synchronize temporary snapshot");
        if (publication_hook) publication_hook(SnapshotPublicationStage::FileSynced);
        if (::close(fd) != 0) {
            throw WalError("failed to close temporary snapshot: " + std::string(std::strerror(errno)));
        }
        if (publication_hook) publication_hook(SnapshotPublicationStage::BeforeRename);
        std::error_code rename_error;
        std::filesystem::rename(temp_path, snapshot_path, rename_error);
        if (rename_error) {
            throw WalError("failed to publish snapshot atomically: " + rename_error.message());
        }
        temp_exists = false;
        if (publication_hook) publication_hook(SnapshotPublicationStage::Renamed);
        sync_directory_or_throw(directory);
        if (publication_hook) publication_hook(SnapshotPublicationStage::DirectorySynced);
    } catch (...) {
        if (temp_exists) {
            ::close(fd);
            std::error_code ignored;
            std::filesystem::remove(temp_path, ignored);
        }
        throw;
    }
    return SnapshotInfo{wal_offset, state.orders.size()};
}

SnapshotInfo SnapshotStore::load(const std::filesystem::path& snapshot_path, OrderBook& book) {
    std::ifstream input(snapshot_path, std::ios::binary);
    if (!input) {
        throw WalError("failed to open snapshot: " + snapshot_path.string());
    }
    input.seekg(0, std::ios::end);
    const std::streamoff length = input.tellg();
    if (length < 0 || static_cast<std::uint64_t>(length) > kMaxSnapshotBytes) {
        throw WalError("snapshot file length exceeds supported bounds");
    }
    input.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length));
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!input) {
            throw WalError("failed to read complete snapshot");
        }
    }
    auto [state, info] = decode_snapshot(bytes);
    try {
        book.restore_state(state);
    } catch (const std::exception& error) {
        throw WalError("snapshot engine state is invalid: " + std::string(error.what()));
    }
    return info;
}

ReplaySummary SnapshotStore::recover(const std::filesystem::path& snapshot_path,
                                     const std::filesystem::path& wal_path,
                                     OrderBook& book) {
    if (!std::filesystem::exists(snapshot_path)) {
        return WriteAheadLog::replay(wal_path, book);
    }
    const SnapshotInfo info = load(snapshot_path, book);
    return WriteAheadLog::replay_from(wal_path, info.wal_offset, book);
}

} // namespace atlas
