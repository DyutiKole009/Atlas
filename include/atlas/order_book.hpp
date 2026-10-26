#pragma once

#include "atlas/domain.hpp"

#include <cstddef>
#include <functional>
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace atlas {

enum class SubmitError {
    None,
    InvalidOrderId,
    InvalidClientId,
    InvalidRequestId,
    InvalidInstrumentId,
    InvalidSide,
    InvalidOrderType,
    UnsupportedOrderType,
    InvalidPrice,
    InvalidQuantity,
    DuplicateOrderId,
    DuplicateRequestId,
    InstrumentMismatch,
    SelfTradePrevention,
    BookInvariantViolation,
    CommandSequenceExhausted,
    TradeSequenceExhausted
};

enum class CancelError { None, UnknownOrderId, OrderNotLive, CommandSequenceExhausted };

struct SubmitResult {
    SubmitError error{SubmitError::None};
    std::optional<OrderState> order;
    std::vector<Trade> trades;

    [[nodiscard]] bool accepted() const noexcept { return error == SubmitError::None; }
};

struct CancelResult {
    CancelError error{CancelError::None};
    OrderId order_id{};
    Quantity canceled_quantity{};
    EngineCommandSequence command_sequence{};

    [[nodiscard]] bool canceled() const noexcept { return error == CancelError::None; }
};

struct PriceLevelSnapshot {
    PriceTicks price{};
    std::vector<OrderId> order_ids;
};

struct BookSnapshot {
    std::vector<PriceLevelSnapshot> bids; // highest price first
    std::vector<PriceLevelSnapshot> asks; // lowest price first
};

struct OrderBookState {
    std::optional<InstrumentId> instrument_id;
    EngineCommandSequence next_command_sequence{1};
    TradeSequence next_trade_sequence{1};
    std::vector<OrderState> orders; // includes terminal orders to preserve ID uniqueness
};

class OrderBook {
public:
    [[nodiscard]] SubmitResult submit(const OrderRequest& request);
    [[nodiscard]] CancelResult cancel(OrderId order_id);
    [[nodiscard]] std::optional<OrderState> find_order(OrderId order_id) const;
    [[nodiscard]] BookSnapshot snapshot() const;
    [[nodiscard]] OrderBookState export_state() const;
    void restore_state(const OrderBookState& state);
    [[nodiscard]] bool invariants_hold() const;
    [[nodiscard]] std::optional<InstrumentId> instrument_id() const noexcept;

private:
    using PriceLevel = std::list<OrderId>;
    using BidLevels = std::map<PriceTicks, PriceLevel, std::greater<PriceTicks>>;
    using AskLevels = std::map<PriceTicks, PriceLevel>;

    struct RequestKeyHash {
        std::size_t operator()(const std::pair<ClientId, RequestId>& value) const noexcept {
            std::size_t seed = std::hash<ClientId>{}(value.first);
            seed ^= std::hash<RequestId>{}(value.second) + 0x9e3779b97f4a7c15ULL +
                    (seed << 6) + (seed >> 2);
            return seed;
        }
    };

    struct PlannedFill {
        OrderId maker_order_id{};
        Quantity quantity{};
    };

    [[nodiscard]] SubmitError validate(const OrderRequest& request) const;
    [[nodiscard]] SubmitError plan_matches(const OrderRequest& request,
                                           std::vector<PlannedFill>& fills) const;
    [[nodiscard]] bool trade_sequences_available(std::size_t count) const noexcept;
    void apply_fill(OrderState& order, Quantity quantity) noexcept;

    BidLevels bids_;
    AskLevels asks_;
    std::unordered_map<OrderId, OrderState> orders_;
    std::unordered_set<std::pair<ClientId, RequestId>, RequestKeyHash> accepted_requests_;
    std::optional<InstrumentId> instrument_id_;
    EngineCommandSequence next_command_sequence_{1}; // zero denotes exhausted
    TradeSequence next_trade_sequence_{1};           // zero denotes exhausted
};

} // namespace atlas
