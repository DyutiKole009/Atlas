#pragma once

#include <cstdint>

namespace atlas {

// Identifiers are engine-local unsigned values; zero is reserved as invalid.
using OrderId = std::uint64_t;
using ClientId = std::uint64_t;
using RequestId = std::uint64_t;
using InstrumentId = std::uint64_t;
using ParticipantId = ClientId; // Compatibility name used by self-trade prevention.

// Prices are positive signed ticks; quantities are positive unsigned indivisible units.
using PriceTicks = std::int64_t;
using Quantity = std::uint64_t;

// Assigned by the matching engine, never derived from wall-clock time.
using EngineCommandSequence = std::uint64_t;
using TradeSequence = std::uint64_t;
using Sequence = EngineCommandSequence; // Compatibility name for existing callers.

enum class Side { Buy, Sell };
enum class OrderType { Limit, Market };
enum class OrderStatus { Open, PartiallyFilled, Filled, Canceled };

enum class DomainValidationError {
    None,
    InvalidOrderId,
    InvalidClientId,
    InvalidRequestId,
    InvalidInstrumentId,
    InvalidSide,
    InvalidOrderType,
    UnsupportedOrderType,
    InvalidPrice,
    InvalidQuantity
};

struct OrderRequest {
    OrderId id{};
    ClientId client_id{};
    RequestId request_id{};
    InstrumentId instrument_id{};
    Side side{Side::Buy};
    OrderType type{OrderType::Limit};
    PriceTicks limit_price{};
    Quantity quantity{};
};

struct OrderState {
    OrderRequest request;
    EngineCommandSequence command_sequence{};
    Quantity filled_quantity{};
    Quantity leaves_quantity{};
    Quantity canceled_quantity{};
    OrderStatus status{OrderStatus::Open};
};

using Order = OrderState;

struct Trade {
    TradeSequence trade_sequence{};
    EngineCommandSequence command_sequence{};
    InstrumentId instrument_id{};
    OrderId maker_order_id{};
    OrderId taker_order_id{};
    ClientId maker_client_id{};
    ClientId taker_client_id{};
    Side taker_side{Side::Buy};
    PriceTicks price{};
    Quantity quantity{};
};

[[nodiscard]] constexpr DomainValidationError validate_order_request(
    const OrderRequest& request) noexcept {
    if (request.id == 0) {
        return DomainValidationError::InvalidOrderId;
    }
    if (request.client_id == 0) {
        return DomainValidationError::InvalidClientId;
    }
    if (request.request_id == 0) {
        return DomainValidationError::InvalidRequestId;
    }
    if (request.instrument_id == 0) {
        return DomainValidationError::InvalidInstrumentId;
    }
    if (request.side != Side::Buy && request.side != Side::Sell) {
        return DomainValidationError::InvalidSide;
    }
    if (request.type != OrderType::Limit && request.type != OrderType::Market) {
        return DomainValidationError::InvalidOrderType;
    }
    if (request.type == OrderType::Market) {
        return DomainValidationError::UnsupportedOrderType;
    }
    if (request.limit_price <= 0) {
        return DomainValidationError::InvalidPrice;
    }
    if (request.quantity == 0) {
        return DomainValidationError::InvalidQuantity;
    }
    return DomainValidationError::None;
}

} // namespace atlas
