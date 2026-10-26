#include "atlas/order_book.hpp"

#include <algorithm>
#include <cassert>
#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

namespace atlas {

SubmitError OrderBook::validate(const OrderRequest& request) const {
    switch (validate_order_request(request)) {
    case DomainValidationError::InvalidOrderId:
        return SubmitError::InvalidOrderId;
    case DomainValidationError::InvalidClientId:
        return SubmitError::InvalidClientId;
    case DomainValidationError::InvalidRequestId:
        return SubmitError::InvalidRequestId;
    case DomainValidationError::InvalidInstrumentId:
        return SubmitError::InvalidInstrumentId;
    case DomainValidationError::InvalidSide:
        return SubmitError::InvalidSide;
    case DomainValidationError::InvalidOrderType:
        return SubmitError::InvalidOrderType;
    case DomainValidationError::UnsupportedOrderType:
        return SubmitError::UnsupportedOrderType;
    case DomainValidationError::InvalidPrice:
        return SubmitError::InvalidPrice;
    case DomainValidationError::InvalidQuantity:
        return SubmitError::InvalidQuantity;
    case DomainValidationError::None:
        break;
    }
    if (orders_.find(request.id) != orders_.end()) {
        return SubmitError::DuplicateOrderId;
    }
    if (accepted_requests_.find({request.client_id, request.request_id}) !=
        accepted_requests_.end()) {
        return SubmitError::DuplicateRequestId;
    }
    if (instrument_id_.has_value() && *instrument_id_ != request.instrument_id) {
        return SubmitError::InstrumentMismatch;
    }
    return SubmitError::None;
}

SubmitError OrderBook::plan_matches(const OrderRequest& request,
                                    std::vector<PlannedFill>& fills) const {
    Quantity remaining = request.quantity;

    const auto consider_level = [&](const PriceLevel& level) {
        for (const OrderId maker_id : level) {
            if (remaining == 0) {
                return SubmitError::None;
            }

            const auto maker_it = orders_.find(maker_id);
            if (maker_it == orders_.end()) {
                return SubmitError::BookInvariantViolation;
            }
            const OrderState& maker = maker_it->second;
            if (maker.request.client_id == request.client_id) {
                return SubmitError::SelfTradePrevention;
            }

            const Quantity quantity = std::min(remaining, maker.leaves_quantity);
            fills.push_back(PlannedFill{maker_id, quantity});
            remaining -= quantity;
        }
        return SubmitError::None;
    };

    if (request.side == Side::Buy) {
        for (const auto& [price, level] : asks_) {
            if (price > request.limit_price) {
                break;
            }
            const SubmitError error = consider_level(level);
            if (error != SubmitError::None) {
                return error;
            }
            if (remaining == 0) {
                break;
            }
        }
    } else {
        for (const auto& [price, level] : bids_) {
            if (price < request.limit_price) {
                break;
            }
            const SubmitError error = consider_level(level);
            if (error != SubmitError::None) {
                return error;
            }
            if (remaining == 0) {
                break;
            }
        }
    }
    return SubmitError::None;
}

bool OrderBook::trade_sequences_available(std::size_t count) const noexcept {
    if (count == 0) {
        return true;
    }
    if (next_trade_sequence_ == 0) {
        return false;
    }
    const auto remaining_sequences =
        std::numeric_limits<TradeSequence>::max() - next_trade_sequence_;
    return count - 1 <= remaining_sequences;
}

void OrderBook::apply_fill(OrderState& order, Quantity quantity) noexcept {
    assert(quantity <= order.leaves_quantity);
    assert(quantity <= order.request.quantity - order.filled_quantity);
    order.leaves_quantity -= quantity;
    order.filled_quantity += quantity;
    if (order.leaves_quantity == 0) {
        order.status = OrderStatus::Filled;
    } else if (order.filled_quantity > 0) {
        order.status = OrderStatus::PartiallyFilled;
    }
}

SubmitResult OrderBook::submit(const OrderRequest& request) {
    SubmitResult result;
    result.error = validate(request);
    if (result.error != SubmitError::None) {
        return result;
    }

    if (next_command_sequence_ == 0) {
        result.error = SubmitError::CommandSequenceExhausted;
        return result;
    }

    std::vector<PlannedFill> planned_fills;
    result.error = plan_matches(request, planned_fills);
    if (result.error != SubmitError::None) {
        return result;
    }
    if (!trade_sequences_available(planned_fills.size())) {
        result.error = SubmitError::TradeSequenceExhausted;
        return result;
    }

    Quantity total_filled = 0;
    for (const PlannedFill& fill : planned_fills) {
        // Each planned fill is bounded by the remaining taker quantity.
        assert(fill.quantity <= request.quantity - total_filled);
        total_filled += fill.quantity;
    }
    const Quantity leaves = request.quantity - total_filled;
    const EngineCommandSequence command_sequence = next_command_sequence_;

    OrderState taker_state{
        request,
        command_sequence,
        total_filled,
        leaves,
        0,
        total_filled == request.quantity
            ? OrderStatus::Filled
            : (total_filled == 0 ? OrderStatus::Open : OrderStatus::PartiallyFilled)};

    result.trades.reserve(planned_fills.size());
    auto [taker_it, inserted] = orders_.emplace(request.id, taker_state);
    if (!inserted) {
        result.error = SubmitError::DuplicateOrderId;
        return result;
    }

    try {
        const auto [request_it, request_inserted] =
            accepted_requests_.emplace(request.client_id, request.request_id);
        (void)request_it;
        if (!request_inserted) {
            orders_.erase(taker_it);
            result.error = SubmitError::DuplicateRequestId;
            return result;
        }
    } catch (...) {
        orders_.erase(taker_it);
        throw;
    }

    bool inserted_level = false;
    if (leaves > 0) {
        try {
            if (request.side == Side::Buy) {
                auto [level_it, new_level] = bids_.try_emplace(request.limit_price);
                inserted_level = new_level;
                level_it->second.push_back(request.id);
            } else {
                auto [level_it, new_level] = asks_.try_emplace(request.limit_price);
                inserted_level = new_level;
                level_it->second.push_back(request.id);
            }
        } catch (...) {
            if (inserted_level) {
                if (request.side == Side::Buy) {
                    bids_.erase(request.limit_price);
                } else {
                    asks_.erase(request.limit_price);
                }
            }
            accepted_requests_.erase({request.client_id, request.request_id});
            orders_.erase(taker_it);
            throw;
        }
    }

    if (!instrument_id_.has_value()) {
        instrument_id_ = request.instrument_id;
    }

    for (const PlannedFill& fill : planned_fills) {
        auto maker_it = orders_.find(fill.maker_order_id);
        assert(maker_it != orders_.end());
        OrderState& maker = maker_it->second;
        result.trades.push_back(Trade{
            next_trade_sequence_,
            command_sequence,
            request.instrument_id,
            maker.request.id,
            request.id,
            maker.request.client_id,
            request.client_id,
            request.side,
            maker.request.limit_price,
            fill.quantity});
        if (next_trade_sequence_ == std::numeric_limits<TradeSequence>::max()) {
            next_trade_sequence_ = 0;
        } else {
            ++next_trade_sequence_;
        }

        apply_fill(maker, fill.quantity);
        if (maker.leaves_quantity == 0) {
            if (maker.request.side == Side::Buy) {
                auto level_it = bids_.find(maker.request.limit_price);
                assert(level_it != bids_.end());
                const auto order_it =
                    std::find(level_it->second.begin(), level_it->second.end(), maker.request.id);
                assert(order_it != level_it->second.end());
                level_it->second.erase(order_it);
                if (level_it->second.empty()) {
                    bids_.erase(level_it);
                }
            } else {
                auto level_it = asks_.find(maker.request.limit_price);
                assert(level_it != asks_.end());
                const auto order_it =
                    std::find(level_it->second.begin(), level_it->second.end(), maker.request.id);
                assert(order_it != level_it->second.end());
                level_it->second.erase(order_it);
                if (level_it->second.empty()) {
                    asks_.erase(level_it);
                }
            }
        }
    }

    if (next_command_sequence_ == std::numeric_limits<EngineCommandSequence>::max()) {
        next_command_sequence_ = 0;
    } else {
        ++next_command_sequence_;
    }
    result.order = taker_it->second;
    return result;
}

CancelResult OrderBook::cancel(OrderId order_id) {
    const auto order_it = orders_.find(order_id);
    if (order_it == orders_.end()) {
        return CancelResult{CancelError::UnknownOrderId, order_id, 0, 0};
    }

    OrderState& order = order_it->second;
    if ((order.status != OrderStatus::Open &&
         order.status != OrderStatus::PartiallyFilled) ||
        order.leaves_quantity == 0) {
        return CancelResult{CancelError::OrderNotLive, order_id, 0, 0};
    }

    if (next_command_sequence_ == 0) {
        return CancelResult{CancelError::CommandSequenceExhausted, order_id, 0, 0};
    }

    const Quantity canceled_quantity = order.leaves_quantity;
    if (order.request.side == Side::Buy) {
        auto level_it = bids_.find(order.request.limit_price);
        if (level_it == bids_.end()) {
            return CancelResult{CancelError::OrderNotLive, order_id, 0, 0};
        }
        const auto queued_order =
            std::find(level_it->second.begin(), level_it->second.end(), order_id);
        if (queued_order == level_it->second.end()) {
            return CancelResult{CancelError::OrderNotLive, order_id, 0, 0};
        }
        level_it->second.erase(queued_order);
        if (level_it->second.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.find(order.request.limit_price);
        if (level_it == asks_.end()) {
            return CancelResult{CancelError::OrderNotLive, order_id, 0, 0};
        }
        const auto queued_order =
            std::find(level_it->second.begin(), level_it->second.end(), order_id);
        if (queued_order == level_it->second.end()) {
            return CancelResult{CancelError::OrderNotLive, order_id, 0, 0};
        }
        level_it->second.erase(queued_order);
        if (level_it->second.empty()) {
            asks_.erase(level_it);
        }
    }

    order.leaves_quantity = 0;
    order.canceled_quantity = canceled_quantity;
    order.status = OrderStatus::Canceled;
    const EngineCommandSequence command_sequence = next_command_sequence_;
    if (next_command_sequence_ == std::numeric_limits<EngineCommandSequence>::max()) {
        next_command_sequence_ = 0;
    } else {
        ++next_command_sequence_;
    }
    return CancelResult{CancelError::None, order_id, canceled_quantity, command_sequence};
}

std::optional<OrderState> OrderBook::find_order(OrderId order_id) const {
    const auto order_it = orders_.find(order_id);
    if (order_it == orders_.end()) {
        return std::nullopt;
    }
    return order_it->second;
}

BookSnapshot OrderBook::snapshot() const {
    BookSnapshot result;
    result.bids.reserve(bids_.size());
    result.asks.reserve(asks_.size());

    for (const auto& [price, level] : bids_) {
        result.bids.push_back(PriceLevelSnapshot{price, {level.begin(), level.end()}});
    }
    for (const auto& [price, level] : asks_) {
        result.asks.push_back(PriceLevelSnapshot{price, {level.begin(), level.end()}});
    }
    return result;
}

OrderBookState OrderBook::export_state() const {
    OrderBookState state;
    state.instrument_id = instrument_id_;
    state.next_command_sequence = next_command_sequence_;
    state.next_trade_sequence = next_trade_sequence_;
    state.orders.reserve(orders_.size());
    for (const auto& [id, order] : orders_) {
        (void)id;
        state.orders.push_back(order);
    }
    std::sort(state.orders.begin(), state.orders.end(), [](const OrderState& lhs, const OrderState& rhs) {
        return lhs.request.id < rhs.request.id;
    });
    return state;
}

void OrderBook::restore_state(const OrderBookState& state) {
    OrderBook restored;
    restored.instrument_id_ = state.instrument_id;
    restored.next_command_sequence_ = state.next_command_sequence;
    restored.next_trade_sequence_ = state.next_trade_sequence;

    std::set<EngineCommandSequence> command_sequences;
    EngineCommandSequence max_command_sequence = 0;
    for (const OrderState& order : state.orders) {
        if (order.command_sequence == 0 || !command_sequences.insert(order.command_sequence).second) {
            throw std::invalid_argument("snapshot contains invalid or duplicate order command sequence");
        }
        max_command_sequence = std::max(max_command_sequence, order.command_sequence);
        if (!restored.orders_.emplace(order.request.id, order).second ||
            !restored.accepted_requests_.emplace(order.request.client_id, order.request.request_id).second) {
            throw std::invalid_argument("snapshot contains duplicate order or client request ID");
        }
    }

    std::vector<const OrderState*> priority_order;
    priority_order.reserve(state.orders.size());
    for (const OrderState& order : state.orders) {
        if (order.status == OrderStatus::Open || order.status == OrderStatus::PartiallyFilled) {
            priority_order.push_back(&order);
        }
    }
    std::sort(priority_order.begin(), priority_order.end(), [](const OrderState* lhs, const OrderState* rhs) {
        return lhs->command_sequence < rhs->command_sequence;
    });
    for (const OrderState* order : priority_order) {
        if (order->request.side == Side::Buy) {
            restored.bids_[order->request.limit_price].push_back(order->request.id);
        } else {
            restored.asks_[order->request.limit_price].push_back(order->request.id);
        }
    }

    if ((state.next_command_sequence != 0 && state.next_command_sequence <= max_command_sequence) ||
        (state.instrument_id.has_value() != !state.orders.empty()) ||
        !restored.invariants_hold()) {
        throw std::invalid_argument("snapshot engine state violates matching-engine invariants");
    }

    bids_.swap(restored.bids_);
    asks_.swap(restored.asks_);
    orders_.swap(restored.orders_);
    accepted_requests_.swap(restored.accepted_requests_);
    instrument_id_.swap(restored.instrument_id_);
    next_command_sequence_ = restored.next_command_sequence_;
    next_trade_sequence_ = restored.next_trade_sequence_;
}

bool OrderBook::invariants_hold() const {
    if (!bids_.empty() && !asks_.empty() && bids_.begin()->first >= asks_.begin()->first) {
        return false;
    }

    std::size_t live_order_count = 0;
    std::set<OrderId> resting_order_ids;
    const auto check_side = [&](const auto& levels, Side side) {
        for (const auto& [price, level] : levels) {
            if (level.empty()) {
                return false;
            }
            EngineCommandSequence previous_sequence = 0;
            for (const OrderId order_id : level) {
                if (!resting_order_ids.insert(order_id).second) {
                    return false;
                }
                const auto order_it = orders_.find(order_id);
                if (order_it == orders_.end()) {
                    return false;
                }
                const OrderState& order = order_it->second;
                if (order.request.side != side || order.request.type != OrderType::Limit ||
                    order.request.limit_price != price || order.leaves_quantity == 0 ||
                    (order.status != OrderStatus::Open &&
                     order.status != OrderStatus::PartiallyFilled) ||
                    order.command_sequence <= previous_sequence) {
                    return false;
                }
                previous_sequence = order.command_sequence;
                ++live_order_count;
            }
        }
        return true;
    };

    if (!check_side(bids_, Side::Buy) || !check_side(asks_, Side::Sell)) {
        return false;
    }

    std::size_t expected_live_count = 0;
    for (const auto& [id, order] : orders_) {
        if (id == 0 || order.request.id != id || order.request.quantity == 0 ||
            validate_order_request(order.request) != DomainValidationError::None ||
            order.request.type != OrderType::Limit ||
            order.filled_quantity > order.request.quantity ||
            order.leaves_quantity > order.request.quantity - order.filled_quantity ||
            order.canceled_quantity !=
                order.request.quantity - order.filled_quantity - order.leaves_quantity) {
            return false;
        }

        const bool live = order.status == OrderStatus::Open ||
                          order.status == OrderStatus::PartiallyFilled;
        if (live) {
            if (order.leaves_quantity == 0 || order.canceled_quantity != 0 ||
                (order.status == OrderStatus::Open && order.filled_quantity != 0) ||
                (order.status == OrderStatus::PartiallyFilled && order.filled_quantity == 0)) {
                return false;
            }
            ++expected_live_count;
        } else if (order.status == OrderStatus::Filled) {
            if (order.filled_quantity != order.request.quantity || order.leaves_quantity != 0 ||
                order.canceled_quantity != 0) {
                return false;
            }
        } else if (order.status == OrderStatus::Canceled) {
            if (order.leaves_quantity != 0 || order.canceled_quantity == 0) {
                return false;
            }
        } else {
            return false;
        }
    }

    const bool request_registry_matches =
        orders_.size() == accepted_requests_.size() &&
        std::all_of(orders_.begin(), orders_.end(), [&](const auto& entry) {
            return accepted_requests_.find(
                       {entry.second.request.client_id, entry.second.request.request_id}) !=
                   accepted_requests_.end();
        });

    return live_order_count == expected_live_count && request_registry_matches &&
           (!instrument_id_.has_value() ||
            std::all_of(orders_.begin(), orders_.end(), [&](const auto& entry) {
                return entry.second.request.instrument_id == *instrument_id_;
            }));
}

std::optional<InstrumentId> OrderBook::instrument_id() const noexcept {
    return instrument_id_;
}

} // namespace atlas
