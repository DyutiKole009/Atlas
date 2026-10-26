#include "atlas/order_book.hpp"

#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace atlas;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

OrderRequest limit(OrderId id, ParticipantId participant, Side side,
                   PriceTicks price, Quantity quantity) {
    return OrderRequest{id, participant, id, 1, side, OrderType::Limit, price, quantity};
}

void test_empty_and_non_crossing_book() {
    OrderBook book;
    require(book.invariants_hold(), "empty book invariants");
    const auto empty = book.snapshot();
    require(empty.bids.empty() && empty.asks.empty(), "empty snapshot");

    const auto bid = book.submit(limit(1, 1, Side::Buy, 100, 5));
    const auto ask = book.submit(limit(2, 2, Side::Sell, 105, 3));
    require(bid.accepted() && ask.accepted(), "accept non-crossing limits");
    require(bid.trades.empty() && ask.trades.empty(), "non-crossing limits do not trade");
    require(book.snapshot().bids.front().price == 100, "best bid price");
    require(book.snapshot().asks.front().price == 105, "best ask price");
    require(book.invariants_hold(), "non-crossing book invariants");
}

void test_maker_price_and_partial_fill_both_sides() {
    OrderBook asks;
    require(asks.submit(limit(10, 1, Side::Sell, 101, 4)).accepted(), "rest ask");
    const auto buy = asks.submit(limit(11, 2, Side::Buy, 105, 3));
    require(buy.accepted() && buy.trades.size() == 1, "buy trade emitted");
    require(buy.trades[0].price == 101 && buy.trades[0].quantity == 3,
            "buy executes at maker ask price");
    require(buy.trades[0].maker_order_id == 10 && buy.trades[0].taker_order_id == 11,
            "buy maker/taker identity");
    require(asks.find_order(10)->leaves_quantity == 1, "ask partial leaves");
    require(asks.find_order(11)->status == OrderStatus::Filled, "buy taker filled");
    require(asks.find_order(10)->filled_quantity + asks.find_order(10)->leaves_quantity +
                    asks.find_order(10)->canceled_quantity == 4 &&
                asks.find_order(11)->filled_quantity + asks.find_order(11)->leaves_quantity +
                    asks.find_order(11)->canceled_quantity == 3,
            "buy-side trade quantity conservation");

    OrderBook bids;
    require(bids.submit(limit(20, 1, Side::Buy, 100, 4)).accepted(), "rest bid");
    const auto sell = bids.submit(limit(21, 2, Side::Sell, 95, 2));
    require(sell.accepted() && sell.trades.size() == 1, "sell trade emitted");
    require(sell.trades[0].price == 100 && sell.trades[0].quantity == 2,
            "sell executes at maker bid price");
    require(bids.find_order(20)->leaves_quantity == 2, "bid partial leaves");
    require(bids.find_order(20)->filled_quantity + bids.find_order(20)->leaves_quantity +
                    bids.find_order(20)->canceled_quantity == 4 &&
                bids.find_order(21)->filled_quantity + bids.find_order(21)->leaves_quantity +
                    bids.find_order(21)->canceled_quantity == 2,
            "sell-side trade quantity conservation");
    require(bids.invariants_hold(), "partial-fill invariants");

    OrderBook exact_fill;
    require(exact_fill.submit(limit(30, 1, Side::Sell, 8, 2)).accepted(),
            "rest exact-fill ask");
    const auto exact = exact_fill.submit(limit(31, 2, Side::Buy, 8, 2));
    require(exact.accepted() && exact.trades.size() == 1 &&
                exact_fill.snapshot().bids.empty() && exact_fill.snapshot().asks.empty(),
            "exact fill leaves both sides empty");
}

void test_best_price_then_fifo_and_partial_priority() {
    OrderBook book;
    require(book.submit(limit(1, 1, Side::Sell, 103, 2)).accepted(), "older worse ask");
    require(book.submit(limit(2, 2, Side::Sell, 101, 2)).accepted(), "best ask");
    require(book.submit(limit(3, 3, Side::Sell, 101, 2)).accepted(), "same-price later ask");
    const auto buy = book.submit(limit(4, 4, Side::Buy, 105, 3));
    require(buy.trades.size() == 2, "two best-price maker trades");
    require(buy.trades[0].maker_order_id == 2 && buy.trades[0].price == 101,
            "best price first");
    require(buy.trades[1].maker_order_id == 3 && buy.trades[1].price == 101,
            "FIFO at equal price");
    require(book.find_order(3)->leaves_quantity == 1, "second same-price order partial");
    require(book.find_order(1)->leaves_quantity == 2, "worse price remains untouched");
    require(book.invariants_hold(), "price-time invariants");

    const auto later_buy = book.submit(limit(5, 5, Side::Buy, 103, 1));
    require(later_buy.trades.size() == 1 && later_buy.trades[0].maker_order_id == 3,
            "partially filled maker retains priority");

        OrderBook bid_priority;
        require(bid_priority.submit(limit(10, 1, Side::Buy, 99, 2)).accepted(),
            "older worse bid");
        require(bid_priority.submit(limit(11, 2, Side::Buy, 101, 2)).accepted(),
            "best bid");
        require(bid_priority.submit(limit(12, 3, Side::Buy, 101, 2)).accepted(),
            "same-price later bid");
        const auto sell = bid_priority.submit(limit(13, 4, Side::Sell, 99, 3));
        require(sell.trades.size() == 2 && sell.trades[0].maker_order_id == 11 &&
            sell.trades[1].maker_order_id == 12 && sell.trades[0].price == 101 &&
            sell.trades[1].price == 101,
            "highest bid first and FIFO at equal bid price");
}

void test_limit_sweep_and_residual_rests() {
    OrderBook book;
    require(book.submit(limit(1, 1, Side::Sell, 100, 2)).accepted(), "first ask");
    require(book.submit(limit(2, 2, Side::Sell, 101, 1)).accepted(), "second ask");
    const auto buy = book.submit(limit(3, 3, Side::Buy, 101, 5));
    require(buy.trades.size() == 2, "sweep across two price levels");
    require(buy.trades[0].price == 100 && buy.trades[0].quantity == 2,
            "first sweep execution");
    require(buy.trades[1].price == 101 && buy.trades[1].quantity == 1,
            "second sweep execution");
    require(buy.order->status == OrderStatus::PartiallyFilled &&
                buy.order->filled_quantity == 3 && buy.order->leaves_quantity == 2,
            "limit residual state");
    require(buy.order->filled_quantity + buy.order->leaves_quantity +
                    buy.order->canceled_quantity == buy.order->request.quantity &&
                book.find_order(1)->filled_quantity + book.find_order(1)->leaves_quantity +
                    book.find_order(1)->canceled_quantity == 2 &&
                book.find_order(2)->filled_quantity + book.find_order(2)->leaves_quantity +
                    book.find_order(2)->canceled_quantity == 1,
            "multi-level trade quantity conservation");
    const auto snapshot = book.snapshot();
    require(snapshot.asks.empty() && snapshot.bids.size() == 1 &&
                snapshot.bids.front().price == 101 &&
                snapshot.bids.front().order_ids.front() == 3,
            "residual rests at limit");
    require(book.invariants_hold(), "sweep invariants");
}

void test_self_trade_rejection_is_atomic() {
    OrderBook book;
    require(book.submit(limit(1, 2, Side::Sell, 100, 2)).accepted(), "external maker");
    require(book.submit(limit(2, 1, Side::Sell, 101, 2)).accepted(), "same-owner maker");
    const BookSnapshot before = book.snapshot();
    const auto rejected = book.submit(limit(3, 1, Side::Buy, 101, 5));
    require(rejected.error == SubmitError::SelfTradePrevention,
            "same-owner path rejects incoming order");
    require(rejected.trades.empty() && !rejected.order.has_value(),
            "self-trade rejection emits no changes");
    const auto after = book.snapshot();
    require(before.asks.size() == after.asks.size() &&
                before.asks[0].order_ids == after.asks[0].order_ids &&
                before.asks[1].order_ids == after.asks[1].order_ids,
            "self-trade rejection preserves book");
    require(!book.find_order(3).has_value(), "rejected order is not recorded");

    // An own order beyond the taker's full-fill point is not reached and does not reject.
    const auto filled = book.submit(limit(4, 1, Side::Buy, 100, 2));
    require(filled.accepted() && filled.trades.size() == 1 &&
                filled.trades.front().maker_order_id == 1,
            "unreached same-owner maker does not reject");
    require(book.invariants_hold(), "self-trade policy invariants");
}

void test_order_id_reuse_is_rejected_after_cancel() {
    OrderBook book;
    require(book.submit(limit(50, 1, Side::Buy, 90, 2)).accepted(),
        "original order ID accepted");
    require(book.cancel(50).canceled(), "original order canceled");
    const auto reused = book.submit(limit(50, 2, Side::Buy, 89, 1));
    require(reused.error == SubmitError::DuplicateOrderId,
        "completed order ID cannot be reused in book session");
    require(book.invariants_hold(), "order ID reuse invariants");
}

void test_validation_market_duplicate_and_sequence_behavior() {
    OrderBook book;
    require(book.submit(limit(0, 1, Side::Buy, 1, 1)).error == SubmitError::InvalidOrderId,
            "zero order id rejected");
        require(book.submit(limit(1, 0, Side::Buy, 1, 1)).error == SubmitError::InvalidClientId,
            "zero client id rejected");
    require(book.submit(limit(1, 1, Side::Buy, 0, 1)).error == SubmitError::InvalidPrice,
            "zero price rejected");
    require(book.submit(limit(1, 1, Side::Buy, 1, 0)).error == SubmitError::InvalidQuantity,
            "zero quantity rejected");
    auto invalid_side = limit(1, 1, Side::Buy, 1, 1);
    invalid_side.side = static_cast<Side>(99);
    require(book.submit(invalid_side).error == SubmitError::InvalidSide,
            "invalid side enum rejected");
    auto invalid_type = limit(1, 1, Side::Buy, 1, 1);
    invalid_type.type = static_cast<OrderType>(99);
    require(book.submit(invalid_type).error == SubmitError::InvalidOrderType,
            "invalid order type enum rejected");
        auto invalid_request_id = limit(1, 1, Side::Buy, 1, 1);
        invalid_request_id.request_id = 0;
        require(book.submit(invalid_request_id).error == SubmitError::InvalidRequestId,
            "zero request id rejected");
        auto invalid_instrument_id = limit(1, 1, Side::Buy, 1, 1);
        invalid_instrument_id.instrument_id = 0;
        require(book.submit(invalid_instrument_id).error == SubmitError::InvalidInstrumentId,
            "zero instrument id rejected");
    auto market = limit(1, 1, Side::Buy, 0, 1);
    market.type = OrderType::Market;
    require(book.submit(market).error == SubmitError::UnsupportedOrderType,
            "market order deferred");
    require(book.snapshot().bids.empty() && book.snapshot().asks.empty(),
            "invalid requests leave book unchanged");

    const auto first = book.submit(limit(1, 1, Side::Buy, 1, 1));
    require(first.accepted() && first.order->command_sequence == 1,
            "first accepted arrival sequence");
    require(first.trades.empty(), "first order no trades");
    require(book.submit(limit(1, 2, Side::Sell, 1, 1)).error ==
                SubmitError::DuplicateOrderId,
            "duplicate live id rejected");
        auto duplicate_request = limit(3, 1, Side::Buy, 2, 1);
        duplicate_request.request_id = 1;
        require(book.submit(duplicate_request).error == SubmitError::DuplicateRequestId,
            "duplicate client request ID rejected");
        auto other_instrument = limit(4, 4, Side::Buy, 2, 1);
        other_instrument.instrument_id = 2;
        require(book.submit(other_instrument).error == SubmitError::InstrumentMismatch,
            "single-instrument book rejects a different instrument");

    const auto fill = book.submit(limit(2, 2, Side::Sell, 1, 1));
    require(fill.accepted() && fill.trades.size() == 1 &&
                fill.trades.front().trade_sequence == 1 &&
                fill.trades.front().command_sequence == 2 &&
                fill.trades.front().instrument_id == 1,
            "first trade sequence");
    require(book.submit(limit(1, 3, Side::Buy, 2, 1)).error ==
                SubmitError::DuplicateOrderId,
            "filled id is never reused");
    require(book.invariants_hold(), "validation invariants");
}

void test_cancel_open_and_partially_filled_orders() {
    OrderBook book;
    require(book.submit(limit(1, 1, Side::Sell, 100, 5)).accepted(), "rest cancel target");
    const auto partial = book.submit(limit(2, 2, Side::Buy, 100, 2));
    require(partial.accepted(), "partial order accepted");
    const auto cancel = book.cancel(1);
    require(cancel.canceled() && cancel.canceled_quantity == 3 &&
                cancel.command_sequence == 3,
            "cancel residual quantity and command sequence");
    const auto state = book.find_order(1);
    require(state->status == OrderStatus::Canceled && state->filled_quantity == 2 &&
                state->leaves_quantity == 0 && state->canceled_quantity == 3,
            "canceled order conserves quantity");
    require(book.snapshot().asks.empty(), "canceled order removed from book");
    require(book.cancel(1).error == CancelError::OrderNotLive,
            "repeat cancellation rejected");
    require(book.cancel(999).error == CancelError::UnknownOrderId,
            "unknown cancellation rejected");
    require(book.cancel(2).error == CancelError::OrderNotLive,
            "filled order cancellation rejected");
    require(book.invariants_hold(), "cancellation invariants");
}

void test_fifo_after_head_cancel_and_quantity_limit() {
    OrderBook book;
    require(book.submit(limit(1, 1, Side::Sell, 50, 2)).accepted(), "FIFO head");
    require(book.submit(limit(2, 2, Side::Sell, 50, 2)).accepted(), "FIFO second");
    require(book.cancel(1).canceled(), "cancel FIFO head");
    const auto buy = book.submit(limit(3, 3, Side::Buy, 50, 2));
    require(buy.trades.size() == 1 && buy.trades[0].maker_order_id == 2,
            "next FIFO order executes after head cancellation");

    OrderBook max_quantity_book;
    const Quantity max = std::numeric_limits<Quantity>::max();
    require(max_quantity_book.submit(limit(10, 1, Side::Sell, 1, max)).accepted(),
            "maximum representable quantity accepted");
    const auto max_fill = max_quantity_book.submit(limit(11, 2, Side::Buy, 1, max));
    require(max_fill.accepted() && max_fill.trades.size() == 1 &&
                max_fill.trades[0].quantity == max,
            "maximum quantity fill does not overflow");
    require(max_quantity_book.invariants_hold(), "maximum quantity invariants");

    OrderBook max_price_book;
    const PriceTicks max_price = std::numeric_limits<PriceTicks>::max();
    require(max_price_book.submit(limit(20, 1, Side::Sell, max_price, max)).accepted(),
            "maximum positive price tick accepted");
    const auto max_price_fill =
        max_price_book.submit(limit(21, 2, Side::Buy, max_price, max));
    require(max_price_fill.accepted() && max_price_fill.trades.size() == 1 &&
                max_price_fill.trades.front().price == max_price &&
                max_price_fill.trades.front().quantity == max,
            "maximum price and quantity match without overflow");
    require(max_price_book.invariants_hold(), "maximum price invariants");
}

bool snapshots_equal(const BookSnapshot& lhs, const BookSnapshot& rhs) {
    if (lhs.bids.size() != rhs.bids.size() || lhs.asks.size() != rhs.asks.size()) {
        return false;
    }
    const auto levels_equal = [](const auto& left, const auto& right) {
        for (std::size_t i = 0; i < left.size(); ++i) {
            if (left[i].price != right[i].price || left[i].order_ids != right[i].order_ids) {
                return false;
            }
        }
        return true;
    };
    return levels_equal(lhs.bids, rhs.bids) && levels_equal(lhs.asks, rhs.asks);
}

void test_deterministic_generated_streams() {
    OrderBook first;
    OrderBook second;
    std::uint64_t random = 0xA71A5ULL;
        const auto next = [&random]() {
        random = random * 6364136223846793005ULL + 1442695040888963407ULL;
        return random;
    };

    for (OrderId id = 1; id <= 500; ++id) {
        const Side side = (next() & 1U) == 0 ? Side::Buy : Side::Sell;
        const ParticipantId participant = (next() % 7U) + 1U;
        const PriceTicks price = static_cast<PriceTicks>((next() % 20U) + 1U);
        const Quantity quantity = (next() % 5U) + 1U;
        const OrderRequest request = limit(id, participant, side, price, quantity);
        const auto lhs = first.submit(request);
        const auto rhs = second.submit(request);
        require(lhs.error == rhs.error, "deterministic result errors");
        require(lhs.trades.size() == rhs.trades.size(), "deterministic trade count");
        for (std::size_t i = 0; i < lhs.trades.size(); ++i) {
            require(lhs.trades[i].trade_sequence == rhs.trades[i].trade_sequence &&
                        lhs.trades[i].maker_order_id == rhs.trades[i].maker_order_id &&
                        lhs.trades[i].taker_order_id == rhs.trades[i].taker_order_id &&
                        lhs.trades[i].price == rhs.trades[i].price &&
                        lhs.trades[i].quantity == rhs.trades[i].quantity,
                    "deterministic trade content");
        }
        require(snapshots_equal(first.snapshot(), second.snapshot()),
                "deterministic book snapshot");
        require(first.invariants_hold() && second.invariants_hold(),
                "generated-stream invariants");
    }

    // Mix deterministic cancellations into a second generated scenario and check invariants
    // after each operation, including unknown/completed IDs.
    OrderBook with_cancels;
    std::uint64_t cancel_random = 0xC0FFEEULL;
    auto next_cancel_value = [&cancel_random]() {
        cancel_random = cancel_random * 2862933555777941757ULL + 3037000493ULL;
        return cancel_random;
    };
    for (OrderId id = 1; id <= 300; ++id) {
        const Side side = (next_cancel_value() & 1U) == 0 ? Side::Buy : Side::Sell;
        const ClientId client = (next_cancel_value() % 11U) + 1U;
        const PriceTicks price = static_cast<PriceTicks>((next_cancel_value() % 25U) + 1U);
        const Quantity quantity = (next_cancel_value() % 9U) + 1U;
        const auto result = with_cancels.submit(OrderRequest{
            id, client, id, 1, side, OrderType::Limit, price, quantity});
        require(result.error == SubmitError::None ||
                    result.error == SubmitError::SelfTradePrevention,
                "generated submission returns a documented result");
        require(with_cancels.invariants_hold(), "invariants after generated submission");

        if (id % 3 == 0) {
            const OrderId cancel_id = (next_cancel_value() % id) + 1;
            const auto canceled = with_cancels.cancel(cancel_id);
            require(canceled.error == CancelError::None ||
                        canceled.error == CancelError::UnknownOrderId ||
                        canceled.error == CancelError::OrderNotLive,
                    "generated cancellation returns a documented result");
            require(with_cancels.invariants_hold(), "invariants after generated cancellation");
        }
    }
}

} // namespace

int main() {
    const std::vector<std::pair<const char*, void (*)()>> tests{
        {"empty and non-crossing book", test_empty_and_non_crossing_book},
        {"maker price and partial fills", test_maker_price_and_partial_fill_both_sides},
        {"best price and FIFO", test_best_price_then_fifo_and_partial_priority},
        {"limit sweep and residual", test_limit_sweep_and_residual_rests},
        {"atomic self-trade rejection", test_self_trade_rejection_is_atomic},
        {"order ID reuse", test_order_id_reuse_is_rejected_after_cancel},
        {"validation and sequences", test_validation_market_duplicate_and_sequence_behavior},
        {"cancellation", test_cancel_open_and_partially_filled_orders},
        {"FIFO cancellation and max quantity", test_fifo_after_head_cancel_and_quantity_limit},
        {"deterministic generated streams", test_deterministic_generated_streams},
    };

    try {
        for (const auto& [name, test] : tests) {
            test();
            std::cout << "[PASS] " << name << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
    return 0;
}
