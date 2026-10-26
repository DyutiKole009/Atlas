#include "atlas/order_book.hpp"

#include <charconv>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>

namespace {

using namespace atlas;

constexpr InstrumentId kDemoInstrument = 1;

bool parse_integer(std::string_view text, std::uint64_t& value) {
    if (text.empty()) {
        return false;
    }
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_signed_integer(std::string_view text, std::int64_t& value) {
    if (text.empty()) {
        return false;
    }
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    return error == std::errc{} && end == text.data() + text.size();
}

void print_help() {
    std::cout << "Commands:\n"
              << "  NEW BUY <order_id> <client_id> <request_id> <price_ticks> <quantity>\n"
              << "  NEW SELL <order_id> <client_id> <request_id> <price_ticks> <quantity>\n"
              << "  CANCEL <order_id>\n"
              << "  SHOW BOOK\n"
              << "  SHOW TRADES\n"
              << "  HELP\n"
              << "  QUIT\n"
              << "Market orders are not supported; instrument ID is fixed to 1 for this demo.\n";
}

void print_book(const OrderBook& book) {
    const BookSnapshot snapshot = book.snapshot();
    std::cout << "Asks (best first):\n";
    for (const auto& level : snapshot.asks) {
        std::cout << "  " << level.price << " ticks: ";
        for (std::size_t index = 0; index < level.order_ids.size(); ++index) {
            if (index != 0) {
                std::cout << ", ";
            }
            const auto order = book.find_order(level.order_ids[index]);
            if (order.has_value()) {
                std::cout << '#' << order->request.id << " x " << order->leaves_quantity;
            }
        }
        std::cout << '\n';
    }
    if (snapshot.asks.empty()) {
        std::cout << "  (empty)\n";
    }

    std::cout << "Bids (best first):\n";
    for (const auto& level : snapshot.bids) {
        std::cout << "  " << level.price << " ticks: ";
        for (std::size_t index = 0; index < level.order_ids.size(); ++index) {
            if (index != 0) {
                std::cout << ", ";
            }
            const auto order = book.find_order(level.order_ids[index]);
            if (order.has_value()) {
                std::cout << '#' << order->request.id << " x " << order->leaves_quantity;
            }
        }
        std::cout << '\n';
    }
    if (snapshot.bids.empty()) {
        std::cout << "  (empty)\n";
    }
}

void print_trades(const std::vector<Trade>& trades) {
    if (trades.empty()) {
        std::cout << "No trades yet.\n";
        return;
    }
    for (const Trade& trade : trades) {
        std::cout << "Trade " << trade.trade_sequence << " (command "
                  << trade.command_sequence << "): maker #" << trade.maker_order_id
                  << " / client " << trade.maker_client_id << " -> taker #"
                  << trade.taker_order_id << " / client " << trade.taker_client_id
                  << ", " << trade.quantity << " @ " << trade.price << " ticks\n";
    }
}

void print_submit_error(SubmitError error) {
    switch (error) {
    case SubmitError::InvalidOrderId: std::cout << "Rejected: order ID must be nonzero.\n"; break;
    case SubmitError::InvalidClientId: std::cout << "Rejected: client ID must be nonzero.\n"; break;
    case SubmitError::InvalidRequestId: std::cout << "Rejected: request ID must be nonzero.\n"; break;
    case SubmitError::InvalidInstrumentId: std::cout << "Rejected: instrument ID must be nonzero.\n"; break;
    case SubmitError::InvalidSide: std::cout << "Rejected: invalid side.\n"; break;
    case SubmitError::InvalidOrderType: std::cout << "Rejected: invalid order type.\n"; break;
    case SubmitError::UnsupportedOrderType: std::cout << "Rejected: only limit orders are supported.\n"; break;
    case SubmitError::InvalidPrice: std::cout << "Rejected: price ticks must be positive.\n"; break;
    case SubmitError::InvalidQuantity: std::cout << "Rejected: quantity must be positive.\n"; break;
    case SubmitError::DuplicateOrderId: std::cout << "Rejected: order ID has already been used.\n"; break;
    case SubmitError::DuplicateRequestId: std::cout << "Rejected: client request ID has already been used.\n"; break;
    case SubmitError::InstrumentMismatch: std::cout << "Rejected: this demo book accepts one instrument.\n"; break;
    case SubmitError::SelfTradePrevention: std::cout << "Rejected: order would self-trade.\n"; break;
    case SubmitError::BookInvariantViolation: std::cout << "Rejected: book invariant violation.\n"; break;
    case SubmitError::CommandSequenceExhausted: std::cout << "Rejected: command sequence exhausted.\n"; break;
    case SubmitError::TradeSequenceExhausted: std::cout << "Rejected: trade sequence exhausted.\n"; break;
    case SubmitError::None: break;
    }
}

bool process_line(const std::string& line, OrderBook& book, std::vector<Trade>& trades) {
    std::istringstream input(line);
    std::string command;
    if (!(input >> command)) {
        return true;
    }
    if (!command.empty() && command.front() == '#') {
        return true;
    }

    if (command == "QUIT" || command == "EXIT") {
        std::string extra;
        if (input >> extra) {
            std::cout << "Malformed input: QUIT takes no arguments.\n";
            return true;
        }
        return false;
    }
    if (command == "HELP") {
        std::string extra;
        if (input >> extra) {
            std::cout << "Malformed input: HELP takes no arguments.\n";
        } else {
            print_help();
        }
        return true;
    }
    if (command == "SHOW") {
        std::string what;
        std::string extra;
        if (!(input >> what) || (input >> extra)) {
            std::cout << "Malformed input: use SHOW BOOK or SHOW TRADES.\n";
        } else if (what == "BOOK") {
            print_book(book);
        } else if (what == "TRADES") {
            print_trades(trades);
        } else {
            std::cout << "Malformed input: use SHOW BOOK or SHOW TRADES.\n";
        }
        return true;
    }
    if (command == "CANCEL") {
        std::string id_text;
        std::uint64_t order_id = 0;
        std::string extra;
        if (!(input >> id_text) || (input >> extra) || !parse_integer(id_text, order_id)) {
            std::cout << "Malformed input: use CANCEL <order_id>.\n";
            return true;
        }
        const CancelResult result = book.cancel(order_id);
        if (result.canceled()) {
            std::cout << "Canceled order #" << order_id << ", removed "
                      << result.canceled_quantity << " units (command "
                      << result.command_sequence << ").\n";
        } else if (result.error == CancelError::UnknownOrderId) {
            std::cout << "Cancel rejected: unknown order ID.\n";
        } else if (result.error == CancelError::CommandSequenceExhausted) {
            std::cout << "Cancel rejected: command sequence exhausted.\n";
        } else {
            std::cout << "Cancel rejected: order is already completed or not resting.\n";
        }
        return true;
    }
    if (command == "NEW") {
        std::string side_text;
        std::string id_text;
        std::string client_text;
        std::string request_text;
        std::string price_text;
        std::string quantity_text;
        std::string extra;
        std::uint64_t order_id = 0;
        std::uint64_t client_id = 0;
        std::uint64_t request_id = 0;
        std::uint64_t quantity = 0;
        std::int64_t price = 0;
        if (!(input >> side_text >> id_text >> client_text >> request_text >> price_text >> quantity_text) ||
            (input >> extra) || !parse_integer(id_text, order_id) ||
            !parse_integer(client_text, client_id) || !parse_integer(request_text, request_id) ||
            !parse_signed_integer(price_text, price) || !parse_integer(quantity_text, quantity) ||
            (side_text != "BUY" && side_text != "SELL")) {
            std::cout << "Malformed input: use NEW BUY|SELL <order_id> <client_id> <request_id> <price_ticks> <quantity>.\n";
            return true;
        }

        const Side side = side_text == "BUY" ? Side::Buy : Side::Sell;
        const OrderRequest request{order_id, client_id, request_id, kDemoInstrument,
                                   side, OrderType::Limit, price, quantity};
        const SubmitResult result = book.submit(request);
        if (!result.accepted()) {
            print_submit_error(result.error);
            return true;
        }

        trades.insert(trades.end(), result.trades.begin(), result.trades.end());
        std::cout << "Accepted order #" << order_id << " (command "
                  << result.order->command_sequence << "): "
                  << (side == Side::Buy ? "BUY" : "SELL") << ' ' << quantity
                  << " @ " << price << " ticks; filled "
                  << result.order->filled_quantity << ", remaining "
                  << result.order->leaves_quantity << ".\n";
        if (result.trades.empty()) {
            std::cout << "No trades generated by this order.\n";
        } else {
            print_trades(result.trades);
        }
        return true;
    }

    std::cout << "Unknown command. Type HELP for commands.\n";
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    bool show_prompt = true;
    if (argc == 2 && std::string_view(argv[1]) == "--no-prompt") {
        show_prompt = false;
    } else if (argc != 1) {
        std::cerr << "Usage: atlas_cli [--no-prompt]\n";
        return 2;
    }

    OrderBook book;
    std::vector<Trade> trades;
    print_help();
    std::string line;
    while ((!show_prompt || static_cast<bool>(std::cout << "atlas> ")) &&
           std::getline(std::cin, line)) {
        if (!process_line(line, book, trades)) {
            break;
        }
        if (!book.invariants_hold()) {
            std::cerr << "Internal error: order-book invariant check failed.\n";
            return 1;
        }
    }
    return 0;
}
