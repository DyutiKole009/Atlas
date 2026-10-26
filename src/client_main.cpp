#include "atlas/protocol.hpp"

#include <arpa/inet.h>
#include <charconv>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <variant>

namespace {

std::uint64_t parse_u64(const char* text, const char* label) {
    std::uint64_t value = 0;
    const std::string input(text);
    const auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), value);
    if (error != std::errc{} || end != input.data() + input.size())
        throw std::invalid_argument(std::string("invalid ") + label);
    return value;
}

std::int64_t parse_i64(const char* text, const char* label) {
    std::int64_t value = 0;
    const std::string input(text);
    const auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), value);
    if (error != std::errc{} || end != input.data() + input.size())
        throw std::invalid_argument(std::string("invalid ") + label);
    return value;
}

void usage() {
    std::cout << "Usage:\n"
              << "  atlas-client HOST PORT CLIENT_ID submit REQUEST_ID ORDER_ID INSTRUMENT BUY|SELL PRICE QUANTITY\n"
              << "  atlas-client HOST PORT CLIENT_ID cancel REQUEST_ID ORDER_ID\n";
}

void print_message(const atlas::protocol::Message& message) {
    using namespace atlas::protocol;
    if (const auto* response = std::get_if<OrderResponse>(&message)) {
        std::cout << "ORDER_RESPONSE client=" << response->client_id << " request=" << response->request_id
                  << " order=" << response->order_id << " result=" << static_cast<unsigned>(response->result)
                  << " status=" << static_cast<unsigned>(response->status)
                  << " command_sequence=" << response->command_sequence
                  << " filled=" << response->filled_quantity << " leaves=" << response->leaves_quantity
                  << " canceled=" << response->canceled_quantity << '\n';
    } else if (const auto* trade = std::get_if<TradeEvent>(&message)) {
        std::cout << "TRADE sequence=" << trade->trade_sequence << " command=" << trade->command_sequence
                  << " maker=" << trade->maker_order_id << " taker=" << trade->taker_order_id
                  << " quantity=" << trade->quantity << " price=" << trade->price << '\n';
    } else if (const auto* heartbeat = std::get_if<Heartbeat>(&message)) {
        std::cout << (heartbeat->kind == HeartbeatKind::Pong ? "PONG" : "PING")
                  << " nonce=" << heartbeat->nonce << '\n';
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 6 || std::string(argv[1]) == "--help") {
            usage();
            return argc < 6 ? 2 : 0;
        }
        const std::string host = argv[1];
        const auto port_value = parse_u64(argv[2], "port");
        if (port_value == 0 || port_value > UINT16_MAX) throw std::invalid_argument("port out of range");
        const auto client_id = parse_u64(argv[3], "client ID");
        const std::string command = argv[4];
        atlas::protocol::Message request;
        std::uint64_t request_id = 0;
        if (command == "submit") {
            if (argc != 11) throw std::invalid_argument("submit requires 6 arguments");
            request_id = parse_u64(argv[5], "request ID");
            const auto order_id = parse_u64(argv[6], "order ID");
            const auto instrument_id = parse_u64(argv[7], "instrument ID");
            const std::string side_text = argv[8];
            atlas::protocol::Side side;
            if (side_text == "BUY") side = atlas::protocol::Side::Buy;
            else if (side_text == "SELL") side = atlas::protocol::Side::Sell;
            else throw std::invalid_argument("side must be BUY or SELL");
            const auto price = parse_i64(argv[9], "price");
            const auto quantity = parse_u64(argv[10], "quantity");
            request = atlas::protocol::SubmitOrder{client_id, request_id, order_id, instrument_id, side,
                atlas::protocol::OrderType::Limit, price, quantity};
        } else if (command == "cancel") {
            if (argc != 7) throw std::invalid_argument("cancel requires request ID and order ID");
            request_id = parse_u64(argv[5], "request ID");
            request = atlas::protocol::CancelOrder{client_id, request_id, parse_u64(argv[6], "order ID")};
        } else {
            usage();
            return 2;
        }
        const auto encoded = atlas::protocol::encode(request);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(static_cast<std::uint16_t>(port_value));
        if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1)
            throw std::invalid_argument("HOST must be an IPv4 numeric address");
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) throw std::runtime_error("socket: " + std::string(std::strerror(errno)));
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            const std::string error = std::strerror(errno);
            ::close(fd);
            throw std::runtime_error("connect: " + error);
        }
        try {
            atlas::protocol::write_message(request, [fd](const std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
                for (;;) {
                    const ssize_t count = ::send(fd, data, size, MSG_NOSIGNAL);
                    if (count < 0 && errno == EINTR) continue;
                    return static_cast<std::ptrdiff_t>(count);
                }
            });
            for (;;) {
                auto response = atlas::protocol::read_message([fd](std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
                    for (;;) {
                        const ssize_t count = ::recv(fd, data, size, 0);
                        if (count < 0 && errno == EINTR) continue;
                        return static_cast<std::ptrdiff_t>(count);
                    }
                });
                print_message(response);
                if (const auto* result = std::get_if<atlas::protocol::OrderResponse>(&response)) {
                    if (result->client_id == client_id && result->request_id == request_id) break;
                }
            }
        } catch (...) {
            ::close(fd);
            throw;
        }
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas-client: " << error.what() << '\n';
        return 1;
    }
}
