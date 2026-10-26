#include "atlas/node.hpp"
#include "atlas/protocol.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <variant>
#include <vector>

namespace {
using atlas::NodeConfig;
using atlas::TcpNode;
using namespace atlas::protocol;

void require(bool value, const std::string& message) {
    if (!value) throw std::runtime_error(message);
}

int connect_loopback(std::uint16_t port, int timeout_ms = 1500) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::runtime_error("test socket failed");
    timeval timeout{};
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(fd);
        throw std::runtime_error("loopback connect failed");
    }
    return fd;
}

void send_message(int fd, const Message& message) {
    write_message(message, [fd](const std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
        const ssize_t count = ::send(fd, data, size, MSG_NOSIGNAL);
        return static_cast<std::ptrdiff_t>(count);
    });
}

Message receive_message(int fd) {
    return read_message([fd](std::uint8_t* data, std::size_t size) -> std::ptrdiff_t {
        const ssize_t count = ::recv(fd, data, size, 0);
        return static_cast<std::ptrdiff_t>(count);
    });
}

OrderResponse response_for(int fd, std::uint64_t request_id, std::vector<TradeEvent>* trades = nullptr) {
    for (;;) {
        Message message = receive_message(fd);
        if (const auto* response = std::get_if<OrderResponse>(&message)) {
            if (response->request_id == request_id) return *response;
        } else if (const auto* trade = std::get_if<TradeEvent>(&message)) {
            if (trades) trades->push_back(*trade);
        } else {
            throw std::runtime_error("unexpected server message");
        }
    }
}

void close_socket(int fd) {
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
}

void test_requests_dedup_and_multiple_clients() {
    NodeConfig config;
    config.port = 0;
    config.max_connections = 8;
    config.command_queue_capacity = 16;
    TcpNode node(config);
    node.start();
    const int first = connect_loopback(node.port());
    const int second = connect_loopback(node.port());

    const SubmitOrder resting{101, 1, 1001, 7, Side::Sell, OrderType::Limit, 105, 5};
    send_message(first, resting);
    const auto first_response = response_for(first, resting.request_id);
    require(first_response.result == ResultCode::Accepted && first_response.command_sequence == 1 &&
                first_response.status == OrderStatus::Open && first_response.leaves_quantity == 5,
            "first client order accepted by execution thread");

    send_message(first, resting); // exact retry returns cached original result
    const auto duplicate_retry = response_for(first, resting.request_id);
    require(duplicate_retry.result == ResultCode::Accepted &&
                duplicate_retry.command_sequence == first_response.command_sequence,
            "identical request retry is idempotent");

    auto conflicting = resting;
    conflicting.quantity = 9;
    send_message(first, conflicting);
    require(response_for(first, resting.request_id).result == ResultCode::Duplicate,
            "reused request ID with different payload is rejected");

    const SubmitOrder incoming{202, 2, 2002, 7, Side::Buy, OrderType::Limit, 105, 2};
    send_message(second, incoming);
    std::vector<TradeEvent> trades;
    const auto second_response = response_for(second, incoming.request_id, &trades);
    require(second_response.result == ResultCode::Accepted && second_response.status == OrderStatus::Filled &&
                trades.size() == 1 && trades[0].maker_order_id == resting.order_id &&
                trades[0].quantity == 2 && trades[0].price == 105,
            "second client receives trade event and order response");

    send_message(second, CancelOrder{999, 3, resting.order_id});
    require(response_for(second, 3).result == ResultCode::NotFound,
            "client cannot cancel another client's order");
        send_message(second, SubmitOrder{202, 4, 2003, 8, Side::Buy, OrderType::Limit, 90, 1});
        const auto invalid_engine_request = response_for(second, 4);
        require(invalid_engine_request.result == ResultCode::Invalid &&
            invalid_engine_request.status == OrderStatus::Rejected,
            "engine-level instrument validation returns a rejection response");
    send_message(first, CancelOrder{101, 4, resting.order_id});
    const auto cancel = response_for(first, 4);
    require(cancel.result == ResultCode::Accepted && cancel.status == OrderStatus::Canceled &&
                cancel.canceled_quantity == 3,
            "owner successfully cancels remaining leaves");
    close_socket(first);
    close_socket(second);
    node.stop();
}

void test_malformed_disconnect_and_recovery_for_other_clients() {
    NodeConfig config;
    config.port = 0;
    TcpNode node(config);
    node.start();
    const int malformed = connect_loopback(node.port());
    const std::uint8_t bad_prefix[] = {0, 0, 0, 1};
    (void)::send(malformed, bad_prefix, sizeof(bad_prefix), MSG_NOSIGNAL);
    std::uint8_t byte{};
    const ssize_t closed = ::recv(malformed, &byte, 1, 0);
    require(closed == 0 || (closed < 0 && (errno == ECONNRESET || errno == EPIPE)),
            "malformed frame closes the offending connection");
    close_socket(malformed);

    const int valid = connect_loopback(node.port());
    const SubmitOrder request{77, 1, 770, 1, Side::Buy, OrderType::Limit, 50, 1};
    send_message(valid, request);
    require(response_for(valid, 1).result == ResultCode::Accepted,
            "server remains available after malformed peer disconnect");
    close_socket(valid);
    node.stop();
}

void test_bounded_command_queue_overload() {
    NodeConfig config;
    config.port = 0;
    config.max_connections = 8;
    config.command_queue_capacity = 1;
    config.execution_delay_ms = 1500;
    TcpNode node(config);
    node.start();
    const int first = connect_loopback(node.port(), 3000);
    const int second = connect_loopback(node.port(), 3000);
    const int third = connect_loopback(node.port(), 3000);

    send_message(first, SubmitOrder{1, 1, 11, 1, Side::Buy, OrderType::Limit, 10, 1});
    std::this_thread::sleep_for(std::chrono::milliseconds(350)); // let execution thread dequeue and enter delay
    send_message(second, SubmitOrder{2, 1, 22, 1, Side::Buy, OrderType::Limit, 9, 1});
    send_message(third, SubmitOrder{3, 1, 33, 1, Side::Buy, OrderType::Limit, 8, 1});

    require(response_for(first, 1).result == ResultCode::Accepted, "in-flight command completes");
        const auto second_result = response_for(second, 1);
        const auto third_result = response_for(third, 1);
        const bool second_rejected = second_result.result == ResultCode::InternalError &&
                     second_result.status == OrderStatus::Rejected;
        const bool third_rejected = third_result.result == ResultCode::InternalError &&
                    third_result.status == OrderStatus::Rejected;
        require(second_rejected != third_rejected,
            "full bounded command queue rejects exactly one concurrent overflow request");
        require((second_result.result == ResultCode::Accepted) != (third_result.result == ResultCode::Accepted),
            "one queued command completes and one over-capacity command is rejected");
    close_socket(first);
    close_socket(second);
    close_socket(third);
    node.stop();
}

void test_connection_limit_and_disconnect() {
    NodeConfig config;
    config.port = 0;
    config.max_connections = 1;
    TcpNode node(config);
    node.start();
    const int first = connect_loopback(node.port());
    const int second = connect_loopback(node.port());
    std::uint8_t byte{};
    const ssize_t result = ::recv(second, &byte, 1, 0);
    require(result == 0 || (result < 0 && (errno == ECONNRESET || errno == EPIPE)),
            "connection above configured limit is closed");
    close_socket(second);
    close_socket(first);
    node.stop();
}

} // namespace

int main() {
    try {
        test_requests_dedup_and_multiple_clients();
        test_malformed_disconnect_and_recovery_for_other_clients();
        test_bounded_command_queue_overload();
        test_connection_limit_and_disconnect();
        std::cout << "atlas_node_tests: all loopback tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas_node_tests: " << error.what() << '\n';
        return 1;
    }
}
