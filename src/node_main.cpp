#include "atlas/node.hpp"

#include <charconv>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <pthread.h>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

std::uint64_t parse_number(const char* text, const char* label) {
    std::uint64_t value = 0;
    const std::string input(text);
    const auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), value);
    if (error != std::errc{} || end != input.data() + input.size()) {
        throw std::invalid_argument(std::string("invalid ") + label);
    }
    return value;
}

void usage() {
    std::cout << "Usage: atlas-node [--bind IPv4] [--port PORT] [--max-connections N] [--queue-capacity N]\n"
                 "       atlas-node --replicated --node-id ID --storage-dir DIR --peer ID,IPv4,PORT "
                 "--peer ID,IPv4,PORT [--heartbeat-ms N] [--election-min-ms N] [--election-max-ms N]\n";
}

atlas::NodeConfig::PeerEndpoint parse_peer(std::string_view value) {
    const auto first = value.find(',');
    const auto second = first == std::string_view::npos ? first : value.find(',', first + 1);
    if (first == std::string_view::npos || second == std::string_view::npos ||
        value.find(',', second + 1) != std::string_view::npos) {
        throw std::invalid_argument("peer must use ID,IPv4,PORT format");
    }
    const std::string id_text(value.substr(0, first));
    const std::string address(value.substr(first + 1, second - first - 1));
    const std::string port_text(value.substr(second + 1));
    const auto id = parse_number(id_text.c_str(), "peer node ID");
    const auto port = parse_number(port_text.c_str(), "peer port");
    if (id == 0 || port == 0 || port > UINT16_MAX) throw std::invalid_argument("peer ID/port out of range");
    return {id, address, static_cast<std::uint16_t>(port)};
}

} // namespace

int main(int argc, char** argv) {
    try {
        atlas::NodeConfig config;
        for (int index = 1; index < argc; ++index) {
            const std::string option = argv[index];
            if (option == "--help" || option == "-h") {
                usage();
                return 0;
            }
            if (option == "--replicated") {
                config.replicated_mode = true;
                continue;
            }
            if (index + 1 >= argc) throw std::invalid_argument("missing value for " + option);
            const char* value = argv[++index];
            if (option == "--bind") config.bind_address = value;
            else if (option == "--port") {
                const auto port = parse_number(value, "port");
                if (port > UINT16_MAX) throw std::invalid_argument("port out of range");
                config.port = static_cast<std::uint16_t>(port);
            } else if (option == "--max-connections") {
                config.max_connections = static_cast<std::size_t>(parse_number(value, "max connections"));
            } else if (option == "--queue-capacity") {
                config.command_queue_capacity = static_cast<std::size_t>(parse_number(value, "queue capacity"));
            } else if (option == "--node-id") {
                config.node_id = parse_number(value, "node ID");
            } else if (option == "--storage-dir") {
                config.storage_directory = value;
            } else if (option == "--peer") {
                config.peers.push_back(parse_peer(value));
            } else if (option == "--heartbeat-ms") {
                const auto parsed = parse_number(value, "heartbeat interval");
                if (parsed > UINT32_MAX) throw std::invalid_argument("heartbeat interval out of range");
                config.heartbeat_interval_ms = static_cast<std::uint32_t>(parsed);
            } else if (option == "--election-min-ms") {
                const auto parsed = parse_number(value, "minimum election timeout");
                if (parsed > UINT32_MAX) throw std::invalid_argument("minimum election timeout out of range");
                config.election_timeout_min_ms = static_cast<std::uint32_t>(parsed);
            } else if (option == "--election-max-ms") {
                const auto parsed = parse_number(value, "maximum election timeout");
                if (parsed > UINT32_MAX) throw std::invalid_argument("maximum election timeout out of range");
                config.election_timeout_max_ms = static_cast<std::uint32_t>(parsed);
            } else {
                throw std::invalid_argument("unknown option: " + option);
            }
        }

        sigset_t signals;
        ::sigemptyset(&signals);
        ::sigaddset(&signals, SIGINT);
        ::sigaddset(&signals, SIGTERM);
        ::pthread_sigmask(SIG_BLOCK, &signals, nullptr);

        atlas::TcpNode node(config);
        node.start();
        std::cout << "atlas-node listening on " << config.bind_address << ':' << node.port()
                  << (config.replicated_mode ? " (replicated Raft node " : " (single-node execution thread")
                  << (config.replicated_mode ? std::to_string(config.node_id) + ")\n" : ")\n") << std::flush;
        int received_signal = 0;
        (void)::sigwait(&signals, &received_signal);
        node.stop();
        std::cout << "atlas-node stopped\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "atlas-node: " << error.what() << '\n';
        return 1;
    }
}
