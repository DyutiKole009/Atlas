#include "atlas/replicated_exchange.hpp"

#include <algorithm>
#include <stdexcept>
#include <variant>

namespace atlas {
namespace {

protocol::OrderResponse rejected(std::uint64_t client_id, std::uint64_t request_id,
                                 std::uint64_t order_id, protocol::ResultCode result) {
    return {client_id, request_id, order_id, 0, protocol::OrderStatus::Rejected, result, 0, 0, 0};
}

protocol::ResultCode map_submit_error(SubmitError error) {
    if (error == SubmitError::DuplicateOrderId || error == SubmitError::DuplicateRequestId)
        return protocol::ResultCode::Duplicate;
    return error == SubmitError::None ? protocol::ResultCode::Accepted : protocol::ResultCode::Invalid;
}

protocol::OrderStatus map_status(OrderStatus status) {
    switch (status) {
    case OrderStatus::Open: return protocol::OrderStatus::Open;
    case OrderStatus::PartiallyFilled: return protocol::OrderStatus::PartiallyFilled;
    case OrderStatus::Filled: return protocol::OrderStatus::Filled;
    case OrderStatus::Canceled: return protocol::OrderStatus::Canceled;
    }
    return protocol::OrderStatus::Rejected;
}

protocol::TradeEvent to_trade(const Trade& trade) {
    return {trade.trade_sequence, trade.command_sequence, trade.instrument_id,
            trade.maker_order_id, trade.taker_order_id, trade.maker_client_id,
            trade.taker_client_id,
            trade.taker_side == Side::Buy ? protocol::Side::Buy : protocol::Side::Sell,
            trade.price, trade.quantity};
}

} // namespace

ReplicatedExchangeStateMachine::ReplicatedExchangeStateMachine(raft::PersistentStorage& storage)
    : storage_(storage) {
    for (const auto& outcome : storage_.load_request_outcomes()) {
        const protocol::Message decoded_request = protocol::decode(outcome.request_fingerprint);
        const auto* request = std::get_if<protocol::SubmitOrder>(&decoded_request);
        const auto* cancellation = std::get_if<protocol::CancelOrder>(&decoded_request);
        const std::uint64_t client_id = request ? request->client_id : cancellation ? cancellation->client_id : 0;
        const std::uint64_t request_id = request ? request->request_id : cancellation ? cancellation->request_id : 0;
        if (client_id != outcome.client_id || request_id != outcome.request_id)
            throw std::runtime_error("durable request outcome key does not match request bytes");
        results_.emplace(RequestKey{outcome.client_id, outcome.request_id},
                         CachedResult{outcome.request_fingerprint,
                             ReplicatedCommandResult{{protocol::decode(outcome.encoded_response)}}});
    }
}

std::vector<std::uint8_t> encode_replicated_command(const protocol::Message& request) {
    if (!std::holds_alternative<protocol::SubmitOrder>(request) &&
        !std::holds_alternative<protocol::CancelOrder>(request)) {
        throw protocol::ProtocolError("only submit and cancel requests can be replicated");
    }
    return protocol::encode(request);
}

void ReplicatedExchangeStateMachine::restore_committed(const std::vector<raft::LogEntry>& entries) {
    for (std::size_t i = 0; i < entries.size(); ++i) {
        const auto index = static_cast<raft::LogIndex>(i + 1);
        apply(index, entries[i]);
        applied_results_.erase(index); // Recovery rebuilds state; it has no live request waiter.
    }
}

void ReplicatedExchangeStateMachine::apply(raft::LogIndex index, const raft::LogEntry& entry) {
    if (index != applied_index_ + 1) throw std::logic_error("exchange state machine index is not contiguous");
    const protocol::Message request = protocol::decode(entry.command);
    const auto encoded = entry.command;
    ReplicatedCommandResult outcome;

    if (const auto* submit = std::get_if<protocol::SubmitOrder>(&request)) {
        const RequestKey key{submit->client_id, submit->request_id};
        const auto prior = results_.find(key);
        if (applied_request_keys_.contains(key)) {
            if (prior != results_.end() && prior->second.encoded_request == encoded) {
                const auto response = std::find_if(prior->second.result.messages.begin(),
                    prior->second.result.messages.end(), [](const auto& item) {
                        return std::holds_alternative<protocol::OrderResponse>(item);
                    });
                if (response != prior->second.result.messages.end()) outcome.messages.push_back(*response);
            } else {
                outcome.messages.emplace_back(rejected(submit->client_id, submit->request_id,
                    submit->order_id, protocol::ResultCode::Duplicate));
            }
        } else {
            const OrderRequest domain{submit->order_id, submit->client_id, submit->request_id,
                submit->instrument_id,
                submit->side == protocol::Side::Buy ? Side::Buy : Side::Sell,
                OrderType::Limit, submit->limit_price, submit->quantity};
            const SubmitResult result = book_.submit(domain);
            if (!result.accepted() || !result.order) {
                outcome.messages.emplace_back(rejected(submit->client_id, submit->request_id,
                                                        submit->order_id, map_submit_error(result.error)));
            } else {
                for (const Trade& trade : result.trades) outcome.messages.emplace_back(to_trade(trade));
                const auto& state = *result.order;
                outcome.messages.emplace_back(protocol::OrderResponse{submit->client_id, submit->request_id,
                    submit->order_id, state.command_sequence, map_status(state.status),
                    protocol::ResultCode::Accepted, state.filled_quantity, state.leaves_quantity,
                    state.canceled_quantity});
            }
            const auto response = std::find_if(outcome.messages.begin(), outcome.messages.end(), [](const auto& item) {
                return std::holds_alternative<protocol::OrderResponse>(item);
            });
            if (response == outcome.messages.end()) throw std::logic_error("exchange command has no order response");
            const auto response_bytes = protocol::encode(*response);
            if (prior == results_.end()) {
                storage_.save_request_outcome({key.client_id, key.request_id, encoded, response_bytes});
                results_.emplace(key, CachedResult{encoded, outcome});
            } else if (prior->second.encoded_request != encoded ||
                       protocol::encode(prior->second.result.messages.back()) != response_bytes) {
                throw std::logic_error("replayed request outcome differs from deterministic execution");
            } else {
                // On restart, retain deterministic replayed trade events for the current apply; durable
                // result tracking intentionally stores only the terminal order response.
            }
            applied_request_keys_.insert(key);
        }
    } else if (const auto* cancel = std::get_if<protocol::CancelOrder>(&request)) {
        const RequestKey key{cancel->client_id, cancel->request_id};
        const auto prior = results_.find(key);
        if (applied_request_keys_.contains(key)) {
            if (prior != results_.end() && prior->second.encoded_request == encoded) {
                const auto response = std::find_if(prior->second.result.messages.begin(),
                    prior->second.result.messages.end(), [](const auto& item) {
                        return std::holds_alternative<protocol::OrderResponse>(item);
                    });
                if (response != prior->second.result.messages.end()) outcome.messages.push_back(*response);
            } else {
                outcome.messages.emplace_back(rejected(cancel->client_id, cancel->request_id,
                    cancel->order_id, protocol::ResultCode::Duplicate));
            }
        } else {
            const auto existing = book_.find_order(cancel->order_id);
            if (!existing || existing->request.client_id != cancel->client_id) {
                outcome.messages.emplace_back(rejected(cancel->client_id, cancel->request_id,
                                                        cancel->order_id, protocol::ResultCode::NotFound));
            } else {
                const CancelResult result = book_.cancel(cancel->order_id);
                if (!result.canceled()) {
                    outcome.messages.emplace_back(rejected(cancel->client_id, cancel->request_id,
                        cancel->order_id, result.error == CancelError::UnknownOrderId
                            ? protocol::ResultCode::NotFound : protocol::ResultCode::NotLive));
                } else {
                    const auto state = book_.find_order(cancel->order_id);
                    outcome.messages.emplace_back(protocol::OrderResponse{cancel->client_id, cancel->request_id,
                        cancel->order_id, result.command_sequence, protocol::OrderStatus::Canceled,
                        protocol::ResultCode::Accepted, state->filled_quantity, 0, state->canceled_quantity});
                }
            }
            const auto response = std::find_if(outcome.messages.begin(), outcome.messages.end(), [](const auto& item) {
                return std::holds_alternative<protocol::OrderResponse>(item);
            });
            if (response == outcome.messages.end()) throw std::logic_error("exchange command has no order response");
            const auto response_bytes = protocol::encode(*response);
            if (prior == results_.end()) {
                storage_.save_request_outcome({key.client_id, key.request_id, encoded, response_bytes});
                results_.emplace(key, CachedResult{encoded, outcome});
            } else if (prior->second.encoded_request != encoded ||
                       protocol::encode(prior->second.result.messages.back()) != response_bytes) {
                throw std::logic_error("replayed cancellation outcome differs from deterministic execution");
            }
            applied_request_keys_.insert(key);
        }
    } else {
        throw protocol::ProtocolError("committed Raft entry is not an exchange command");
    }
    applied_results_[index] = outcome;
    applied_index_ = index;
}

std::optional<ReplicatedCommandResult> ReplicatedExchangeStateMachine::result(
    std::uint64_t client_id, std::uint64_t request_id) const {
    const auto it = results_.find(RequestKey{client_id, request_id});
    if (it == results_.end()) return std::nullopt;
    const auto response = std::find_if(it->second.result.messages.begin(), it->second.result.messages.end(),
        [](const auto& item) { return std::holds_alternative<protocol::OrderResponse>(item); });
    if (response == it->second.result.messages.end()) return std::nullopt;
    return ReplicatedCommandResult{{*response}};
}

std::optional<ReplicatedCommandResult> ReplicatedExchangeStateMachine::take_applied_result(
    raft::LogIndex index) {
    const auto it = applied_results_.find(index);
    if (it == applied_results_.end()) return std::nullopt;
    ReplicatedCommandResult result = std::move(it->second);
    applied_results_.erase(it);
    return result;
}

} // namespace atlas
