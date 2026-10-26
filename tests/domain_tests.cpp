#include "atlas/domain.hpp"

#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using namespace atlas;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

OrderRequest valid_order() {
    return OrderRequest{1, 2, 3, 4, Side::Buy, OrderType::Limit, 10, 5};
}

void test_fixed_width_aliases_and_valid_domain_values() {
    static_assert(sizeof(OrderId) == sizeof(std::uint64_t));
    static_assert(sizeof(ClientId) == sizeof(std::uint64_t));
    static_assert(sizeof(RequestId) == sizeof(std::uint64_t));
    static_assert(sizeof(InstrumentId) == sizeof(std::uint64_t));
    static_assert(sizeof(Quantity) == sizeof(std::uint64_t));
    static_assert(sizeof(PriceTicks) == sizeof(std::int64_t));
    static_assert(sizeof(EngineCommandSequence) == sizeof(std::uint64_t));
    static_assert(sizeof(TradeSequence) == sizeof(std::uint64_t));

    require(validate_order_request(valid_order()) == DomainValidationError::None,
            "valid limit order passes validation");

    auto max_values = valid_order();
    max_values.id = std::numeric_limits<OrderId>::max();
    max_values.client_id = std::numeric_limits<ClientId>::max();
    max_values.request_id = std::numeric_limits<RequestId>::max();
    max_values.instrument_id = std::numeric_limits<InstrumentId>::max();
    max_values.limit_price = std::numeric_limits<PriceTicks>::max();
    max_values.quantity = std::numeric_limits<Quantity>::max();
    require(validate_order_request(max_values) == DomainValidationError::None,
            "maximum representable positive values pass validation");
}

void test_zero_ids_and_unsupported_or_invalid_types() {
    auto order = valid_order();
    order.id = 0;
    require(validate_order_request(order) == DomainValidationError::InvalidOrderId,
            "zero order ID rejected");

    order = valid_order();
    order.client_id = 0;
    require(validate_order_request(order) == DomainValidationError::InvalidClientId,
            "zero client ID rejected");

    order = valid_order();
    order.request_id = 0;
    require(validate_order_request(order) == DomainValidationError::InvalidRequestId,
            "zero request ID rejected");

    order = valid_order();
    order.instrument_id = 0;
    require(validate_order_request(order) == DomainValidationError::InvalidInstrumentId,
            "zero instrument ID rejected");

    order = valid_order();
    order.side = static_cast<Side>(255);
    require(validate_order_request(order) == DomainValidationError::InvalidSide,
            "invalid side rejected");

    order = valid_order();
    order.type = static_cast<OrderType>(255);
    require(validate_order_request(order) == DomainValidationError::InvalidOrderType,
            "invalid order type rejected");

    order = valid_order();
    order.type = OrderType::Market;
    require(validate_order_request(order) == DomainValidationError::UnsupportedOrderType,
            "market order currently unsupported");
}

void test_price_quantity_and_negative_price_rules() {
    auto order = valid_order();
    order.limit_price = 0;
    require(validate_order_request(order) == DomainValidationError::InvalidPrice,
            "zero price rejected");

    order = valid_order();
    order.limit_price = -1;
    require(validate_order_request(order) == DomainValidationError::InvalidPrice,
            "negative price rejected");

    order = valid_order();
    order.quantity = 0;
    require(validate_order_request(order) == DomainValidationError::InvalidQuantity,
            "zero quantity rejected");
}

} // namespace

int main() {
    const std::vector<std::pair<const char*, void (*)()>> tests{
        {"fixed-width and valid values", test_fixed_width_aliases_and_valid_domain_values},
        {"zero IDs and order types", test_zero_ids_and_unsupported_or_invalid_types},
        {"price and quantity rules", test_price_quantity_and_negative_price_rules},
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
