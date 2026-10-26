# Atlas Domain Models

This milestone defines exchange concepts separately from console output and transport. The public C++20 value types are in `include/atlas/domain.hpp`; there are no networking, persistence, concurrency, or consensus dependencies.

## Types and roles

| Type | Representation | Role |
|---|---|---|
| `OrderId` | `std::uint64_t` | Engine-assigned or caller-provided identity for an order; nonzero and never reused within the `OrderBook` lifetime, including after completion. |
| `ClientId` | `std::uint64_t` | Identifies the submitting client; nonzero and used for self-trade prevention. |
| `RequestId` | `std::uint64_t` | Identifies a client's request; nonzero and unique per client for the `OrderBook` lifetime. |
| `InstrumentId` | `std::uint64_t` | Identifies the traded instrument; nonzero. One `OrderBook` instance accepts only one instrument. |
| `PriceTicks` | `std::int64_t` | Integer count of the instrument's minimum price increment. No floating-point price values are used. |
| `Quantity` | `std::uint64_t` | Positive number of indivisible units. |
| `Side` | `enum class { Buy, Sell }` | Direction of an order and trade aggressor. Other enum representations are invalid. |
| `OrderType` | `enum class { Limit, Market }` | Market is represented for future extensibility but rejected as unsupported in this milestone; only limits are accepted. |
| `OrderStatus` | `enum class { Open, PartiallyFilled, Filled, Canceled }` | Lifecycle state for accepted orders. A canceled order has zero leaves; its executed and canceled quantities remain recorded. |
| `OrderRequest` | Value struct | Client request fields: order, client, request, instrument IDs, side, type, limit price, and original quantity. |
| `OrderState` / `Order` | Value struct | Accepted request plus engine command sequence, filled/leaves/canceled quantities, and lifecycle status. |
| `TradeSequence` | `std::uint64_t` | Strictly increasing sequence assigned to each execution record. |
| `EngineCommandSequence` | `std::uint64_t` | Strictly increasing sequence assigned to each accepted submission or successful cancellation; rejected operations do not consume it. |
| `Trade` | Value struct | Execution record with trade and command sequences, instrument, maker/taker order and client IDs, taker side, integer price, and integer quantity. |

`ParticipantId` and `Sequence` are compatibility aliases for `ClientId` and `EngineCommandSequence` respectively.

## Validation and numeric policy

- Zero order, client, request, and instrument IDs are invalid. Order IDs do not wrap and are never reused within the `OrderBook` lifetime. `(ClientId, RequestId)` is unique for the book's lifetime.
- Limit prices must be positive integer ticks: $1 \ldots 2^{63}-1$. Zero and negative values are rejected. The tick-to-currency conversion belongs to instrument configuration, not this domain layer.
- Quantities must be positive integers: $1 \ldots 2^{64}-1$. Zero quantity is rejected.
- `std::uint64_t`/`std::int64_t` represent the allowed numeric ranges. Overflow is never permitted: sequence counters reject further accepted commands before mutating state; quantity updates use checked/bounded arithmetic. Price and quantity inputs are already integer domain values, so no floating-point conversion occurs here.
- Invalid enum values, duplicate IDs/requests, unsupported market orders, and orders for a different instrument are rejected without modifying the order book.
- Cancellation returns `CancelError::UnknownOrderId` for an unknown ID and `CancelError::OrderNotLive` for filled, canceled, or otherwise inactive orders. These errors do not consume a command sequence; a successful cancellation returns the assigned sequence and canceled leaves quantity.
- The full validation helper is `validate_order_request()`. Book-specific duplicate request, duplicate order, and instrument consistency checks are applied by `OrderBook`.

Validation tests live in `tests/domain_tests.cpp`; order-book integration tests in `tests/order_book_tests.cpp` cover request identity, instrument consistency, and sequence assignment.
