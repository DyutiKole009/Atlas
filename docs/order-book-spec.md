# Atlas Order Book and Deterministic Matching Specification

**Status:** Behavioral specification for the initial matching implementation. The specification is not proof of correctness; validation and test results are reported separately.

## 1. Scope and terminology

This is a single-instrument, in-memory, single-threaded matching-core specification. It deliberately excludes networking, concurrency, persistence, replication, Raft, market-data distribution, and performance optimization. One `OrderBook` instance represents one instrument. Cross-instrument behavior is outside scope.

A command is applied synchronously and completely before the next command. Determinism means the same initial empty book and same ordered command stream produce identical accept/reject results, trades, order states, and final book. Wall-clock time, thread scheduling, hash iteration, and external randomness must not affect matching.

## 2. Data model

All identifiers and numeric bounds are fixed by the domain types in `include/atlas/domain.hpp`:

- `OrderId`: unsigned 64-bit integer, supplied by the caller, unique within the engine's lifetime, and never reused after fill or cancellation. The initial single-book engine has the same lifetime/scope as its book.
- `ClientId`: unsigned 64-bit integer, supplied by the caller; used for self-trade prevention.
- `RequestId`: unsigned 64-bit integer, unique per client for the engine lifetime.
- `InstrumentId`: unsigned 64-bit integer identifying the instrument. Each `OrderBook` accepts one instrument.
- `PriceTicks`: signed 64-bit integer count of the instrument's smallest configured price increment. No floating-point prices are accepted.
- `Quantity`: unsigned 64-bit integer count of indivisible units. No floating-point quantities are accepted.
- `EngineCommandSequence`: unsigned 64-bit sequence assigned to each accepted submit or cancel operation; rejected commands consume none.
- `TradeSequence`: unsigned 64-bit sequence assigned to each emitted trade; never derived from a clock.

```cpp
using OrderId = std::uint64_t;
using ClientId = std::uint64_t;
using RequestId = std::uint64_t;
using InstrumentId = std::uint64_t;
using PriceTicks = std::int64_t;
using Quantity = std::uint64_t;
using EngineCommandSequence = std::uint64_t;
using TradeSequence = std::uint64_t;

enum class Side { Buy, Sell };
enum class OrderType { Limit, Market }; // Market is reserved and rejected in the initial milestone.
enum class OrderStatus {
    Open,     // accepted and resting, with no fills
    PartiallyFilled,
    Filled,
    Canceled
};

struct OrderRequest {
    OrderId id;
    ClientId client_id;
    RequestId request_id;
    InstrumentId instrument_id;
    Side side;
    OrderType type;
    PriceTicks limit_price; // positive for Limit; Market is unsupported initially
    Quantity quantity;      // original quantity, strictly positive
};

struct OrderState {
    OrderRequest request;
    EngineCommandSequence command_sequence;
    Quantity filled_quantity;
    Quantity leaves_quantity;   // positive only while resting/live
    Quantity canceled_quantity; // removed from leaves on cancellation
    OrderStatus status;
};

struct Trade {
    TradeSequence trade_sequence;
    EngineCommandSequence command_sequence;
    InstrumentId instrument_id;
    OrderId maker_order_id;
    OrderId taker_order_id;
    ClientId maker_client_id;
    ClientId taker_client_id;
    Side taker_side;
    PriceTicks price;
    Quantity quantity;
};
```

`OrderState` exists for every accepted order, including filled and canceled orders, so callers can query terminal state and IDs remain reserved for the engine lifetime. The initial single-book implementation can retain this registry for the same lifetime. Persistence/retention policy is not specified here.

## 3. Order lifecycle

1. Validate the request without changing book state. Reject invalid requests, duplicate order IDs, duplicate `(client_id, request_id)` pairs, instrument mismatches, and `Market` orders (unsupported in the initial milestone) with a typed result. These rejections create no order record, trade, or sequence.
2. For a valid limit order, preflight the deterministic match path. If it would reach an eligible resting order owned by the same `client_id` while the taker still has quantity, reject the incoming order atomically. No hypothetical external fills are committed, and no order record or sequence is consumed.
3. If preflight succeeds, reserve the next engine-command sequence and create an order with `filled_quantity = 0`, `leaves_quantity = quantity`, and `canceled_quantity = 0`.
4. Match against eligible resting orders in price-time priority, generating one trade per maker order consumed.
5. If leaves reach zero, status is `Filled`. Otherwise add the limit order's residual to its price level as `Open` if it had no fills, or `PartiallyFilled` if it did.

For every accepted order, `filled_quantity + leaves_quantity + canceled_quantity == request.quantity`. `leaves_quantity` is the quantity still eligible for execution and becomes zero when an order is fully filled or canceled.

**Cancellation lifecycle:** cancel by order ID. Only a live resting order (`Open` or `PartiallyFilled` with positive leaves) can be canceled. A successful cancel removes its remaining quantity from the book, moves that amount from `leaves_quantity` to `canceled_quantity`, sets status to `Canceled`, and consumes one engine-command sequence; `filled_quantity` is unchanged. Canceling an unknown, filled, or already canceled order returns a typed error and changes nothing or consumes no sequence.

## 4. Book representation and priority

For the initial clear implementation, use:

- A descending ordered map from bid `PriceTicks` to bid price levels.
- An ascending ordered map from ask `PriceTicks` to ask price levels.
- Each price level stores FIFO order IDs in arrival order.
- An order lookup table maps each accepted ID to its `OrderState` and current book location.

No match decision may depend on lookup-table iteration order. Matching inspects only the best opposite-side price and the first ID at that level. A level is erased when its FIFO becomes empty. Cancellation removes the ID from its FIFO and removes an empty level. A simple sequence-ordered map for lookup is a reasonable first implementation; changing container choice later must preserve observable behavior.

Best bid is the highest bid; best ask is the lowest ask. A limit buy is eligible to match asks whose price is at most its limit. A limit sell is eligible to match bids whose price is at least its limit. Matching stops at the first absent level or a price that fails the limit's crossing condition. Market orders are unsupported in the initial milestone and must be rejected without mutation. Any later market-order support requires explicit protection against excessive execution prices.

Price-time priority is lexicographic: better price first, then smaller accepted `command_sequence` first at the same price. A partially filled resting order retains its original sequence and FIFO position. A newly accepted order never jumps ahead of an older order at the same price.

## 5. Matching, fills, and trade price

- The newly submitted order is the **taker**; the resting order is the **maker**.
- The trade price is always the resting maker order's price, including when the resting order is itself a previously partially filled order.
- **Self-trade prevention:** before changing state, deterministically simulate the incoming limit order's matching path. If the path reaches an eligible resting order with the same `client_id` while the taker still has quantity, reject the entire incoming order without mutation, fills, record creation, or sequence consumption. Earlier hypothetical external fills are not committed. An own order that would not be reached because the taker would already be fully filled does not cause rejection.
- Trade quantity is `min(taker.leaves_quantity, maker.leaves_quantity)`.
- Decrement both leaves quantities and increment both filled quantities by exactly the trade quantity.
- Emit trades in the order in which maker orders are consumed. One maker order produces at most one trade for a given taker command; a taker can produce multiple trades across makers/price levels.
- Remove a maker from the FIFO immediately when its leaves reach zero; retain its terminal `Filled` order record.
- A limit taker's residual quantity rests at its submitted limit price unless fully filled.
- Market orders are rejected as unsupported in the initial milestone. Later support requires explicit execution-price protection.
- No trade occurs unless prices cross. The book must never finish a command with a crossed resting market (best bid >= best ask), assuming all prior accepted commands obeyed this specification.

## 6. Numeric representation, validation, and overflow

Prices are integer ticks, with the instrument's tick size defined outside the matching core. A limit price must be greater than zero; quantity must be greater than zero. Order, client, request, and instrument IDs must be in the nonzero domain. The `(client_id, request_id)` pair must be unique. The `Market` order type is recognized but unsupported and rejected initially; market-price encoding is not defined until that later milestone.

Before any mutation, validate side/type enum values, all ID domains, quantity, and limit-price rules. Rejection must not consume engine-command or trade sequence numbers and must not modify the book. Duplicate order IDs and duplicate per-client request IDs are rejected even after the original order is filled or canceled. A request that would match a same-client resting order is rejected atomically as described above.

All additions and subtractions use checked arithmetic. The following are errors, never wraparound:

- `filled_quantity + trade_quantity` or related quantity arithmetic exceeding `Quantity` range;
- engine-command or trade sequence exhaustion;
- any aggregate quantity if an aggregate is maintained;
- conversion from external price/quantity representations into integer ticks/units.

The matcher should calculate and validate all quantities and required sequence capacity for a command before committing its state changes, so a rejected command cannot leave partial mutations. Given positive bounded inputs and `min`-based fills, individual leaves arithmetic is safe, but counters still require checked capacity. If the required engine-command or trade sequence cannot be represented, reject the current command without mutation; never wrap or partially commit.

## 7. Determinism and replay invariants

- Commands are applied in a single, explicit input order. Arrival order is the only tie-breaker for equal prices.
- No timestamps, floating point, random values, OS scheduling, or unordered-container traversal participate in a decision.
- Accepted engine-command sequences and emitted trade sequences are explicit, strictly increasing, and never reused. Rejected commands consume neither.
- For every accepted order, `filled_quantity + leaves_quantity + canceled_quantity == request.quantity`; leaves are positive only while live.
- Every live order appears exactly once in exactly one side/price FIFO; every FIFO ID resolves to a live order whose side, price, and leaves agree with that level.
- No terminal order appears in a FIFO; no empty price level remains.
- Trades have positive quantities, equal quantity changes on maker and taker, and execute at the maker's limit price. Trade price is never a taker's market price.
- After each command the book is uncrossed, all levels are correctly sorted, and FIFO sequence order is strictly increasing.
- Given identical initial state, configured instrument tick rules, and ordered command stream, accepted/rejected results, sequences, trades, order states, and book snapshot are byte-for-byte equivalent after canonical serialization. Canonical serialization format is future work; this is a behavioral requirement, not a persistence design.

## 8. Policy decisions recorded

The examples use the supplied policy decisions to make expected outcomes concrete. They remain proposed expected behavior until implementation and tests establish correctness.

| Decision | Specified behavior | Status |
|---|---|---|
| Duplicate order ID | Unique engine-lifetime ID; never reused, including after fill/cancel | Accepted |
| Matching priority | Best price, then FIFO within price | Accepted |
| Trade price | Resting maker's price | Accepted |
| Self-trading | Preflight the match path; reject the entire incoming order if it would reach a same-owner resting order | Accepted |
| Market orders | Deferred; reject as unsupported now; future support requires execution-price protection | Accepted |
| Price and quantity | Integer ticks and integer quantities | Accepted |
| Invalid order | Reject without mutation or sequence consumption | Accepted |
| Cancellation | Cancel only remaining quantity; reject unknown or completed order | Accepted |
| Partial fill then cancellation | Preserve fills, set status to `Canceled`, and move residual to canceled quantity | Accepted |
| Determinism | Explicit engine-command and trade sequences; no wall-clock-dependent matching | Accepted |

These recorded decisions replace the earlier open policy choices. Market-remainder status is not applicable because market orders are deferred. The implementation reserves order IDs for the lifetime of their `OrderBook`; a new book instance is a separate lifetime because no multi-book engine registry exists yet.

## 9. C++20 interface

The implemented interface is in `include/atlas/order_book.hpp`. The summary below highlights its public surface. It avoids third-party libraries and deliberately leaves transport, storage, threading, and optimization out.

```cpp
struct SubmitResult {
    SubmitError error;
    std::optional<OrderState> order;
    std::vector<Trade> trades; // emitted in deterministic execution order
};

struct CancelResult {
    CancelError error;
    OrderId order_id;
    Quantity canceled_quantity;
    EngineCommandSequence command_sequence;
};

class OrderBook {
public:
    SubmitResult submit(const OrderRequest& request);
    CancelResult cancel(OrderId order_id);
    std::optional<OrderState> find_order(OrderId order_id) const;
    BookSnapshot snapshot() const; // canonical best-to-worst prices, FIFO within level
    std::optional<InstrumentId> instrument_id() const noexcept;
};
```

The implementation exposes strongly typed rejection/error enums rather than string matching. `BookSnapshot` is a deterministic value representation suitable for assertions; canonical persistence serialization is future work.

## 10. Test coverage and additional test plan

The existing CTest suite includes matching tests in `tests/order_book_tests.cpp` and domain validation tests in `tests/domain_tests.cpp`. The following list records tested behavior and useful further coverage; it is not a claim that every listed case is already tested.

### Unit tests

1. Empty book snapshot and best bid/ask.
2. Accept valid limit orders; reject zero price/quantity and invalid ID/enum values without mutation.
3. Reject duplicate IDs after open, partial, filled, and canceled lifecycles; reject market orders as unsupported.
4. No cross for non-crossing limit orders; correct bid/ask ordering.
5. Match at maker price for incoming buy and incoming sell.
6. Better price beats older worse price; FIFO breaks ties at equal price.
7. Partial maker fill preserves its priority and correct leaves/status.
8. Taker sweeps multiple price levels in price-time order.
9. Limit taker residual rests; unsupported market orders reject without mutation.
10. Cancel open and partially filled order; verify level removal and quantities.
11. Reject cancellation of unknown/terminal/non-resting order without mutation; successful cancellation consumes one engine-command sequence.
12. Verify self-trade rejection is atomic even when external makers precede the same-owner order in a hypothetical match path.
13. Checked sequence/quantity overflow behavior at testable boundaries.
14. Verify all invariants after every command in table-driven scenarios.

### Property-based tests

The current matching suite uses a deterministic generated stream without an added dependency. Further property-based coverage can generate command streams and verify after every command:

- quantities remain nonnegative and per-order conservation (`filled + leaves + canceled = original`) holds;
- only crossing prices trade and each trade uses maker price;
- maker selection obeys best-price then FIFO priority;
- no crossed resting book remains;
- every live order occurs exactly once in the matching FIFO and terminal orders do not occur there;
- canceling a live order moves only its leaves to canceled quantity and does not alter prior trades;
- applying the same command stream twice from empty books gives identical results and snapshots;
- rejected commands leave the snapshot and all prior order states/trades unchanged;
- total buy-side and sell-side filled quantity agree with total trade quantity.

Keep generator seeds and failing command streams in test output so failures are reproducible. Differential testing against another implementation is deferred until there is an independently specified oracle.

## 11. Implementation boundary

The first engine milestone implements only one in-memory deterministic `OrderBook`, these value types, cancellation/submission, and unit tests. Do not add networking, persistence, replication, concurrency, Raft, GUI, benchmark infrastructure, speculative libraries, or low-level performance tuning in this milestone.
