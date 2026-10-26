# Proposed Matching Examples

**Status:** Worked specification examples. Matching behavior is implemented; individual examples are not all separate automated test cases. Each example starts from a fresh empty book. Price values are integer ticks; quantities are integer units. `B` means buy and `S` means sell. Orders are `id/client: side type price x quantity`. Trade notation is `maker -> taker @ price x quantity`. Book notation lists bids and asks best-to-worst, with FIFO order IDs within each level. `filled/leaves/canceled/status` is shown for canceled orders; otherwise `filled/leaves/status` is shown.

Rules: maker-price execution; best-price then FIFO priority; limit residuals rest; market orders are rejected as unsupported; order IDs and per-client request IDs are reserved for the lifetime of the book; any self-trade in the incoming order's reachable match path rejects the entire command atomically; invalid requests do not mutate state or consume sequences.

## 1. Non-crossing limit orders rest

**Input:** `10/1 B Limit 100 x 5`, then `20/2 S Limit 105 x 3`.

**Trades:** none.

**Final book:** bids `100: [10 x 5]`; asks `105: [20 x 3]`.

**States:** `10: 0/5 Open`; `20: 0/3 Open`.

## 2. Incoming buy crosses and executes at resting ask price

**Input:** Rest `20/2 S Limit 101 x 4`; submit `10/1 B Limit 105 x 3`.

**Trades:** `20 -> 10 @ 101 x 3`.

**Final book:** bids empty; asks `101: [20 x 1]`.

**States:** maker `20: 3/1 PartiallyFilled`; taker `10: 3/0 Filled`.

## 3. Incoming sell crosses and executes at resting bid price

**Input:** Rest `10/1 B Limit 100 x 4`; submit `20/2 S Limit 95 x 2`.

**Trades:** `10 -> 20 @ 100 x 2`.

**Final book:** bids `100: [10 x 2]`; asks empty.

**States:** maker `10: 2/2 PartiallyFilled`; taker `20: 2/0 Filled`.

## 4. Better price takes precedence over older worse price

**Input:** Rest `20/2 S Limit 103 x 2`; then `30/3 S Limit 101 x 2`; submit `10/1 B Limit 105 x 3`.

**Trades:** `30 -> 10 @ 101 x 2`, then `20 -> 10 @ 103 x 1`.

**Final book:** bids empty; asks `103: [20 x 1]`.

**States:** `30: 2/0 Filled`; `20: 1/1 PartiallyFilled`; `10: 3/0 Filled`.

## 5. FIFO at the same price

**Input:** Rest `20/2 S Limit 101 x 2`; then `30/3 S Limit 101 x 2`; submit `10/1 B Limit 101 x 3`.

**Trades:** `20 -> 10 @ 101 x 2`, then `30 -> 10 @ 101 x 1`.

**Final book:** bids empty; asks `101: [30 x 1]`.

**States:** `20: 2/0 Filled`; `30: 1/1 PartiallyFilled`; `10: 3/0 Filled`.

## 6. Partially filled maker retains FIFO priority

**Input:** Rest `20/2 S Limit 101 x 5`; then `30/3 S Limit 101 x 2`; submit `10/1 B Limit 101 x 2`; then submit `11/4 B Limit 101 x 2`.

**Trades:** `20 -> 10 @ 101 x 2`, then `20 -> 11 @ 101 x 2`.

**Final book:** bids empty; asks `101: [20 x 1, 30 x 2]`.

**States:** `20: 4/1 PartiallyFilled`; `30: 0/2 Open`; `10: 2/0 Filled`; `11: 2/0 Filled`.

## 7. Limit taker sweeps levels and rests its residual

**Input:** Rest `20/2 S Limit 101 x 2`; `30/3 S Limit 102 x 3`; submit `10/1 B Limit 103 x 7`.

**Trades:** `20 -> 10 @ 101 x 2`, then `30 -> 10 @ 102 x 3`.

**Final book:** bids `103: [10 x 2]`; asks empty.

**States:** `20: 2/0 Filled`; `30: 3/0 Filled`; `10: 5/2 PartiallyFilled`.

## 8. Limit price prevents a non-crossing execution

**Input:** Rest `20/2 S Limit 102 x 4`; submit `10/1 B Limit 101 x 5`.

**Trades:** none.

**Final book:** bids `101: [10 x 5]`; asks `102: [20 x 4]`.

**States:** `20: 0/4 Open`; `10: 0/5 Open`.

## 9. Limit order consumes multiple levels and rests residual

**Input:** Rest `20/2 S Limit 100 x 2`; `30/3 S Limit 101 x 1`; submit `10/1 B Limit 101 x 5`.

**Trades:** `20 -> 10 @ 100 x 2`, then `30 -> 10 @ 101 x 1`.

**Final book:** bids `101: [10 x 2]`; asks empty.

**States:** `20: 2/0 Filled`; `30: 1/0 Filled`; taker `10: 3/2 PartiallyFilled`.

## 10. Market order is rejected as unsupported

**Input:** Submit `10/1 S Market x 4` to an empty book.

**Trades:** none.

**Final book:** bids empty; asks empty.

**States/results:** Request rejected as unsupported. No order record is created, no sequence is consumed, and the book remains empty.

## 11. Cancel a partially filled resting order

**Input:** Rest `20/2 S Limit 101 x 5`; submit `10/1 B Limit 101 x 2`; cancel order `20`.

**Trades:** `20 -> 10 @ 101 x 2`.

**Final book:** bids empty; asks empty.

**States:** `20: 2/0/3 Canceled`; `10: 2/0 Filled`. Cancellation moves the 3 remaining units to canceled quantity and does not reverse the trade.

## 12. Duplicate ID rejection leaves the book unchanged

**Input:** Rest `10/1 B Limit 100 x 2`; submit `10/2 S Limit 100 x 2`.

**Trades:** none.

**Final book:** bids `100: [10 x 2]`; asks empty.

**States/results:** Original `10: 0/2 Open`; second request rejected as duplicate. Rejection creates no order state and consumes no arrival or trade sequence.

## 13. Invalid zero quantity is rejected without mutation

**Input:** Submit `10/1 B Limit 100 x 0` to an empty book.

**Trades:** none.

**Final book:** bids empty; asks empty.

**States/results:** Request rejected for invalid quantity; no order record or sequence is created.

## 14. Same-owner match rejects the incoming order atomically

**Input:** Rest `20/2 S Limit 100 x 2`; then `30/1 S Limit 101 x 2`; submit `10/1 B Limit 101 x 5`.

**Trades:** none. The incoming order would first match external maker `20`, then reach its own resting order `30`; preflight detects that before execution and rejects the whole incoming order.

**Final book:** asks `100: [20 x 2]`, `101: [30 x 2]`; bids empty.

**States/results:** `20: 0/2 Open`; `30: 0/2 Open`; incoming `10` rejected. No order state or sequence is created for `10`.

This is atomic rejection: no earlier hypothetical fills are committed, and the resting book is unchanged.

## Proposed additional edge-case scenarios

These need no worked book trace but should have explicit unit tests: cancel unknown/filled/already-canceled IDs; sequence exhaustion; arithmetic limits; invalid enum values; unsupported market order rejection; same-price FIFO after canceling the head; a limit order matching exactly at its limit; duplicate IDs after terminal states; and maker/taker identity for both sides.
