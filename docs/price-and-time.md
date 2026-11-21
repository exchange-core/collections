# Price ladder and time windows

[exchange collections](../README.md) › Price ladder and time windows

- [`LongPriceLadder`](#longpriceladder) — the volume of one side of a book over a range of ticks
- [`LongRollingWindow`](#longrollingwindow) — the count and sum of deltas over the last span of time
- [`LongTimeSeries`](#longtimeseries) — append-only `(time, value)` pairs, as-of lookups, merged replay

---

## LongPriceLadder

Aggregated volume of one side of a book over a dense range of integer ticks: how much is available up to a price,
which tick a quantity reaches, what taking it costs.

```java
import exchange.core2.collections.price.LongPriceLadder;

final var asks = LongPriceLadder.withCosts(10_000, 4_096);   // ticks 10,000 .. 14,095
asks.addVolume(10_005, 300);
asks.addVolume(10_007, 200);

asks.lowestNonEmptyTick();      // 10,005: the best ask
asks.volumeAtOrBelow(10_006);   // 300
asks.priceFromLow(400);         // 10,007: a buy of 400 lots reaches it
asks.costFromLow(400);          // 300 × 10,005 + 100 × 10,007 = 4,002,200
```

A Fenwick tree over the levels answers range sums and walks by cumulative volume in O(log ticks); a point read is
O(1). `priceFromHigh`, `costFromHigh`, `highestNonEmptyTick` and `volumeAtOrAbove` serve the bids. A ladder from
`withCosts` keeps a second tree of `(tick − lowestTick) × volume` interleaved with the first, so an update touches
the same cache lines; the plain constructor skips it. The levels, their total and that weighted sum must fit a
non-negative `long`: an update that would break one throws and changes nothing. At most 2^30 ticks, 2^29 with
costs. Single-threaded.

`LongPriceLadderJmh` compares updates, `priceFromLow` and `costFromLow` with an array of levels walked from the best
tick: 1,024 levels with volume in a range of 1,024 or 1,048,576 ticks, quantities up to a quarter of the volume (the
walk passes 128 levels on average). Same machine and settings as the
[queue benchmarks](queues.md#longindexedfifoqueues), 1 GiB heap, ns/op:

| Ticks | Operation | `LongPriceLadder` | `withCosts` | Array walk |
|---:|---|---:|---:|---:|
| 1,024 | Set a level | 12.9 ± 0.1 | 18.7 ± 0.1 | 2.0 ± 0.0 |
| 1,048,576 | Set a level | 28.9 ± 0.0 | 33.9 ± 0.0 | 2.0 ± 0.0 |
| 1,024 | Tick a quantity reaches | 41.3 ± 0.1 | | 99.3 ± 0.0 |
| 1,048,576 | Tick a quantity reaches | 53.3 ± 0.1 | | 99.3 ± 0.0 |
| 1,024 | Cost of a quantity | | 41.2 ± 0.2 | 154.2 ± 0.0 |
| 1,048,576 | Cost of a quantity | | 54.6 ± 0.1 | 154.1 ± 0.0 |

The ladder pays at every update for queries that take O(log ticks) whatever their depth; the walk is cheap to
update and grows with the levels it passes. Keeping the costs adds 5-6 ns to an update.

---

## LongRollingWindow

The count and exact sum of signed deltas over the last `duration` units of time, `(now − duration, now]`: the
orders an account sent in the last second, the volume traded in the last 100 ms. The caller supplies the time,
nondecreasing, so live and replay behave alike.

```java
import exchange.core2.collections.time.LongRollingWindow;

final var orders = new LongRollingWindow(1_000_000_000L, 100);   // 1 s in ns, at most 100 at a time
if (!orders.tryAdd(nowNanos, 1)) {
    reject();                                                    // 100 orders in the last second
}
```

A ring of `(time, delta)` pairs, 16 bytes a slot, fixed at construction. `tryAdd` refuses, changing nothing, while
every slot is still in the window; `add` throws instead. `advanceTo` expires without adding. The sum is kept in 128
bits, exact even when it leaves the `long` range in between; `sum()` throws only if the current sum does not fit.
A window keeps every event: for a limit per account over many accounts, a counter per time bucket takes less
memory. Single-threaded.

---

## LongTimeSeries

Append-only `(time, value)` pairs in nondecreasing time, for backtests and replay: the quote a strategy knew at the
moment of a decision, then the events after it in one deterministic order.

```java
import exchange.core2.collections.time.LongTimeSeries;

final var quotes = new LongTimeSeries(1 << 20);
final var trades = new LongTimeSeries(1 << 20);
// ... appends in time order

final int known = quotes.lowerIndex(decisionTime);   // strictly before: a quote at decisionTime is not known yet
final var replay = LongTimeSeries.merge(decisionTime, quotes, trades);
while (replay.advance()) {
    handle(replay.source(), replay.time(), replay.value());   // equal times: quotes first, then by insertion
}
```

`floorIndex` finds the last observation at or before a time (the last of equal times), `lowerIndex` the last one
strictly before it; both are binary searches. Indices stay valid as the series grows. `cursor(from)` replays one
series up to its size at the call; `merge(fromTime, series...)` replays several from `fromTime` in time order, equal
times in the order of the arguments and then of insertion, in O(number of series) per step. `fixedCapacity(n)`
refuses appends past `n`; otherwise the arrays double. Single-threaded.
