# Queues

[exchange collections](../README.md) › Queues

- [`LongIndexedFifoQueues`](#longindexedfifoqueues) — FIFO queues with one entry ID index: the orders of price levels
- [`LongIndexedEventQueue`](#longindexedeventqueue) — events by time, then priority, cancelled by ID: timers, replay
- [`LongSequenceBuffer`](#longsequencebuffer) — a reorder window: messages by sequence, delivered in order without gaps

---

## LongIndexedFifoQueues

A family of FIFO queues with a shared entry ID index. Use the price tick as the queue key and the order ID
as the entry ID. Field 0 is the value. Store a handle to an external order there, or keep application fields
inline with the links:

```java
import exchange.core2.collections.queue.LongIndexedFifoQueues;

final int SIZE = 0, FILLED = 1, UID = 2;
final var bids = LongIndexedFifoQueues.fixedCapacity(1_000, 100_000, 3);
final long level = bids.getOrCreateQueue(50);   // keep this handle in the price index
final long e = bids.appendTo(level, 101, 30);   // size 30, other fields initially zero
bids.setField(e, UID, 42);
bids.setField(e, FILLED, 5);

final long order = bids.findEntry(101);         // one lookup to inspect the order
if (order != LongIndexedFifoQueues.NO_HANDLE && bids.getField(order, UID) == 42) {
    long remaining = bids.getField(order, SIZE) - bids.getField(order, FILLED);
    bids.removeEntry(order);                   // release the order; apply remaining to the level total
}

// Execution by cached level: save the successor before invalidating the entry.
for (long h = bids.first(level); h != LongIndexedFifoQueues.NO_HANDLE; ) {
    long next = bids.next(h);
    bids.removeEntry(h);
    h = next;
}
bids.releaseEmptyQueue(level);                  // returns false if orders remain
```

IDs are unique across the family. A duplicate `append(key, id, value)` returns false; `appendTo` returns
zero. Neither changes the original order. `valueOf(id)` reads field 0 (throws if absent); `remove(id, ifAbsent)`
cancels and returns field 0 using **one ID index removal**. It copies the owner, neighbours and value before
freeing the record. A conditional cancel that first looks up a handle also pays for that lookup.

`first(queueHandle)` and `next(entryHandle)` return zero at the end. `removeFirstByHandle(queueHandle)`
returns field 0 without a price lookup. `moveToLast(entryHandle, destinationQueueHandle)` relinks an entry
in the same or another queue, keeping its handle and fields; no entry allocation or ID lookup. A moved
entry's `next` reflects its new position. Entry removal, queue release and clear invalidate the respective
handles. Passing a queue handle to an entry method or vice versa throws `IllegalArgumentException`.
Handles belong to their originating instance. Inline fields share the record (with
three application fields it is 64 bytes including the header and ID); cache-line alignment is not guaranteed.

Levels created solely through `append(key, ...)` disappear when empty. `findQueue(key)` borrows a handle
without changing that rule. `getOrCreateQueue(key)` explicitly retains a level (also an existing one), so
its handle survives empty/refill cycles. Retained empty levels count toward the fixed queue limit.
If you create a level for a reprice, call `moveToLast(entry, level)` followed by `unretainQueue(level)`
to recycle the destination when its last order leaves. On an already empty level, `unretainQueue` frees it
immediately. `releaseEmptyQueue` still returns false while orders remain.
`firstId`, `firstValue`, `removeFirst` and `removeFirstByHandle` throw
`NoSuchElementException` for an empty level. Zero is a valid key, ID and value.

Both record tables and their indexes are shared by all levels. An append or removal does a constant number
of hash operations and link updates. For a growing collection use `new LongIndexedFifoQueues(expectedQueues,
expectedEntries)`; growth may rehash on the calling thread. With `fixedCapacity` both limits are exact:
an append that needs more room throws `IllegalStateException` without changing any queue. Single-threaded.
Price ordering (e.g. finding the best bid) is a separate index. The key-to-queue hash index still exists for
the key API; traversing a retained level by handle bypasses it.

**Queue benchmark setup.** All three queue benchmarks ran on a Xeon Gold 6244 (HT enabled), JDK 27+35,
JMH 1.34. The JVM started on two housekeeping CPUs of one socket with memory bound to its NUMA node,
`-XX:+AlwaysPreTouch` and `-XX:+UseTransparentHugePages`. With `-Daffinity.reserved` set to the isolated CPUs,
each benchmark thread pinned itself to an isolated core of that socket and reserved both of its hyper-threads.
All runs used `-t 1 -f 2 -wi 3 -w 500ms -i 5 -r 500ms`. Times are means with JMH's 99.9% confidence intervals.

`LongIndexedFifoQueuesJmh.cancelAndAppend` cancels a random live ID and submits a replacement at the same
price. Both implementations maintain a global ID index. The run used 4,096 levels × 16 orders
(`-p levels=4096 -prof gc`) and a fixed 256 MiB JVM heap.

| Implementation | ns per cancel + submit | Allocated bytes per pair |
|---|---:|---:|
| `LongIndexedFifoQueues` | 167 ± 1 | ≈0 |
| `LinkedHashMap` per price | 206 ± 13 | 136 |

**Order-book workloads.** `FifoOrderBookJmh` starts with 4,096 price levels and **1M or 10M live orders**.
Initial orders are interleaved across levels, not laid out consecutively within a level. Both implementations
keep size, filled quantity and uid on the order. The baseline uses pooled order objects with parent, prev and
next references, head/tail references per level and an Eclipse Collections primitive ID-to-object hash map.
It models the linked orders in `OrderBookDirectImpl`; it does not run that matching engine or its ART ID index.
The test assigns consecutive order IDs. Eclipse Collections 13.0.0 uses the low bits of the ID for the first
probe, while `LongLongHashtable` mixes the ID before probing. The timing includes this difference in ID indexes.

- **Cancel + submit:** cancel a random live order by ID, submit its replacement at the same price.
- **Execute heads:** choose an existing level, follow and fully fill 16 orders, count their remaining volume and
  remove their IDs. Price selection and replenishment are outside the timed region; the unit is one filled order.
- **Empty/refill:** one hot price with a changing order ID cycles 0 → 1 → 0 against the large background.
  By key, the price leaves and re-enters the index on every cycle. The retained variant reuses its handle/reference.

`FifoOrderBookJmh` used `-Xms2g -Xmx3g` on the same isolated core. Times are in ns per operation.

| Operation | Records, 1M | Object links, 1M | Records, 10M | Object links, 10M |
|---|---:|---:|---:|---:|
| Cancel + submit, per pair | 468 ± 6 | 365 ± 203 | 609 ± 4 | 659 ± 834 |
| Execute heads, per filled order | 171.42 ± 0.45 | 53 ± 30 | 221 ± 7 | 48 ± 3 |
| Empty/refill by key, per cycle | 159 ± 1 | 85 ± 18 | 230.3 ± 0.5 | 211 ± 124 |
| Empty/refill retained, per cycle | 117 ± 5 | 73 ± 18 | 200 ± 3 | 134 ± 132 |

Head execution took 3.2× as long with records at 1M and 4.6× at 10M. A separate laptop JFR profile
identified the record-header check when writing the next order's `PREV` field and ID-index removal as hot spots.
Internal slot links are a candidate for a separate benchmark.

For 10M object orders, cancel + submit peaked in the second measurement iteration of **both** forks.
The five iteration means were 319, 1,647, 490, 464, 328 ns and 318, 1,743, 491, 460, 327 ns.
An Eclipse Collections ID-index rebuild is one possible cause. The records iteration means ranged from
605 to 611 ns for the same workload. Measuring per-order tail latency requires a latency distribution.

---

## LongIndexedEventQueue

Timers and replay events ordered by time, then priority (smaller first), then insertion order. Time is a
signed `long` in application-defined units; the queue never reads a clock. Each live event has a unique
`long` ID. Cancelling by ID frees its slot immediately.

```java
import exchange.core2.collections.queue.LongIndexedEventQueue;

final var events = LongIndexedEventQueue.fixedCapacity(100_000);
events.schedule(101, 500, 1);     // ID, time, priority
events.schedule(102, 500, 1);
events.reschedule(101, 500, 1);   // 102 is now first among the peers
events.schedule(103, 500, 0);     // higher priority at the same time
events.cancel(102);

while (!events.isEmpty() && events.firstTime() <= now) {
    dispatch(events.removeFirst());    // at now == 500: 103, then 101
}
```

Scheduling an existing ID returns `false` without changing it, even at capacity. Cancel and reschedule return
`false` for an absent ID. A reschedule gets a fresh insertion sequence, even when its deadline and priority
stay the same. The head accessors (`firstId`, `firstTime`, `firstPriority`) and `removeFirst` throw
`NoSuchElementException` on an empty queue. The sequence resets when emptied or cleared; exhaustion of the
signed 64-bit sequence before then refuses a new insertion or reschedule with `IllegalStateException`.

Use `new LongIndexedEventQueue(expectedSize)` for growing storage, or `fixedCapacity(n)` for an exact,
preallocated limit. At the limit a new ID throws `IllegalStateException` and leaves the queue unchanged.
Reading the head takes O(1). A mutation does O(log n) heap work plus the hash index operation. Growing
storage may also resize. A four-way heap keeps time, priority, sequence and record handles in parallel
primitive arrays (28 bytes per allocated heap slot). Each indexed record holds a position, ID and header
(24 bytes per record before page rounding). Comparing children only reads heap arrays. Moving an event
updates its indexed position by handle. Single-threaded.

`LongIndexedEventQueueJmh` compares cancel-and-schedule, reschedule and draining 64 simultaneous deadlines
against a `TreeSet` with an ID index. The burst benchmark reports time per removed event.

The event queue ran on the same isolated Xeon core with the FIFO warm-up and measurement settings,
`-p size=65536,1000000` and a fixed 1 GiB JVM heap. Times are means in ns/op with JMH's 99.9% confidence
intervals.

| Live events | Operation | Four-way heap | Tree + hash map |
|---:|---|---:|---:|
| 65,536 | Drain 64 tied deadlines, per event | 113.8 ± 0.1 | 31.0 ± 0.4 |
| 1,000,000 | Drain 64 tied deadlines, per event | 130 ± 1 | 35.8 ± 0.5 |
| 65,536 | Cancel + schedule, per pair | 172.9 ± 0.4 | 721 ± 26 |
| 1,000,000 | Cancel + schedule, per pair | 501 ± 5 | 2,003 ± 629 |
| 65,536 | Reschedule | 84.7 ± 0.1 | 709 ± 21 |
| 1,000,000 | Reschedule | 293 ± 2 | 1,990 ± 662 |

At 1M, the heap was 4.0× faster for cancel + schedule and 6.8× faster for rescheduling. The tree drained
the burst 3.6× faster. Each invocation inserts the same 64 IDs (−1 through −64) at a single deadline
ahead of a fixed backlog, outside the timed region.

---

## LongSequenceBuffer

A reorder window for one feed session: messages arrive by sequence number, out of order or twice, and leave in
order without gaps.

```java
import exchange.core2.collections.queue.LongSequenceBuffer;

final var feed = new LongSequenceBuffer(1024, 1);   // capacity, first sequence
feed.offer(2, payload2);                            // ACCEPTED: waits for 1
feed.offer(2, other);                               // DUPLICATE: the first value stays
feed.drainContiguous(handler, 64);                  // 0: sequence 1 is missing
requestAgain(feed.nextExpected(), feed.firstBuffered());   // the gap [1, 2)
feed.offer(1, payload1);
feed.drainContiguous(handler, 64);                  // 2: delivers 1, then 2
```

`offer` answers `ACCEPTED`, `DUPLICATE`, `TOO_OLD` (already delivered) or `TOO_FAR_AHEAD` (past the window); only
`ACCEPTED` changes the buffer. A pair is consumed once its callback returns: if the callback throws, the pair stays
for a retry; the callback may not change the buffer. After recovering from a snapshot up to sequence S,
`skipTo(S + 1)` drops what the snapshot covers and keeps the messages buffered after it; `reset` starts a new
session. Every signed sequence works; once `Long.MAX_VALUE` is delivered, the session is exhausted. Fixed memory,
single-threaded.
