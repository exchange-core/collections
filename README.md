# exchange collections
[![][license img]][license]

**Exchange Collections** is an open source high-performance Java collections library, extracted from
the [exchange-core](https://github.com/exchange-core/exchange-core) matching engine.

All collections are primitive-specialized (`long` keys, `long` or object values), allocation-conscious
and designed for low-latency, single-threaded hot paths.

### Modules

| Module                  | Artifact                | Contents                                                      |
|-------------------------|-------------------------|---------------------------------------------------------------|
| `collections-core`      | `collections-core`      | Adaptive Radix Tree, long-long hashtables, objects pool        |
| `collections-orderbook` | `collections-orderbook` | Order book implementation built on top of the core collections |
| `collections-affinity`  | `collections-affinity`  | Pinning threads to CPU cores (Linux, Windows), no dependencies |
| `tests-perf`            | –                       | Latency / throughput benchmarks (not published)                |
| `tests-stress`          | –                       | Long-running randomized stress tests (not published)           |

### Requirements

- **To use the published artifacts:** Java 26+ (0.6.0). The next release drops the published
  bytecode level to Java 17; `collections-affinity` (new) needs Java 22+, as it uses the Foreign
  Function and Memory API.
- **To build from source:** JDK 26 and Maven 3.9+ — the benchmark and stress modules use newer
  language APIs than the published ones.

### Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-core</artifactId>
    <version>0.6.0</version>
</dependency>
```

And, if you need the order book:

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-orderbook</artifactId>
    <version>0.6.0</version>
</dependency>
```

To build from source and install into the local repository:

```bash
mvn clean install
```

---

## Collections

| Class                      | Type                      | Keys → values    | Status       |
|----------------------------|---------------------------|------------------|--------------|
| `LongAdaptiveRadixTreeMap` | Adaptive Radix Tree (ART) | `long` → object  | stable       |
| `LongLongHashtable`        | open-addressing hashtable | `long` → `long`  | stable       |
| `LongLongLL2Hashtable`     | hashtable, async resizing | `long` → `long`  | stable       |
| `ObjectsPool`              | object pool               | –                | stable       |

---

### LongAdaptiveRadixTreeMap

An [Adaptive Radix Tree](https://db.in.tum.de/~leis/papers/ART.pdf) map: a sorted `long` → object map
that behaves like `TreeMap<Long, V>`, but never re-balances and never re-hashes, so there are no
latency spikes caused by insertion.

**Features**
- keys are kept in **signed** ascending order (same order as `TreeMap<Long, V>`)
- no re-balancing, no re-sizing — stable worst-case latency
- internal nodes are recycled through `ObjectsPool` — low GC pressure
- neighbour lookups (`getHigherValue` / `getLowerValue`) and ordered traversal

```java
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;

final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();

map.put(1_000L, "first");
map.put(2_000L, "second");
map.put(-500L, "negative");

map.get(2_000L);        // "second"
map.get(12_345L);       // null — missing keys return null

map.getHigherValue(1_000L);  // "second"   — closest key strictly above
map.getLowerValue(1_000L);   // "negative" — closest key strictly below

map.remove(1_000L);
map.size(Integer.MAX_VALUE); // 2
```

Ordered traversal — `forEach` walks ascending, `forEachDesc` descending. Both take a `limit` and
return the number of entries actually visited, which makes them a natural fit for top-N views
(L2 market data, for example):

```java
map.forEach((key, value) -> System.out.println(key + " -> " + value), 10);
map.forEachDesc((key, value) -> System.out.println(key + " -> " + value), 10);

// materialize everything (produces garbage — avoid on hot paths)
map.entriesList().forEach(e -> System.out.println(e.getKey() + " -> " + e.getValue()));
```

To recycle nodes across many short-lived maps, pass a shared pool:

```java
import exchange.core2.collections.objpool.ObjectsPool;

final ObjectsPool pool = ObjectsPool.createDefaultTestPool();
final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>(pool);
```

…or configure the pool sizes explicitly:

```java
final Map<Integer, Integer> config = new HashMap<>();
config.put(ObjectsPool.ART_NODE_4, 1024);
config.put(ObjectsPool.ART_NODE_16, 512);
config.put(ObjectsPool.ART_NODE_48, 256);
config.put(ObjectsPool.ART_NODE_256, 128);

final ObjectsPool pool = new ObjectsPool(config);
```

**Notes**
- not thread-safe — intended for a single application thread
- `getOrInsert` and `removeRange` are not implemented yet
- `size(limit)` walks the tree, it is not an O(1) counter — always pass a bound

---

### LongLongHashtable

A primitive `long` → `long` open-addressing hashtable with linear probing, storing keys and values
interleaved in a single flat `long[]`. No boxing, no `Entry` objects, one array for the whole map.

```java
import exchange.core2.collections.hashtable.LongLongHashtable;

final LongLongHashtable table = new LongLongHashtable();       // default capacity
final LongLongHashtable sized = new LongLongHashtable(1_000);  // pre-sized for ~1000 entries

table.put(42L, 1234L);      // returns the previous value, or 0 if the key was absent
table.get(42L);             // 1234
table.get(999L);            // 0 — missing keys return 0
table.containsKey(42L);     // true
table.remove(42L);          // 1234 — returns the removed value
table.size();               // 0
table.put(0L, 5L);          // any key, 0 included

```

Everything else is set with a builder — all parameters optional:

```java
final LongLongHashtable prices = LongLongHashtable.builder()
        .expectedSize(1_000)
        .missingValue(-1L)      // a missing key reads as -1 instead of 0
        .build();

prices.get(42L);            // -1
prices.put(42L, 0L);        // -1 — was absent
prices.get(42L);            // 0 — a regular value
prices.getOrDefault(43L, -2L); // -2 — absent, whatever missingValue is (one probe)

final LongLongHashtable fixed = LongLongHashtable.builder()
        .expectedSize(1_000_000)
        .fixedCapacity()        // never resizes: refuses new keys at maxLoad instead
        .maxLoad(0.9)           // default 0.95
        .build();               // or .maxCapacity(1 << 22): grows up to 2^22 slots, then refuses
```

A table that can not grow takes new keys up to `maxLoad` of its capacity (from 0.65 to below 1, default
0.95; two slots always stay free), with longer probes past the expected size; then `put` of a new key
throws `IllegalStateException` and changes nothing — updates and removals go on working. How much longer,
with linear probing and random keys:

| Load | lookup of a present key | insert of a new key, lookup / removal of a missing one |
|---|---|---|
| 0.65 (where a table grows) | ~2 slots | ~5 slots |
| 0.9 | ~5.5 | ~50 |
| 0.95 | ~10.5 | ~200 |
| 0.99 | ~50 | ~5000 |

**Hash seed.** The hash function is public, so whoever chooses the keys (client order ids, account ids
coming from outside) can pick thousands of them for one slot: they pile up into one cluster, and every
operation on them walks it — microseconds turn into milliseconds. A seed mixed into the hash, unknown
outside, prevents that. It is one for all tables of the JVM:

```
-Dexchange.hashtable.seed=random     # unpredictable, different in every JVM
-Dexchange.hashtable.seed=0x5bd1e995 # fixed (hex or decimal)
```

The default is `0`, which keeps the slot order — and the iteration order — the same from run to run
(replay, snapshots); `seed()` of any table reports the seed in use, to reproduce a random one. Being a
constant for the JIT, seed `0` costs nothing; any other seed adds 1-3 instructions per operation — a
fraction of a nanosecond, +5..15% in a benchmark of back-to-back operations.

Measured (`CollectionsLatencySamples -Dsamples.groups=hashtable-collide`): 2000 keys picked under seed `0`
for the first 64 of 2^21 slots, among 1M random keys — a get of one of them takes 1.0 µs (p50) / 2.0 µs
(p99) under seed `0`, 30 ns / 59 ns under a random seed.

Compound operations, one probe each (`computeIfAbsent` of a missing key: two — the function may change
the table):

```java
table.putIfAbsent(7L, 70L);              // 0 — was absent, now 70
table.putIfAbsent(7L, 71L);              // 70 — present, unchanged
table.addAndGet(7L, 5L);                 // 75 — a missing key counts as 0, whatever missingValue is
table.computeIfAbsent(8L, k -> k * 10);  // 80 — the function runs only for a missing key
table.remove(7L, 74L);                   // false — removes only with the matching value
table.toString();                        // {7=75, 8=80} (table order)

table.capacity();                        // slots; the table grows at 65% of them
table.ensureCapacity(1_000_000);         // one rehash now instead of a series of resizes later
```

Iteration:

```java
table.forEach((key, value) -> System.out.println(key + " -> " + value));   // no allocation

final long sumOfValues = table.valuesStream().sum();
final long maxKey = table.keysStream().max().orElse(0L);

table.clear();              // removes all entries, keeps the capacity
```

Do not modify the table from the `forEach` consumer or while a stream is being consumed.

**Notes**
- any key can be stored; key `0` marks the empty slots, so its entry is kept in two fields of the
  table (one predictable branch per operation)
- `get`, `put` and `remove` return `missingValue` (`0` unless set in the builder) for a missing
  key, so through `get` a stored `missingValue` looks the same as a missing key — `containsKey` tells
  them apart. Free slots hold the missing value: a non-zero one costs a pass over every new array
- the expected size (constructor, `expectedSize`, `ensureCapacity`) is the number of entries: that
  many fit without a resize, up to ~349 million (2^29 slots); a negative or larger value throws
  `IllegalArgumentException`
- at the maximum capacity (2^29 slots, or `maxCapacity` / `fixedCapacity`) the table stops growing: it
  takes new keys up to `maxLoad` (0.95), then `put` of a new key throws `IllegalStateException` and changes
  nothing
- the table grows automatically at 65% load factor; that resize is synchronous and shows up as a
  latency spike — pre-size the table if this matters
- not thread-safe

**Cost of the operations** (both hashtables; `LongLongLL2Hashtable` adds the migration, below)

| Operation | Cost |
|---|---|
| `get`, `containsKey`, `getOrDefault`, `put` (existing key) | one probe: the cluster of the key, from its home slot to the key or the first free slot |
| `put` / `putIfAbsent` / `addAndGet` of a new key | one probe; may trigger a resize: a copy of the whole table |
| `remove` | one probe, then the rest of the cluster shifts back by one slot |
| `computeIfAbsent` | one probe if present, two if absent (plus the function) |
| `forEach`, `keysStream`, `valuesStream`, `clear`, `toString` | the whole array |
| `ensureCapacity` | a rehash of the whole table, if it grows |

Clusters are short at load 0.65 with keys nobody picked to collide — 2000 keys picked for one slot
make a cluster of 2000 under seed `0`: set a seed where keys come from outside.

---

### LongLongLL2Hashtable

Same `long` → `long` contract as `LongLongHashtable`, but the resize happens **asynchronously**: the
application thread keeps serving `put`/`get`/`remove` while the worker of a `MigrationService` copies the old
array into the new one segment by segment, which removes the stop-the-world resize pause.

```java
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import exchange.core2.collections.hashtable.MigrationService;

try (MigrationService service = MigrationService.builder().start()) {        // one for many tables

    try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder()
            .migrationService(service)                                       // required
            .expectedSize(1_000_000)
            .build()) {

        table.put(42L, 1234L);
        table.get(42L);         // 1234
        table.remove(42L);      // 1234
        table.size();           // 0
    }
}
```

**The migration service** — two daemon threads for any number of tables: a worker that copies the old
arrays into the new ones, a portion at a time, in turns, and an allocator that allocates the new arrays:

```java
final MigrationService service = MigrationService.builder()
        .waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN)   // default SPIN_THEN_PARK
        .threadInit(() -> CpuAffinity.acquireCore(5))           // runs on the worker, before start() returns
        .allocatorThreadInit(() -> ...)                          // the same for the allocator
        .threadName("ll2-migration")                             // the allocator: "ll2-migration-allocator"
        .start();                                                // the threads run when this returns
```

- `start()` starts the threads and returns once they run — after `threadInit` / `allocatorThreadInit`, which
  pin them, for one (what they return is closed when the thread ends). Without that, a thread inherits the
  CPU affinity of the thread calling `start()`: start the service before that thread pins itself to a core.
  The allocator zeroes memory now and then — a housekeeping core is a good place for it.
- Waiting for work: `SPIN_THEN_PARK` (default) spins ~1-3 ms, then parks; the table that brings work wakes
  it — an unpark on the table's thread, only after such a quiet period. `BUSY_SPIN` spins all the time:
  a core of its own, the shortest hand-over. The allocator only parks.
- The worker goes round the tables with a migration in flight — not every registered one — and takes a
  portion of each in turn: ~256 slots (~2 µs), on to the end of the cluster there. A table that authorizes
  the whole rest of its migration at once still gets it portion by portion.
- The allocator takes the large arrays — above 2^18 slots, 4 MB; zeroing 256 MB takes tens of
  milliseconds — off the worker: the copying goes on for the other tables, and so do their small arrays,
  which the worker allocates between portions (~0.2 ms at most). `allocatorThread(false)` makes it one
  thread, the worker allocating all, and then the copying waits for each allocation.
- A table registers when it is built and leaves at `close()`. Close the tables first: `service.close()`
  throws while a table is registered — built, not closed, and not garbage-collected (a forgotten table is
  dropped once collected, with the arrays of its migration; `close()` drops the rest).

**Who does the work.** Each authorized segment is copied by whichever gets to it first: the worker in its
turn, or the table, where an operation needs a segment the worker has not taken — the table waits only for
a segment the worker is copying at that very moment. A put that brings the old array to 90% load allocates
the new array itself if the worker has not started it. So a busy, slow or absent worker costs the
application the work, never an unbounded wait for work nobody has started.

`close()` finishes an in-flight migration and leaves the service. The table stays fully usable after
`close()`; its later resizes are synchronous, like those of `LongLongHashtable`. `finishMigration()` only
finishes the migration.

**Cost during a migration** — on top of the table above, which holds between migrations:

| Operation | Cost |
|---|---|
| any single-key operation on a key whose home is in an authorized segment not copied yet | copies the segment itself: 256 slots (~2 µs), up to the end of the cluster at its boundary — a segment ends at a free slot; or, if the worker is copying it right now, waits for it |
| `put` that reaches the start of the migration | copies the cluster before it into the new array, on the calling thread |
| `put` that brings the old array to 90% load | the whole rest of the migration: copies it, or waits for the segment the worker is copying; allocates the new array if the worker has not started it |
| the compound operations, `getOrDefault` | made of the basic operations: two or three lookups |
| `clear`, `forEach`, the streams, `ensureCapacity` beyond the new capacity, `finishMigration`, `close` | finish the migration first: the copying of everything left |
| the operations that start and end a migration | a request in the table's slot of the service: volatile writes, an unpark if the worker sleeps |

During a migration both arrays are alive: three times the memory of the old one. `fixedCapacity()` (or a
`maxCapacity` reached) makes a table that never migrates, with none of this.

**Measured** — `CollectionsLatencySamples -Dsamples.groups=ll2-scenarios,ll2-many-tables,ll2-big-allocation`:
1M operations/s, latency from the planned arrival (a stall is charged to every operation queued behind it),
the measured table growing from 16 to 4M entries; Xeon Gold 6244, application pinned to an isolated core,
JDK 27, two runs:

| Configuration | p50 | p99 | p99.9 | max |
|---|---|---|---|---|
| `BUSY_SPIN` worker on its own core, `syncResizeBelow(300)` | 124-126 ns | 343-345 ns | 483-501 ns | 3.7-4.1 µs |
| `SPIN_THEN_PARK` worker, unpinned, default `syncResizeBelow` | 124-126 ns | 345 ns | 504-506 ns | 81-143 µs (the synchronous resizes below 8200 entries) |
| the service started by the pinned application thread: the worker shares its core | 145 ns | 11.6-11.9 **ms** | 31-33 ms | 35-36 ms |
| 4 tables growing at once on one `BUSY_SPIN` worker | 94-100 ns | 306-316 ns | 470-473 ns | 1.3-5.1 µs |
| 4 tables growing at once on one `SPIN_THEN_PARK` worker | 94-101 ns | 306-314 ns | 468-479 ns | 2.1-2.5 µs |
| 64 tables growing at once on one `BUSY_SPIN` worker | 126-128 ns | 347-351 ns | 533-564 ns | 56-58 µs (one worker for 64 migrations at once: tables at 90% load finish their own) |
| one table growing next to 10 000 idle ones, `BUSY_SPIN` | 123-125 ns | 344-351 ns | 484-493 ns | 5.7-6.9 µs |
| a neighbour's 512 MB allocation starts with the run, `BUSY_SPIN`, allocator thread | 128 ns | 346-354 ns | 497-506 ns | 23-28 µs |
| the same with `allocatorThread(false)` | 128 ns | 356-358 ns | 545-553 ns | 0.98 **ms** (the measured table's next array waits behind the large one: at 90% load it allocates and copies itself) |
| 50% puts, 30% gets, 20% removals | 145-150 ns | 381-387 ns | 531-535 ns | 1.2-1.7 µs |
| the same with `LongLongHashtable` (synchronous resizes) | 151-153 ns | 1.3-1.4 ms | 18 ms | 21 ms |

Against the executor-based version this service replaced (same machine, interleaved, three runs of
`hashtable-put`): samples over 1 µs per 100 000 puts 7-12 → 1-6, over 5 µs 1-5 → 0-1, max 12-16 µs → 1-8 µs.
Those were the start and the end of the migrations - a task for the executor, a `CompletableFuture`, a
`Cleaner`, all of it interpreted code on the application thread; now a few volatile writes.
With 10 000 idle tables registered the worker no longer goes over each of them every round: p99.9 541-543 →
484-493 ns against the first version of the service, and registering them allocates 8 MB instead of 712 MB.

The application and the worker yield the CPU to each other while they wait: where they share a core, the
other one runs. Sharing still costs the application the CPU of the allocation and the copying - give the
worker a core of its own.

**Notes**
- same key/value rules, `missingValue`, hash seed, `maxCapacity` / `fixedCapacity` as `LongLongHashtable`
- `toString()` during a migration shows only the size
- `ensureCapacity` beyond the capacity of an in-flight migration finishes it first
  (`finishMigration()`), then rehashes on the calling thread
- `clear()`, `forEach()`, `keysStream()` and `valuesStream()` work on one array: an in-flight migration
  is finished first (`finishMigration()`) — a pause, meant for the set-up, snapshots or tests
- the application drives a migration: every operation authorizes the next segment once the current one is
  copied, whatever its key, and wakes the worker if it went to sleep while the application was quiet;
  `migrationInProgress()` tells whether one is in flight
- the public API is single-threaded: exactly one application thread, plus the worker of the service
- a wait for the segment the worker is copying is bounded by `hangTimeout` (builder, default 10s): past it
  the operation throws `IllegalStateException` with the migration state instead of hanging; the waits spin,
  interrupts do not end them
- `-Dexchange.hashtable.verify=true` runs a full integrity check after every migration —
  for debugging migration corruption only, it is expensive

---

### ObjectsPool

A simple, non-thread-safe, array-backed pool of pre-allocated objects, used internally by
`LongAdaptiveRadixTreeMap` to recycle its nodes.

```java
import exchange.core2.collections.objpool.ObjectsPool;

final ObjectsPool pool = ObjectsPool.createDefaultTestPool();

// take from the pool, or construct if the pool is empty
final MyOrder order = pool.get(ObjectsPool.ORDER, MyOrder::new);

// ... use it ...

// hand it back
pool.put(ObjectsPool.ORDER, order);
```

Pool slots are identified by the `int` constants on `ObjectsPool`: `ORDER`, `DIRECT_ORDER`,
`DIRECT_BUCKET`, `ART_NODE_4` / `ART_NODE_16` / `ART_NODE_48` / `ART_NODE_256`,
`SYMBOL_POSITION_RECORD`. An object handed back to the pool must be treated as dead by the caller.

---

## Thread affinity

`collections-affinity` pins a thread to one CPU and reserves its whole physical core (hyper-threading
siblings included) — within the process and, through lock files, across processes on the same machine.
Linux (`sched_setaffinity`) and Windows (`SetThreadAffinityMask`, processor group 0); elsewhere the handle
reports that nothing was pinned. It calls the OS through the Foreign Function and Memory API: Java 22+, no
native library, and it also works in a GraalVM native image.

```java
import exchange.core2.collections.affinity.CpuAffinity;

try (CpuAffinity affinity = CpuAffinity.acquireCore()) {   // or acquireCore(cpu)
    // hot loop on affinity.cpu()
}
```

- `acquireCore()` takes the highest free core from the reserved CPUs: `-Daffinity.reserved=<hex mask>`
  (e.g. `AAAA` = CPUs 1,3,…,15), otherwise the kernel's isolated CPUs (`isolcpus`). If none is free it
  returns a handle that is not pinned (`isPinned()`), it never throws.
- `acquireCore(int cpu)` pins to that CPU and throws `IllegalStateException` if its core is held.
- `close()` restores the thread's previous mask and releases the core — call it from the pinned thread.
- Run with `--enable-native-access=ALL-UNNAMED` to avoid the JDK's restricted-method warning.
- Lock files: `exchange-core2-cpu-<n>.lock` in `-Daffinity.lockDir` (default `java.io.tmpdir`).
- Threads started by a pinned thread inherit its single-CPU mask: start helper threads before pinning, or
  let them pin themselves - e.g. `MigrationService.builder().threadInit(() -> CpuAffinity.acquireCore(5))`.

---

## Order book

`collections-orderbook` contains the `IOrderBook` interface and a naive reference implementation
(`OrderBookNaiveImpl`) built on top of `LongAdaptiveRadixTreeMap`. Commands are passed as flat binary
records inside an Agrona `DirectBuffer`, so the hot path stays allocation-free:

```java
import exchange.core2.collections.orderbook.IOrderBook;

orderBook.newOrder(buffer, offset);     // place (and match) an order
orderBook.cancelOrder(buffer, offset);  // cancel completely
orderBook.reduceOrder(buffer, offset);  // decrease size by N lots
orderBook.moveOrder(buffer, offset);    // move to a new price
```

The exact command layouts are documented in the javadoc on `IOrderBook`.

---

## Testing

Unit tests, including property-based tests and an oracle that compares ART against `TreeMap`:

```bash
mvn test
```

Long-running randomized stress tests (`tests-stress`) and latency / throughput benchmarks
(`tests-perf`) live in separate modules — run them explicitly:

```bash
mvn -pl tests-stress test
```

```bash
mvn -pl tests-perf test
```

Benchmarks are latency-sensitive: run them on an idle machine, ideally with CPU isolation — the
measuring threads pin themselves to the isolated CPUs through `collections-affinity`.

### Contributing
Exchange Collections is an open-source project and contributions are welcome!

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
