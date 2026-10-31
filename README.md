# exchange collections
[![][license img]][license]

**Exchange Collections** is an open source high-performance Java collections library, extracted from
the [exchange-core](https://github.com/exchange-core/exchange-core) matching engine.

All collections are primitive-specialized (`long` keys, `long` or object values), allocation-conscious
and designed for low-latency, single-threaded hot paths.

### Modules

| Module                  | Artifact                | Contents                                                      |
|-------------------------|-------------------------|---------------------------------------------------------------|
| `collections-core`      | `collections-core`      | Adaptive Radix Tree, hashtables (long-long, tuples), record store, objects pool |
| `collections-orderbook` | `collections-orderbook` | Order book implementation built on top of the core collections |
| `collections-ledger`    | `collections-ledger`    | Balances (available, reserved) with checked postings and batches |
| `collections-affinity`  | `collections-affinity`  | Pinning threads to CPU cores (Linux, Windows), no dependencies |
| `tests-perf`            | –                       | Latency / throughput benchmarks (not published)                |
| `tests-stress`          | –                       | Long-running randomized stress tests (not published)           |

### Requirements

- **To use the published artifacts:** Java 26+ (0.6.0). The next release drops the published
  bytecode level to Java 21; `collections-affinity` (new) needs Java 22+, as it uses the Foreign
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

The balances (`collections-ledger`) come with the next release.

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
| `RecordStore`              | paged records by handle   | handle → `long` fields | new    |
| `LongRecordTable`          | hashtable index + records | `long` → `long` fields | new    |
| `LongTupleHashtable`       | open-addressing hashtable | `long`s → `long`s, fixed widths | new |
| `ObjectsPool`              | object pool               | –                | stable       |

---

### LongAdaptiveRadixTreeMap

An [Adaptive Radix Tree](https://db.in.tum.de/~leis/papers/ART.pdf) map: a sorted `long` → object map
that behaves like `TreeMap<Long, V>`, but never re-balances and never re-hashes, so there are no
latency spikes caused by insertion.

**Features**
- keys are kept in **signed** ascending order (same order as `TreeMap<Long, V>`)
- no re-balancing, no whole-tree resize — a put or remove changes at most the nodes on the key's path
- internal nodes are recycled through `ObjectsPool` — low GC pressure
- neighbour lookups (`higherValue` / `lowerValue`, `ceilingValue` / `floorValue`), first / last
  key and value, ordered traversal, a cursor, `keysStream()` / `valuesStream()`

```java
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;

final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();

map.put(1_000L, "first");
map.put(2_000L, "second");
map.put(-500L, "negative");

map.get(2_000L);        // "second"
map.get(12_345L);       // null — missing keys return null
map.put(2_000L, "2nd"); // "second" — the previous value

map.higherValue(1_000L);   // "2nd"      — closest key strictly above
map.lowerValue(1_000L);    // "negative" — closest key strictly below
map.ceilingValue(1_000L);  // "first"    — the key itself, or the closest above
map.firstKey();            // -500
map.lastValue();           // "2nd"
map.toString();            // "{-500=negative, 1000=first, 2000=2nd}"

map.remove(1_000L);     // "first" — the removed value
map.size();             // 2
```

For an order book: `putAndGetLowerValue` / `putAndGetHigherValue` put a new price level and return its
neighbour (to link it into the chain of levels), `computeIfAbsent` returns a level or creates it,
`pollFirstValue` / `pollLastValue` remove the lowest / highest one:

```java
final PriceLevel level = new PriceLevel(price);
final PriceLevel better = asks.putAndGetLowerValue(price, level); // null: the new best ask
```

Ordered traversal — `forEach` walks ascending, `forEachDesc` descending. Both take a `limit` and
return the number of entries actually visited, which makes them a natural fit for top-N views
(L2 market data, for example):

```java
map.forEach((key, value) -> System.out.println(key + " -> " + value), 10);
map.forEachDesc((key, value) -> System.out.println(key + " -> " + value), 10);

// from a key: the 10 keys at or above 1_500, the 10 at or below it
map.forEach(1_500L, (key, value) -> System.out.println(key + " -> " + value), 10);
map.forEachDesc(1_500L, (key, value) -> System.out.println(key + " -> " + value), 10);

// materialize everything (produces garbage — avoid on hot paths)
map.entriesList().forEach(e -> System.out.println(e.getKey() + " -> " + e.getValue()));
```

A `Cursor` walks step by step, both ways. Make it once and keep it: seeks and moves allocate nothing. It
seeks a key — `seekCeiling`, `seekHigher`, `seekFloor`, `seekLower`, `seekFirst`, `seekLast` — then moves
with `next()` / `prev()`; a seek or a move returns `false` when there is no such key. `remove()` removes
the key the cursor is at and moves on to the next one:

```java
final LongAdaptiveRadixTreeMap.Cursor<StopOrder> cursor = stops.cursor();   // once, reused

// the stop orders triggered by a trade at lastPrice
for (boolean on = cursor.seekFirst(); on && cursor.key() <= lastPrice; on = cursor.isValid()) {
    trigger(cursor.remove());
}
```

The map may change only through the cursor while it is in use: any other change of the key set makes
the next call of the cursor throw `ConcurrentModificationException` (a seek starts it over). Updating
the value of a present key is not a change of the key set. For a run of keys from a key,
`forEach(from, ...)` is faster than a cursor.

The nodes come from a pool and go back to it, so a map of a steady size does not allocate. A map has a pool
of its own (256 free leaves and 4-key nodes, fewer of the bigger ones) - of another size, or shared by the maps
of one thread:

```java
import exchange.core2.collections.objpool.ObjectsPool;

final LongAdaptiveRadixTreeMap<String> map = LongAdaptiveRadixTreeMap.builder()
        .poolCapacity(4096) // free nodes kept for the next puts; 0 - none
        .build();

final ObjectsPool pool = ObjectsPool.forArt(4096);
final LongAdaptiveRadixTreeMap<PriceLevel> bids = LongAdaptiveRadixTreeMap.builder().sharedPool(pool).build();
final LongAdaptiveRadixTreeMap<PriceLevel> asks = LongAdaptiveRadixTreeMap.builder().sharedPool(pool).build();
```

…or a pool of several types, with the sizes of each (as exchange-core configures it):

```java
final Map<Integer, Integer> config = new HashMap<>();
config.put(ObjectsPool.ART_NODE_4, 1024);
config.put(ObjectsPool.ART_NODE_16, 512);
config.put(ObjectsPool.ART_NODE_48, 256);
config.put(ObjectsPool.ART_NODE_256, 128);
config.put(ObjectsPool.ART_LEAF, 1024);     // a key alone in its 256-key block; default: as ART_NODE_4

final ObjectsPool pool = new ObjectsPool(config);
```

**Notes**
- values must not be null: a missing key reads as null, so `put(key, null)` throws `NullPointerException`
- `removeRange(from, toExclusive)` removes a range of keys (e.g. the stop orders up to a price) and returns how
  many
- `validateInternalState()`, `printDiagram()` and `entriesList()` are diagnostics: O(n), the diagram format may change

**Threads.** Not thread-safe, neither is `ObjectsPool`: a map is used by one thread at a time, a pool is
shared only by the maps of one thread. A map passed to another thread needs a happens-before edge (a
queue, a volatile write and read), as any object that is not thread-safe.

**Cost of the operations.** A descent walks the key from its highest byte down, a node per byte where the
keys of the map differ — at most 8; for keys within a 2^32 range, such as prices in ticks, at most 4: a
node skips the bytes all the keys below it share (path compression). Nothing is rebalanced or rehashed: a
put or a removal changes the nodes on the path of its key only.

| Operation | Cost |
|---|---|
| `get`, `containsKey`, `getOrDefault` | one descent |
| `put`, `remove` | one descent; a node that fills up is replaced by the next size (4 → 16 → 48 → 256 keys), one that empties by the previous one; a new branch point takes a 4-key node |
| `higherValue`, `lowerValue`, `ceilingValue`, `floorValue`, `firstKey` / `lastKey`, `firstValue` / `lastValue` | one descent; where the subtree of the key has no neighbour, back up to the closest node that has one and down its edge |
| `putAndGetLowerValue` / `putAndGetHigherValue` | one descent when the neighbour is in the same 256-key block (the node of the last level remembers the put), otherwise two |
| `putIfAbsent`, `computeIfAbsent` | one descent if the key is present, two if absent (a lookup and a put) |
| `pollFirstValue` / `pollLastValue` | two descents: the key, then its removal |
| `forEach` / `forEachDesc`, with or without `from` | one descent, then O(1) per key visited: 48- and 256-key nodes skip their empty slots with a bitmap |
| `Cursor` | a seek is one descent, `next()` / `prev()` O(1) on average, `remove()` a removal and a seek |
| `removeRange` | a seek, then a removal per key removed |
| `size`, `isEmpty` | a counter |
| `clear` | every node, back to the pool |
| `keysStream`, `valuesStream`, `toString`, `entriesList` | the whole map |

**Allocation.** A map of a steady size allocates nothing: the nodes a removal frees go back to the pool,
a put takes them from there (and allocates when the pool is empty). A node that grows or shrinks goes back
to the pool too. None of the lookups, the walks, the cursor moves and seeks, `removeRange` (its first call
makes a cursor for the map) and `clear` allocate; `forEach` allocates nothing but the consumer, so keep a
non-capturing one or one made once. `cursor()` makes a cursor; the streams, `toString` and `entriesList`
allocate as they go — for tests, snapshots and debugging. `computeIfAbsent` allocates what its function
does.

**Memory** per key — `ArtMemoryPerKey` (`tests-perf`): the heap after a full GC, values not counted (one
object for all keys), JDK 26 with compressed oops; small maps built in copies, 1M keys in all:

| Keys | ART | `TreeMap<Long, V>` | fastutil `Long2ObjectRBTreeMap` |
|---|---|---|---|
| sequential, 1M | 4.4 B | 64 B | 40 B |
| clustered: 16 runs of consecutive keys, 1M | 4.5 B | 64 B | 40 B |
| an order book: 1k / 20k price levels around the mid, hyperbolic distance | 12.7 / 11.9 B | 64 B | 40 B |
| ids: 1M, 5% of a range | 18.2 B | 64 B | 40 B |
| sparse: every key alone in its 256-key block, 1M | 36.4 B | 64 B | 40 B |
| random 64-bit, 1M | 53.5 B | 64 B | 40 B |

Dense keys share the nodes of the last level, up to 256 keys in a 1128-byte node; a key alone in its
256-key block is a leaf of 32 bytes (a 4-key node is 56 bytes, a 16-key one 168, a 48-key one 584). An
empty map with a pool of its own takes 3.6 KB, most of it the arrays of the pool (0.7 KB with
`poolCapacity(0)`, a few dozen bytes on a shared pool); the pool also keeps the free nodes — up to ~115 KB at
the default capacity, once the map has shrunk by that much.

**Measured** — `CollectionsLatencySamples -Dsamples.groups=art-fill`: one operation at a time on a map of
1M keys (a new key for `put`, removed after the sample; a present one for `get` and `remove`, put back), ns,
p50 / p99, the average of the runs; Xeon Gold 6244, the measuring thread on an isolated core, JDK 27; a
sample includes the timer read, ~16 ns:

| Operation, keys | ART | `TreeMap` | fastutil RB tree |
|---|---|---|---|
| `get`, sequential | 60 / 87 | 476 / 864 | 414 / 677 |
| `get`, clustered | 71 / 112 | 407 / 783 | 287 / 537 |
| `get`, sparse | 96 / 237 | 510 / 885 | 420 / 728 |
| `get`, random | 182 / 461 | 387 / 766 | 282 / 530 |
| `put`, sequential | 37 / 54 | 122 / 126 | 179 / 196 |
| `put`, clustered | 62 / 86 | 132 / 189 | 183 / 234 |
| `put`, sparse | 40 / 56 | 142 / 153 | 201 / 221 |
| `put`, random | 102 / 318 | 506 / 907 | 440 / 744 |
| `remove`, sequential | 73 / 112 | 815 / 1292 | 452 / 772 |
| `remove`, clustered | 85 / 158 | 712 / 1186 | 460 / 790 |
| `remove`, sparse | 119 / 329 | 837 / 1319 | 452 / 775 |
| `remove`, random | 212 / 506 | 698 / 1176 | 469 / 804 |

The walks of an order book (`ArtOpsJmh`, average time, a book of 1k / 20k levels; same machine): a seek and
10 moves of a cursor 60 / 83 ns, fastutil's iterator 72 / 126 ns, `TreeMap`'s 84 / 140 ns; 10 levels from
a price with `forEach(from, ...)` 44 / 62 ns, 73 / 125 ns, 81 / 140 ns.

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
turn, or the table, where an operation needs a segment the worker has not taken. Nor does the table wait
for a worker that stops in the middle of its portion — preempted, say: the worker copies in batches of 32
slots, each started with a CAS that tells where it begins, and a table that sees no new batch for ~2 µs
takes the portion over. It copies the worker's batch again — the same entries go to the same slots — and
the rest. The worker, back on a core, finishes writing that batch, finds its CAS failed and stops; until
then the table leaves those entries (at most 32) as they are: a new value for one of them, or a removal in
their cluster, waits for the worker, and the migration ends - the table switches arrays - only after it.
A put that brings the old array to 90% load allocates the new array itself if the worker has not started
it. So a busy, slow, preempted or absent worker costs the application the work, not a wait for it.

`close()` finishes an in-flight migration and leaves the service. The table stays fully usable after
`close()`; its later resizes are synchronous, like those of `LongLongHashtable`. `finishMigration()` only
finishes the migration.

**Cost during a migration** — on top of the table above, which holds between migrations:

| Operation | Cost |
|---|---|
| any single-key operation on a key whose home is in an authorized segment not copied yet | copies the segment itself: 256 slots (~2 µs), up to the end of the cluster at its boundary — a segment ends at a free slot; or, if the worker is copying it right now, waits for it - and takes it over if the worker starts no new batch for ~2 µs |
| `put` that reaches the start of the migration | copies the cluster before it into the new array, on the calling thread |
| `put` that brings the old array to 90% load | the whole rest of the migration: copies it, or waits for the segment the worker is copying (as above); allocates the new array if the worker has not started it |
| a new value for one of the ≤32 entries of a batch taken over from a stalled worker, or a removal in their cluster | waits until the worker runs again and has written the batch |
| the compound operations, `getOrDefault` | made of the basic operations: two or three lookups |
| `clear`, `forEach`, the streams, `ensureCapacity` beyond the new capacity, `finishMigration`, `close` | finish the migration first: the copying of everything left |
| the operations that start and end a migration | a request in the table's slot of the service: volatile writes, an unpark if the worker sleeps |

During a migration both arrays are alive: three times the memory of the old one. `fixedCapacity()` (or a
`maxCapacity` reached) makes a table that never migrates, with none of this.

**Measured** — `CollectionsLatencySamples -Dsamples.groups=ll2-scenarios,ll2-many-tables,ll2-big-allocation,ll2-preempted-worker`:
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
| a `BUSY_SPIN` worker pinned to a core it shares with a busy thread: off the core for milliseconds, in the middle of its work too | 127 ns | 345-350 ns | 493-502 ns | 22-27 µs (before the take-over: p99.9 0.5-3.4 ms, max 0.2-6.7 ms) |
| the same, 4 tables growing at once | 110-122 ns | 332-339 ns | 499-504 ns | 74-85 µs (before: p99.9 5.7-9.9 ms, max 9.1-11.9 ms) |
| 50% puts, 30% gets, 20% removals | 145-150 ns | 381-387 ns | 531-535 ns | 1.2-1.7 µs |
| the same with `LongLongHashtable` (synchronous resizes) | 151-153 ns | 1.3-1.4 ms | 18 ms | 21 ms |

Against the executor-based version this service replaced (same machine, interleaved, three runs of
`hashtable-put`): samples over 1 µs per 100 000 puts 7-12 → 1-6, over 5 µs 1-5 → 0-1, max 12-16 µs → 1-8 µs.
Those were the start and the end of the migrations - a task for the executor, a `CompletableFuture`, a
`Cleaner`, all of it interpreted code on the application thread; now a few volatile writes.
With 10 000 idle tables registered the worker no longer goes over each of them every round: p99.9 541-543 →
484-493 ns against the first version of the service, and registering them allocates 8 MB instead of 712 MB.
The take-over costs the worker a CAS per batch: it copies ~12% slower (2^23 slots: 59 → 66 ms) - not
visible with 64 tables growing at once on it (p99.9 607-611 ns, max 59-60 µs either way).

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
- a wait for the worker - for a batch taken over from it, or for one it is copying until it is taken over -
  is bounded by `hangTimeout` (builder, default 10s): past it
  the operation throws `IllegalStateException` with the migration state instead of hanging; the waits spin,
  interrupts do not end them
- `-Dexchange.hashtable.verify=true` runs a full integrity check after every migration —
  for debugging migration corruption only, it is expensive

---

### RecordStore

Records of a fixed number of `long` fields, addressed by a `long` handle — no keys. Several values that
belong together (a balance: available and reserved) live side by side in one record instead of one hashtable
each; the handle is what the rest of the system keeps — in a hashtable value, in a field of another record.

```java
import exchange.core2.collections.records.RecordStore;

static final int AVAILABLE = 0;
static final int RESERVED = 1;

final RecordStore balances = RecordStore.builder()
        .fields(2)
        .expectedSize(1_000_000)    // the pages for 1M records, allocated now
        .build();

final long h = balances.allocate();         // a new record, all fields 0
balances.set(h, AVAILABLE, 1_000);
balances.addAndGet(h, AVAILABLE, -100);     // 900
balances.addAndGet(h, RESERVED, 100);       // 100
balances.get(h, RESERVED);                  // 100
balances.addAndGetExact(h, AVAILABLE, Long.MAX_VALUE); // ArithmeticException, the field keeps 900

balances.free(h);
balances.isValid(h);                        // false
balances.get(h, AVAILABLE);                 // IllegalArgumentException: stale handle
```

Keys map to handles in a `long` hashtable — a missing key reads as `0`, which is `RecordStore.NO_RECORD`:

```java
final LongLongHashtable byAccount = new LongLongHashtable(1_000_000);

long h = byAccount.get(accountId);
if (h == RecordStore.NO_RECORD) {
    h = balances.allocate();
    byAccount.put(accountId, h);
}
```

**Features**
- a record never moves: its handle stays valid until it is freed, whatever else is allocated or freed
- generations: a freed slot is reused under a new one, and every access checks it — a stale handle throws
  `IllegalArgumentException` instead of reaching the record allocated after it (an object returned to a pool
  would be silently shared). The check is one compare: the header of a live record is its handle, in the cache
  line of the fields
- pages of `pageSize` records (default 1024), each a `long[]`: growing allocates a page, nothing is copied but
  the page directory — no pause proportional to the size. `fixedCapacity()` allocates all pages at the build,
  `maxCapacity(n)` limits the records; beyond them `allocate` throws `IllegalStateException`
- deterministic: the same allocations and frees hand out the same handles — last freed, first reused, then the
  slots never used in their order; `forEach` visits the records in the slot order, whatever the hash seed
- `addAndGetExact` — checked addition: `ArithmeticException` on overflow, the field unchanged. `addAndGet`
  wraps around, as `+` does
- `RecordStore.index(handle)` — the slot of a record, from 1 to `capacity()`, for arrays kept beside the store

**Cost of the operations**

| Operation | Cost |
|---|---|
| `get`, `set`, `addAndGet`, `addAndGetExact`, `isValid` | the page from the directory, the header compare, the field — no probe |
| `allocate` | a slot from the free list, or the next slot never used; one allocation in `pageSize` adds a page (24 KB to zero for 2 fields and 1024 records) |
| `free` | the header and the fields of the record |
| `forEach`, `clear` | every slot used so far |
| `ensureCapacity` | the pages it adds |

**Measured** — `RecordStoreJmh`, balances of two fields against an object per record (the caller holding the
reference, no check; or a `long` id into an `ArrayList`, checked against a generation in the object) and a flat
`long[]` (no check, no pages); ns per operation, 10k / 1M / 10M records reached in a random order. Xeon Gold 6244,
JDK 27, the benchmark thread pinned to an isolated core; one fork, 3 × 1 s of warm-up and of measurement:

| Operation | `RecordStore` | object, reference | object, `ArrayList` id | `long[]` |
|---|---|---|---|---|
| `chase` — each access waits for the one before | 6.4 / 44 / 91 | 3.8 / 53 / 88 | 7.2 / 79 / 153 | 4.3 / 20 / 83 |
| `read` both fields — accesses independent, misses overlap | 3.4 / 13 / 29 | 1.6 / 13 / 16 | 3.5 / 27 / 46 | 1.9 / 8.3 / 18 |
| `reserve` (available down, reserved up) | 3.9 / 20 / 29 | 1.9 / 14 / 17 | 3.7 / 27 / 45 | 2.1 / 5.0 / 19 |
| `freeAllocate` | 7.8 / 26 / 52 | 3.3 / 14 / 18 | 8.3 / 51 / 88 | – |
| `scan` of one field, per record, 10M | 3.1 | 2.9 | 3.2 | 1.4 |

- The latency of an access (`chase`) is one cache miss, as for an object held by reference or a flat array; an
  `ArrayList` id takes two — the list's array, then the object. At 1M the records (24 bytes) fit the 24.75 MB L3,
  the objects (40 bytes, with the link of `chase`) do not: 44 against 53 ns.
- Independent accesses overlap their misses; the page, the header and the field checks are more instructions
  per access, fewer misses in flight — 29 against 16-18 ns at 10M.
- `free` reads the header to check it: a miss an unchecked pool does not take — its stale references reach the
  next owner of the object instead.
- A scan of 10M records is bound by the memory: about 3 ns per record for the store and the objects alike.
- Not measured here: the GC (the loops allocate nothing), the layout of objects scattered by churn.

**Notes**
- a record takes `fields + 1` longs: the header; slot 0 is never allocated (its header keeps `NO_RECORD`
  from passing the check)
- a slot is reused up to 2^31 - 1 times; then it is retired, never allocated again
- up to 2^30 records; a page of at most 2^20 records
- not thread-safe

---

### LongRecordTable

`long` keys to records of `long` fields: a `LongLongHashtable` maps every key to the handle of its record in a
`RecordStore`. A key is one probe; the handle it returns reaches the fields with no probe at all — keep it (in an
order, say), and the next update of the balance is one cache line.

```java
import exchange.core2.collections.hashtable.LongRecordTable;

static final int AVAILABLE = 0;
static final int RESERVED = 1;

final LongRecordTable balances = LongRecordTable.builder()
        .fields(2)
        .expectedSize(1_000_000)    // no resize of the index, no new page, up to 1M keys
        .build();

final long h = balances.getOrCreate(accountId);  // a new key gets a record, all fields 0 - one probe
balances.addAndGetExact(h, AVAILABLE, 1_000);
balances.find(accountId);                        // h; NO_RECORD (0) for a missing key
balances.keyOf(h);                               // accountId

// later, through the handle kept: no probe
balances.addAndGet(h, AVAILABLE, -100);
balances.addAndGet(h, RESERVED, 100);

balances.remove(accountId);                      // true - h is stale now
balances.get(h, AVAILABLE);                      // IllegalArgumentException
```

**Features**
- handles stay valid however the index grows: the records never move
- a record holds its key: `keyOf(handle)`, and `forEach((key, handle) -> ...)` in the order of the records — the
  same from run to run, whatever the hash seed; the consumer may remove keys
- a full table refuses a new key before its record is allocated: `IllegalStateException`, nothing changes.
  `fixedCapacity()` — neither the index nor the store allocates after the build; `maxCapacity(n)` limits the keys
- the field accessors take the handle, as those of `RecordStore`; `addAndGetExact` for checked sums

**Cost of the operations**

| Operation | Cost |
|---|---|
| `find`, `containsKey`, `getOrCreate` of a present key | one probe of the index |
| `getOrCreate` of a new key | one probe; may resize the index (a copy of it), may add a page to the store |
| `remove` | one probe, the backward shift of the rest of the cluster, the record freed |
| `get`, `set`, `addAndGet`, `addAndGetExact`, `keyOf`, `isValid` | the record only |
| `forEach`, `clear` | every record |

**Measured** — `LongRecordTableJmh`, balances of two fields by key (random keys, in a random order) against a
`LongLongHashtable` per field, Eclipse's `LongObjectHashMap` of an object per balance (as exchange-core keeps
them) and a `LongTupleHashtable` with the key and both fields in a slot; ns per operation, 10k / 1M / 10M keys.
The machine and the runs of `RecordStoreJmh`:

| Operation | `LongRecordTable` | two `LongLongHashtable` | `LongObjectHashMap` | `LongTupleHashtable` |
|---|---|---|---|---|
| `chaseByKey` — each lookup waits for the one before | 17 / 152 / 205 | 15 / 92 / 140 | 9.5 / 123 / 187 | 16 / 78 / 119 |
| `readByKey` — independent lookups, misses overlap | 11 / 65 / 83 | 13 / 44 / 64 | 5.8 / 43 / 56 | 13 / 44 / 63 |
| `reserveByKey` (available down, reserved up) | 12 / 68 / 87 | 14 / 49 / 69 | 6.1 / 45 / 58 | 13 / 42 / 64 |
| `createRemove` — a key removed, a new one added | 69 / 182 / 279 | 90 / 208 / 365 | 59 / 267 / 431 | 67 / 171 / 266 |

- A lookup by key out of the caches (`chaseByKey`): the index, then the record — two misses one after the other,
  205 ns at 10M. The two hashtables take their two misses at once (140), the map its arrays and then the object
  (187), `LongTupleHashtable` one miss (119).
- Independent lookups overlap their misses: the table is the slowest there too, 65-68 against 42-49 ns at 1M — more
  instructions per lookup (the probe, then the page, the header and the field checks), fewer misses in flight.
- Through a handle the caller keeps, the same reservation is 29 ns at 10M and an access out of the caches one miss,
  91 ns (`RecordStoreJmh`): keeping the handles is what the table is for. Where every access comes with a key and
  nothing keeps a reference to the record, `LongTupleHashtable` is the faster structure.
- A key removed and another added: one probe each way and the freed record reused — with the tuples the fastest at
  1M and 10M.
- Memory per key, two fields: 16 bytes per slot of the index and 32 per record (header, fields, key) — about
  what two hashtables take, 16 bytes per slot each; every further field adds 8 bytes to a record, a further
  hashtable 16 per slot.

**Notes**
- a record takes `fields + 2` longs: its header and the key; the index 16 bytes per slot, up to 65% full between
  its resizes
- the index resizes synchronously, as `LongLongHashtable` does — pre-size the table where that pause matters
- not thread-safe

---

### LongTupleHashtable

Keys of a fixed number of `long`s to values of a fixed number of `long`s, side by side in the slots of one
`long[]` — a key of two or three longs (an account and a currency; a UUID, a token and a subaccount) without
packing it into one, and its values in the slot of the key: one probe reaches them.

```java
import exchange.core2.collections.hashtable.LongTupleHashtable;

static final int AVAILABLE = 0;
static final int RESERVED = 1;

// (user UUID high, user UUID low, token << 8 | subaccount) -> available, reserved
final LongTupleHashtable balances = LongTupleHashtable.builder()
        .keyLongs(3)
        .valueLongs(2)
        .expectedSize(1_000_000)
        .build();

int pos = balances.getOrInsert(uuidHi, uuidLo, token << 8 | sub);  // a new key gets zero values
balances.addAndGetValueExact(pos, AVAILABLE, 1_000);
balances.addAndGetValue(pos, AVAILABLE, -100);
balances.addAndGetValue(pos, RESERVED, 100);

pos = balances.find(uuidHi, uuidLo, token << 8 | sub);              // NO_SLOT for a missing key
balances.getValue(pos, RESERVED);                                  // 100
balances.getKey(pos, 2);                                           // token << 8 | sub
balances.remove(uuidHi, uuidLo, token << 8 | sub);                  // true
```

**Features**
- a slot is a tag, the key and the values: `1 + keyLongs + valueLongs` longs. The tag is the hash of the key with
  the top bit set — never 0, which marks a free slot — so any key can be stored, all zeros included; a probe
  compares the tags before the keys, a resize or a removal never hashes a key again
- keys of 1 to 4 longs have methods of their own, without an array; keys of any length go through a `long[]`
  (`find(key, offset)`, ...). `valueLongs(0)` makes a set of keys
- the hash seed of the JVM, `maxCapacity` / `fixedCapacity` / `maxLoad`, backward-shift deletion: as in
  `LongLongHashtable`; a full table refuses a new key, unchanged
- `addAndGetValueExact` for checked sums

**A position is valid until the next insert or removal.** An operation on a key returns the position of its slot
and the values are reached through it — no second structure, no handle to check. An insert may resize the table,
a removal shifts the entries after the removed one back: after either, find the key again. A position kept longer
reads another key's values; for references that live — in an order, say — use the handles of `LongRecordTable`.

**Cost of the operations**

| Operation | Cost |
|---|---|
| `find`, `getOrInsert` of a present key, `remove` of a missing one | the hash of the key (a mix per long), one probe |
| `getOrInsert` of a new key | one probe; may resize the table: a copy of every slot, no key hashed again |
| `remove` | one probe, then the rest of the cluster shifts back by one slot |
| `getValue`, `setValue`, `addAndGetValue`, `addAndGetValueExact`, `getKey` | the slot only |
| `forEach`, `clear`, `toString` | the whole array |

**Measured** — `LongTupleHashtableJmh`, balances under a key of three longs (a UUID: a time-ordered high half, a
random low half; a token and a subaccount, 4 × 3 per user) against a `HashMap` of a `record` key to an object, what
such a key gets without a primitive table; keys added in one random order, reached in another; ns per operation,
12k / 1.2M / 12M keys, the machine and the runs of `RecordStoreJmh`:

| Operation | `LongTupleHashtable` | `HashMap<Key, Balance>` |
|---|---|---|
| `readByKey` — independent lookups | 18 / 79 / 81 | 18 / 91 / 114 |
| `reserveByKey` (available down, reserved up) | 19 / 79 / 81 | 19 / 94 / 125 |
| `createRemove` — a key removed, a new one added | 73 / 274 / 261 | 51 / 233 / 494 |

- The `HashMap` reaches a node, its key object and the balance — three objects, where they were allocated; the
  tuples one slot of 48 bytes.
- Its removal and insert allocate (the new key, a node) and at 12M the GC takes its share — 324-600 ns from
  iteration to iteration; the tuples allocate nothing. Small tables remove and insert faster in the `HashMap`:
  the backward shift moves 48-byte slots, and the key is mixed long by long.
- With a key of one long, `LongTupleHashtable` against `LongRecordTable`, two hashtables and `LongObjectHashMap`:
  see the table of `LongRecordTable` — the shortest lookup out of the caches.
- Memory per key: a slot of 48 bytes, 33% to 65% of the slots taken between the resizes — 74 to 148 bytes; the
  `HashMap`: a node of 32, a key of 40, a balance of 32 and 5 to 11 of its table — 109 to 115.

**Notes**
- a slot takes `8 * (1 + keyLongs + valueLongs)` bytes: 48 for a key of three longs and two values, up to 65%
  full between the resizes
- the resize is synchronous, on the calling thread — pre-size the table where that pause matters
- `forEach` hands out the positions; its consumer must not insert or remove keys
- not thread-safe

---

### ObjectsPool

A simple, non-thread-safe, array-backed pool of pre-allocated objects, used internally by
`LongAdaptiveRadixTreeMap` to recycle its nodes.

```java
import exchange.core2.collections.objpool.ObjectsPool;

final ObjectsPool pool = new ObjectsPool(Map.of(ObjectsPool.ORDER, 1024)); // up to 1024 free orders

// take from the pool, or construct if the pool is empty
final MyOrder order = pool.get(ObjectsPool.ORDER, MyOrder::new);

// ... use it ...

// hand it back
pool.put(ObjectsPool.ORDER, order);
```

Pool slots are identified by the `int` constants on `ObjectsPool`: `ORDER`, `DIRECT_ORDER`,
`DIRECT_BUCKET`, `ART_NODE_4` / `ART_NODE_16` / `ART_NODE_48` / `ART_NODE_256`, `ART_LEAF`,
`SYMBOL_POSITION_RECORD`. A type left out of the configuration gets no places: `get` constructs, `put` drops
the object (`ART_LEAF` gets as many places as `ART_NODE_4`). An object handed back to the pool must be treated as
dead by the caller.

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
(`OrderBookNaiveImpl`), with the price levels in a `TreeMap`. Commands are passed as flat binary
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

## Balances

`collections-ledger` contains `BalanceTable`: balances — an available and a reserved amount — under keys of a
fixed number of longs, (account, currency) or (user UUID, token, subaccount), in a `LongTupleHashtable`. Neither
amount ever goes below 0 and no sum overflows: an operation checks before it writes and returns a status — a
refusal is not an exception, and nothing allocates.

```java
import exchange.core2.collections.ledger.BalanceTable;

final BalanceTable balances = BalanceTable.builder()
        .keyLongs(2)                // (account, currency)
        .expectedSize(1_000_000)
        .build();

balances.deposit(balances.getOrCreate(buyer, USD), 1_000);        // OK

// an order holds what it may spend
balances.reserve(balances.find(buyer, USD), 500);                 // OK
int status = balances.reserve(balances.find(buyer, USD), 600);    // INSUFFICIENT_AVAILABLE, nothing changes
BalanceTable.statusName(status);                                  // "INSUFFICIENT_AVAILABLE"

// a trade: all the postings or none
final BalanceTable.Batch trade = balances.newBatch(8);  // made once, cleared and reused
trade.add(buyer, USD, 0, -240)       // (key, available delta, reserved delta): the buyer's hold spent
        .add(buyer, BTC, 1, 0)       // the base to the buyer - a new balance
        .add(seller, BTC, 0, -1)
        .add(seller, USD, 236, 0)    // the quote to the seller, minus the fee
        .add(FEES, USD, 4, 0);
balances.apply(trade);                 // OK, or the status of the posting refused - trade.failedPosting()
trade.clear();

// the rest of the order cancelled
balances.release(balances.find(buyer, USD), 260);
```

**References.** `find` and `getOrCreate` return a `long` reference to the balance, and the operations take it — one
probe, then the slot. A reference is valid until the entries move: a resize, a removal, `clear`. One used after
that throws `IllegalArgumentException` instead of reaching another balance; take it right before the operations.
In a table that never resizes (`expectedSize` enough, or `fixedCapacity()`) only a removal makes it stale.
`NO_REF` — `find` of a missing key — is a zero balance.

| Operation | available | reserved | refused with |
|---|---|---|---|
| `deposit(ref, x)` | + x | | `OVERFLOW`; `NO_BALANCE` for `NO_REF` (`getOrCreate` it) |
| `withdraw(ref, x)` | − x | | `INSUFFICIENT_AVAILABLE` |
| `reserve(ref, x)` | − x | + x | `INSUFFICIENT_AVAILABLE`, `OVERFLOW` |
| `release(ref, x)` | + x | − x | `INSUFFICIENT_RESERVED`, `OVERFLOW` |
| `settle(ref, x)` | | − x | `INSUFFICIENT_RESERVED` |
| `post(ref, dA, dR)` | + dA | + dR | any of these |
| `remove(ref)` | | | `NOT_ZERO`, `NO_BALANCE` |

A negative `x` is refused with `INVALID_AMOUNT`. Every refusal leaves the balance as it was.

**Batches.** `apply(batch)` takes the postings in their order, the same key any number of times; the first one
refused undoes those before it, the balances they created included. A posting to a missing key creates its balance
if it takes nothing (no negative delta). A new key that does not fit a full table: `IllegalStateException`, nothing
changed. New balances may resize the table — the references taken before are stale then.

**Notes**
- a probe for `find` / `getOrCreate` and for each posting of a batch; the undo of a refused batch probes again
- keys of 1 to 4 longs have methods of their own, any length goes through a `long[]`
- a balance takes a slot of `8 * (3 + keyLongs)` bytes; the table grows as `LongTupleHashtable` does
- not thread-safe

---

## Testing

Unit tests, including property-based tests and an oracle that compares ART against `TreeMap`:

```bash
mvn test
```

Long-running randomized stress tests (`tests-stress`) and latency / throughput benchmarks
(`tests-perf`) live in separate modules and are left out of a plain build — run them with their
profiles (`-Dtest=<class>` for one class of benchmarks):

```bash
mvn -Pstress -pl tests-stress -am test
```

```bash
mvn -Pbenchmarks -pl tests-perf -am test
```

Benchmarks are latency-sensitive: run them on an idle machine, ideally with CPU isolation — the
measuring threads pin themselves to the isolated CPUs through `collections-affinity`.

The API of the published modules against the last release ([japicmp](https://github.com/siom79/japicmp)):
a binary or source incompatible change fails the build unless it is listed in the excludes of the
`api-check` profile; the report goes to `target/japicmp`.

```bash
mvn -Papi-check -DskipTests verify
```

The javadoc is strict: a broken reference or bad HTML fails the build.

### Contributing
Exchange Collections is an open-source project and contributions are welcome!

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
