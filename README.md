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
```

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

Compound operations, one probe each:

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
- at the maximum capacity of 2^29 slots the table stops growing: it takes new keys up to load 0.9, then
  `put` of a new key throws `IllegalStateException` and changes nothing
- the table grows automatically at 65% load factor; that resize is synchronous and shows up as a
  latency spike — pre-size the table if this matters
- not thread-safe

---

### LongLongLL2Hashtable

Same `long` → `long` contract as `LongLongHashtable`, but the resize happens **asynchronously** on a
background thread. The application thread keeps serving `put`/`get`/`remove` while a migrator copies
the old array into the new one segment by segment, which removes the stop-the-world resize pause.

```java
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;

try (LongLongLL2Hashtable table = new LongLongLL2Hashtable(1_000_000)) {

    table.put(42L, 1234L);
    table.get(42L);         // 1234
    table.remove(42L);      // 1234
    table.size();           // 0
}
```

`close()` blocks until an in-flight migration finishes and releases the migrator thread. The table
stays fully usable after `close()` — it simply has no background work attached. A forgotten `close()`
is not fatal (a `Cleaner` stops the migrator once the table becomes unreachable), but the thread —
and, with an affinity-locked executor, its core — stays occupied until the next GC cycle notices.

A custom executor can be supplied for the migration work:

```java
final LongLongLL2Hashtable table = new LongLongLL2Hashtable(1_000_000, myExecutor);

final LongLongLL2Hashtable tuned = LongLongLL2Hashtable.builder()
        .expectedSize(1_000_000)
        .executor(myExecutor)
        .syncResizeBelow(300)   // smaller tables resize synchronously (default 8200)
        .missingValue(-1L)
        .build();
```

> **Warning:** the executor runs both the destination array allocation and the copying task, and the
> copying task spins while waiting for the application to authorize the next segment. A *bounded*
> executor therefore deadlocks as soon as all of its threads are occupied by spinning migrators —
> pass an unbounded / thread-per-task executor. The default one is exactly that, with daemon threads.

**Notes**
- same key/value rules, `missingValue` and hash seed as `LongLongHashtable`
- the compound operations take one probe when no resize is in flight; during a migration they are
  made of the basic operations; `toString()` then shows only the size
- `ensureCapacity` beyond the capacity of an in-flight migration finishes it first (like `close()`),
  then rehashes on the calling thread
- `clear()`, `forEach()`, `keysStream()` and `valuesStream()` work on one array: an in-flight migration
  is finished first (like `close()`) — a pause, meant for the set-up, snapshots or tests
- the public API is single-threaded: exactly one application thread, plus the internal migrator
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
- Threads started by a pinned thread inherit its single-CPU mask: start helper threads (e.g.
  `LongLongLL2Hashtable.newMigratorExecutor()`) before pinning.

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
