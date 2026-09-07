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
| `tests-perf`            | –                       | Latency / throughput benchmarks (not published)                |
| `tests-stress`          | –                       | Long-running randomized stress tests (not published)           |

### Requirements

- Java 26+
- Maven 3.9+

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
| `LongLongRadixHashtable`   | sharded hashtable         | `long` → `long`  | experimental |
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
```

Iteration:

```java
table.forEach((key, value) -> System.out.println(key + " -> " + value));

final long sumOfValues = table.valuesStream().sum();
final long maxKey = table.keysStream().max().orElse(0L);
```

**Notes**
- key `0` is reserved as the empty-slot marker: `put(0, v)` throws `IllegalArgumentException`
- value `0` means "absent" — storing `0` as a value is indistinguishable from a missing key, and
  `containsKey` reports `false` for such an entry
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
```

> **Warning:** the executor runs both the destination array allocation and the copying task, and the
> copying task spins while waiting for the application to authorize the next segment. A *bounded*
> executor therefore deadlocks as soon as all of its threads are occupied by spinning migrators —
> pass an unbounded / thread-per-task executor. The default one is exactly that, with daemon threads.

**Notes**
- same key/value rules as `LongLongHashtable`: key `0` is reserved, value `0` means absent
- `clear()`, `forEach()`, `keysStream()` and `valuesStream()` throw `UnsupportedOperationException`
- the public API is single-threaded: exactly one application thread, plus the internal migrator
- `-Dexchange.hashtable.verify=true` runs a full integrity check after every migration —
  for debugging migration corruption only, it is expensive

---

### LongLongRadixHashtable

**Experimental — not usable yet.** Shards keys across several `LongLongHashtable` instances by the
high bits of the hash, so that each shard can be resized independently. Currently only `put` and
`size` are implemented; `get`, `remove`, `containsKey`, `clear` and the iteration methods are still
stubs. It is present so that benchmarks can track the approach.

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

Benchmarks are latency-sensitive: run them on an idle machine, ideally with CPU isolation and thread
affinity enabled.

### Contributing
Exchange Collections is an open-source project and contributions are welcome!

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
