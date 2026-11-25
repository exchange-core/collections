# exchange collections
[![][license img]][license]

**Exchange Collections** is an open source high-performance Java collections library, extracted from
the [exchange-core](https://github.com/exchange-core/exchange-core) matching engine.

All collections are primitive-specialized (`long` keys, `long` or object values), allocation-conscious
and designed for low-latency, single-threaded hot paths.

### Quick start

Java 21+, Maven:

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-core</artifactId>
    <version>0.8.0</version>
</dependency>
```

```java
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;

// ask levels by price in cents: the keys stay sorted
LongAdaptiveRadixTreeMap<String> asks = new LongAdaptiveRadixTreeMap<>();
asks.put(10050, "100.50 x 300");
asks.put(10025, "100.25 x 100");
asks.put(10100, "101.00 x 500");

System.out.println(asks.firstKey());             // 10025 - the best ask
System.out.println(asks.higherValue(10025));     // 100.50 x 300 - the next level up
asks.forEach((price, level) -> System.out.println(price + ": " + level), 2);
// 10025: 100.25 x 100
// 10050: 100.50 x 300
```

The other structures are in the [catalog](#collections), the map above in [ART](docs/art.md).

### Modules

| Module                  | Release | Java | Contents                                                      | Docs |
|-------------------------|---------|------|---------------------------------------------------------------|------|
| `collections-core`      | 0.8.0   | 21+  | Adaptive Radix Tree, hashtables (long-long, int-long, long-object, UUID-object, tuples, snapshots), record store, queues (indexed FIFO, events, reorder window), price ladder, time windows and series, objects pool | [Collections](#collections) |
| `collections-orderbook` | 0.8.0   | 21+  | Order book implementation built on top of the core collections | [Order book](docs/orderbook.md) |
| `collections-ledger`    | 0.8.0   | 21+  | Balances (available, reserved) with checked postings and batches | [Balances](docs/ledger.md) |
| `collections-affinity`  | 0.7.0   | 22+  | Pinning threads to CPU cores, NUMA topology and memory placement checks, no dependencies | [Thread affinity](docs/affinity.md) |
| `tests-perf`            | –       | –    | Latency / throughput benchmarks (not published)                | [Benchmarks](docs/benchmarks.md) |
| `tests-stress`          | –       | –    | Long-running randomized stress tests (not published)           | [Testing](CONTRIBUTING.md#testing) |

### Requirements

- **To use the published artifacts:** Java 21+ (`collections-core`, `collections-orderbook` and
  `collections-ledger` 0.8.0); `collections-affinity` (0.7.0) needs Java 22+, as it uses the Foreign
  Function and Memory API.
- **To build from source:** JDK 26 and Maven 3.9+ — the benchmark and stress modules use newer
  language APIs than the published ones.

### Installation

`collections-core` as in the [quick start](#quick-start). The order book:

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-orderbook</artifactId>
    <version>0.8.0</version>
</dependency>
```

The balances:

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-ledger</artifactId>
    <version>0.8.0</version>
</dependency>
```

For pinning threads to cores (Java 22+, no dependencies, see [Thread affinity](docs/affinity.md)):

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-affinity</artifactId>
    <version>0.7.0</version>
</dependency>
```

The modules share one version line, but not every release has all of them: 0.7.0 was `collections-affinity`
alone, 0.8.0 is the other three - `collections-affinity` had no changes.

To build from source and install into the local repository:

```bash
mvn clean install
```

---

## Collections

| Class | Use for | Type | Keys → values | Since |
|---|---|---|---|---|
| [`LongAdaptiveRadixTreeMap`](docs/art.md#longadaptiveradixtreemap) | a sorted map: price levels, stop orders, top-N walks | Adaptive Radix Tree (ART) | `long` → object | 0.5.0 |
| [`LongLongHashtable`](docs/hashtables.md#longlonghashtable) | `long` → `long` lookups: ids, handles, counters | open-addressing hashtable | `long` → `long` | 0.6.0 |
| [`LongLongLL2Hashtable`](docs/hashtables.md#longlongll2hashtable) | the same, where a large table must grow without pausing the thread (below 8200 entries, by default, it resizes synchronously) | hashtable, async resizing | `long` → `long` | 0.6.0 |
| [`IntLongHashtable`](docs/hashtables.md#intlonghashtable) | `int` keys: symbols, currencies | `LongLongHashtable` under `int` keys | `int` → `long` | 0.8.0 |
| [`LongObjectHashtable`](docs/hashtables.md#longobjecthashtable) | objects by `long` id: orders, accounts | open-addressing hashtable, keys and values in two arrays | `long` → object | 0.8.0 |
| [`UuidObjectHashtable`](docs/hashtables.md#uuidobjecthashtable) | objects by UUID, as two longs or a `UUID` | open-addressing hashtable, keys and values in two arrays | UUID → object | 0.8.0 |
| [`RecordStore`](docs/records.md#recordstore) | several fields of one thing, reached by a handle kept elsewhere | paged records by handle | handle → `long` fields | 0.8.0 |
| [`LongRecordTable`](docs/records.md#longrecordtable) | records under keys, handles that stay valid | hashtable index + records | `long` → `long` fields | 0.8.0 |
| [`LongTupleHashtable`](docs/records.md#longtuplehashtable) | keys of several longs: (account, currency), a UUID | open-addressing hashtable | `long`s → `long`s, fixed widths | 0.8.0 |
| [`LongSnapshotRecordTable`](docs/records.md#longsnapshotrecordtable) | records serialized as of a sequence while the writer goes on | sharded hashtable with snapshots | `long` → `long` fields | 0.8.0 |
| [`LongIndexedFifoQueues`](docs/queues.md#longindexedfifoqueues) | the orders of price levels, cancelled by order ID | FIFO queues, one ID index | queue key, entry ID, value: `long` | 0.8.0 |
| [`LongIndexedEventQueue`](docs/queues.md#longindexedeventqueue) | timers and replay events, cancelled by ID | stable four-way min-heap with ID index | event ID, time: `long`; priority: `int` | 0.8.0 |
| [`LongSequenceBuffer`](docs/queues.md#longsequencebuffer) | a feed in sequence order: duplicates dropped, gaps found | reorder window by sequence | sequence → `long` | 0.8.0 |
| [`LongPriceLadder`](docs/price-and-time.md#longpriceladder) | volume up to a price, the tick and the cost of a quantity | Fenwick tree over ticks | tick → volume | 0.8.0 |
| [`LongRollingWindow`](docs/price-and-time.md#longrollingwindow) | rate limits, sums over the last span of time | ring of timed deltas | time, delta: `long` | 0.8.0 |
| [`LongTimeSeries`](docs/price-and-time.md#longtimeseries) | backtests: as-of lookups, several series replayed in time order | append-only time series | time → `long` | 0.8.0 |
| [`ObjectsPool`](docs/art.md#objectspool) | objects used again instead of allocated: ART nodes, orders | object pool | – | 0.5.0 |

## Documentation

- [Adaptive Radix Tree](docs/art.md) — `LongAdaptiveRadixTreeMap`, `ObjectsPool`
- [Hashtables](docs/hashtables.md) — `LongLongHashtable`, `LongLongLL2Hashtable` and its `MigrationService`,
  `IntLongHashtable`, `LongObjectHashtable`, `UuidObjectHashtable`
- [Records](docs/records.md) — `RecordStore`, `LongRecordTable`, `LongTupleHashtable`, `LongSnapshotRecordTable`
- [Queues](docs/queues.md) — `LongIndexedFifoQueues`, `LongIndexedEventQueue`, `LongSequenceBuffer`
- [Price ladder and time windows](docs/price-and-time.md) — `LongPriceLadder`, `LongRollingWindow`, `LongTimeSeries`
- [Thread affinity](docs/affinity.md) — `collections-affinity`: pinning threads, NUMA
- [Order book](docs/orderbook.md) — `collections-orderbook`
- [Balances](docs/ledger.md) — `collections-ledger`
- [Benchmarks](docs/benchmarks.md) — the machine and the settings behind the numbers, how to run them
- [Building, testing, contributing](CONTRIBUTING.md)
- [Release notes](RELEASE_NOTES.md)

A collection's section: an example, the features, the cost of the operations, measurements and notes.

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
