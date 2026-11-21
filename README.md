# exchange collections
[![][license img]][license]

**Exchange Collections** is an open source high-performance Java collections library, extracted from
the [exchange-core](https://github.com/exchange-core/exchange-core) matching engine.

All collections are primitive-specialized (`long` keys, `long` or object values), allocation-conscious
and designed for low-latency, single-threaded hot paths.

### Modules

| Module                  | Artifact                | Contents                                                      | Docs |
|-------------------------|-------------------------|---------------------------------------------------------------|------|
| `collections-core`      | `collections-core`      | Adaptive Radix Tree, hashtables (long-long, tuples, snapshots), record store, queues (indexed FIFO, events, reorder window), price ladder, time windows and series, objects pool | [Collections](#collections) |
| `collections-orderbook` | `collections-orderbook` | Order book implementation built on top of the core collections | [Order book](docs/orderbook.md) |
| `collections-ledger`    | `collections-ledger`    | Balances (available, reserved) with checked postings and batches | [Balances](docs/ledger.md) |
| `collections-affinity`  | `collections-affinity`  | Pinning threads to CPU cores, NUMA topology and memory placement checks, no dependencies | [Thread affinity](docs/affinity.md) |
| `tests-perf`            | –                       | Latency / throughput benchmarks (not published)                | [Benchmarks](docs/benchmarks.md) |
| `tests-stress`          | –                       | Long-running randomized stress tests (not published)           | [Testing](CONTRIBUTING.md#testing) |

### Requirements

- **To use the published artifacts:** `collections-core` and `collections-orderbook` 0.6.0 need Java 26+;
  their next release, 0.8.0, drops the published bytecode level to Java 21. `collections-affinity` (0.7.0)
  needs Java 22+, as it uses the Foreign Function and Memory API.
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

For pinning threads to cores (Java 22+, no dependencies, see [Thread affinity](docs/affinity.md)):

```xml
<dependency>
    <groupId>exchange.core2</groupId>
    <artifactId>collections-affinity</artifactId>
    <version>0.7.0</version>
</dependency>
```

The modules share one version line, but 0.7.0 is `collections-affinity` alone: there is no `collections-core`
0.7.0. The balances (`collections-ledger`) come with 0.8.0.

To build from source and install into the local repository:

```bash
mvn clean install
```

---

## Collections

| Class | Use for | Type | Keys → values | Status |
|---|---|---|---|---|
| [`LongAdaptiveRadixTreeMap`](docs/art.md#longadaptiveradixtreemap) | a sorted map: price levels, stop orders, top-N walks | Adaptive Radix Tree (ART) | `long` → object | stable |
| [`LongLongHashtable`](docs/hashtables.md#longlonghashtable) | `long` → `long` lookups: ids, handles, counters | open-addressing hashtable | `long` → `long` | stable |
| [`LongLongLL2Hashtable`](docs/hashtables.md#longlongll2hashtable) | the same, where a resize must not pause the thread | hashtable, async resizing | `long` → `long` | stable |
| [`RecordStore`](docs/records.md#recordstore) | several fields of one thing, reached by a handle kept elsewhere | paged records by handle | handle → `long` fields | new |
| [`LongRecordTable`](docs/records.md#longrecordtable) | records under keys, handles that stay valid | hashtable index + records | `long` → `long` fields | new |
| [`LongTupleHashtable`](docs/records.md#longtuplehashtable) | keys of several longs: (account, currency), a UUID | open-addressing hashtable | `long`s → `long`s, fixed widths | new |
| [`LongSnapshotRecordTable`](docs/records.md#longsnapshotrecordtable) | records serialized as of a sequence while the writer goes on | sharded hashtable with snapshots | `long` → `long` fields | new |
| [`LongIndexedFifoQueues`](docs/queues.md#longindexedfifoqueues) | the orders of price levels, cancelled by order ID | FIFO queues, one ID index | queue key, entry ID, value: `long` | new |
| [`LongIndexedEventQueue`](docs/queues.md#longindexedeventqueue) | timers and replay events, cancelled by ID | stable four-way min-heap with ID index | event ID, time: `long`; priority: `int` | new |
| [`LongSequenceBuffer`](docs/queues.md#longsequencebuffer) | a feed in sequence order: duplicates dropped, gaps found | reorder window by sequence | sequence → `long` | new |
| [`LongPriceLadder`](docs/price-and-time.md#longpriceladder) | volume up to a price, the tick and the cost of a quantity | Fenwick tree over ticks | tick → volume | new |
| [`LongRollingWindow`](docs/price-and-time.md#longrollingwindow) | rate limits, sums over the last span of time | ring of timed deltas | time, delta: `long` | new |
| [`LongTimeSeries`](docs/price-and-time.md#longtimeseries) | backtests: as-of lookups, several series replayed in time order | append-only time series | time → `long` | new |
| [`ObjectsPool`](docs/art.md#objectspool) | objects used again instead of allocated: ART nodes, orders | object pool | – | stable |

## Documentation

- [Adaptive Radix Tree](docs/art.md) — `LongAdaptiveRadixTreeMap`, `ObjectsPool`
- [Hashtables](docs/hashtables.md) — `LongLongHashtable`, `LongLongLL2Hashtable` and its `MigrationService`
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
