# Adaptive Radix Tree

[exchange collections](../README.md) › Adaptive Radix Tree

- [`LongAdaptiveRadixTreeMap`](#longadaptiveradixtreemap) — a sorted `long` → object map
- [`ObjectsPool`](#objectspool) — the pool its nodes come from and go back to

---

## LongAdaptiveRadixTreeMap

An [Adaptive Radix Tree](https://db.in.tum.de/~leis/papers/ART.pdf) map: a sorted `long` → object map,
ordered as `TreeMap<Long, V>`, with an API of its own — primitive keys, no null values, not a
`java.util.Map`. It never rebalances and never resizes as a whole: a put or a removal changes the nodes
on the path of its key, and allocates only when the pool has no free node of the size it takes.

**Features**
- keys are kept in **signed** ascending order (same order as `TreeMap<Long, V>`)
- no re-balancing, no whole-tree resize — a put or remove changes at most the nodes on the key's path
- internal nodes are recycled through `ObjectsPool` — low GC pressure
- neighbour lookups (`higherValue` / `lowerValue`, `ceilingValue` / `floorValue`), first / last
  key and value, ordered traversal, a cursor, `keysStream()` / `valuesStream()`
- a `NavigableMap<Long, V>` view for the code that takes a `java.util` map: `asNavigableMap()`

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

A load from a snapshot: `putAllSorted(keys, values)` with the keys ascending builds the tree bottom-up, every
node of its final size at once — no node grows through the smaller sizes as puts make it. On a desktop,
roughly: 1.9x faster than puts for consecutive keys, 1.7x for an order book, 1.5x for keys alone in their
256-key blocks, 1.1x for random ones. Into a map that is not empty it puts the keys one by one:

```java
final LongAdaptiveRadixTreeMap<PriceLevel> asks = new LongAdaptiveRadixTreeMap<>();
asks.putAllSorted(snapshotPrices, snapshotLevels);   // long[] ascending, PriceLevel[] of the same length
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

For code that takes a `java.util` map — a report, a library, a test — `asNavigableMap()` is a view of the
map as a `NavigableMap<Long, V>`: its reads and writes go to the map. Its sub-maps, `descendingMap`, key
sets and iterators are views too, with the rules of `TreeMap`'s. It boxes every key and allocates the
entries — not for hot paths:

```java
final NavigableMap<Long, PriceLevel> view = asks.asNavigableMap();
view.headMap(price, true).size();     // the levels at or below a price - a sub-map counts its keys
view.descendingMap().firstEntry();    // the highest level, a snapshot entry
publish(view.tailMap(price, false));  // to the code that takes a SortedMap
```

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
- the consumer of `forEach` / `forEachDesc` and the function of `computeIfAbsent` must not change the key set
  (updating the value of a present key is fine): a walk throws `ConcurrentModificationException` once it is
  over, `computeIfAbsent` right after the function, putting nothing; `limit` 0 or less visits nothing
- `removeRange(from, toExclusive)` removes a range of keys (e.g. the stop orders up to a price) and returns how
  many
- `asNavigableMap()`: a null key or value throws `NullPointerException`, a put out of the bounds of a sub-map
  `IllegalArgumentException`; the size of a sub-map is O(keys in it), as in `TreeMap`, of the whole map O(1)
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
| `putAllSorted` | into an empty map: a pass over the keys per level, every node built once; otherwise a put per key |
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

## ObjectsPool

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
