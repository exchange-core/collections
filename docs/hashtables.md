# Hashtables

[exchange collections](../README.md) › Hashtables

- [`LongLongHashtable`](#longlonghashtable) — `long` → `long`, open addressing with linear probing
- [`LongLongLL2Hashtable`](#longlongll2hashtable) — the same contract, resized in the background by a `MigrationService`
- [`IntLongHashtable`](#intlonghashtable) — `int` → `long`, a `LongLongHashtable` under `int` keys
- [`LongObjectHashtable`](#longobjecthashtable) — `long` → object, the keys and the values in two arrays
- [`UuidObjectHashtable`](#uuidobjecthashtable) — UUID → object, the key as two longs or a `java.util.UUID`
- [Load factors](#load-factors) — of the object tables: a lower one for small tables

Keys of several longs, and records of several fields under a key: [records](records.md).

The profile of these tables: fast operations at a capacity given in advance — no allocation per operation, nothing
done in the background. A resize is a rehash of the whole table on the calling thread (except in
`LongLongLL2Hashtable`): pre-size the table (`expectedSize`, `ensureCapacity`), or fix its capacity, where that
pause matters. The numbers below are averages; tails (p99, p99.9, max) are measured only for `LongLongHashtable`
and `LongLongLL2Hashtable`, in [the LL2 scenarios](#longlongll2hashtable).

---

## LongLongHashtable

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
throws `IllegalStateException` and changes nothing — updates and removals go on working, and a removal makes
room for a new key. The limit is of the keys in the array: the key equal to the seed is kept outside it and
always fits. `expectedSize` and `ensureCapacity` take up to the same limit at the maximum capacity —
`ensureCapacity(size())` changes nothing. How much longer the probes get, with linear probing and random keys:

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

## LongLongLL2Hashtable

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
the measured table growing from 16 to 4M entries; [Xeon Gold 6244](benchmarks.md#the-machine), application pinned to an isolated core,
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

## IntLongHashtable

`int` keys to `long` values: a [`LongLongHashtable`](#longlonghashtable) under `int` keys, widened to `long`. The
same layout — a key and its value side by side, one cache line for a lookup — the same builder, `missingValue`,
hash seed, maximum capacity and costs; 16 bytes per slot.

```java
import exchange.core2.collections.hashtable.IntLongHashtable;

final IntLongHashtable bySymbol = IntLongHashtable.builder()
        .expectedSize(10_000)
        .missingValue(-1L)
        .build();

bySymbol.put(42, 1234L);
bySymbol.get(42);                           // 1234
bySymbol.get(7);                            // -1
bySymbol.addAndGet(42, 6L);                 // 1240
bySymbol.computeIfAbsent(7, s -> s * 10L);  // 70
bySymbol.forEach((symbol, value) -> ...);   // no allocation
bySymbol.keysStream();                      // IntStream
```

**Notes**
- every call goes to the `LongLongHashtable` inside: one reference more on the way to its array
- any `int` key, `0` included; the rest as for `LongLongHashtable`

---

## LongObjectHashtable

`long` keys to object values. Open addressing with linear probing over two arrays of the same length: the keys in
a `long[]`, the value of each in an `Object[]` at the same index; backward-shift removal, no tombstones.

```java
import exchange.core2.collections.hashtable.LongObjectHashtable;

final LongObjectHashtable<Order> orders = LongObjectHashtable.builder()
        .expectedSize(1_000_000)
        .build();

orders.put(orderId, order);              // the previous value, or null
orders.get(orderId);                     // order; null for a missing key
orders.putIfAbsent(orderId, other);      // order - present, unchanged
orders.computeIfAbsent(id, Order::new);  // the function runs only for a missing key
orders.getOrDefault(id, fallback);
orders.remove(orderId);                  // order
orders.remove(orderId, order);           // only with an equal value
orders.forEach((id, o) -> ...);          // no allocation
orders.put(1L, null);                    // NullPointerException
```

**Layout.** A reference can not live in a `long[]`, so a key and its value can not share a cache line. They lie at
the same index of their arrays, so the address of the value is known as soon as the key is hashed: the CPU loads
both lines at once, and a lookup waits for one miss of the table, then for the object. A probe walks the keys only,
8 to a line. There are no `null` values, so a free slot holds `null` and `get` reads the value of the slot its probe
stopped at without comparing the key again. A removal clears the value cell it frees: the table keeps no removed
object alive.

**Notes**
- any key, `0` included: the value of the key equal to the seed — stored as 0, the free-slot marker — is kept in
  a field, outside the array: the load factors and `maxLoad` do not count it
- the builder: `expectedSize`, `maxCapacity`, `fixedCapacity`, `maxLoad` as for `LongLongHashtable`; no
  `missingValue` — `null` is the missing value. The hash seed of the JVM, as for `LongLongHashtable`
- 12 bytes per slot (4-byte references, a heap below 32 GB; 16 bytes above)
- two load factors, see [Load factors](#load-factors): a table grows by doubling at `loadFactor` of its slots
  (default 0.65), and while it has fewer than `smallTableCapacity` slots (default 2^16) at the lower
  `smallTableLoadFactor` (default 0.5). A resize copies both arrays on the calling thread — pre-size the table where
  that pause matters
- a reference written into the array costs a GC write barrier: in `put`, in `remove` and for each entry a removal
  shifts back
- `remove(key, value)` compares with `equals`; `computeIfAbsent` puts nothing if the function returns `null`
- not thread-safe

**Measured** — `LongObjectHashtableJmh`, against the maps of the same layout, each pre-sized for the keys with its
own load factor: Eclipse's `LongObjectHashMap` (at most 0.5 full, tombstones; as in exchange-core), agrona's
`Long2ObjectHashMap` (0.65, compacts the chain on removal) and fastutil's `Long2ObjectOpenHashMap` (0.75,
backward shift). `LongObjectHashtable` 2× is the table pre-sized for twice the keys: the load of Eclipse's map
(0.3 here, against 0.61). ns per operation, 10k / 1M / 10M random keys in a random order;
[Xeon Gold 6244](benchmarks.md#the-machine), JDK 27, the benchmark thread on an isolated core, one fork, 3 × 1 s
of warm-up and of measurement:

| Operation | `LongObjectHashtable` | the same, 2× | `LongObjectHashMap` | `Long2ObjectHashMap` | `Long2ObjectOpenHashMap` |
|---|---|---|---|---|---|
| `chase` — each lookup and the read of its object wait for the one before | 14.6 / 100 / 192 | 11.7 / 119 / 184 | 9.1 / 107 / 185 | 15.6 / 108 / 189 | 15.4 / 99 / 191 |
| `get` — independent lookups of present keys | 10.7 / 45 / 69 | 6.3 / 47 / 61 | 5.3 / 41 / 55 | 10.7 / 48 / 74 | 11.8 / 42 / 67 |
| `getMiss` — absent keys | 16.5 / 27 / 70 | 8.3 / 30 / 40 | 7.8 / 18 / 30 | 15.8 / 23 / 67 | 20.3 / 21 / 62 |
| `replace` — a key removed, a new one added | 54 / 119 / 242 | 30 / 110 / 152 | 59 / 172 / 573 | 56 / 98 / 211 | 54 / 121 / 215 |

- Out of the caches a lookup that reads its object costs the same in all four, ~100 ns at 1M and ~190 at 10M: a
  miss of the table — the key and the value lines at once — then a miss of the object.
- In the caches, and for missing keys, the load decides: the shorter the clusters, the fewer probe steps and
  mispredicted exits. Pre-sized for twice the keys the table matches Eclipse's map (6.3 against 5.3 ns at 10k,
  8.3 against 7.8 for a miss) — and takes its memory: that map at least 24 bytes per key (12 per slot, at most half
  of them used), this table at least 18.5 at its own load.
- Eclipse's removal leaves tombstones until a rehash of the whole table: iterations of `replace` at 132, 251 and
  132 ns at 1M; 186, 136 and 1395 at 10M. A backward shift, or agrona's compaction, has no such pauses.
- Measured with one load factor, 0.65, for every size: 16384 / 2^21 / 2^24 slots, 2^15 / 2^22 / 2^25 for 2×.
  With the [defaults](#load-factors) a table of 10k keys grows at 0.5: 32768 slots, the table of the 2× column;
  1M and 10M as measured. The benchmark sets the load with `-p loadFactor` — 0.65 by default, `default` for the
  defaults — and prints the capacity the table got:
  `java -cp test-classes:lib/* org.openjdk.jmh.Main LongObjectHashtableJmh -rf json -rff out.json`.
- No allocation per operation: `-prof gc` gives ~0 B/op for the four operations (10k keys, JDK 26).

---

## UuidObjectHashtable

UUID keys to object values: the layout of [`LongObjectHashtable`](#longobjecthashtable) with keys of two longs —
the most and the least significant bits side by side in a `long[]`, the value in an `Object[]` at the slot index.

```java
import exchange.core2.collections.hashtable.UuidObjectHashtable;

final UuidObjectHashtable<Account> accounts = new UuidObjectHashtable<>(1_000_000);

accounts.put(hi, lo, account);           // the halves, as decoded from a message
accounts.get(hi, lo);                    // account
accounts.get(uuid);                      // the same entry, through a java.util.UUID
accounts.computeIfAbsent(hi, lo, (h, l) -> new Account(h, l));
accounts.computeIfAbsent(uuid, Account::new);
accounts.remove(uuid);
accounts.forEach((h, l, a) -> ...);      // the key as its halves, no allocation
accounts.keysStream();                   // Stream<UUID>, a new UUID each
```

Every operation takes the key in either form: the `UUID` overloads read its halves and call the others. The table
never keeps or creates `UUID` objects, except in `keysStream`.

**Notes**
- the hash mixes both halves with the seed of the JVM: mix64(mix64(seed + hi) + lo), SplitMix64's finalizer
  twice, as the tags of [`LongTupleHashtable`](records.md#longtuplehashtable). Keys picked to collide can not be
  found without the seed; sequential UUIDs — a prefix and a counter, UUIDv7 of one millisecond — spread like random
  ones. A removal hashes again each entry it shifts back
- any key: the nil UUID — two zero longs, the free-slot marker — is kept in a field, outside the array: the load
  factors and `maxLoad` do not count it
- 20 bytes per slot (4-byte references); a probe walks 4 keys to a cache line
- no `null` values, the builder, the two [load factors](#load-factors), the resize, `remove(key, value)` and
  `computeIfAbsent` as for `LongObjectHashtable`
- not thread-safe

**Measured** — `UuidObjectHashtableJmh`, random UUIDs (v4) arriving as two longs: the table given the halves,
pre-sized for twice the keys (2×), given a `new UUID(hi, lo)`, and `HashMap<UUID, V>` given one. ns per
operation, 10k / 1M / 10M keys, the machine and the runs of `LongObjectHashtableJmh`:

| Operation | `UuidObjectHashtable` | the same, 2× | the same, through `UUID` | `HashMap<UUID, V>` |
|---|---|---|---|---|
| `chase` — each lookup and the read of its object wait for the one before | 17.8 / 127 / 200 | 15.1 / 139 / 188 | 18.4 / 132 / 200 | 16.9 / 212 / 298 |
| `get` — independent lookups of present keys | 12.9 / 79 / 108 | 9.3 / 80 / 96 | 15.6 / 79 / 109 | 12.8 / 102 / 145 |
| `getMiss` — absent keys | 22.9 / 59 / 100 | 13.0 / 54 / 65 | 22.9 / 58 / 101 | 7.4 / 29 / 56 |
| `replace` — a key removed, a new one added | 77 / 197 / 319 | 44 / 171 / 227 | 80 / 196 / 319 | 47 / 144 / 358 |

- A lookup that reads its object is a third shorter than with `HashMap` out of the caches, 127 against 212 ns at
  1M and 200 against 298 at 10M: the arrays of keys and values at once, then the object — where `HashMap` reads its
  table, the node, the `UUID` of the node to compare it, then the object.
- A missing key is cheaper in `HashMap`: an empty bucket ends its lookup in one read, and the node of a full one
  holds the hash to compare. A miss here walks to the end of the cluster, ~4 slots at load 0.61 — pre-sized for
  twice the keys, 13 / 54 / 65 ns.
- Given a `UUID`, the table costs the same as given the halves, out of the caches; 2.7 ns more at 10k.
- Measured with one load factor, 0.65, as `LongObjectHashtableJmh` (`-p loadFactor`, 0.65 by default): with the
  [defaults](#load-factors) the table of 10k keys is the one of the 2× column.
- No allocation per operation given the halves (`-prof gc`, ~0 B/op); given a `UUID`, below 0.5 B/op — the
  `new UUID` of the caller is scalar-replaced but for a few calls.

---

## Load factors

`LongObjectHashtable` and `UuidObjectHashtable` grow at one of two loads: a small table, below
`smallTableCapacity` slots, at the lower `smallTableLoadFactor` — sparse while that costs little memory.

```java
LongObjectHashtable.builder()
        .loadFactor(0.65)             // from smallTableCapacity slots on: 0.25 to 0.9
        .smallTableLoadFactor(0.5)    // below: 0.25 to loadFactor; unset - min(0.5, loadFactor)
        .smallTableCapacity(1 << 16)  // a power of two; 2 - every table at loadFactor
        .build();
```

- A table doubles when its size reaches the load factor of its slots: its load runs from half the load factor to
  the load factor. `expectedSize` and `ensureCapacity` follow the same thresholds — at the maximum capacity, up to
  `maxLoad`; `maxLoad` must be at least the load factor the table grows at near its maximum capacity. The key
  kept in a field (equal to the seed, the nil UUID) is not in the array and counts for neither.
- The load sets the probe: linear probing walks ~(1 + 1/(1 − α))/2 slots for a present key and
  ~(1 + 1/(1 − α)²)/2 for a missing one — 1.5 and 2.5 at 0.5, 1.9 and 4.6 at 0.65, 3.8 and 23 at 0.85. Each slot is
  work and its exit a mispredicted branch; a removal shifts back the rest of the cluster, a put walks to its end.
- Memory: 12 / α bytes a key (20 / α for UUIDs) — at 0.65 18.5 just before the table grows and 37 after; a small
  table at 0.5 takes 24 to 48. 2^16 slots are 768 KB, 1.25 MB with UUID keys.
- `LongLongHashtable` and `IntLongHashtable` grow at 0.65.
- Many small tables — a map per user — take up to twice the memory at 0.5: 16 keys need 64 slots, not 32. For them
  `smallTableLoadFactor(0.65)`, or `smallTableCapacity(2)`.

**Measured** — `HashtableLoadSweepJmh`: a table of exactly 2^n slots at a load, random keys, the value an object;
`chase` — each lookup and the read of its object wait for the one before, `getMiss` — independent lookups of absent
keys, `replace` — a key removed and a new one put. ns per operation; AWS m8azn.xlarge (AMD EPYC 9R05, Zen 5, 5 GHz;
L2 1 MB a core, L3 20 MB), JDK 27, the benchmark thread pinned, one fork, 2 × 0.5 s of warm-up, 3 × 0.5 s of
measurement. At one capacity a higher load is also more keys and more objects to read.

`LongObjectHashtable`:

| Operation, slots (table) | 0.25 | 0.35 | 0.45 | 0.55 | 0.65 | 0.75 | 0.85 |
|---|---|---|---|---|---|---|---|
| `chase`, 2^12 (48 KB) | 5.8 | 6.4 | 6.6 | 6.9 | 7.1 | 7.4 | 8.0 |
| `chase`, 2^18 (3 MB) | 19.0 | 20.7 | 22.1 | 23.5 | 24.7 | 26.8 | 28.6 |
| `chase`, 2^24 (192 MB) | 249 | 252 | 258 | 259 | 271 | 292 | 307 |
| `getMiss`, 2^12 | 2.0 | 2.1 | 2.4 | 2.8 | 3.4 | 5.0 | 11.1 |
| `getMiss`, 2^18 | 6.3 | 9.2 | 12.9 | 15.9 | 18.7 | 23.7 | 35.3 |
| `getMiss`, 2^24 | 29 | 33 | 40 | 56 | 85 | 105 | 125 |
| `replace`, 2^12 | 19.1 | 26.7 | 33.2 | 39.9 | 46.3 | 62.5 | 107 |
| `replace`, 2^18 | 23.2 | 32.9 | 42.3 | 50.0 | 57.8 | 73.9 | 120 |
| `replace`, 2^24 | 117 | 141 | 176 | 221 | 280 | 340 | 471 |

`UuidObjectHashtable`:

| Operation, slots (table) | 0.25 | 0.35 | 0.45 | 0.55 | 0.65 | 0.75 | 0.85 |
|---|---|---|---|---|---|---|---|
| `chase`, 2^12 (80 KB) | 9.0 | 9.4 | 9.5 | 9.9 | 10.2 | 10.6 | 11.7 |
| `chase`, 2^18 (5 MB) | 21.7 | 23.8 | 25.6 | 26.9 | 28.4 | 30.8 | 33.6 |
| `chase`, 2^24 (320 MB) | 253 | 262 | 263 | 267 | 279 | 299 | 324 |
| `getMiss`, 2^12 | 4.2 | 4.5 | 4.9 | 5.6 | 6.9 | 9.8 | 18.2 |
| `getMiss`, 2^18 | 9.5 | 12.7 | 16.5 | 21.1 | 23.5 | 26.2 | 34.8 |
| `getMiss`, 2^24 | 48 | 54 | 65 | 92 | 122 | 140 | 172 |
| `replace`, 2^12 | 27.5 | 36.3 | 44.2 | 51.4 | 63.0 | 86.3 | 153 |
| `replace`, 2^18 | 33.8 | 44.2 | 53.5 | 64.5 | 78.4 | 103 | 174 |
| `replace`, 2^24 | 186 | 220 | 267 | 323 | 381 | 462 | 597 |

- A lower load is faster at every size: `replace` and `getMiss` the most, `chase` hardly in the caches. Out of the
  caches too — a longer probe is more work between the misses, and the misses of independent lookups overlap less.
  So the load factor is a choice of memory.
- Taken as a policy — a table grown at 0.5 instead of 0.65, averaged over sizes: in L1 and L2 `replace` −20%
  (−18% with UUID keys), `getMiss` −11% (−9%), `chase` +4% (+5%); out of the caches `replace` −22% (−18%),
  `getMiss` −26% (−24%), `chase` −2%. The table takes 35 bytes a key instead of 27 (58 instead of 44).
- The defaults: 0.5 below 2^16 slots, where the sparser table is at most 384 KB (640 KB with UUID keys) and stays in
  a core's L2; 0.65 from there on, as `LongLongHashtable`. A table sparse enough to outgrow the L3 loses: at 0.65,
  `chase` goes from 27 ns at 2^20 slots (12 MB) to 216 at 2^22 (48 MB).
