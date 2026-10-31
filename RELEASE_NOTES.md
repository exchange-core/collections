# Release notes

## Unreleased

### Added

- **`collections-affinity`: pinning threads to CPU cores.** `CpuAffinity.acquireCore()` pins the calling
  thread to a free core of the reserved CPUs (`isolcpus` or `-Daffinity.reserved`), reserving all its
  hyper-threading siblings, within the process and across processes (lock files). Linux and Windows,
  through the Foreign Function and Memory API: Java 22+, no dependencies, no native library, works in a
  GraalVM native image (reachability metadata included). Replaces OpenHFT Affinity (JNA) in the benchmarks;
  `net.openhft:affinity` is no longer used anywhere in the build.
- **`LongLongHashtable`: `forEach`, `keysStream`, `valuesStream` and `clear`.** The README documented
  them, but all four threw `UnsupportedOperationException`. `forEach` does not allocate.
- **`LongLongLL2Hashtable`: `forEach`, `keysStream`, `valuesStream` and `clear`** - they threw
  `UnsupportedOperationException`. They finish an in-flight migration first, as `close()` does: a pause, for
  the set-up, snapshots or tests.
- **Hashtables: `putIfAbsent`, `addAndGet`, `computeIfAbsent`, `remove(key, value)`, `capacity()`,
  `ensureCapacity(expectedSize)` and `toString()`.** The compound operations take one probe, like `put`
  (in `LongLongLL2Hashtable` when no resize is in flight - during a migration they are made of the basic
  operations). `ensureCapacity` grows the table in one rehash. `toString` prints `{key=value, ...}`;
  during a migration `LongLongLL2Hashtable` prints only the size. In `ILongLongHashtable` the compound
  operations are default methods; `capacity()` and `ensureCapacity()` are new abstract methods.
- **Hashtables: builders.** `LongLongHashtable.builder()` / `LongLongLL2Hashtable.builder()` set the expected
  size, `missingValue` and, for LL2, the executor and `syncResizeBelow`; the plain constructors stay, those
  with `missingValue` are replaced by the builders.
- **Hashtables: hash seed** - `-Dexchange.hashtable.seed=random` or a number, one for all tables of the JVM,
  default `0`. It is mixed into the hash of every key: where the keys come from outside, a seed unknown there
  keeps them from being picked to pile up in one probe cluster - 2000 keys picked for one slot form a cluster
  of 2000 under seed 0, under another seed none longer than 30. The arrays hold `key ^ seed`, so the probing,
  resizing and migration code is unchanged. A static final: seed `0` costs nothing (the JIT folds the XOR
  away; `HashtableOpsJmh` within ±1%), another seed 1-3 instructions per operation - in that back-to-back
  benchmark +5..15% (up to +27%), a fraction of a nanosecond for an operation on its own. A seed per table
  was dropped: it cost 1.4-7.3% even at `0`, the XOR with a field can not be folded. `seed()` reports the seed
  in use; `0` keeps the iteration order the same from run to run. The core tests run a second time under a
  seed.
- **Hashtables: `maxCapacity(int)`, `fixedCapacity()` and `maxLoad(double)` in the builders** - a table that
  grows only up to a given capacity, or never resizes at all (the capacity for the expected size). There it
  takes new keys up to `maxLoad` (from 0.65 to below 1, default 0.95, two slots always free), then `put` of a
  new key throws `IllegalStateException` and changes nothing. The javadoc and the README give the probe
  lengths: at 0.95 an insert or a miss probes ~200 slots on average, at 0.99 ~5000. A `LongLongLL2Hashtable`
  of fixed capacity never migrates: no background task, no wait, no second array.
- **Hashtables: `getOrDefault(key, defaultValue)`** - presence and value in one probe, where `get` can not tell
  a missing key from one stored with the missing value. The cost of each operation (probes, waits, passes over
  the array) is now documented in the README and the javadoc - "one probe" in `ILongLongHashtable` was too
  broad.
- **`CollectionsLatencySamples`: groups `ll2-scenarios` and `hashtable-collide`** (run only when named).
  `ll2-scenarios`: the put latency of `LongLongLL2Hashtable` configurations - spinning migrator, thread per
  task, the default executor in a pinned application, tasks started late, four tables growing at once, mixed
  operations against `LongLongHashtable` - plus what the measuring thread allocated and the CPU of the other
  threads. `hashtable-collide`: gets of keys picked to collide, under seed 0 and a random seed. Results in the
  README.
- **`HashtableOpsJmh`** (`tests-perf`, JMH 1.34): average time of the single-key hashtable operations, hits
  and misses, from an L1-resident table to one in DRAM - for A/B runs of `collections-core` builds.
- **`Automatic-Module-Name`** in the published jars: `exchange.core2.collections`,
  `exchange.core2.collections.orderbook`, `exchange.core2.collections.affinity`.
- **`LongAdaptiveRadixTreeMap`: `containsKey`, `putIfAbsent`, `computeIfAbsent`, `putAndGetLowerValue` /
  `putAndGetHigherValue`, `ceilingValue` / `floorValue`, `firstKey` / `lastKey`, `firstValue` /
  `lastValue`, `pollFirstValue` / `pollLastValue`, `size()` and `isEmpty()`.** The fused puts are what an order
  book does for a new price level (put it, link it to its neighbour): one descent when the neighbour is in the same
  256-key block - the node of the last level remembers the put, the neighbour is next to the key there - otherwise
  a second descent finds it (in `ArtOpsJmh` on rhel-lat a new level in a 20k-level book -9%, in a 1k-level one -1%;
  a removal + put of a level pays about 1 ns, +3%, for this and the change counter of the cursor). `putIfAbsent`,
  `computeIfAbsent` and the polls are compositions for now - two descents.
- **`LongAdaptiveRadixTreeMap.Cursor`** (`map.cursor()`): a position in the map, reusable and allocation-free - seek
  the ceiling, higher, floor or lower key, the first or the last, then move to the next or the previous one; `key()`,
  `value()`, `remove()` (and move on). A change of the key set not made through the cursor makes its next call throw
  `ConcurrentModificationException`. For the stop orders up to a price, a sweep over the levels, and the
  key-returning navigation `TreeMap` has (`higherKey` = `seekHigher` + `key()`). A seek is a recursion like a lookup
  (each node pushes itself onto the path), the current key is kept in fields of the cursor, the path above it in
  arrays - a move within a node asks the node one thing. In `ArtOpsJmh` on rhel-lat a seek and 10 moves take 60-83 ns
  in a book, the iterators of fastutil's tree 72-126 ns, of `TreeMap` 84-140 ns.
- **`LongAdaptiveRadixTreeMap.forEach(from, consumer, limit)` / `forEachDesc(from, ...)`**: the keys from a key up /
  down - L2 market data from a price. One descent to the key, then the subtrees beyond it are walked whole: 10
  levels from a price in 44-62 ns, fastutil's tree 73-125 ns, `TreeMap` 81-140 ns (`ArtOpsJmh` `range10`, rhel-lat).
- **`LongAdaptiveRadixTreeMap.builder()`**: `poolCapacity(nodes)` sizes the pool of the map's own (default 256 free
  leaves and 4-key nodes, fewer of the bigger ones; 0 - none), `sharedPool(pool)` takes a pool shared with other maps
  of the thread. `ObjectsPool.forArt(nodes)` makes a pool for ART nodes only - no exchange-core types to know.
  `new LongAdaptiveRadixTreeMap<>()` gets a pool of ART nodes of the same sizes as before, no longer the test pool
  with places for orders and buckets.
- **`LongAdaptiveRadixTreeMap`: `keysStream()`, `valuesStream()`, `forEach(consumer)`, `getOrDefault(key, value)`
  and `toString()`** - as the hashtables have them. The streams are ascending and lazy: a cursor of their own,
  made when the stream is consumed; a change of the key set during the walk throws
  `ConcurrentModificationException`. `toString` prints `{key=value, ...}` as `TreeMap` does.
- **`CollectionsLatencySamples`: group `art-fill`** (run only when named): `get`, `put` and `remove` latency with 1M
  keys of four densities (as in `art-keys`) - the ART against `TreeMap` and fastutil's `Long2ObjectRBTreeMap`;
  `-Dsamples.art.label` names the ART series, to put two builds of collections-core side by side. The initial ART
  (`8df3bb5`) against the current one, with charts: exchange-charts `samples-art`.
- **`ArtOpsJmh`** (`tests-perf`): average time of the `LongAdaptiveRadixTreeMap` operations against `TreeMap`
  and fastutil's `Long2ObjectRBTreeMap`, on the key sets of `ArtKeyProfiles`: order-book price levels (asks and
  bids, the distance from the mid hyperbolic - dense near the mid, thinning out further), order ids with 95%
  of them gone, random keys. Lookups, neighbour, removal + put, new level (put + neighbour), top 10, 10 keys from
  a key (`range10`; `cursor10` the same with the cursor), and a mixed
  order-book stream.
- **`RecordStore`: records of `long` fields, addressed by a `long` handle** (new package
  `exchange.core2.collections.records`). No keys - a `long` hashtable maps keys to handles, `NO_RECORD` (`0`) being
  its default missing value. A record never moves; a freed one is reused under a new generation, and every access
  checks it: a stale handle throws instead of reaching the record allocated after it. Pages of `pageSize` records
  (default 1024): growing allocates a page, nothing is copied but the page directory. `addAndGetExact` throws
  `ArithmeticException` on overflow and keeps the value. The same operations hand out the same handles. A handle
  holding the offset of the record in its page instead of the slot (no multiplication per access) was measured:
  within ±5%, not taken.
- **`RecordStoreJmh`** (`tests-perf`): `RecordStore` against an object per record (the caller holding the
  reference, or a `long` id into an `ArrayList` with a generation check) and a flat `long[]` - read, reservation,
  free + allocate, a scan, and a chase through the records (the latency of one access), from 10k records (L2) to
  10M (DRAM). Results in the README.
- **`LongRecordTable`: `long` keys to records of `long` fields** - a `LongLongHashtable` index over a
  `RecordStore`. `getOrCreate` takes one probe: the record of a new key is allocated once the index has made room
  for it, so a full table refuses the key with nothing allocated. The handles stay valid through the resizes of the
  index; a record holds its key (`keyOf`, and `forEach` in the order of the records, whatever the hash seed). An
  access through `NO_RECORD` says so in the exception of `RecordStore`.
- **`LongRecordTableJmh`** (`tests-perf`): balances by key in a `LongRecordTable`, in a `LongLongHashtable` per
  field and in Eclipse's `LongObjectHashMap` of objects - read, reservation, a key removed and another added, and a
  chase from key to key (the latency of one lookup), from 10k keys to 10M. Results in the README.
- **`LongTupleHashtable`: keys of a fixed number of `long`s to values of a fixed number of `long`s**, side by
  side in the slots of one `long[]` (open addressing, linear probing, backward-shift deletion). A slot holds a tag -
  the hash of the key with the top bit set, never 0 - so any key can be stored, a probe compares tags before keys,
  and a resize or a removal never hashes a key again. Methods for keys of 1 to 4 longs, a `long[]` for any length;
  `valueLongs(0)` is a set. The values are reached through the position of the slot, valid until the next insert or
  removal. Hash seed, `maxCapacity` / `fixedCapacity` / `maxLoad` as in `LongLongHashtable`.
- **`LongTupleHashtableJmh`** (`tests-perf`): balances under a key of three longs (a UUID, a token and a
  subaccount) in a `LongTupleHashtable` against a `HashMap` of a `record` key to an object; `LongRecordTableJmh`
  measures it too, with a key of one long. Results in the README.
- **`collections-ledger` (new module): `BalanceTable`** - balances (available, reserved) under keys of a fixed
  number of longs, in a `LongTupleHashtable`. `deposit`, `withdraw`, `reserve`, `release`, `settle` and `post`
  check before they write and return a status (`OK`, `INSUFFICIENT_AVAILABLE`, `INSUFFICIENT_RESERVED`, `OVERFLOW`,
  ...): neither amount goes below 0, no sum overflows, a refusal changes nothing and throws nothing. They take a
  reference from `find` / `getOrCreate`, valid until the entries move (a resize, a removal) - a stale one throws. A
  `Batch` of postings to several balances applies all or nothing, the same key any number of times, the balances it
  created removed again on a refusal. Java 21, no dependencies but `collections-core`.

### Fixed

- **`remove(0)` corrupted the size** of `LongLongHashtable` and `LongLongLL2Hashtable`. Key `0` marks
  empty slots, and `hash(0)` is `0`: when slot 0 was empty, the removal took it for the key and
  decremented the size, without removing anything. A negative size never reaches the resize threshold,
  so the table then filled up completely and `put`/`get` failed with `IllegalStateException`.
  `remove(0)` now returns `0` and changes nothing.
- **`containsKey` returned `false` for a key stored with value `0`** (both tables) — it tested the
  value instead of the key. It now reports any present key, whatever its value; `size()` already
  counted such entries.
- **Constructor sizes were not validated.** `new LongLongHashtable(Integer.MAX_VALUE)` silently built a
  zero-length table that failed on the first `put` with `ArrayIndexOutOfBoundsException`, and sizes from
  ~349 million up threw `NegativeArraySizeException`. Both tables now throw `IllegalArgumentException` for
  a negative size or one above the maximum capacity (2^29 slots).
- **`LongLongLL2Hashtable` could lose keys during a migration.** Keys whose home is already migrated go
  straight into the new array, while the migrator copies the next segment into the same array. When such a
  key's cluster grew past the migration frontier, both threads wrote the same slots without synchronization:
  the entry of one of them was lost, and `size()` no longer matched the contents. Inserts and removals that
  reach the frontier now wait until the migrator finishes its segment (256 slots, see below). Rare with random
  keys and the default settings — about one write in 20 000 during a migration reaches the frontier — but in
  a stress test that kept growing clusters across the frontier, 15% of the tables lost keys.
- **`LongLongLL2Hashtable.put` could fail with `IllegalStateException` ("stuck waiting for migration")**
  at the 90% block threshold. A put reaching the migration start extends the migrated area backwards; when
  that moved the start onto the migrator's position, the migrator waited for an authorization that never
  came, and the put waited for the migrator until the 10 s hang detection. Seen with small tables migrated
  asynchronously (low `syncResizeBelow`).
- **At the maximum capacity (2^29 slots) both tables filled up completely.** They stopped growing but kept
  taking new keys, with ever longer probes, until the last free slot - and then every operation that probed
  for a missing key failed. Now a table there takes new keys up to `maxLoad` (0.95 by default, see Added) and
  then refuses them with `IllegalStateException` ("table is full"); the refused put changes nothing, updates and
  removals still work.
- **A table sized for n entries could resize at the n-th insert.** The resize starts when the size reaches
  (int) (capacity * 0.65), and for every capacity one expected size was exactly that - e.g. 681 574 for
  2^20 slots, where `LongLongHashtable` then copies the table for ~7 ms on the last pre-sized insert.
  Expected size 0 got a single slot, resized by the first put. Now the constructors (and `ensureCapacity`)
  take the smallest capacity that holds n entries without a resize.
- **`LongLongLL2Hashtable` broke when the executor rejected a task.** A rejected allocation failed the put
  that crossed the resize threshold - with its key already stored; the following puts did the same, until
  the array was full and every lookup of a missing key failed. A rejected copying task left a migration
  without a migrator: the operations waiting for it, and `close()`, failed after the hang timeout. Now a
  rejected allocation means a synchronous resize, and the table copies a rejected migration itself.
- **`LongLongLL2Hashtable` hung forever with an executor that runs tasks on the calling thread**
  (`Runnable::run`, `CallerRunsPolicy`): the copying task, on the table's thread, waited for the next
  segment, which only that thread could authorize. The hang timeout did not apply - the wait was the
  migrator's. Now the task returns at once there, and the table copies itself; the same for the
  application's own event loop as the executor, where the task runs later on the table's thread.
- **`LongLongLL2Hashtable.newMigratorExecutor()` leaked a thread per call** - the `ll2-spawner` helper, with no
  way to stop it. It now returns a `MigratorExecutor`, `AutoCloseable`.
- **A `LongLongLL2Hashtable` migration stalled under lookups and removals.** Only puts and the operations on
  keys already in the migrated area authorized the next segment: lookups or removals of keys still in the old
  array never moved the migration on - both arrays and the migrator stayed alive for as long as they went on.
  Now every operation during a migration authorizes the next segment once the migrator has copied the current
  one (one volatile read). New `migrationInProgress()` tells whether a migration is in flight.
- **`LongLongLL2Hashtable` stalled for hundreds of milliseconds when the migrator shared the application's
  core** - the default executor in a pinned application, whose threads inherit its core. The application
  spun waiting for the migrator, which could run only once the scheduler preempted it; then the migrator spun
  waiting for the next segment. Both now yield the CPU every 1024 spins (a `sched_yield` that returns at once
  on a core of their own). In the new `ll2-scenarios` benchmark: the pinned default configuration went from
  270-350 ms to 144 ns at p50 (p99 25 ms: the allocation and the copying still take the application's CPU);
  four tables growing at once with a thread per task on two shared CPUs from 0.6-2.8 ms to 510 ns at p99.9;
  the spinning migrator on its own core unchanged.
- **Operations after a quiet period waited up to ~1 ms for a sleeping migrator.** After a few milliseconds
  without new segments the migrator backs off, parking up to 1 ms at a time, and an authorization did not wake
  it: the operations needing the new segment waited out the rest of the park. Now the authorizing operation
  unparks a parked migrator - one volatile read per segment otherwise, the wake-up syscall only after a quiet
  period. In the new `PerfLL2IdleBurst` (`tests-perf`: 200 lookups after 20 ms quiet in the middle of a
  migration) the slowest lookup of a burst went from ~920 us to 3.4 us (median of 900 bursts), p99.9 of all
  lookups from ~930 us to 4 us; the put latency benchmark is unchanged.
- **`LongAdaptiveRadixTreeMap.getOrInsert` returned null and inserted nothing** - it was a stub. Replaced by
  `computeIfAbsent` (see Changed).
- **`LongAdaptiveRadixTreeMap.put(key, null)` corrupted the map**: the null took a slot that the nodes read as
  empty - `size` counted the key, `get` could not tell it from a missing one, internal validation failed after
  its removal. Null values are now rejected with `NullPointerException`.
- **The ART leaked a node per insert/remove cycle at a branching point.** Removing the second-to-last child of
  an inner `ArtNode4` collapses it into its remaining child, and the collapsed node was dropped instead of
  returned to the pool: e.g. keys 0 and 256 - 96 bytes of garbage per cycle.
- **`LongAdaptiveRadixTreeMap.forEach` / `forEachDesc` allocated on every call** - a lambda that converted the
  internal keys back to the signed ones. The leaf nodes now hand out the signed keys themselves.
- **`ArtNode16` inserted every new branch twice** - a second put of the same key right after creating the
  node for it.
- **The entries of `LongAdaptiveRadixTreeMap.entriesList()` did not implement `equals` / `hashCode`** - not
  equal to `Map.entry` with the same key and value.

### Removed

- **`LongLongRadixHashtable` is no longer published.** It was an experiment with stubbed methods: `get`
  returned `0`, `size()` was always `0`, `keysStream()` returned `null`. Removed together with
  `extractMatching` (below), its benchmark too.
- **`HashtableAsyncResizer` and `HashtableResizer.resizeParallelSync()`** - unused. The parallel resize was
  not safe either: its segments could overlap, and it ran on the common ForkJoinPool.
- **`printLayout()` of both hashtables** - it computed nothing it returned or printed, and in
  `LongLongLL2Hashtable` it also finished an in-flight migration.
- **Hashtable internals are no longer public:** `HashingUtils` (hash function, empty-slot key),
  `HashtableAsync2Resizer`, `HashtableResizer`, `LongLongLL2Hashtable.debugLocate`, `upsizeThreshold()` of both
  tables. `LongLongLL2Hashtable.mask()` (deprecated) is gone - `capacity() - 1` is the same, and the tables
  grow at 65% of `capacity()`.
- **`extractMatching` of both hashtables** - only the Radix sharding experiment used it.
- **`remove(key, hash)` of both hashtables.** Besides exposing the internal hash, it would have overloaded
  the new `remove(key, value)`: `remove(key, 5)` with an int literal resolved to the hash variant.
- **`LongAdaptiveRadixTreeMap.getOrInsert(key, Supplier)` and `getOrInsertFromNode(node, Supplier)`** - stubs
  that did nothing; `computeIfAbsent(key, LongFunction)` replaces them.

### Changed

- **`LongLongHashtable` and `LongLongLL2Hashtable` are `final`.**
- **Hashtables: any key, and a configurable value for missing keys** (breaking). Key `0` can be stored - it
  still marks the empty slots, so its entry is kept in two fields of the table; `put(0, v)` no longer throws.
  `missingValue` (builder, `0` by default) is what `get`, `put`, `remove` and `putIfAbsent`
  return for a missing key, see `ILongLongHashtable.missingValue()`; `addAndGet` counts a missing key as `0`.
  Free slots hold the missing value, so `get` still returns the value of the slot its probe stops at without
  comparing the key: in `HashtableOpsJmh` the lookups stay within run-to-run noise (±5%) of the previous
  build, while comparing the key made `get` up to 21% slower on a 3M-entry table. A non-zero missing value
  costs one more pass over every new array (in `LongLongLL2Hashtable` on the migration executor).
- **Hashtable removal is linear in the cluster length.** Both hashtables delete by backward shift in one pass
  (Knuth's Algorithm R, shared code) instead of searching the cluster backwards from its end for every shifted
  entry. At load 0.65 (1.36M keys, 2^21 slots) the worst removal hashes 118 entries instead of 454
  (p99.99: 83 instead of 197).
- **`LongLongLL2Hashtable`: `put`, `get` and `remove` inline into the caller.** The path without a resize in
  flight is separated from the migration code: `put` shrank from 625 to 121 bytes of bytecode, below the JIT's
  inlining limit (FreqInlineSize = 325).
- **`LongLongLL2Hashtable`: shorter waits during a migration.** An operation on a key whose home is in the
  segment being copied waits until the migrator finishes the segment. Segments are now 256 slots instead
  of 1024 (each goes on to the end of the cluster at its boundary): in the put benchmark (1M puts/s, growing to 4M entries) p99.99 went from 4.6 us to 1.0 us,
  puts slower than 2 us halved, p99 +1.5%. Each segment costs a hand-over, so copying a large table takes 25%
  longer; the new `PerfLL2MigrationThroughput` benchmark measures that.
- **`LongLongLL2Hashtable`: configurable hang timeout, interrupts handled.** `Builder.hangTimeout(Duration)`
  (default 10s) bounds every wait for the migrator; past it an operation fails with `IllegalStateException`
  and the migration state, the table stays consistent. An interrupt no longer fails a `put` or `close()`
  waiting for the new array or for the migrator to end - the waits are short, they go on and the interrupt
  status is set again. An interrupted migrator spun on a whole core while waiting for the next segment
  (`parkNanos` returns at once with the interrupt status set); it now backs off as before and gets the
  status back when its task ends - the migration itself is completed either way, the table needs it.
- **`LongLongLL2Hashtable`: a failed migrator is reported right away** - with its exception as the cause,
  within milliseconds, instead of a bare "stuck waiting for migration" after the 10 s hang detection.
- **`LongLongLL2Hashtable` does not wait for a task the executor has not started.** Where an operation
  would wait for the migrator to copy a segment and the copying task has not started yet (a thread being
  created, a queue behind busy threads), the table copies that segment itself - one segment, microseconds -
  and the task, once it starts, continues from there. A put that reaches 90% load before the new array is
  allocated allocates it itself if the allocation has not started. The hang timeout applies only to a
  started migrator; it is now checked every 1024 spins (was every ~1M spins: a 1 ms timeout fired after
  ~100 ms).
- **`LongLongLL2Hashtable`: `finishMigration()`; `close()` makes the later resizes synchronous.** `close()`
  used to leave a table that started background work again at its next resize. `finishMigration()` does
  what `close()` did. The builder rejects a `null` executor and a negative `syncResizeBelow`.
- **`LongLongLL2Hashtable`: a `MigrationService` instead of an executor** (breaking). One worker thread,
  started (`MigrationService.builder().start()`) and closed by the application, does the background work of
  any number of tables: it allocates their new arrays and copies them, a segment at a time, in turns. The
  table needs one: `LongLongLL2Hashtable.builder().migrationService(service)` - the constructors,
  `Builder.executor(...)`, `newMigratorExecutor()` and `MigratorExecutor` are gone; the executor entries in
  this list are about that API.
  - Starting and ending a migration are writes into the table's slot of the service, set up once when the
    table is built: no task, no `CompletableFuture`, no `Cleaner` - their interpreted code cost 2-16 us at the
    start and the end of every migration. Put latency (`hashtable-put`, three interleaved runs against the
    executor version): samples over 5 us per 100 000 puts 1-5 -> 0-1, max 12-16 us -> 1-8 us.
  - `start()` waits for the worker at most 10 s: started from a static initializer of a class that `threadInit`
    needs, it fails instead of deadlocking.
  - Each segment is copied by whichever gets to it first, the worker or the table where an operation needs
    it; the table waits only for a segment the worker is copying at that moment. A put at 90% load allocates
    the new array itself if the worker has not started it.
  - `threadInit` (e.g. `CpuAffinity::acquireCore`) runs on the worker before `start()` returns;
    `WaitStrategy.SPIN_THEN_PARK` (default) or `BUSY_SPIN`.
  - The tables are closed first: `service.close()` throws while one is registered; a forgotten table is
    dropped once collected by the GC, and `close()` drops those left.
  - Many tables: the worker goes round the tables with a migration in flight only - a table joins with a CAS
    when it brings work - and takes a portion of ~256 slots of each in turn, also when a table authorizes the
    whole rest of its migration at once (at 90% load, `finishMigration()`). A second thread, the allocator
    (`allocatorThread(false)` to do without, `allocatorThreadInit` to pin it), allocates the arrays above 4 MB,
    so a large allocation does not hold up the copying for the other tables, nor their small arrays - the worker
    allocates those between portions. Registering and closing a table are O(1) (10 000 tables: 8 MB allocated, was
    712 MB), the GC's reference queue tells the worker which tables it collected. Measured: one table growing next to
    10 000 idle ones p99.9 541-543 -> 484-493 ns; a neighbour's 512 MB allocation max 1 ms -> 23-28 us.
  - A worker preempted in the middle of its work no longer holds a table up. It copies in batches of 32 slots,
    each started with a CAS; a table that needs its portion and sees no new batch for ~2 us takes the portion
    over - copies the worker's batch again (the same entries go to the same slots) and the rest. The worker,
    back on a core, finishes its batch and stops; until then a new value for one of those 32 entries, or a
    removal in their cluster, waits for it, and the migration ends after it (so do `close()` and
    `finishMigration()`). Measured with the worker sharing its core with a busy thread: p99.9 0.5-3.4 ms ->
    0.5 us, max 0.2-6.7 ms -> 22-27 us; four tables p99.9 5.7-9.9 ms -> 0.5 us, max 9.1-11.9 ms -> 74-85 us.
    The worker copies ~12% slower (a CAS per batch).
- **Published artifacts are now compiled for Java 21 instead of Java 26.** `collections-core`,
  `collections-orderbook` and `collections-ledger` no longer force consumers onto the latest JDK. 21 is
  an LTS; the floor would be 17 (agrona 2.x ships Java 17 class files, `collections-core` has no
  compile-scope dependencies and would go as low as 9), but 17 is not targeted.

  Development still happens on JDK 26: the lower level is applied by the `release-bytecode` profile,
  activated by `-DperformRelease=true` (which the release plugin already passes). Benchmark and
  stress modules keep targeting 26 — they use `SequencedCollection` and are not published. To run
  the release-level compile check on demand:

  ```bash
  mvn -DperformRelease=true clean verify
  ```
- **`LongAdaptiveRadixTreeMap.put` returns the previous value, `remove` the removed one** (were `void`; null
  for a missing key). Source compatible, but callers have to be recompiled.
- **`LongAdaptiveRadixTreeMap` keeps its size in a counter**: `size()` is O(1). `size(limit)` used to walk the
  whole tree up to the limit; it is removed (breaking) - `Math.min(size(), limit)`.
- **`LongAdaptiveRadixTreeMap`: `getHigherValue` / `getLowerValue` are now `higherValue` / `lowerValue`**
  (breaking) - the names of `NavigableMap` (`higherKey`, `ceilingEntry`), as `firstValue` and `ceilingValue`
  already were; the map's lookups have no `get` prefix except `get` itself and `getOrDefault`. A rename, nothing
  else: replace the calls.
- **The nodes of `LongAdaptiveRadixTreeMap` are no longer public** (breaking): `ArtNode4`, `ArtNode16`, `ArtNode48`,
  `ArtNode256` and their base class are package-private, as the hashtable internals - a change of the nodes no
  longer changes the API. `IArtNode`, the old interface of the nodes, is gone (see below). The public API of the ART
  is the map, its `Cursor` and `Entry`, `LongObjConsumer` and `ObjectsPool`.
- **`LongAdaptiveRadixTreeMap.removeRange(fromInclusive, toExclusive)`** works and returns the number of keys removed
  (was `void` and threw `UnsupportedOperationException`): one removal per key, a cursor kept by the map moves from
  key to key - nothing allocated per call.
- **`LongAdaptiveRadixTreeMap.clear()` returns the nodes to the pool** - O(number of nodes) instead of dropping
  the root, so a map that is cleared and refilled no longer allocates its nodes again.
- **`LongAdaptiveRadixTreeMap`: ordered walks and neighbour searches skip the empty slots.** `ArtNode48` and
  `ArtNode256` keep a 256-bit occupancy bitmap (a `long[4]`): `forEach` / `forEachDesc`, the ceiling, floor,
  higher and lower lookups, `firstKey` / `lastKey` and the shrinking of a node jump to the next occupied slot instead
  of reading up to 255 empty ones. In `ArtOpsJmh`: the top 10 levels of a book 70-87% faster (book-20k: 283-305 ns
  to 43 ns), the mixed order-book stream 19-31%, the neighbour of a level up to 20%; lookups unchanged, a removal +
  put of a level within +5% (it updates one word of the bitmap - four long fields instead of the array cost
  +15-28% there).
- **`LongAdaptiveRadixTreeMap`: a key alone below a branch is a leaf** (`ArtLeaf`, the lazy expansion of the ART
  paper): the key and the value in one object of 24 bytes and one load, where an `ArtNode4` with a single key
  took three objects (96 bytes) and three loads. A second key in the same 256-key block makes an `ArtNode4` again,
  and the last key of an `ArtNode4` at the lowest level goes back into a leaf. Random 64-bit keys: 128 to 64 bytes
  per entry (as much as `TreeMap` with its `Long` keys), 9.4 to 7.4 dependent loads per lookup; order ids with
  0.5% still live: 58 to 40 bytes. In `ArtOpsJmh` on rhel-lat, 1M random keys: lookup -23%, neighbour -38%, new
  level -34%, removal + put -23%, the mixed stream -29%; 100k random keys -12% to -21%, except a missing key +12%
  (one more class at the call sites below the 64K prefixes - gone with the `ArtNode4` below); order books and
  order ids unchanged (within ±3%).
- **`ArtNode4` without arrays**: the four key bytes in one int, the children in four fields - one object of 56
  bytes (48 with compact object headers) instead of three (96 bytes), two dependent loads less on every visit. An
  exact key is found comparing the four bytes at once (a zero-byte test of `keys ^ (byte * 0x01010101)`), the
  insertion and removal shift bytes and fields instead of arrays. Order ids with 0.5% still live: 40 to 29 bytes per
  entry; 100k random keys: 64 to 53 bytes, 7.4 to 5.9 dependent loads per lookup; a 20k-level book: 10.2 to 8.1
  loads. In `ArtOpsJmh` on rhel-lat, 100k random keys: missing key -21%, the mixed stream -12%, removal + put -8%,
  new level -8%; the other key sets within ±4%.
- **`IArtNode` is now the abstract class `ArtNode`**, package-private (the node type inside
  `LongAdaptiveRadixTreeMap`): a call on a node of a class the JIT can not predict goes through the vtable instead of
  a search of the itable. In `ArtOpsJmh` on rhel-lat within ±4%, removal + put in a 20k-level book -9%. A lookup in a
  loop over the levels instead of the recursion was measured too and left out: misses on 1M random keys -12%, hits in
  a 20k-level book +14-19%.
- **`ObjectsPool`: a type left out of the configuration gets an empty stack** - `get` constructs, `put` drops - where
  it used to fail with `NullPointerException` (or `ArrayIndexOutOfBoundsException` above the highest configured
  type). The new `ART_LEAF` type, unless configured, gets as many places as `ART_NODE_4`, so configurations written
  before it (exchange-core's among them) keep recycling the leaves.

### Testing

- **API check against the last release**: `mvn -Papi-check -DskipTests verify` compares the published modules with
  0.6.0 (japicmp) and fails on a binary or source incompatible change not listed in the profile's excludes - the
  breaks of this release are listed there.
- **Strict javadoc**: doclint checks everything but missing comments, and a warning fails the build (it found an
  empty `<p>` in `IOrderBook`).

## 0.6.0

First release since 0.5.1. The project was split into modules, gained a family of primitive
`long` → `long` hashtables, and had a correctness bug fixed in the Adaptive Radix Tree. There are
breaking changes — see below before upgrading.

### Breaking changes

- **Artifact coordinates changed.** The single `exchange.core2:collections` artifact is gone. The
  collections now live in `exchange.core2:collections-core`, and the order book in
  `exchange.core2:collections-orderbook`.

  ```xml
  <!-- 0.5.1 -->
  <dependency>
      <groupId>exchange.core2</groupId>
      <artifactId>collections</artifactId>
      <version>0.5.1</version>
  </dependency>

  <!-- 0.6.0 -->
  <dependency>
      <groupId>exchange.core2</groupId>
      <artifactId>collections-core</artifactId>
      <version>0.6.0</version>
  </dependency>
  ```

- **Minimum Java version raised from 8 to 26.** Both artifacts are compiled with
  `--release 26` and will not load on an older JVM.

- **`LongAdaptiveRadixTreeMap` key order changed from unsigned to signed.** Negative keys used to
  sort *after* all positive ones; the map now orders keys exactly like `TreeMap<Long, V>`. This
  affects `forEach`, `forEachDesc`, `entriesList`, `getHigherValue` and `getLowerValue`. Code that
  only ever used non-negative keys is unaffected.

### Fixed

- **`LongAdaptiveRadixTreeMap.get()` could return `null` for a key that was present.**
  `ArtNode4.initTwoKeys` ordered its two children by comparing the *full* keys with a signed
  comparison, while the node array is ordered by branch index. When the split fell on the sign bit
  the children ended up swapped, leaving the node unsorted — and the search gives up as soon as it
  passes the index it is looking for. 108 of the 120 insertion orders of `{-5,-1,0,1,5}` produced a
  tree that `validateInternalState()` also rejected.
- **`getCeilingValue` / `getFloorValue` compared full keys with signed `<` and `>`** in all four
  node classes; they now use `Long.compareUnsigned`. Related: "take the highest key" was spelled
  `Long.MAX_VALUE` (the maximum *signed* value), which capped the descent at branch index `0x7F`.
- **`getLowerValue` guarded against `key != 0`** — the bottom of the unsigned range. It is now
  `Long.MIN_VALUE`, matching the signed order.

### Added

- **`LongLongHashtable`** — a primitive `long` → `long` open-addressing hashtable with linear
  probing, keys and values interleaved in a single flat `long[]`. No boxing, no `Entry` objects.
- **`LongLongLL2Hashtable`** — the same contract, but the resize runs asynchronously on a background
  thread: a migrator copies the old array into the new one segment by segment while the application
  thread keeps serving `put`/`get`/`remove`, removing the stop-the-world resize pause. Implements
  `AutoCloseable`; migration threads are daemons, and a `Cleaner` stops the migrator if the table is
  abandoned without `close()`. Migration hangs fail fast with the full state instead of hanging, and
  `-Dexchange.hashtable.verify=true` runs an integrity check after every migration.
- **`LongLongRadixHashtable`** — *experimental*. Shards keys across several `LongLongHashtable`
  instances so each shard resizes independently. Only `put` and `size` are implemented so far;
  the remaining methods are stubs. Published so benchmarks can track the approach — not usable yet.
- **`collections-orderbook` module** — `IOrderBook` plus a naive reference implementation built on
  `LongAdaptiveRadixTreeMap`. Commands are passed as flat binary records in an Agrona `DirectBuffer`,
  so the hot path stays allocation-free.
- **Documentation** — the README now covers every collection with runnable examples and the
  non-obvious constraints (reserved key `0`, value `0` meaning "absent", the bounded-executor
  deadlock on `LongLongLL2Hashtable`, unimplemented methods).

### Changed

- **Multi-module layout**: `collections-core`, `collections-orderbook`, `tests-common`,
  `tests-perf`, `tests-stress`. Benchmarks and stress tests are no longer part of the default test
  run, and the test-only dependencies they drag in are no longer visible to consumers.
- **`collections-core` is dependency-free at compile scope.** The Agrona helpers it used
  (`Hashing.hash`, `BitUtil.findNextPositivePowerOfTwo`, `LongLongConsumer`) are inlined, and there
  is no logging framework on the API path.
- **Publishing migrated to the Central Portal** (`central-publishing-maven-plugin`), replacing the
  legacy OSSRH staging flow.
- Dependency cleanup: Chronicle Map removed, and the unused dependencies flagged by Dependabot
  dropped. Remaining dependencies updated.

### Testing

- **ART test oracle** (contributed in [#1](https://github.com/exchange-core/collections/pull/1)) —
  property-based tests comparing the map against `TreeMap` across four key distributions, checking
  the whole read surface: `entriesList`, `forEach`, `forEachDesc`, `size`, per-key `get`,
  higher/lower lookups around every key and at the range boundaries, plus `validateInternalState`.
  This is what found the signed-key bugs above.
- **`ArtSignedOrderRegressionTest`** — deterministic cases, including all 120 insertion permutations
  the original bug hid in.
- **Hashtable tests** — a shared abstract suite run against every `ILongLongHashtable`
  implementation, plus an avalanche test for the hash function.
- **`tests-stress`** — long-running randomized stress test for `LongLongLL2Hashtable`, exercising
  concurrent migration.
- **`tests-perf`** — reworked latency and throughput benchmarks (fixed warmup, load factor, TPS
  accounting and accidental boxing), hiccup calibration, thread affinity, and a latency
  side-channel test.
