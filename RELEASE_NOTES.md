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
  them, but all four threw `UnsupportedOperationException`. `forEach` does not allocate. They stay
  unsupported in `LongLongLL2Hashtable`.

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

### Removed

- **`LongLongRadixHashtable` is no longer published.** It was an experiment with stubbed methods: `get`
  returned `0`, `size()` was always `0`, `keysStream()` returned `null`. It moved next to its benchmark in
  `tests-perf`.

### Changed

- **Published artifacts are now compiled for Java 17 instead of Java 26.** `collections-core` and
  `collections-orderbook` no longer force consumers onto the latest JDK. 17 is the floor imposed by
  agrona 2.x (Java 17 class files) in `collections-orderbook`; `collections-core` has no
  compile-scope dependencies and would go as low as 9.

  Development still happens on JDK 26: the lower level is applied by the `release-bytecode` profile,
  activated by `-DperformRelease=true` (which the release plugin already passes). Benchmark and
  stress modules keep targeting 26 — they use `SequencedCollection` and are not published. To run
  the release-level compile check on demand:

  ```bash
  mvn -DperformRelease=true clean verify
  ```

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
