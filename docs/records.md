# Records

[exchange collections](../README.md) › Records

Several `long` fields that belong together — a balance: available and reserved — in one record instead of a
hashtable each.

- [`RecordStore`](#recordstore) — records by handle, no keys
- [`LongRecordTable`](#longrecordtable) — `long` keys to records, handles that stay valid
- [`LongTupleHashtable`](#longtuplehashtable) — keys of several longs, the values in the slot of the key
- [`LongSnapshotRecordTable`](#longsnapshotrecordtable) — records with point-in-time snapshots for other threads

Balances with checked postings on top of `LongTupleHashtable`: [balances](ledger.md).

---

## RecordStore

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

## LongRecordTable

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
| `remove`, `removeAndCopyFields` | one probe, backward shift, record freed. The latter copies a prefix of fields into a caller-supplied buffer before freeing |
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

## LongTupleHashtable

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

## LongSnapshotRecordTable

Records of fixed width under `long` keys, with point-in-time snapshots that other threads read while the writer
goes on: balances captured at sequence S and serialized in the background exactly as of S.

```java
import exchange.core2.collections.hashtable.LongSnapshotRecordTable;

final var balances = LongSnapshotRecordTable.builder().fields(2).expectedSize(1_000_000).build();
balances.set(account, AVAILABLE, 500);

final var snapshot = balances.snapshot(sequence);   // on the writer thread
executor.submit(() -> {
    try (snapshot) {
        snapshot.forEach(new long[2], (key, row) -> out.write(key, row[AVAILABLE], row[RESERVED]));
    }
});
balances.set(account, AVAILABLE, 400);              // the snapshot still reads 500
```

The records live in shards: small `LongTupleHashtable`s of 256 slots (`shardCapacity`), reached through a
directory by the low bits of the key's hash (extendible hashing). A full shard splits in two by the next bit, and
the directory doubles when it has to, so no shard grows however many keys arrive. `snapshot` copies the directory,
one reference per entry. The first write to a shard after a snapshot copies that shard (256 × (2 + fields) longs,
12 KiB with four fields); later writes to it do not. A replaced shard returns to a pool once every snapshot that
can see it is closed, so writes between snapshots closed in time do not allocate. A snapshot dropped without
`close` counts as closed once it is garbage collected; until then it holds back every shard replaced after it.
Reading a closed snapshot throws `IllegalStateException`.

`shardCapacity` trades the copy against the directory: 64-slot shards copy a quarter as much on a first write, and
make the directory that every snapshot copies four times larger. Keys whose hashes the splits cannot separate stop
the directory at a bound proportional to the size; their shard grows instead. Iteration goes in hash order, which
depends on the JVM's hash seed. One writer; any number of reader threads per snapshot, published safely.

`LongSnapshotRecordTableJmh` measures writes with a snapshot every 0, 1,000 or 100,000 writes, the previous one
closed as the next is taken: the mean, the percentiles (`writeSampled`) and, with `-prof gc`, the bytes allocated
per write. It ran with the setup of the [queue benchmarks](queues.md#longindexedfifoqueues) (Xeon Gold 6244, the thread on an isolated core,
`-t 1 -f 2 -wi 3 -w 500ms -i 5 -r 500ms`), with a 4 GiB heap, four fields per record and random keys. Means in ns
per write with JMH's 99.9% confidence intervals; the percentiles come from the sampled run, whose timer reads add
25-70 ns to each write (compare p50 with the mean):

| Keys | Snapshot every | Shard slots | Mean | p50 | p99 | p99.9 | Max |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | never | 256 | 16.7 ± 0.1 | 43 | 88 | 519 | 1,616 |
| 10,000 | 1,000 writes | 256 | 93.7 ± 0.0 | 51 | 946 | 1,024 | 10,704 |
| 1,000,000 | never | 64 | 87.4 ± 4.5 | 155 | 477 | 663 | 3,932 |
| 1,000,000 | never | 256 | 74.8 ± 0.7 | 140 | 447 | 631 | 1,148 |
| 1,000,000 | 1,000 writes | 64 | 583.6 ± 0.9 | 580 | 1,028 | 16,704 | 55,296 |
| 1,000,000 | 1,000 writes | 256 | 1,853.9 ± 1.9 | 1,980 | 2,688 | 4,340 | 12,720 |
| 1,000,000 | 100,000 writes | 64 | 345.2 ± 5.6 | 299 | 1,224 | 1,512 | 39,296 |
| 1,000,000 | 100,000 writes | 256 | 277.0 ± 1.8 | 168 | 2,616 | 3,138 | 21,696 |

A write allocated about 0.1 B: the objects of a snapshot, spread over the writes between two. With a snapshot every
1,000 writes to a million keys, most writes are the first to their shard since the snapshot and copy it - 12 KiB
with 256 slots, 3 KiB with 64 - which sets the mean and p99. One write in 1,000 takes the snapshot and copies the
directory, 65,536 entries with 64-slot shards and 8,192 with 256: 16.7 µs at p99.9 against 4.3 µs. The shards
that closed snapshots release are reused one at a time, as copies need them, so neither the snapshot nor a write
pays for a whole interval at once.
