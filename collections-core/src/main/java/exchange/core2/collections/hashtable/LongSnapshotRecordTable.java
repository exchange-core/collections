package exchange.core2.collections.hashtable;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * Single-writer table of fixed-width records under long keys, with immutable point-in-time snapshots.
 *
 * <p>Layout: the records live in shards, small {@link LongTupleHashtable}s of {@code shardCapacity} slots, reached
 * through a directory by the low bits of the key's hash (extendible hashing). A shard that fills up splits in two by
 * the next bit; the directory doubles when the shard is as deep as the directory. So a shard does not grow past its
 * capacity however many keys come, and neither does the copy a write makes. A run of keys whose hashes the splits
 * cannot separate stops the directory at a bound proportional to the size; their shard grows instead.
 *
 * <p>Snapshots: {@link #snapshot} copies the directory, one reference per entry. The first write to a shard after a
 * snapshot copies the shard ({@code shardCapacity × (2 + fields)} longs); further writes to it before the next
 * snapshot do not. The copies come from a pool: a replaced shard returns to it once every snapshot that can see it
 * is closed, so with the snapshots closed in time writes do not allocate. The pool keeps what it took, like the
 * capacity of a table. A snapshot dropped without close counts as closed once it is garbage collected; until then,
 * as an open one, it holds back every shard replaced after it was taken.
 *
 * <p>Threads: one writer owns the table, the snapshots included. A snapshot, safely published, can be read by any
 * number of threads at once. Close it after its last read, in the reading thread or after a happens-before edge:
 * its shards may be reused then. Each read checks first that the snapshot is open.
 */
public final class LongSnapshotRecordTable {

    public static final int DEFAULT_SHARD_CAPACITY = 256;

    private static final float SHARD_LOAD = 0.65f;
    /** the directory index takes the low bits of a 32-bit hash */
    private static final int MAX_DEPTH = 30;
    /**
     * The directory may double while it has at most this many entries per split-size of keys. Random keys need
     * about 4; the bound only stops keys that share their hash bits.
     */
    private static final int DIRECTORY_SLACK = 32;
    private static final int POOLED_DIRECTORIES = 4;

    /** A version of a shard: its keys share the low {@code depth} bits of their hash. */
    static final class Shard {
        final LongTupleHashtable table;
        int depth;
        /** the writer's epoch when it was made: a snapshot taken since then can see it */
        long epoch;
        /** while retired: the id of the last snapshot that can see it */
        long lastSeenBy;

        Shard(LongTupleHashtable table) {
            this.table = table;
        }
    }

    /** Shared by a snapshot and the writer's reference to it. */
    private static final class Lifetime {
        volatile boolean closed;
    }

    /** The writer's handle on an open snapshot: weak, so a snapshot dropped without close is noticed. */
    private static final class Tracked extends WeakReference<Snapshot> {
        final long id;
        final Shard[] directory;
        final Lifetime lifetime;

        Tracked(Snapshot snapshot, long id, Shard[] directory, Lifetime lifetime) {
            super(snapshot);
            this.id = id;
            this.directory = directory;
            this.lifetime = lifetime;
        }

        boolean ended() {
            return lifetime.closed || refersTo(null);
        }
    }

    private final int fields;
    private final int shardCapacity;
    /** keys a shard takes before it splits: one more would make its table resize */
    private final int splitKeys;
    private final LongTupleHashtable.Builder shardBuilder;

    private Shard[] directory;
    private int depth;
    private long size;
    /** the id the next snapshot gets */
    private long epoch;

    private final ArrayDeque<Shard> free = new ArrayDeque<>();
    private final ArrayDeque<Shard> retired = new ArrayDeque<>();
    private final ArrayDeque<Tracked> open = new ArrayDeque<>();
    private final ArrayDeque<Shard[]> freeDirectories = new ArrayDeque<>();
    private long reused;

    private LongSnapshotRecordTable(Builder builder) {
        this.fields = builder.fields;
        this.shardCapacity = builder.shardCapacity;
        this.splitKeys = (int) (shardCapacity * SHARD_LOAD) - 1;
        this.shardBuilder = LongTupleHashtable.builder().keyLongs(1).valueLongs(fields).expectedSize(splitKeys);
        // about one full shard per expected splitKeys keys, so most start between half and full
        final int shards = HashingUtils.nextPositivePowerOfTwo(Math.max(1, Math.ceilDiv(builder.expectedSize, splitKeys)));
        this.depth = Integer.numberOfTrailingZeros(shards);
        this.directory = new Shard[shards];
        for (int i = 0; i < shards; i++) {
            final Shard shard = new Shard(newTable());
            shard.depth = depth;
            directory[i] = shard;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int fields = 1;
        private int expectedSize;
        private int shardCapacity = DEFAULT_SHARD_CAPACITY;

        private Builder() {
        }

        /** Longs per record, at least 1. */
        public Builder fields(int fields) {
            if (fields < 1) {
                throw new IllegalArgumentException("fields must be positive: " + fields);
            }
            this.fields = fields;
            return this;
        }

        /** Keys to make room for at once; more can come. */
        public Builder expectedSize(int expectedSize) {
            if (expectedSize < 0) {
                throw new IllegalArgumentException("negative expected size: " + expectedSize);
            }
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * Slots per shard, a power of two from 16 to 65536, default {@value #DEFAULT_SHARD_CAPACITY}: what the first
         * write to a shard after a snapshot copies. Smaller shards copy less and make a larger directory, which every
         * snapshot copies.
         */
        public Builder shardCapacity(int shardCapacity) {
            if (shardCapacity < 16 || shardCapacity > 65536 || Integer.bitCount(shardCapacity) != 1) {
                throw new IllegalArgumentException("shard capacity must be a power of two from 16 to 65536: "
                        + shardCapacity);
            }
            this.shardCapacity = shardCapacity;
            return this;
        }

        public LongSnapshotRecordTable build() {
            return new LongSnapshotRecordTable(this);
        }
    }

    private LongTupleHashtable newTable() {
        final LongTupleHashtable table = shardBuilder.build();
        if (table.capacity() != shardCapacity) {
            throw new AssertionError("shard of " + table.capacity() + " slots, expected " + shardCapacity);
        }
        return table;
    }

    private static int hash(long key) {
        return HashingUtils.hash(key ^ HashingUtils.SEED);
    }

    public int fields() {
        return fields;
    }

    /** Sets a field, creating a zero-initialized record if the key is new. */
    public void set(long key, int field, long value) {
        Objects.checkIndex(field, fields);
        final LongTupleHashtable table = tableForWrite(key);
        table.setValue(getOrInsert(table, key), field, value);
    }

    /** Inserts or replaces an entire record. The caller may immediately reuse the values array. */
    public void put(long key, long[] values) {
        if (values.length != fields) {
            throw new IllegalArgumentException("expected " + fields + " fields");
        }
        final LongTupleHashtable table = tableForWrite(key);
        final int slot = getOrInsert(table, key);
        for (int i = 0; i < fields; i++) {
            table.setValue(slot, i, values[i]);
        }
    }

    public boolean remove(long key) {
        final int index = hash(key) & (directory.length - 1);
        final Shard shard = directory[index];
        if (shard.epoch != epoch && shard.table.find(key) == LongTupleHashtable.NO_SLOT) {
            return false; // nothing to copy the shard for
        }
        if (!writable(index).table.remove(key)) {
            return false;
        }
        size--;
        return true;
    }

    public boolean containsKey(long key) {
        return directory[hash(key) & (directory.length - 1)].table.find(key) != LongTupleHashtable.NO_SLOT;
    }

    /** Reads a field, throwing NoSuchElementException for an absent key. */
    public long get(long key, int field) {
        return read(directory, key, Objects.checkIndex(field, fields));
    }

    public long size() {
        return size;
    }

    /**
     * Captures the current records with the caller's sequence label, normally after committing that event. Takes
     * O(directory entries): the shards the closed snapshots release are reused one by one as writes need copies.
     */
    public Snapshot snapshot(long sequence) {
        forgetEnded();
        Shard[] copy = freeDirectories.pollFirst();
        if (copy == null) {
            copy = new Shard[directory.length];
        }
        System.arraycopy(directory, 0, copy, 0, directory.length);
        final Lifetime lifetime = new Lifetime();
        final Snapshot snapshot = new Snapshot(copy, fields, size, sequence, lifetime);
        open.addLast(new Tracked(snapshot, epoch, copy, lifetime));
        epoch++;
        return snapshot;
    }

    private static long read(Shard[] directory, long key, int field) {
        final LongTupleHashtable table = directory[hash(key) & (directory.length - 1)].table;
        final int slot = table.find(key);
        if (slot == LongTupleHashtable.NO_SLOT) {
            throw new NoSuchElementException("missing key: " + key);
        }
        return table.getValue(slot, field);
    }

    private int getOrInsert(LongTupleHashtable table, long key) {
        final int before = table.size();
        final int slot = table.getOrInsert(key);
        size += table.size() - before;
        return slot;
    }

    /** The writable table of the key's shard, split first if a new key would make it grow. */
    private LongTupleHashtable tableForWrite(long key) {
        final int hash = hash(key);
        while (true) {
            final int index = hash & (directory.length - 1);
            final Shard shard = directory[index];
            if (shard.table.growsOnNextInsert() && shard.table.find(key) == LongTupleHashtable.NO_SLOT
                    && split(index)) {
                continue;
            }
            return writable(index).table;
        }
    }

    /** The shard at the index, copied first if a snapshot can see it. */
    private Shard writable(int index) {
        final Shard shard = directory[index];
        if (shard.epoch == epoch) {
            return shard;
        }
        final Shard copy = takeShard();
        copy.table.copyFrom(shard.table);
        copy.depth = shard.depth;
        copy.epoch = epoch;
        point(index, shard.depth, copy);
        retire(shard);
        return copy;
    }

    /**
     * Splits the shard at the index by bit {@code depth} of the hash.
     *
     * @return false if that needs a larger directory and the directory may not double
     */
    private boolean split(int index) {
        final Shard old = directory[index];
        final int d = old.depth;
        if (d == depth) {
            if (depth == MAX_DEPTH
                    || ((long) directory.length << 1) * splitKeys > (long) DIRECTORY_SLACK * Math.max(size, splitKeys)) {
                return false;
            }
            doubleDirectory();
        }
        final Shard low = takeEmptyShard(d + 1);
        final Shard high = takeEmptyShard(d + 1);
        final LongTupleHashtable from = old.table;
        final int bit = 1 << d;
        from.forEach(pos -> {
            final long key = from.getKey(pos, 0);
            final LongTupleHashtable to = (hash(key) & bit) == 0 ? low.table : high.table;
            final int slot = to.getOrInsert(key);
            for (int f = 0; f < fields; f++) {
                to.setValue(slot, f, from.getValue(pos, f));
            }
        });
        final int base = index & (bit - 1);
        point(base, d + 1, low);
        point(base | bit, d + 1, high);
        retire(old);
        return true;
    }

    private void doubleDirectory() {
        final int length = directory.length;
        final Shard[] doubled = Arrays.copyOf(directory, length << 1);
        System.arraycopy(directory, 0, doubled, length, length);
        directory = doubled;
        depth++;
        freeDirectories.clear();
    }

    /** Points every directory entry of a shard of the given depth - those equal to index in its low bits - at it. */
    private void point(int index, int shardDepth, Shard shard) {
        final int step = 1 << shardDepth;
        for (int i = index & (step - 1); i < directory.length; i += step) {
            directory[i] = shard;
        }
    }

    private Shard takeEmptyShard(int shardDepth) {
        final Shard shard = takeShard();
        shard.table.clear();
        shard.depth = shardDepth;
        shard.epoch = epoch;
        return shard;
    }

    /**
     * A shard to overwrite: an unseen one from the pool, else the oldest retired one no open snapshot can see - one
     * at a time, so neither a snapshot nor a write pays for releasing a whole interval - else a new one.
     */
    private Shard takeShard() {
        final Shard pooled = free.pollFirst();
        if (pooled != null) {
            reused++;
            return pooled;
        }
        if (!retired.isEmpty()) {
            forgetEnded();
            final long oldestOpen = oldestOpen();
            while (!retired.isEmpty() && retired.peekFirst().lastSeenBy < oldestOpen) {
                final Shard shard = retired.pollFirst();
                if (shard.table.capacity() == shardCapacity) {
                    reused++;
                    return shard;
                }
            }
        }
        return new Shard(newTable());
    }

    /** A shard out of the directory: back to the pool now if no snapshot saw it, else reused once those are closed. */
    private void retire(Shard shard) {
        if (shard.epoch == epoch) {
            recycle(shard);
        } else {
            shard.lastSeenBy = epoch - 1;
            retired.addLast(shard);
        }
    }

    private void recycle(Shard shard) {
        if (shard.table.capacity() == shardCapacity) {
            free.addLast(shard);
        }
    }

    /** Forgets the ended snapshots from the oldest on, keeping a few of their directories for the next ones. */
    private void forgetEnded() {
        while (!open.isEmpty() && open.peekFirst().ended()) {
            final Shard[] ended = open.pollFirst().directory;
            if (ended.length == directory.length && freeDirectories.size() < POOLED_DIRECTORIES) {
                freeDirectories.addLast(ended);
            }
        }
    }

    /** The id of the oldest snapshot not known to have ended, after forgetEnded. */
    private long oldestOpen() {
        return open.isEmpty() ? Long.MAX_VALUE : open.peekFirst().id;
    }

    /* for the tests */

    int directoryLength() {
        return directory.length;
    }

    int largestShardCapacity() {
        int largest = 0;
        for (Shard shard : directory) {
            largest = Math.max(largest, shard.table.capacity());
        }
        return largest;
    }

    /** Shard copies and splits served by an old shard rather than a new one. */
    long reusedShards() {
        return reused;
    }

    /** Retired shards an open snapshot can still see. */
    int pinnedShards() {
        forgetEnded();
        final long oldestOpen = oldestOpen();
        int pinned = 0;
        for (Shard shard : retired) {
            if (shard.lastSeenBy >= oldestOpen) {
                pinned++;
            }
        }
        return pinned;
    }

    @FunctionalInterface
    public interface RecordConsumer {
        void accept(long key, long[] fields);
    }

    public static final class Snapshot implements AutoCloseable {
        private final Shard[] directory;
        private final int fields;
        private final long size;
        private final long sequence;
        private final Lifetime lifetime;

        private Snapshot(Shard[] directory, int fields, long size, long sequence, Lifetime lifetime) {
            this.directory = directory;
            this.fields = fields;
            this.size = size;
            this.sequence = sequence;
            this.lifetime = lifetime;
        }

        public long sequence() {
            return sequence;
        }

        public long size() {
            return size;
        }

        public boolean containsKey(long key) {
            checkOpen();
            try {
                return directory[hash(key) & (directory.length - 1)].table.find(key) != LongTupleHashtable.NO_SLOT;
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        /** Reads a field, throwing NoSuchElementException for an absent key. */
        public long get(long key, int field) {
            checkOpen();
            try {
                return read(directory, key, Objects.checkIndex(field, fields));
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        /**
         * Visits all records in hash order, which depends on the hash seed of the JVM
         * ({@code -Dexchange.hashtable.seed}). Supply an array of exactly the record width per reader.
         * It is overwritten for each row and can be modified in the callback without affecting the snapshot.
         */
        public void forEach(long[] scratch, RecordConsumer consumer) {
            Objects.requireNonNull(consumer);
            if (scratch.length != fields) {
                throw new IllegalArgumentException("expected " + fields + " fields");
            }
            checkOpen();
            try {
                final var visitor = new Visitor(scratch, consumer);
                for (int i = 0; i < directory.length; i++) {
                    final Shard shard = directory[i];
                    if (i < 1 << shard.depth) { // a shard's first entry: the others repeat it
                        visitor.table = shard.table;
                        shard.table.forEach(visitor);
                    }
                }
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        /** Lets the writer reuse what only this snapshot still sees. Idempotent. */
        @Override
        public void close() {
            lifetime.closed = true;
        }

        public boolean isClosed() {
            return lifetime.closed;
        }

        private void checkOpen() {
            if (lifetime.closed) {
                throw new IllegalStateException("snapshot " + sequence + " is closed");
            }
        }

        private static final class Visitor implements IntConsumer {
            private final long[] scratch;
            private final RecordConsumer consumer;
            LongTupleHashtable table;

            Visitor(long[] scratch, RecordConsumer consumer) {
                this.scratch = scratch;
                this.consumer = consumer;
            }

            @Override
            public void accept(int pos) {
                for (int f = 0; f < scratch.length; f++) {
                    scratch[f] = table.getValue(pos, f);
                }
                consumer.accept(table.getKey(pos, 0), scratch);
            }
        }
    }
}
