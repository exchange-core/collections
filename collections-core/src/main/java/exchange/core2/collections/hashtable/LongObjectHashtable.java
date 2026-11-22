package exchange.core2.collections.hashtable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;
import static exchange.core2.collections.hashtable.HashingUtils.SEED;

/**
 * Primitive {@code long} keys to object values: open addressing with linear probing over two arrays of the same
 * length - the keys in a {@code long[]}, the value of each in an {@code Object[]} at the same index - and
 * backward-shift deletion (no tombstones).
 * <ul>
 *     <li>Any key, {@code 0} included. No {@code null} values: a put of {@code null} throws
 *     {@link NullPointerException}, and {@code null} is what get and remove return for a missing key.</li>
 *     <li>A probe walks the keys only, 8 to a cache line. The value of the slot it stops at is read without comparing
 *     the key again - a free slot holds {@code null} - and its address is known as soon as the key is hashed: the
 *     line of the value is loaded together with the line of the key.</li>
 *     <li>12 bytes per slot (references of 4 bytes, a heap below 32 GB).</li>
 *     <li>Hash seed, one for the JVM: {@code -Dexchange.hashtable.seed=random} (or a number; default 0), see
 *     {@link LongLongHashtable}.</li>
 *     <li>Grows by doubling at its load factor ({@link Builder#loadFactor}) - a small table at a lower one
 *     ({@link Builder#smallTableLoadFactor}, below {@link Builder#smallTableCapacity}): sparse while that costs
 *     little memory. A resize copies the whole table on the calling thread - pre-size it
 *     ({@link Builder#expectedSize}, {@link #ensureCapacity}). {@link Builder#maxCapacity},
 *     {@link Builder#fixedCapacity} and {@link Builder#maxLoad} as in {@link LongLongHashtable}: at the maximum
 *     capacity a new key is refused with {@link IllegalStateException}, the table unchanged.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 * The keys array holds every key XOR-ed with the seed; stored key 0 marks the free slots, so the value of the key
 * equal to the seed (key 0 with the default seed) is kept in a field.
 *
 * @param <V> the type of the values
 */
public final class LongObjectHashtable<V> {

    public static final int DEFAULT_ARRAY_SIZE = 16;

    /** the load a table of {@link #DEFAULT_SMALL_TABLE_CAPACITY} slots and more grows at */
    public static final double DEFAULT_LOAD_FACTOR = 0.65;
    /** the load a smaller table grows at */
    public static final double DEFAULT_SMALL_TABLE_LOAD_FACTOR = 0.5;
    /** slots from which a table grows at the load factor, not the small table one */
    public static final int DEFAULT_SMALL_TABLE_CAPACITY = 1 << 16;

    private final LoadFactors loadFactors;
    private final int maxCapacity;
    private final double maxLoad;

    private long[] keys;
    /** the value of keys[i] at values[i]; null in a free slot */
    private Object[] values;
    private int mask;
    private long size;
    private long upsizeThreshold;

    /** the value of the key stored as 0 - the gap marker - the key equal to the seed; null if absent */
    private Object zeroKeyValue;

    public LongObjectHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    /**
     * @param expectedSize expected number of entries - that many fit without a resize
     */
    public LongObjectHashtable(int expectedSize) {
        this(builder().expectedSize(expectedSize));
    }

    private LongObjectHashtable(Builder builder) {
        this.loadFactors = new LoadFactors(builder.smallTableLoadFactor, DEFAULT_SMALL_TABLE_LOAD_FACTOR,
                builder.smallTableCapacity, builder.loadFactor);
        final int capacity = loadFactors.capacityFor(builder.expectedSize, builder.maxCapacity);
        this.maxCapacity = builder.fixedCapacity ? capacity : builder.maxCapacity;
        this.maxLoad = loadFactors.checkMaxLoad(builder.maxLoad, maxCapacity);
        switchTo(new long[capacity], new Object[capacity]);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link LongObjectHashtable}, all optional.
     */
    public static final class Builder {

        private int expectedSize = DEFAULT_ARRAY_SIZE;
        private int maxCapacity = HashingUtils.MAX_CAPACITY;
        private boolean fixedCapacity;
        private double maxLoad = HashingUtils.DEFAULT_MAX_LOAD;
        private double loadFactor = DEFAULT_LOAD_FACTOR;
        /** NaN: the default, at most the load factor */
        private double smallTableLoadFactor = Double.NaN;
        private int smallTableCapacity = DEFAULT_SMALL_TABLE_CAPACITY;

        private Builder() {
        }

        /**
         * The load a table of {@link #smallTableCapacity} slots and more grows at: from 0.25 to 0.9, default
         * {@value LongObjectHashtable#DEFAULT_LOAD_FACTOR}. Lower - shorter probes, more memory: the table takes
         * 12 / loadFactor bytes per key just before it grows, twice that just after.
         */
        public Builder loadFactor(double loadFactor) {
            this.loadFactor = LoadFactors.check(loadFactor, "load factor");
            return this;
        }

        /**
         * The load a table below {@link #smallTableCapacity} slots grows at: from 0.25 to the load factor, default
         * {@value LongObjectHashtable#DEFAULT_SMALL_TABLE_LOAD_FACTOR} (or the load factor, if lower).
         */
        public Builder smallTableLoadFactor(double loadFactor) {
            this.smallTableLoadFactor = LoadFactors.check(loadFactor, "small table load factor");
            return this;
        }

        /**
         * Capacity in slots, a power of two, from which a table grows at the {@link #loadFactor} - below it at the
         * {@link #smallTableLoadFactor}. Default {@value LongObjectHashtable#DEFAULT_SMALL_TABLE_CAPACITY}; 2 makes
         * every table grow at the load factor.
         */
        public Builder smallTableCapacity(int capacity) {
            this.smallTableCapacity = LoadFactors.checkSmallTableCapacity(capacity);
            return this;
        }

        /**
         * Entries that fit without a resize (default {@value LongObjectHashtable#DEFAULT_ARRAY_SIZE}).
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * Capacity in slots the table grows up to: a power of two, at most and by default 2^29. There it stops
         * growing - it takes new keys up to {@link #maxLoad}, then a put of a new key throws
         * {@link IllegalStateException} and changes nothing.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = HashingUtils.checkMaxCapacity(maxCapacity);
            return this;
        }

        /**
         * No resize, ever: the capacity for {@link #expectedSize} is also the maximum ({@link #maxCapacity}).
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        /**
         * Load up to which a table that can not grow any more takes new keys: from the load factor it grows at there
         * to below 1, default 0.95 - checked when the table is built. See {@link LongLongHashtable.Builder#maxLoad}.
         */
        public Builder maxLoad(double maxLoad) {
            this.maxLoad = maxLoad;
            return this;
        }

        public <V> LongObjectHashtable<V> build() {
            return new LongObjectHashtable<>(this);
        }
    }

    /*
     * Every public operation XORs the key with the seed first (k = key ^ SEED) and works with k from there on.
     */

    /**
     * The slot of the (seeded) key k, or the free slot its probe stopped at - where it would be inserted. The probe
     * ends only at a free slot: without one it throws instead of spinning forever.
     */
    private static int probe(long k, long[] keys, int mask) {
        int slot = HashingUtils.hash(k) & mask;
        long existing = keys[slot];
        int steps = 0;
        while (existing != NOT_ALLOWED_KEY && existing != k) {
            slot = (slot + 1) & mask;
            if (++steps > mask) {
                throw new IllegalStateException("probe: no free slot for key=" + k + " capacity=" + (mask + 1));
            }
            existing = keys[slot];
        }
        return slot;
    }

    /**
     * @return the previous value, or null if the key was absent
     * @throws NullPointerException if the value is null
     */
    @SuppressWarnings("unchecked")
    public V put(long key, V value) {
        Objects.requireNonNull(value, "value");
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            return putZeroKey(value);
        }
        final int slot = probe(k, keys, mask);
        final Object prev = values[slot];
        if (prev == null) {
            insertAt(slot, k, value);
            return null;
        }
        values[slot] = value;
        return (V) prev;
    }

    /**
     * Puts the value only if the key is absent.
     *
     * @return the value the key already has (then the table is unchanged), or null if it was absent
     * @throws NullPointerException if the value is null
     */
    @SuppressWarnings("unchecked")
    public V putIfAbsent(long key, V value) {
        Objects.requireNonNull(value, "value");
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            return zeroKeyValue != null ? (V) zeroKeyValue : putZeroKey(value);
        }
        final int slot = probe(k, keys, mask);
        final Object prev = values[slot];
        if (prev == null) {
            insertAt(slot, k, value);
        }
        return (V) prev;
    }

    /**
     * The value of the key; a missing key is first put with the value the function computes for it - unless that is
     * null, then nothing is put. One probe if present, two if absent: the function may change the table, its result
     * ends up under the key all the same.
     *
     * @return the existing or the computed value, null if the function returned null
     */
    @SuppressWarnings("unchecked")
    public V computeIfAbsent(long key, LongFunction<? extends V> mappingFunction) {
        final long k = key ^ SEED;
        final Object existing = k == NOT_ALLOWED_KEY ? zeroKeyValue : values[probe(k, keys, mask)];
        if (existing != null) {
            return (V) existing;
        }
        final V value = mappingFunction.apply(key);
        if (value != null) {
            put(key, value);
        }
        return value;
    }

    /**
     * Stores a new (seeded) key in the free slot its probe stopped at.
     */
    private void insertAt(int slot, long k, Object value) {
        keys[slot] = k;
        values[slot] = value;
        if (++size >= upsizeThreshold) {
            resize(slot);
        }
    }

    /**
     * @return the value, or null if the key is absent
     */
    @SuppressWarnings("unchecked")
    public V get(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            return (V) zeroKeyValue;
        }
        // a free slot holds null
        return (V) values[probe(k, keys, mask)];
    }

    /**
     * @return the value, or defaultValue if the key is absent
     */
    public V getOrDefault(long key, V defaultValue) {
        final V value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * @return true if the key is present - reads the keys array only
     */
    public boolean containsKey(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            return zeroKeyValue != null;
        }
        return keys[probe(k, keys, mask)] == k;
    }

    /**
     * @return the removed value, or null if the key was absent
     */
    @SuppressWarnings("unchecked")
    public V remove(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            final Object prev = zeroKeyValue;
            if (prev != null) {
                zeroKeyValue = null;
                size--;
            }
            return (V) prev;
        }
        final int slot = probe(k, keys, mask);
        final Object prev = values[slot];
        if (prev != null) {
            deleteAt(slot);
            size--;
        }
        return (V) prev;
    }

    /**
     * Removes the key only if its value equals the given one ({@link Object#equals}).
     *
     * @return true if the entry was removed
     */
    public boolean remove(long key, Object value) {
        if (value == null) {
            return false;
        }
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            if (value.equals(zeroKeyValue)) {
                zeroKeyValue = null;
                size--;
                return true;
            }
            return false;
        }
        final int slot = probe(k, keys, mask);
        final Object current = values[slot];
        if (current == null || !value.equals(current)) {
            return false;
        }
        deleteAt(slot);
        size--;
        return true;
    }

    @SuppressWarnings("unchecked")
    private V putZeroKey(Object value) {
        final Object prev = zeroKeyValue;
        if (prev == null) {
            size++;
        }
        zeroKeyValue = value;
        return (V) prev;
    }

    /**
     * Removes the entry at slot by backward-shift deletion: one pass over the rest of the cluster, moving back every
     * entry the gap would cut off from its home - see {@link LinearProbing#deleteAt}.
     */
    private void deleteAt(int slot) {
        final long[] keys = this.keys;
        final Object[] values = this.values;
        final int mask = this.mask;
        int gap = slot;
        int pos = slot;
        for (int i = 0; ; i++) {
            pos = (pos + 1) & mask;
            final long key = keys[pos];
            if (key == NOT_ALLOWED_KEY) {
                break;
            }
            if (LinearProbing.canFillGap(gap, HashingUtils.hash(key) & mask, pos, mask)) {
                keys[gap] = key;
                values[gap] = values[pos];
                gap = pos;
            }
            if (i > mask) {
                throw new IllegalStateException("deleteAt: no free slot, slot=" + slot + " capacity=" + (mask + 1));
            }
        }
        keys[gap] = NOT_ALLOWED_KEY;
        values[gap] = null;
    }

    /**
     * @param insertedSlot where the put that crossed the threshold stored its new key - taken back if the table is
     *                     full
     */
    private void resize(int insertedSlot) {
        if (mask + 1 >= maxCapacity) {
            // can not grow any more: take new keys up to maxLoad, then refuse them
            final long limit = HashingUtils.maxEntries(mask + 1, maxLoad);
            if (size <= limit) {
                upsizeThreshold = limit + 1;
                return;
            }
            deleteAt(insertedSlot);
            size--;
            throw new IllegalStateException("table is full: " + size + " entries at the maximum capacity of "
                    + (mask + 1) + " slots");
        }
        rehash((mask + 1) << 1);
    }

    /**
     * Copies all entries into new arrays of the given capacity (in slots, a power of two).
     */
    private void rehash(int capacity) {
        final long[] keys = this.keys;
        final Object[] values = this.values;
        final long[] newKeys = new long[capacity];
        final Object[] newValues = new Object[capacity];
        final int newMask = capacity - 1;
        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            if (key != NOT_ALLOWED_KEY) {
                final int slot = probe(key, newKeys, newMask);
                newKeys[slot] = key;
                newValues[slot] = values[i];
            }
        }
        switchTo(newKeys, newValues);
    }

    private void switchTo(long[] newKeys, Object[] newValues) {
        this.keys = newKeys;
        this.values = newValues;
        this.mask = newKeys.length - 1;
        this.upsizeThreshold = loadFactors.threshold(newKeys.length);
    }

    /**
     * @return the number of slots; the table grows when its size reaches the load factor of them (the small table
     * one below the small table capacity)
     */
    public int capacity() {
        return mask + 1;
    }

    /**
     * Grows the table, if needed, to take expectedSize entries in total without a resize. One rehash on the calling
     * thread instead of a series of resizes - for the set-up, not for the latency-critical path.
     *
     * @throws IllegalArgumentException if expectedSize is negative or needs more than the maximum capacity
     */
    public void ensureCapacity(int expectedSize) {
        final int capacity = loadFactors.capacityFor(expectedSize, maxCapacity);
        if (capacity > mask + 1) {
            rehash(capacity);
        }
    }

    /**
     * Removes all entries, keeping the current capacity.
     */
    public void clear() {
        Arrays.fill(keys, NOT_ALLOWED_KEY);
        Arrays.fill(values, null);
        zeroKeyValue = null;
        size = 0;
    }

    public long size() {
        return size;
    }

    /**
     * @return the hash seed - one for all tables in the JVM, see {@link LongLongHashtable#seed()}
     */
    public long seed() {
        return SEED;
    }

    /*
     * Iteration: the value of the key equal to the seed first, then the occupied slots in the array order.
     */

    /**
     * Allocation-free iteration over all entries. The consumer must not modify the table.
     */
    @SuppressWarnings("unchecked")
    public void forEach(LongObjectConsumer<? super V> consumer) {
        if (zeroKeyValue != null) {
            consumer.accept(SEED, (V) zeroKeyValue);
        }
        final long[] keys = this.keys;
        final Object[] values = this.values;
        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            if (key != NOT_ALLOWED_KEY) {
                consumer.accept(key ^ SEED, (V) values[i]);
            }
        }
    }

    /**
     * Keys in the order of {@link #forEach}. The stream reads the table lazily - do not modify the table until it is
     * consumed.
     */
    public LongStream keysStream() {
        final long[] keys = this.keys;
        final LongStream slots = IntStream.range(0, keys.length)
                .filter(i -> keys[i] != NOT_ALLOWED_KEY)
                .mapToLong(i -> keys[i] ^ SEED);
        return zeroKeyValue != null ? LongStream.concat(LongStream.of(SEED), slots) : slots;
    }

    /**
     * Values in the order of {@link #keysStream()}, with the same restriction.
     */
    @SuppressWarnings("unchecked")
    public Stream<V> valuesStream() {
        final Object[] values = this.values;
        final Stream<V> slots = IntStream.range(0, values.length)
                .filter(i -> values[i] != null)
                .mapToObj(i -> (V) values[i]);
        return zeroKeyValue != null ? Stream.concat(Stream.of((V) zeroKeyValue), slots) : slots;
    }

    /**
     * All entries as {@code {key=value, ...}} in the order of {@link #forEach} - for tests and debugging, it walks the
     * whole table.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach((key, value) -> {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(key).append('=').append(value);
        });
        return sb.append('}').toString();
    }

    /**
     * Verifies the invariants: every (seeded) key is found by probing from its home, an occupied slot holds a value
     * and a free one null, size matches, at least one slot is free. For tests - scans the whole table.
     */
    void integrityCheck() {
        long occupied = 0;
        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            if (key == NOT_ALLOWED_KEY) {
                if (values[i] != null) {
                    throw new IllegalStateException("integrityCheck: free slot " + i + " holds a value");
                }
                continue;
            }
            if (values[i] == null) {
                throw new IllegalStateException("integrityCheck: slot " + i + " of key " + key + " holds null");
            }
            occupied++;
            final int reachableAt = probe(key, keys, mask);
            if (reachableAt != i) {
                throw new IllegalStateException("integrityCheck: key=" + key + " stored at " + i
                        + " but probing from its home reaches " + reachableAt);
            }
        }
        if (occupied + (zeroKeyValue != null ? 1 : 0) != size) {
            throw new IllegalStateException("integrityCheck: size=" + size + " but " + occupied + " occupied slots"
                    + (zeroKeyValue != null ? " and the key in a field" : ""));
        }
        if (occupied > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
