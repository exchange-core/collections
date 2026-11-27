package exchange.core2.collections.hashtable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static exchange.core2.collections.hashtable.HashingUtils.INT_SEED;

/**
 * Primitive {@code int} keys to object values: the layout of {@link LongObjectHashtable} with the keys in an
 * {@code int[]} - open addressing with linear probing over two arrays of the same length, the value of each key in an
 * {@code Object[]} at the same index, backward-shift deletion (no tombstones).
 * <ul>
 *     <li>Any key, {@code 0} included. No {@code null} values: a put of {@code null} throws
 *     {@link NullPointerException}, and {@code null} is what get and remove return for a missing key.</li>
 *     <li>A probe walks the keys only, 16 to a cache line. The value of the slot it stops at is read without comparing
 *     the key again - a free slot holds {@code null} - and its address is known as soon as the key is hashed: the
 *     line of the value is loaded together with the line of the key.</li>
 *     <li>8 bytes per slot (references of 4 bytes, a heap below 32 GB) - 12 for {@link LongObjectHashtable}.</li>
 *     <li>Hash seed, one for the JVM: {@code -Dexchange.hashtable.seed=random} (or a number; default 0), see
 *     {@link LongLongHashtable}. All 64 bits of it go into the hash, as for a long key.</li>
 *     <li>The load factors, the resize, {@link Builder#maxCapacity}, {@link Builder#fixedCapacity} and
 *     {@link Builder#maxLoad} as in {@link LongObjectHashtable}: at the maximum capacity a new key is refused with
 *     {@link IllegalStateException}, the table unchanged.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 * The keys array holds every key XOR-ed with the low 32 bits of the seed; stored key 0 marks the free slots, so the
 * value of the key equal to {@code (int) seed()} (key 0 with the default seed) is kept in a field.
 *
 * @param <V> the type of the values
 */
public final class IntObjectHashtable<V> {

    public static final int DEFAULT_ARRAY_SIZE = 16;

    /** the load a table of {@link #DEFAULT_SMALL_TABLE_CAPACITY} slots and more grows at */
    public static final double DEFAULT_LOAD_FACTOR = 0.65;
    /** the load a smaller table grows at */
    public static final double DEFAULT_SMALL_TABLE_LOAD_FACTOR = 0.5;
    /** slots from which a table grows at the load factor, not the small table one */
    public static final int DEFAULT_SMALL_TABLE_CAPACITY = 1 << 16;

    /** stored key of a free slot */
    private static final int FREE = 0;

    private final LoadFactors loadFactors;
    private final int maxCapacity;
    private final double maxLoad;

    private int[] keys;
    /** the value of keys[i] at values[i]; null in a free slot */
    private Object[] values;
    private int mask;
    /** keys in the array - what the load factors and maxLoad limit; the key in the field is not one of them */
    private long occupied;
    private long upsizeThreshold;

    /** the value of the key stored as 0 - the gap marker - the key equal to (int) seed; null if absent */
    private Object zeroKeyValue;

    public IntObjectHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    /**
     * @param expectedSize expected number of entries - that many fit without a resize
     */
    public IntObjectHashtable(int expectedSize) {
        this(builder().expectedSize(expectedSize));
    }

    private IntObjectHashtable(Builder builder) {
        this.loadFactors = new LoadFactors(builder.smallTableLoadFactor, DEFAULT_SMALL_TABLE_LOAD_FACTOR,
                builder.smallTableCapacity, builder.loadFactor);
        final int capacity = loadFactors.capacityFor(builder.expectedSize, builder.maxCapacity, builder.maxLoad);
        this.maxCapacity = builder.fixedCapacity ? capacity : builder.maxCapacity;
        this.maxLoad = loadFactors.checkMaxLoad(builder.maxLoad, maxCapacity);
        switchTo(new int[capacity], new Object[capacity]);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of an {@link IntObjectHashtable}, all optional - those of {@link LongObjectHashtable.Builder}.
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
         * {@value IntObjectHashtable#DEFAULT_LOAD_FACTOR}. Lower - shorter probes, more memory: the table takes
         * 8 / loadFactor bytes per key just before it grows, twice that just after.
         */
        public Builder loadFactor(double loadFactor) {
            this.loadFactor = LoadFactors.check(loadFactor, "load factor");
            return this;
        }

        /**
         * The load a table below {@link #smallTableCapacity} slots grows at: from 0.25 to the load factor, default
         * {@value IntObjectHashtable#DEFAULT_SMALL_TABLE_LOAD_FACTOR} (or the load factor, if lower).
         */
        public Builder smallTableLoadFactor(double loadFactor) {
            this.smallTableLoadFactor = LoadFactors.check(loadFactor, "small table load factor");
            return this;
        }

        /**
         * Capacity in slots, a power of two, from which a table grows at the {@link #loadFactor} - below it at the
         * {@link #smallTableLoadFactor}. Default {@value IntObjectHashtable#DEFAULT_SMALL_TABLE_CAPACITY}; 2 makes
         * every table grow at the load factor.
         */
        public Builder smallTableCapacity(int capacity) {
            this.smallTableCapacity = LoadFactors.checkSmallTableCapacity(capacity);
            return this;
        }

        /**
         * Entries that fit without a resize (default {@value IntObjectHashtable#DEFAULT_ARRAY_SIZE}).
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

        public <V> IntObjectHashtable<V> build() {
            return new IntObjectHashtable<>(this);
        }
    }

    /*
     * Every public operation XORs the key with the seed first (k = key ^ INT_SEED) and works with k from there on.
     */

    /**
     * The slot of the (seeded) key k, or the free slot its probe stopped at - where it would be inserted. The probe
     * ends only at a free slot: without one it throws instead of spinning forever.
     */
    private static int probe(int k, int[] keys, int mask) {
        int slot = HashingUtils.hashInt(k) & mask;
        int existing = keys[slot];
        int steps = 0;
        while (existing != FREE && existing != k) {
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
    public V put(int key, V value) {
        Objects.requireNonNull(value, "value");
        final int k = key ^ INT_SEED;
        if (k == FREE) {
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
    public V putIfAbsent(int key, V value) {
        Objects.requireNonNull(value, "value");
        final int k = key ^ INT_SEED;
        if (k == FREE) {
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
    public V computeIfAbsent(int key, IntFunction<? extends V> mappingFunction) {
        final int k = key ^ INT_SEED;
        final Object existing = k == FREE ? zeroKeyValue : values[probe(k, keys, mask)];
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
    private void insertAt(int slot, int k, Object value) {
        keys[slot] = k;
        values[slot] = value;
        if (++occupied >= upsizeThreshold) {
            resize(slot);
        }
    }

    /**
     * @return the value, or null if the key is absent
     */
    @SuppressWarnings("unchecked")
    public V get(int key) {
        final int k = key ^ INT_SEED;
        if (k == FREE) {
            return (V) zeroKeyValue;
        }
        // a free slot holds null
        return (V) values[probe(k, keys, mask)];
    }

    /**
     * @return the value, or defaultValue if the key is absent
     */
    public V getOrDefault(int key, V defaultValue) {
        final V value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * @return true if the key is present - reads the keys array only
     */
    public boolean containsKey(int key) {
        final int k = key ^ INT_SEED;
        if (k == FREE) {
            return zeroKeyValue != null;
        }
        return keys[probe(k, keys, mask)] == k;
    }

    /**
     * @return the removed value, or null if the key was absent
     */
    @SuppressWarnings("unchecked")
    public V remove(int key) {
        final int k = key ^ INT_SEED;
        if (k == FREE) {
            final Object prev = zeroKeyValue;
            zeroKeyValue = null;
            return (V) prev;
        }
        final int slot = probe(k, keys, mask);
        final Object prev = values[slot];
        if (prev != null) {
            deleteAt(slot);
            occupied--;
        }
        return (V) prev;
    }

    /**
     * Removes the key only if its value equals the given one ({@link Object#equals}).
     *
     * @return true if the entry was removed
     */
    public boolean remove(int key, Object value) {
        if (value == null) {
            return false;
        }
        final int k = key ^ INT_SEED;
        if (k == FREE) {
            if (value.equals(zeroKeyValue)) {
                zeroKeyValue = null;
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
        occupied--;
        return true;
    }

    @SuppressWarnings("unchecked")
    private V putZeroKey(Object value) {
        final Object prev = zeroKeyValue;
        zeroKeyValue = value;
        return (V) prev;
    }

    /**
     * Removes the entry at slot by backward-shift deletion: one pass over the rest of the cluster, moving back every
     * entry the gap would cut off from its home - see {@link LinearProbing#deleteAt}.
     */
    private void deleteAt(int slot) {
        final int[] keys = this.keys;
        final Object[] values = this.values;
        final int mask = this.mask;
        int gap = slot;
        int pos = slot;
        for (int i = 0; ; i++) {
            pos = (pos + 1) & mask;
            final int key = keys[pos];
            if (key == FREE) {
                break;
            }
            if (LinearProbing.canFillGap(gap, HashingUtils.hashInt(key) & mask, pos, mask)) {
                keys[gap] = key;
                values[gap] = values[pos];
                gap = pos;
            }
            if (i > mask) {
                throw new IllegalStateException("deleteAt: no free slot, slot=" + slot + " capacity=" + (mask + 1));
            }
        }
        keys[gap] = FREE;
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
            if (occupied <= limit) {
                upsizeThreshold = limit + 1;
                return;
            }
            deleteAt(insertedSlot);
            occupied--;
            throw new IllegalStateException("table is full: " + occupied + " keys in the array at the maximum"
                    + " capacity of " + (mask + 1) + " slots");
        }
        rehash((mask + 1) << 1);
    }

    /**
     * Copies all entries into new arrays of the given capacity (in slots, a power of two).
     */
    private void rehash(int capacity) {
        final int[] keys = this.keys;
        final Object[] values = this.values;
        final int[] newKeys = new int[capacity];
        final Object[] newValues = new Object[capacity];
        final int newMask = capacity - 1;
        for (int i = 0; i < keys.length; i++) {
            final int key = keys[i];
            if (key != FREE) {
                final int slot = probe(key, newKeys, newMask);
                newKeys[slot] = key;
                newValues[slot] = values[i];
            }
        }
        switchTo(newKeys, newValues);
    }

    private void switchTo(int[] newKeys, Object[] newValues) {
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
     * Grows the table, if needed, to take expectedSize entries in total without a resize - the present ones included:
     * {@code ensureCapacity(size())} changes nothing. One rehash on the calling thread instead of a series of resizes -
     * for the set-up, not for the latency-critical path.
     *
     * @throws IllegalArgumentException if expectedSize is negative or more than the table takes at its maximum
     *                                  capacity and {@link Builder#maxLoad}
     */
    public void ensureCapacity(int expectedSize) {
        final int capacity = loadFactors.capacityFor(HashingUtils.keysInArray(expectedSize, zeroKeyValue != null),
                maxCapacity, maxLoad);
        if (capacity > mask + 1) {
            rehash(capacity);
        }
    }

    /**
     * Removes all entries, keeping the current capacity.
     */
    public void clear() {
        Arrays.fill(keys, FREE);
        Arrays.fill(values, null);
        zeroKeyValue = null;
        occupied = 0;
    }

    public long size() {
        return zeroKeyValue != null ? occupied + 1 : occupied;
    }

    /**
     * @return the hash seed - one for all tables in the JVM, see {@link LongLongHashtable#seed()}; the key kept in a
     * field is {@code (int) seed()}
     */
    public long seed() {
        return HashingUtils.SEED;
    }

    /*
     * Iteration: the value of the key equal to (int) seed first, then the occupied slots in the array order.
     */

    /**
     * Allocation-free iteration over all entries. The consumer must not modify the table.
     */
    @SuppressWarnings("unchecked")
    public void forEach(IntObjectConsumer<? super V> consumer) {
        if (zeroKeyValue != null) {
            consumer.accept(INT_SEED, (V) zeroKeyValue);
        }
        final int[] keys = this.keys;
        final Object[] values = this.values;
        for (int i = 0; i < keys.length; i++) {
            final int key = keys[i];
            if (key != FREE) {
                consumer.accept(key ^ INT_SEED, (V) values[i]);
            }
        }
    }

    /**
     * Keys in the order of {@link #forEach}. The stream reads the table lazily - do not modify the table until it is
     * consumed.
     */
    public IntStream keysStream() {
        final int[] keys = this.keys;
        final IntStream slots = IntStream.range(0, keys.length)
                .filter(i -> keys[i] != FREE)
                .map(i -> keys[i] ^ INT_SEED);
        return zeroKeyValue != null ? IntStream.concat(IntStream.of(INT_SEED), slots) : slots;
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
     * and a free one null, the count of keys in the array matches, at least one slot is free. For tests - scans the
     * whole table.
     */
    void integrityCheck() {
        long found = 0;
        for (int i = 0; i < keys.length; i++) {
            final int key = keys[i];
            if (key == FREE) {
                if (values[i] != null) {
                    throw new IllegalStateException("integrityCheck: free slot " + i + " holds a value");
                }
                continue;
            }
            if (values[i] == null) {
                throw new IllegalStateException("integrityCheck: slot " + i + " of key " + key + " holds null");
            }
            found++;
            final int reachableAt = probe(key, keys, mask);
            if (reachableAt != i) {
                throw new IllegalStateException("integrityCheck: key=" + key + " stored at " + i
                        + " but probing from its home reaches " + reachableAt);
            }
        }
        if (found != occupied) {
            throw new IllegalStateException("integrityCheck: " + occupied + " keys counted in the array but "
                    + found + " found");
        }
        if (found > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
