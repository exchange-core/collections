package exchange.core2.collections.hashtable;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * UUID keys to object values: open addressing with linear probing over two arrays - the keys in a {@code long[]}, two
 * longs a slot (the most and the least significant bits), the value of each in an {@code Object[]} at the slot index -
 * and backward-shift deletion (no tombstones).
 * <ul>
 *     <li>Every operation takes the key either as a {@link UUID} or as its two halves
 *     ({@link UUID#getMostSignificantBits()}, {@link UUID#getLeastSignificantBits()}) - the same entry; the table never
 *     keeps or creates UUID objects, except {@link #keysStream()}.</li>
 *     <li>Any key, the nil UUID (all zeros) included. No {@code null} values: a put of {@code null} throws
 *     {@link NullPointerException}, and {@code null} is what get and remove return for a missing key.</li>
 *     <li>A probe walks the keys only, 4 to a cache line. The value of the slot it stops at is read without comparing
 *     the key again - a free slot holds {@code null} - and its line is loaded together with the line of the key.</li>
 *     <li>20 bytes per slot (references of 4 bytes, a heap below 32 GB).</li>
 *     <li>The hash mixes both halves with the hash seed of the JVM ({@code -Dexchange.hashtable.seed=random}, or a
 *     number; default 0, see {@link LongLongHashtable}): keys chosen outside can not be picked to pile up in one probe
 *     cluster without knowing it. The halves are stored as they are.</li>
 *     <li>Grows by doubling at its load factor ({@link Builder#loadFactor}) - a small table at a lower one
 *     ({@link Builder#smallTableLoadFactor}, below {@link Builder#smallTableCapacity}): sparse while that costs
 *     little memory. A resize copies the whole table on the calling thread - pre-size it
 *     ({@link Builder#expectedSize}, {@link #ensureCapacity}). {@link Builder#maxCapacity},
 *     {@link Builder#fixedCapacity} and {@link Builder#maxLoad} as in {@link LongLongHashtable}: at the maximum
 *     capacity a new key is refused with {@link IllegalStateException}, the table unchanged.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 * A slot of two zero longs is free, so the value of the nil UUID is kept in a field.
 *
 * @param <V> the type of the values
 */
public final class UuidObjectHashtable<V> {

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

    /** slot s: the most significant bits at 2s, the least significant at 2s + 1; both 0 in a free slot */
    private long[] keys;
    /** the value of slot s at s; null in a free slot */
    private Object[] values;
    /** capacity - 1, in slots */
    private int mask;
    private long size;
    private long upsizeThreshold;

    /** the value of the nil UUID - two zero longs mark a free slot; null if absent */
    private Object nilKeyValue;

    public UuidObjectHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    /**
     * @param expectedSize expected number of entries - that many fit without a resize
     */
    public UuidObjectHashtable(int expectedSize) {
        this(builder().expectedSize(expectedSize));
    }

    private UuidObjectHashtable(Builder builder) {
        this.loadFactors = new LoadFactors(builder.smallTableLoadFactor, DEFAULT_SMALL_TABLE_LOAD_FACTOR,
                builder.smallTableCapacity, builder.loadFactor);
        final int capacity = loadFactors.capacityFor(builder.expectedSize, builder.maxCapacity);
        this.maxCapacity = builder.fixedCapacity ? capacity : builder.maxCapacity;
        this.maxLoad = loadFactors.checkMaxLoad(builder.maxLoad, maxCapacity);
        switchTo(new long[capacity << 1], new Object[capacity]);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link UuidObjectHashtable}, all optional.
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
         * {@value UuidObjectHashtable#DEFAULT_LOAD_FACTOR}. Lower - shorter probes, more memory: the table takes
         * 20 / loadFactor bytes per key just before it grows, twice that just after.
         */
        public Builder loadFactor(double loadFactor) {
            this.loadFactor = LoadFactors.check(loadFactor, "load factor");
            return this;
        }

        /**
         * The load a table below {@link #smallTableCapacity} slots grows at: from 0.25 to the load factor, default
         * {@value UuidObjectHashtable#DEFAULT_SMALL_TABLE_LOAD_FACTOR} (or the load factor, if lower).
         */
        public Builder smallTableLoadFactor(double loadFactor) {
            this.smallTableLoadFactor = LoadFactors.check(loadFactor, "small table load factor");
            return this;
        }

        /**
         * Capacity in slots, a power of two, from which a table grows at the {@link #loadFactor} - below it at the
         * {@link #smallTableLoadFactor}. Default {@value UuidObjectHashtable#DEFAULT_SMALL_TABLE_CAPACITY}; 2 makes
         * every table grow at the load factor.
         */
        public Builder smallTableCapacity(int capacity) {
            this.smallTableCapacity = LoadFactors.checkSmallTableCapacity(capacity);
            return this;
        }

        /**
         * Entries that fit without a resize (default {@value UuidObjectHashtable#DEFAULT_ARRAY_SIZE}).
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

        public <V> UuidObjectHashtable<V> build() {
            return new UuidObjectHashtable<>(this);
        }
    }

    /**
     * The slot of the key, or the free slot its probe stopped at - where it would be inserted. The probe ends only at
     * a free slot: without one it throws instead of spinning forever.
     */
    private static int probe(long hi, long lo, long[] keys, int mask) {
        int slot = HashingUtils.hash(hi, lo) & mask;
        int steps = 0;
        while (true) {
            final int pos = slot << 1;
            final long h = keys[pos];
            final long l = keys[pos + 1];
            if (((h ^ hi) | (l ^ lo)) == 0L || (h | l) == 0L) {
                return slot;
            }
            slot = (slot + 1) & mask;
            if (++steps > mask) {
                throw new IllegalStateException("probe: no free slot for key=" + new UUID(hi, lo)
                        + " capacity=" + (mask + 1));
            }
        }
    }

    private static boolean isNil(long hi, long lo) {
        return (hi | lo) == 0L;
    }

    /**
     * @return the previous value, or null if the key was absent
     * @throws NullPointerException if the value is null
     */
    @SuppressWarnings("unchecked")
    public V put(long mostSigBits, long leastSigBits, V value) {
        Objects.requireNonNull(value, "value");
        if (isNil(mostSigBits, leastSigBits)) {
            return putNilKey(value);
        }
        final int slot = probe(mostSigBits, leastSigBits, keys, mask);
        final Object prev = values[slot];
        if (prev == null) {
            insertAt(slot, mostSigBits, leastSigBits, value);
            return null;
        }
        values[slot] = value;
        return (V) prev;
    }

    /**
     * {@link #put(long, long, Object)} of the halves of the key.
     */
    public V put(UUID key, V value) {
        return put(key.getMostSignificantBits(), key.getLeastSignificantBits(), value);
    }

    /**
     * Puts the value only if the key is absent.
     *
     * @return the value the key already has (then the table is unchanged), or null if it was absent
     * @throws NullPointerException if the value is null
     */
    @SuppressWarnings("unchecked")
    public V putIfAbsent(long mostSigBits, long leastSigBits, V value) {
        Objects.requireNonNull(value, "value");
        if (isNil(mostSigBits, leastSigBits)) {
            return nilKeyValue != null ? (V) nilKeyValue : putNilKey(value);
        }
        final int slot = probe(mostSigBits, leastSigBits, keys, mask);
        final Object prev = values[slot];
        if (prev == null) {
            insertAt(slot, mostSigBits, leastSigBits, value);
        }
        return (V) prev;
    }

    /**
     * {@link #putIfAbsent(long, long, Object)} of the halves of the key.
     */
    public V putIfAbsent(UUID key, V value) {
        return putIfAbsent(key.getMostSignificantBits(), key.getLeastSignificantBits(), value);
    }

    /**
     * The value of the key; a missing key is first put with the value the function computes for it - unless that is
     * null, then nothing is put. One probe if present, two if absent: the function may change the table, its result
     * ends up under the key all the same.
     *
     * @return the existing or the computed value, null if the function returned null
     */
    public V computeIfAbsent(long mostSigBits, long leastSigBits, UuidFunction<? extends V> mappingFunction) {
        final V existing = get(mostSigBits, leastSigBits);
        if (existing != null) {
            return existing;
        }
        final V value = mappingFunction.apply(mostSigBits, leastSigBits);
        if (value != null) {
            put(mostSigBits, leastSigBits, value);
        }
        return value;
    }

    /**
     * {@link #computeIfAbsent(long, long, UuidFunction)} with a function of the UUID: it gets the given key object.
     */
    public V computeIfAbsent(UUID key, Function<? super UUID, ? extends V> mappingFunction) {
        final long hi = key.getMostSignificantBits();
        final long lo = key.getLeastSignificantBits();
        final V existing = get(hi, lo);
        if (existing != null) {
            return existing;
        }
        final V value = mappingFunction.apply(key);
        if (value != null) {
            put(hi, lo, value);
        }
        return value;
    }

    /**
     * Stores a new key in the free slot its probe stopped at.
     */
    private void insertAt(int slot, long hi, long lo, Object value) {
        final int pos = slot << 1;
        keys[pos] = hi;
        keys[pos + 1] = lo;
        values[slot] = value;
        if (++size >= upsizeThreshold) {
            resize(slot);
        }
    }

    /**
     * @return the value, or null if the key is absent
     */
    @SuppressWarnings("unchecked")
    public V get(long mostSigBits, long leastSigBits) {
        if (isNil(mostSigBits, leastSigBits)) {
            return (V) nilKeyValue;
        }
        // a free slot holds null
        return (V) values[probe(mostSigBits, leastSigBits, keys, mask)];
    }

    /**
     * {@link #get(long, long)} of the halves of the key.
     */
    public V get(UUID key) {
        return get(key.getMostSignificantBits(), key.getLeastSignificantBits());
    }

    /**
     * @return the value, or defaultValue if the key is absent
     */
    public V getOrDefault(long mostSigBits, long leastSigBits, V defaultValue) {
        final V value = get(mostSigBits, leastSigBits);
        return value != null ? value : defaultValue;
    }

    /**
     * {@link #getOrDefault(long, long, Object)} of the halves of the key.
     */
    public V getOrDefault(UUID key, V defaultValue) {
        return getOrDefault(key.getMostSignificantBits(), key.getLeastSignificantBits(), defaultValue);
    }

    /**
     * @return true if the key is present - reads the keys array only
     */
    public boolean containsKey(long mostSigBits, long leastSigBits) {
        if (isNil(mostSigBits, leastSigBits)) {
            return nilKeyValue != null;
        }
        final int pos = probe(mostSigBits, leastSigBits, keys, mask) << 1;
        // the probe stops at the key or at a free slot, all zeros - which the key is not
        return (keys[pos] | keys[pos + 1]) != 0L;
    }

    /**
     * {@link #containsKey(long, long)} of the halves of the key.
     */
    public boolean containsKey(UUID key) {
        return containsKey(key.getMostSignificantBits(), key.getLeastSignificantBits());
    }

    /**
     * @return the removed value, or null if the key was absent
     */
    @SuppressWarnings("unchecked")
    public V remove(long mostSigBits, long leastSigBits) {
        if (isNil(mostSigBits, leastSigBits)) {
            final Object prev = nilKeyValue;
            if (prev != null) {
                nilKeyValue = null;
                size--;
            }
            return (V) prev;
        }
        final int slot = probe(mostSigBits, leastSigBits, keys, mask);
        final Object prev = values[slot];
        if (prev != null) {
            deleteAt(slot);
            size--;
        }
        return (V) prev;
    }

    /**
     * {@link #remove(long, long)} of the halves of the key.
     */
    public V remove(UUID key) {
        return remove(key.getMostSignificantBits(), key.getLeastSignificantBits());
    }

    /**
     * Removes the key only if its value equals the given one ({@link Object#equals}).
     *
     * @return true if the entry was removed
     */
    public boolean remove(long mostSigBits, long leastSigBits, Object value) {
        if (value == null) {
            return false;
        }
        if (isNil(mostSigBits, leastSigBits)) {
            if (value.equals(nilKeyValue)) {
                nilKeyValue = null;
                size--;
                return true;
            }
            return false;
        }
        final int slot = probe(mostSigBits, leastSigBits, keys, mask);
        final Object current = values[slot];
        if (current == null || !value.equals(current)) {
            return false;
        }
        deleteAt(slot);
        size--;
        return true;
    }

    /**
     * {@link #remove(long, long, Object)} of the halves of the key.
     */
    public boolean remove(UUID key, Object value) {
        return remove(key.getMostSignificantBits(), key.getLeastSignificantBits(), value);
    }

    @SuppressWarnings("unchecked")
    private V putNilKey(Object value) {
        final Object prev = nilKeyValue;
        if (prev == null) {
            size++;
        }
        nilKeyValue = value;
        return (V) prev;
    }

    /**
     * Removes the entry at slot by backward-shift deletion: one pass over the rest of the cluster, moving back every
     * entry the gap would cut off from its home (each hashed once) - see {@link LinearProbing#deleteAt}.
     */
    private void deleteAt(int slot) {
        final long[] keys = this.keys;
        final Object[] values = this.values;
        final int mask = this.mask;
        int gap = slot;
        int pos = slot;
        for (int i = 0; ; i++) {
            pos = (pos + 1) & mask;
            final long hi = keys[pos << 1];
            final long lo = keys[(pos << 1) + 1];
            if ((hi | lo) == 0L) {
                break;
            }
            if (LinearProbing.canFillGap(gap, HashingUtils.hash(hi, lo) & mask, pos, mask)) {
                keys[gap << 1] = hi;
                keys[(gap << 1) + 1] = lo;
                values[gap] = values[pos];
                gap = pos;
            }
            if (i > mask) {
                throw new IllegalStateException("deleteAt: no free slot, slot=" + slot + " capacity=" + (mask + 1));
            }
        }
        keys[gap << 1] = 0L;
        keys[(gap << 1) + 1] = 0L;
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
        final long[] newKeys = new long[capacity << 1];
        final Object[] newValues = new Object[capacity];
        final int newMask = capacity - 1;
        for (int s = 0; s < values.length; s++) {
            final long hi = keys[s << 1];
            final long lo = keys[(s << 1) + 1];
            if ((hi | lo) != 0L) {
                final int slot = probe(hi, lo, newKeys, newMask);
                newKeys[slot << 1] = hi;
                newKeys[(slot << 1) + 1] = lo;
                newValues[slot] = values[s];
            }
        }
        switchTo(newKeys, newValues);
    }

    private void switchTo(long[] newKeys, Object[] newValues) {
        this.keys = newKeys;
        this.values = newValues;
        this.mask = newValues.length - 1;
        this.upsizeThreshold = loadFactors.threshold(newValues.length);
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
        Arrays.fill(keys, 0L);
        Arrays.fill(values, null);
        nilKeyValue = null;
        size = 0;
    }

    public long size() {
        return size;
    }

    /**
     * @return the hash seed - one for all tables in the JVM, see {@link LongLongHashtable#seed()}
     */
    public long seed() {
        return HashingUtils.SEED;
    }

    /*
     * Iteration: the value of the nil UUID first, then the occupied slots in the array order.
     */

    /**
     * Allocation-free iteration over all entries, the key as its two halves. The consumer must not modify the table.
     */
    @SuppressWarnings("unchecked")
    public void forEach(UuidObjectConsumer<? super V> consumer) {
        if (nilKeyValue != null) {
            consumer.accept(0L, 0L, (V) nilKeyValue);
        }
        final long[] keys = this.keys;
        final Object[] values = this.values;
        for (int s = 0; s < values.length; s++) {
            final Object value = values[s];
            if (value != null) {
                consumer.accept(keys[s << 1], keys[(s << 1) + 1], (V) value);
            }
        }
    }

    /**
     * Keys in the order of {@link #forEach}, a new UUID object each. The stream reads the table lazily - do not modify
     * the table until it is consumed.
     */
    public Stream<UUID> keysStream() {
        final long[] keys = this.keys;
        final Object[] values = this.values;
        final Stream<UUID> slots = IntStream.range(0, values.length)
                .filter(s -> values[s] != null)
                .mapToObj(s -> new UUID(keys[s << 1], keys[(s << 1) + 1]));
        return nilKeyValue != null ? Stream.concat(Stream.of(new UUID(0L, 0L)), slots) : slots;
    }

    /**
     * Values in the order of {@link #keysStream()}, with the same restriction.
     */
    @SuppressWarnings("unchecked")
    public Stream<V> valuesStream() {
        final Object[] values = this.values;
        final Stream<V> slots = IntStream.range(0, values.length)
                .filter(s -> values[s] != null)
                .mapToObj(s -> (V) values[s]);
        return nilKeyValue != null ? Stream.concat(Stream.of((V) nilKeyValue), slots) : slots;
    }

    /**
     * All entries as {@code {uuid=value, ...}} in the order of {@link #forEach} - for tests and debugging, it walks
     * the whole table.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach((hi, lo, value) -> {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(new UUID(hi, lo)).append('=').append(value);
        });
        return sb.append('}').toString();
    }

    /**
     * Verifies the invariants: every key is found by probing from its home, an occupied slot holds a value and a free
     * one null, size matches, at least one slot is free. For tests - scans the whole table.
     */
    void integrityCheck() {
        long occupied = 0;
        for (int s = 0; s < values.length; s++) {
            final long hi = keys[s << 1];
            final long lo = keys[(s << 1) + 1];
            if ((hi | lo) == 0L) {
                if (values[s] != null) {
                    throw new IllegalStateException("integrityCheck: free slot " + s + " holds a value");
                }
                continue;
            }
            if (values[s] == null) {
                throw new IllegalStateException("integrityCheck: slot " + s + " of key " + new UUID(hi, lo)
                        + " holds null");
            }
            occupied++;
            final int reachableAt = probe(hi, lo, keys, mask);
            if (reachableAt != s) {
                throw new IllegalStateException("integrityCheck: key=" + new UUID(hi, lo) + " stored at " + s
                        + " but probing from its home reaches " + reachableAt);
            }
        }
        if (occupied + (nilKeyValue != null ? 1 : 0) != size) {
            throw new IllegalStateException("integrityCheck: size=" + size + " but " + occupied + " occupied slots"
                    + (nilKeyValue != null ? " and the nil key in a field" : ""));
        }
        if (occupied > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
