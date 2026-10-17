package exchange.core2.collections.hashtable;


import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;
import static exchange.core2.collections.hashtable.HashingUtils.SEED;

/**
 * Primitive {@code long} to {@code long} hashtable: open addressing with linear probing, keys and values interleaved
 * in one {@code long[]}, backward-shift deletion (no tombstones).
 * <ul>
 *     <li>Any key, {@code 0} included.</li>
 *     <li>A missing key reads as {@code missingValue} ({@code 0} by default) - see {@link ILongLongHashtable}.</li>
 *     <li>Hash seed, one for the JVM: {@code -Dexchange.hashtable.seed=random} (or a number; default 0). With keys
 *     chosen outside (client order ids...) a seed they do not know keeps them from being picked to pile up in one
 *     probe cluster. See {@link #seed()}.</li>
 *     <li>Grows by doubling at load factor 0.65. The resize copies the whole table on the calling thread (~7ns per
 *     slot: ~7ms at 2^20 slots) - pre-size the table ({@link Builder#expectedSize}, {@link #ensureCapacity}), or use
 *     {@link LongLongLL2Hashtable} where that pause matters.</li>
 *     <li>Maximum capacity: 2^29 slots, a {@code long[2^30]} (8 GB), or lower ({@link Builder#maxCapacity},
 *     {@link Builder#fixedCapacity} - no resize at all). There the table stops growing and takes new keys up to
 *     {@link Builder#maxLoad} (0.95 by default), after that a {@code put} of a new key throws
 *     {@link IllegalStateException} and changes nothing.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 * The array holds every key XOR-ed with the seed - hashing it then hashes key ^ seed. Stored key 0 marks the empty
 * slots, so the entry of the key equal to the seed (key 0 with the default seed) is kept in two fields.
 */
public final class LongLongHashtable implements ILongLongHashtable {


    public static final int DEFAULT_ARRAY_SIZE = 16;

    private final float upsizeThresholdPerc;
    private final int maxCapacity;
    private final double maxLoad;

    private final long missingValue;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;

    // the entry stored as key 0 - the gap marker - is kept here (counted in size): the key equal to the seed
    private boolean hasZeroKey;
    private long zeroKeyValue;

    public LongLongHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    /**
     * @param size expected number of entries - that many fit without a resize
     */
    public LongLongHashtable(int size) {
        this(builder().expectedSize(size));
    }

    private LongLongHashtable(Builder builder) {

        this.upsizeThresholdPerc = 0.65f;
        this.missingValue = builder.missingValue;
        final int arraySize = HashingUtils.capacityFor(builder.expectedSize, upsizeThresholdPerc, builder.maxCapacity);
        this.maxCapacity = builder.fixedCapacity ? arraySize : builder.maxCapacity;
        this.maxLoad = builder.maxLoad;

        this.data = LinearProbing.newArray(arraySize, missingValue);
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link LongLongHashtable}, all optional.
     */
    public static final class Builder {

        private int expectedSize = DEFAULT_ARRAY_SIZE;
        private long missingValue = 0L;
        private int maxCapacity = HashingUtils.MAX_CAPACITY;
        private boolean fixedCapacity;
        private double maxLoad = HashingUtils.DEFAULT_MAX_LOAD;

        private Builder() {
        }

        /**
         * Entries that fit without a resize (default {@value LongLongHashtable#DEFAULT_ARRAY_SIZE}).
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * What get, put and remove return for a missing key (default 0). A non-zero one is written into every free
         * slot: one more pass over each new array.
         */
        public Builder missingValue(long missingValue) {
            this.missingValue = missingValue;
            return this;
        }

        /**
         * Capacity in slots the table grows up to: a power of two, at most and by default 2^29. There it stops
         * growing - it takes new keys up to {@link #maxLoad}, then a put of a new key throws
         * {@link IllegalStateException} and changes nothing; updates and removals go on working.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = HashingUtils.checkMaxCapacity(maxCapacity);
            return this;
        }

        /**
         * No resize, ever: the capacity for {@link #expectedSize} is also the maximum ({@link #maxCapacity}). Past
         * expectedSize the table takes new keys up to {@link #maxLoad}, with longer probes, then refuses them.
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        /**
         * Load up to which a table that can not grow any more ({@link #maxCapacity}, {@link #fixedCapacity}) takes new
         * keys: from 0.65 to below 1, default 0.95; two slots always stay free. The fuller, the longer the probes of
         * the operations on missing keys - inserts of new keys, lookups and removals of absent ones - with random
         * keys ~(1 + 1/(1-load)^2)/2 slots on average: 50 at 0.9, 200 at 0.95, 5000 at 0.99. Lookups of present keys
         * ~(1 + 1/(1-load))/2: 5.5, 10.5, 50.5. A removal also shifts back the rest of its cluster.
         */
        public Builder maxLoad(double maxLoad) {
            this.maxLoad = HashingUtils.checkMaxLoad(maxLoad);
            return this;
        }

        public LongLongHashtable build() {
            return new LongLongHashtable(this);
        }
    }

    /*
     * Every public operation XORs the key with the seed first (k = key ^ SEED) and works with k from there on.
     */

    @Override
    public long put(long key, long value) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return putZeroKey(value);
        final int offset = HashingUtils.findFreeOffset(k, data, mask);

        if (data[offset] != k) {
            insertAt(offset, k, value);
            return missingValue;
        }

        final long prevValue = data[offset + 1];
        data[offset + 1] = value;
        return prevValue;
    }

    @Override
    public long putIfAbsent(long key, long value) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : putZeroKey(value);
        final int offset = HashingUtils.findFreeOffset(k, data, mask);

        if (data[offset] == k) {
            return data[offset + 1];
        }
        insertAt(offset, k, value);
        return missingValue;
    }

    @Override
    public long addAndGet(long key, long delta) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            final long value = (hasZeroKey ? zeroKeyValue : 0L) + delta;
            putZeroKey(value);
            return value;
        }
        final int offset = HashingUtils.findFreeOffset(k, data, mask);

        if (data[offset] == k) {
            return data[offset + 1] += delta;
        }
        insertAt(offset, k, delta);
        return delta;
    }

    @Override
    public long computeIfAbsent(long key, LongUnaryOperator mappingFunction) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            if (hasZeroKey) {
                return zeroKeyValue;
            }
        } else {
            final int offset = HashingUtils.findFreeOffset(k, data, mask);
            if (data[offset] == k) {
                return data[offset + 1];
            }
        }
        final long value = mappingFunction.applyAsLong(key);
        // probe again rather than insert at the offset found before: the function may have changed the table
        put(key, value);
        return value;
    }

    /**
     * Stores a new (seeded) key in the gap its probe stopped at.
     */
    private void insertAt(int offset, long k, long value) {
        data[offset] = k;
        data[offset + 1] = value;
        if (++size >= upsizeThreshold) {
            resize(offset);
        }
    }


    @Override
    public long get(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : missingValue;
        // a gap holds missingValue in its value cell
        return data[HashingUtils.findFreeOffset(k, data, mask) + 1];
    }


    @Override
    public boolean containsKey(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey;
        return data[HashingUtils.findFreeOffset(k, data, mask)] == k;
    }

    @Override
    public long getOrDefault(long key, long defaultValue) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : defaultValue;
        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        return data[offset] == k ? data[offset + 1] : defaultValue;
    }

    @Override
    public long remove(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            return removeZeroKey();
        }
        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        if (data[offset] != k) {
            return missingValue;
        }
        final long value = data[offset + 1];
        LinearProbing.deleteAt(data, offset, missingValue);
        size--;
        return value;
    }

    private long putZeroKey(long value) {
        final long prev = hasZeroKey ? zeroKeyValue : missingValue;
        if (!hasZeroKey) {
            hasZeroKey = true;
            size++;
        }
        zeroKeyValue = value;
        return prev;
    }

    private long removeZeroKey() {
        if (!hasZeroKey) {
            return missingValue;
        }
        hasZeroKey = false;
        size--;
        return zeroKeyValue;
    }

    @Override
    public boolean remove(long key, long value) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            if (hasZeroKey && zeroKeyValue == value) {
                removeZeroKey();
                return true;
            }
            return false;
        }
        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        if (data[offset] != k || data[offset + 1] != value) {
            return false;
        }
        LinearProbing.deleteAt(data, offset, missingValue);
        size--;
        return true;
    }

    /**
     * @param insertedOffset where the put that crossed the threshold stored its new key - taken back if the table is
     *                       full
     */
    private void resize(int insertedOffset) {

        // log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (mask + 1 >= maxCapacity) {
            // can not grow any more: take new keys up to maxLoad, then refuse them - beyond that the probes, and
            // with them every operation, get longer and longer
            final long limit = HashingUtils.maxEntries(mask + 1, maxLoad);
            if (size <= limit) {
                upsizeThreshold = limit + 1;
                return;
            }
            LinearProbing.deleteAt(data, insertedOffset, missingValue);
            size--;
            throw new IllegalStateException("table is full: " + size + " entries at the maximum capacity of "
                    + (mask + 1) + " slots");
        }

        switchToNewArray(new HashtableResizer(data, missingValue).resizeSync());
    }

    private void switchToNewArray(long[] data2) {
        this.data = data2;
        mask = (data2.length >> 1) - 1;
        upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    @Override
    public int capacity() {
        return mask + 1;
    }

    @Override
    public void ensureCapacity(int expectedSize) {
        final int capacity = HashingUtils.capacityFor(expectedSize, upsizeThresholdPerc, maxCapacity);
        if (capacity > mask + 1) {
            switchToNewArray(HashtableResizer.rehash(data, capacity, missingValue));
        }
    }

    /**
     * Removes all entries, keeping the current capacity.
     */
    @Override
    public void clear() {
        LinearProbing.clear(data, missingValue);
        size = 0;
        hasZeroKey = false;
        zeroKeyValue = 0L;
    }

    /**
     * Keys in the table order, the key equal to the seed first. The stream reads the table lazily - do not modify the
     * table until it is consumed.
     */
    @Override
    public LongStream keysStream() {
        return LinearProbing.keysStream(hasZeroKey, data, SEED);
    }

    /**
     * Values in the same order as {@link #keysStream()}. Same restriction: do not modify the table until it is consumed.
     */
    @Override
    public LongStream valuesStream() {
        return LinearProbing.valuesStream(hasZeroKey, zeroKeyValue, data);
    }

    /**
     * Allocation-free iteration over all entries, in the order of {@link #keysStream()}. The consumer must not modify
     * the table.
     */
    @Override
    public void forEach(LongLongConsumer consumer) {
        LinearProbing.forEach(hasZeroKey, zeroKeyValue, data, SEED, consumer);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long missingValue() {
        return missingValue;
    }

    /**
     * @return the hash seed - one for all tables in the JVM, {@code -Dexchange.hashtable.seed=random|<number>}
     * (default 0). With a random seed, log it to reproduce the iteration order.
     */
    public long seed() {
        return SEED;
    }

    /**
     * All entries as {@code {key=value, ...}} - for tests and debugging, it walks the whole table.
     */
    @Override
    public String toString() {
        return LinearProbing.entriesToString(hasZeroKey, zeroKeyValue, data, SEED);
    }

    /**
     * Verifies the linear-probing invariants: every (seeded) key is found by probing from its home (no gap in its way,
     * no duplicate before it), free slots hold missingValue (what get returns for a missing key), size matches the
     * occupied slots plus the entry kept in fields, and at least one slot is free. For tests - scans the whole array.
     */
    void integrityCheck() {
        long occupied = 0;
        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key == NOT_ALLOWED_KEY) {
                if (data[i + 1] != missingValue) {
                    throw new IllegalStateException("integrityCheck: free slot " + i + " holds value " + data[i + 1]);
                }
                continue;
            }
            occupied++;
            final int reachableAt = HashingUtils.findFreeOffset(key, data, mask);
            if (reachableAt != i) {
                throw new IllegalStateException("integrityCheck: key=" + key + " stored at " + i
                        + " but probing from its home reaches " + reachableAt
                        + " (" + (data[reachableAt] == key ? "duplicate" : "unreachable") + ")");
            }
        }
        if (occupied + (hasZeroKey ? 1 : 0) != size) {
            throw new IllegalStateException("integrityCheck: size=" + size + " but " + occupied + " occupied slots"
                    + (hasZeroKey ? " and the key in fields" : ""));
        }
        if (occupied > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
