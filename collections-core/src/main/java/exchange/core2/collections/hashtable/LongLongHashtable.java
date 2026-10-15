package exchange.core2.collections.hashtable;


import java.util.function.LongUnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

/**
 * Primitive {@code long} to {@code long} hashtable: open addressing with linear probing, keys and values interleaved
 * in one {@code long[]}, backward-shift deletion (no tombstones).
 * <ul>
 *     <li>Any key, {@code 0} included: key {@code 0} marks the empty slots, so its entry is kept in two fields.</li>
 *     <li>A missing key reads as {@code missingValue} (constructor, {@code 0} by default) - see
 *     {@link ILongLongHashtable}.</li>
 *     <li>Grows by doubling at load factor 0.65. The resize copies the whole table on the calling thread (~7ns per
 *     slot: ~7ms at 2^20 slots) - pre-size the table (constructor, {@link #ensureCapacity}), or use
 *     {@link LongLongLL2Hashtable} where that pause matters.</li>
 *     <li>Maximum capacity: 2^29 slots, a {@code long[2^30]} (8 GB). There the table stops growing and takes new keys
 *     up to load 0.9, after that a {@code put} of a new key throws {@link IllegalStateException}.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 */
public final class LongLongHashtable implements ILongLongHashtable {


    public static final int DEFAULT_ARRAY_SIZE = 16;

    private final float upsizeThresholdPerc;
    private final int maxCapacity;

    private final long missingValue;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;

    // key 0 marks the gaps in data, so its entry is kept here (counted in size)
    private boolean hasZeroKey;
    private long zeroKeyValue;

    public LongLongHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    /**
     * @param size expected number of entries - that many fit without a resize
     */
    public LongLongHashtable(int size) {
        this(size, 0L);
    }

    /**
     * @param size         expected number of entries - that many fit without a resize
     * @param missingValue what get, put and remove return for a missing key
     */
    public LongLongHashtable(int size, long missingValue) {
        this(size, missingValue, HashingUtils.MAX_CAPACITY);
    }

    /**
     * @param maxCapacity capacity in slots the table stops growing at - lower than the real maximum only in tests
     */
    LongLongHashtable(int size, long missingValue, int maxCapacity) {

        this.upsizeThresholdPerc = 0.65f;
        this.missingValue = missingValue;
        this.maxCapacity = maxCapacity;
        final int arraySize = HashingUtils.capacityFor(size, upsizeThresholdPerc, maxCapacity);

        this.data = LinearProbing.newArray(arraySize, missingValue);
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    @Override
    public long put(long key, long value) {

        if (key == NOT_ALLOWED_KEY) return putZeroKey(value);
        //        log.debug("PUT key:{} val:{}", key, value);
        final int offset = HashingUtils.findFreeOffset(key, data, mask);

        if (data[offset] != key) {
            insertAt(offset, key, value);
            return missingValue;
        }

        final long prevValue = data[offset + 1];
        data[offset + 1] = value;
        return prevValue;
    }

    @Override
    public long putIfAbsent(long key, long value) {

        if (key == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : putZeroKey(value);
        final int offset = HashingUtils.findFreeOffset(key, data, mask);

        if (data[offset] == key) {
            return data[offset + 1];
        }
        insertAt(offset, key, value);
        return missingValue;
    }

    @Override
    public long addAndGet(long key, long delta) {

        if (key == NOT_ALLOWED_KEY) {
            final long value = (hasZeroKey ? zeroKeyValue : 0L) + delta;
            putZeroKey(value);
            return value;
        }
        final int offset = HashingUtils.findFreeOffset(key, data, mask);

        if (data[offset] == key) {
            return data[offset + 1] += delta;
        }
        insertAt(offset, key, delta);
        return delta;
    }

    @Override
    public long computeIfAbsent(long key, LongUnaryOperator mappingFunction) {

        if (key == NOT_ALLOWED_KEY) {
            if (hasZeroKey) {
                return zeroKeyValue;
            }
        } else {
            final int offset = HashingUtils.findFreeOffset(key, data, mask);
            if (data[offset] == key) {
                return data[offset + 1];
            }
        }
        final long value = mappingFunction.applyAsLong(key);
        // probe again rather than insert at the offset found before: the function may have changed the table
        put(key, value);
        return value;
    }

    /**
     * Stores a new key in the gap its probe stopped at.
     */
    private void insertAt(int offset, long key, long value) {
        data[offset] = key;
        data[offset + 1] = value;
        if (++size >= upsizeThreshold) {
            resize(offset);
        }
    }


    @Override
    public long get(long key) {
        if (key == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : missingValue;
        // a gap holds missingValue in its value cell
        return data[HashingUtils.findFreeOffset(key, data, mask) + 1];
    }


    @Override
    public boolean containsKey(long key) {
        if (key == NOT_ALLOWED_KEY) return hasZeroKey;
        return data[HashingUtils.findFreeOffset(key, data, mask)] == key;
    }

    @Override
    public long remove(long key) {
        if (key == NOT_ALLOWED_KEY) {
            return removeZeroKey();
        }
        final int offset = HashingUtils.findFreeOffset(key, data, mask);
        if (data[offset] != key) {
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
        if (key == NOT_ALLOWED_KEY) {
            if (hasZeroKey && zeroKeyValue == value) {
                removeZeroKey();
                return true;
            }
            return false;
        }
        final int offset = HashingUtils.findFreeOffset(key, data, mask);
        if (data[offset] != key || data[offset + 1] != value) {
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
            // can not grow any more: take new keys up to FULL_LOAD, then refuse them - beyond that the probes, and
            // with them every operation, get longer and longer
            final long limit = (long) ((mask + 1) * HashingUtils.FULL_LOAD);
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
     * Keys in the table order, key 0 first. The stream reads the table lazily - do not modify the table until it is
     * consumed.
     */
    @Override
    public LongStream keysStream() {
        final long[] d = data;
        final LongStream slots = IntStream.range(0, d.length >> 1)
                .filter(i -> d[i << 1] != NOT_ALLOWED_KEY)
                .mapToLong(i -> d[i << 1]);
        return hasZeroKey ? LongStream.concat(LongStream.of(0L), slots) : slots;
    }

    /**
     * Values in the same order as {@link #keysStream()}. Same restriction: do not modify the table until it is consumed.
     */
    @Override
    public LongStream valuesStream() {
        final long[] d = data;
        final LongStream slots = IntStream.range(0, d.length >> 1)
                .filter(i -> d[i << 1] != NOT_ALLOWED_KEY)
                .mapToLong(i -> d[(i << 1) + 1]);
        return hasZeroKey ? LongStream.concat(LongStream.of(zeroKeyValue), slots) : slots;
    }

    /**
     * Allocation-free iteration over all entries, key 0 first. The consumer must not modify the table.
     */
    @Override
    public void forEach(LongLongConsumer consumer) {
        if (hasZeroKey) {
            consumer.accept(0L, zeroKeyValue);
        }
        final long[] d = data;
        for (int i = 0; i < d.length; i += 2) {
            final long key = d[i];
            if (key != NOT_ALLOWED_KEY) {
                consumer.accept(key, d[i + 1]);
            }
        }
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
     * All entries as {@code {key=value, ...}} - for tests and debugging, it walks the whole table.
     */
    @Override
    public String toString() {
        return LinearProbing.entriesToString(hasZeroKey, zeroKeyValue, data);
    }

    /**
     * Verifies the linear-probing invariants: every key is found by probing from its home (no gap in its way, no
     * duplicate before it), free slots hold missingValue (what get returns for a missing key), size matches the
     * occupied slots plus key 0, and at least one slot is free. For tests - scans the whole array.
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
                    + (hasZeroKey ? " and key 0" : ""));
        }
        if (occupied > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
