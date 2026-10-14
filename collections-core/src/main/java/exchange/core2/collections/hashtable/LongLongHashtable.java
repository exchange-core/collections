package exchange.core2.collections.hashtable;


import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

/**
 * Primitive {@code long} to {@code long} hashtable: open addressing with linear probing, keys and values interleaved
 * in one {@code long[]}, backward-shift deletion (no tombstones).
 * <ul>
 *     <li>Key {@code 0} marks empty slots and can not be stored; a missing key reads as value {@code 0} - see
 *     {@link ILongLongHashtable}.</li>
 *     <li>Grows by doubling at load factor 0.65. The resize copies the whole table on the calling thread (~7ns per
 *     slot: ~7ms at 2^20 slots) - pre-size the table, or use {@link LongLongLL2Hashtable} where that pause
 *     matters.</li>
 *     <li>Maximum capacity: 2^29 slots, a {@code long[2^30]} (8 GB). There the table stops growing and takes new keys
 *     up to load 0.9, after that a {@code put} of a new key throws {@link IllegalStateException}.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 */
public class LongLongHashtable implements ILongLongHashtable {


    public static final int DEFAULT_ARRAY_SIZE = 16;

    private final float upsizeThresholdPerc;
    private final int maxCapacity;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;

    public LongLongHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongLongHashtable(int size) {
        this(size, HashingUtils.MAX_CAPACITY);
    }

    /**
     * @param maxCapacity capacity in slots the table stops growing at - lower than the real maximum only in tests
     */
    LongLongHashtable(int size, int maxCapacity) {

        this.upsizeThresholdPerc = 0.65f;
        this.maxCapacity = maxCapacity;
        final int arraySize = HashingUtils.capacityFor(size, upsizeThresholdPerc);
        if (arraySize > maxCapacity) {
            throw new IllegalArgumentException("expected size " + size + " needs " + arraySize
                    + " slots, more than the maximum capacity " + maxCapacity);
        }

        this.data = new long[arraySize * 2];
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    @Override
    public long put(long key, long value) {

        if (key == NOT_ALLOWED_KEY) throw new IllegalArgumentException("Not allowed key " + NOT_ALLOWED_KEY);
        //        log.debug("PUT key:{} val:{}", key, value);
        final int offset = HashingUtils.findFreeOffset(key, data, mask);

        if (data[offset] != key) {
            data[offset] = key;
            data[offset + 1] = value;
            if (++size >= upsizeThreshold) {
                resize(offset);
            }
            return 0L;
        }

        final long prevValue = data[offset + 1];
        data[offset + 1] = value;
        return prevValue;
    }


    @Override
    public long get(long key) {
        final int offset = HashingUtils.findFreeOffset(key, data, mask);
        return data[offset + 1];
    }


    @Override
    public boolean containsKey(long key) {
        // the probe stops either at the key or at a gap - and a gap holds key 0, which is why 0 is excluded
        return key != NOT_ALLOWED_KEY && data[HashingUtils.findFreeOffset(key, data, mask)] == key;
    }

    @Override
    public long remove(long key) {
        return remove(key, HashingUtils.hash(key));
    }

    public long remove(long key, int hash) {
        if (key == NOT_ALLOWED_KEY) {
            // never stored - and the probe would stop at the first gap, which holds key 0
            return 0L;
        }
        final int offset = HashingUtils.findFreeOffset(key, (hash & mask) << 1, data);
        if (data[offset] != key) {
            return 0L;
        }
        final long value = data[offset + 1];
        LinearProbing.deleteAt(data, offset);
        size--;
        return value;
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
            LinearProbing.deleteAt(data, insertedOffset);
            size--;
            throw new IllegalStateException("table is full: " + size + " entries at the maximum capacity of "
                    + (mask + 1) + " slots");
        }

        switchToNewArray(new HashtableResizer(data).resizeSync());
    }

    private void switchToNewArray(long[] data2) {
        this.data = data2;
        mask = mask * 2 + 1;
        upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    /**
     * Removes all entries, keeping the current capacity.
     */
    @Override
    public void clear() {
        Arrays.fill(data, 0L);
        size = 0;
    }

    /**
     * Keys in the table order. The stream reads the table lazily - do not modify the table until it is consumed.
     */
    @Override
    public LongStream keysStream() {
        final long[] d = data;
        return IntStream.range(0, d.length >> 1)
                .filter(i -> d[i << 1] != NOT_ALLOWED_KEY)
                .mapToLong(i -> d[i << 1]);
    }

    /**
     * Values in the same order as {@link #keysStream()}. Same restriction: do not modify the table until it is consumed.
     */
    @Override
    public LongStream valuesStream() {
        final long[] d = data;
        return IntStream.range(0, d.length >> 1)
                .filter(i -> d[i << 1] != NOT_ALLOWED_KEY)
                .mapToLong(i -> d[(i << 1) + 1]);
    }

    /**
     * Allocation-free iteration over all entries. The consumer must not modify the table.
     */
    @Override
    public void forEach(LongLongConsumer consumer) {
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

    public long upsizeThreshold() {
        return upsizeThreshold;
    }

    public void extractMatching(LongLongHashtable dest, int destBits, int destMask) {

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                final int hash = HashingUtils.hash(key);
                final boolean match = (hash & destMask) == destBits;
//                log.debug("key={} hash={} pos={} match={}", key, String.format("%08X", hash), i >> 1, match);
                if (match) {
                    dest.put(key, data[i + 1]);
                    data[i] = NOT_ALLOWED_KEY;
                    data[i + 1] = 0;
                    size--;
                }
            }
        }

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                final int hash = HashingUtils.hash(key);
                int j = (hash & mask) << 1;

//                log.debug("key={} hash={} curPos={} minPos={}", key, String.format("%08X", hash), i >> 1, j >> 1);

                while (j != i) {
                    if (data[j] == NOT_ALLOWED_KEY) {
                        // found gap
                        data[j] = data[i];
                        data[j + 1] = data[i + 1];
                        data[i] = NOT_ALLOWED_KEY;
                        data[i + 1] = 0;

//                        log.debug("replaced to {}", j >> 1);
                        break;
                    }

                    j += 2;
                    if (j == data.length) {
                        j = 0;
                    }
                }
            }
        }

    }

    /**
     * Verifies the linear-probing invariants: every key is found by probing from its home (no gap in its way, no
     * duplicate before it), free slots hold value 0 (a missing key reads as the value of the gap its probe stops at),
     * size matches the occupied slots, and at least one slot is free. For tests - scans the whole array.
     */
    void integrityCheck() {
        long occupied = 0;
        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key == NOT_ALLOWED_KEY) {
                if (data[i + 1] != 0) {
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
        if (occupied != size) {
            throw new IllegalStateException("integrityCheck: size=" + size + " but " + occupied + " occupied slots");
        }
        if (occupied > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
