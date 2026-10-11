package exchange.core2.collections.hashtable;


import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class LongLongHashtable implements ILongLongHashtable {


    public static final int DEFAULT_ARRAY_SIZE = 16;

    private final float upsizeThresholdPerc;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;

    public LongLongHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongLongHashtable(int size) {

        this.upsizeThresholdPerc = 0.65f;
        final int arraySize = HashingUtils.capacityFor(size, upsizeThresholdPerc);

        this.data = new long[arraySize * 2];
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    @Override
    public long put(long key, long value) {

        if (key == NOT_ALLOWED_KEY) throw new IllegalArgumentException("Not allowed key " + NOT_ALLOWED_KEY);
        //        log.debug("PUT key:{} val:{}", key, value);
        final int offset = HashingUtils.findFreeOffset(key, data, mask);

        final long prevValue = data[offset + 1];
        if (data[offset] != key) {
            size++;
        }

        data[offset] = key;
        data[offset + 1] = value;

        if (size >= upsizeThreshold) {
            resize();
        }

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
            // never stored - and removeInternal would take the first gap it meets for this key
            return 0L;
        }
        return removeInternal(key, hash, data, mask);
    }

    private long removeInternal(long key, int hash, long[] datax, int maskx) {
        int lastPos = (hash & maskx);

        // try all keys until either gap (NOT_ALLOWED_KEY)
        long existingKey = datax[lastPos << 1];
        int gapPos = -1;
        long oldValue = 0L;
        while (true) {

            if (existingKey == key) {
                // desired key found
                gapPos = lastPos;
                oldValue = datax[(lastPos << 1) + 1];
                size--;
            }

            // try next element
            final int posNext = (lastPos + 1) & maskx;
            if (datax[posNext << 1] == NOT_ALLOWED_KEY) {
                break;
            } else {
                existingKey = datax[posNext << 1];
                lastPos = posNext;
            }
        }

        if (gapPos == -1) {
            // nothing to remove - can just return
            return 0L;
        } else {
            // doing cleanup starting from last entry (pos)
            moveGap(gapPos, lastPos, datax, maskx);
            return oldValue;
        }
    }


    private static void moveGap(int gapPos, int lastPos, long[] data, int mask) {

        // move gap to the right until it is at the last position
        while (gapPos != lastPos) {

            // find the greatest entry in a series that can fill the gap (hash < gap)
            int p = lastPos;
            while (true) {
                int h = HashingUtils.hash(data[p << 1]) & mask;
                boolean canFillGap = canFillGapAndFinish(p, h, gapPos, mask);
//                log.debug("try p={} h={} gapPos={}, canFillGapAndFinish={} ", p, h, gapPos, canFillGap);
                if (canFillGap) {
                    break;
                }

                p = (p - 1) & mask;

                if (p == gapPos) {
                    // reached beginning of series (all entries has desired position after the gap)
//                    log.debug("final x=lastPos={}", gapPos);
                    data[gapPos << 1] = NOT_ALLOWED_KEY;
                    data[(gapPos << 1) + 1] = 0;

                    return;
                }
            }

            // fill gap with movable entry
            data[gapPos << 1] = data[p << 1];
            data[(gapPos << 1) + 1] = data[(p << 1) + 1];

            gapPos = p;

//            log.debug("new gapPos={}", gapPos);
        }

//        log.debug("final gapPos=lastPos={}", gapPos);

        // because gap is at a last position of the series - it is safe to mark gap as empty and finish
        data[gapPos << 1] = NOT_ALLOWED_KEY;
        data[(gapPos << 1) + 1] = 0;

    }

    private void resize() {

        // log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (data.length * 2L > Integer.MAX_VALUE) {
            upsizeThreshold = Integer.MAX_VALUE;
            return;
        }

        final long[] data2;
        final HashtableResizer hashtableResizer = new HashtableResizer(data);
        // log.debug("Sync resizing...");
//        if (data.length >= 32768) { // TODO find right value
//            data2 = hashtableResizer.resizeParallelSync();
//        } else {
        data2 = hashtableResizer.resizeSync();
//        }
        switchToNewArray(data2);
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

    //public void extractMatching(long[] destArray, int destBits, int destMask) {
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

    void integrityCheck() {
        // TODO check all keys are reachable
        // TODO check size is correct
        // TODO check load factor is correct


    }

    public void printLayout(String comment) {

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                final int hash = HashingUtils.hash(key);
                final int targetPos = hash & mask;
            } else {
            }

            if (i > 256) {
                break;
            }
        }
    }

//    private int desiredPosition(long key) {
//        final int hash = HashingUtils.hash(key);
//        return (hash & mask) << 1;
//    }


    static boolean canFillGapAndFinish(int k, int h, int g, int mask) {
        return ((k - h) & mask) >= ((g - h) & mask);
    }
}
