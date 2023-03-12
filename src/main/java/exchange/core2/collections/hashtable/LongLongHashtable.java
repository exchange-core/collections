package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.LongStream;

public class LongLongHashtable implements ILongLongHashtable {

    private static final Logger log = LoggerFactory.getLogger(LongLongHashtable.class);

    public static final int DEFAULT_ARRAY_SIZE = 16;

    private final float upsizeThresholdPerc;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;

    public LongLongHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongLongHashtable(int arraySize) {

        if (!BitUtil.isPowerOfTwo(arraySize)) {
            throw new IllegalArgumentException();
        }

        this.data = new long[arraySize * 2];
        this.upsizeThresholdPerc = 0.65f;
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }


    @Override
    public long put(long key, long value) {
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
//        log.debug("GET key:{}", key);
        final int offset = HashingUtils.findFreeOffset(key, data, mask);
        return data[offset + 1];
    }

    @Override
    public boolean containsKey(long key) {
        return get(key) != HashingUtils.NOT_ALLOWED_KEY;
    }

    @Override
    public long remove(long key) {
        return remove(key, Hashing.hash(key));
    }

    public long remove(long key, int hash) {

        int lastPos = (hash & mask);

        // try all keys until either gap (NOT_ALLOWED_KEY)
        long existingKey = data[lastPos << 1];
        int gapPos = -1;
        long oldValue = 0L;
        while (true) {

            if (existingKey == key) {
                // desired key found
                gapPos = lastPos;
                oldValue = data[(lastPos << 1) + 1];
                size--;
//                log.debug("found prev value at gapPos={} oldValue={}", gapPos, oldValue);
            }

            // try next element
            final int posNext = (lastPos + 1) & mask;
            if (data[posNext << 1] == HashingUtils.NOT_ALLOWED_KEY) {
//                log.debug("done probing, {} is empty", posNext);
                break;
            } else {
                existingKey = data[posNext << 1];
                lastPos = posNext;
//                log.debug("try next pos={} existingKey={}", lastPos, existingKey);
            }
        }

        if (gapPos == -1) {
//            log.debug("nothing to remove, returning 0");
            // nothing to remove - can just return
            return 0L;
        } else {
            // doing cleanup starting from last entry (pos)
//        log.debug("last pos={}", lastPos);
            moveGap(gapPos, lastPos);
//        log.debug("return oldValue={}", oldValue);
            return oldValue;
        }
    }


    private void moveGap(int gapPos, int lastPos) {

        // move gap to the right until it is at the last position
        while (gapPos != lastPos) {

            // find the greatest entry in a series that can fill the gap (hash < gap)
            int p = lastPos;
            while (true) {
                int h = Hashing.hash(data[p << 1]) & mask;
                boolean canFillGap = canFillGapAndFinish(p, h, gapPos, mask);
//                log.debug("try p={} h={} gapPos={}, canFillGapAndFinish={} ", p, h, gapPos, canFillGap);
                if (canFillGap) {
                    break;
                }

                p = (p - 1) & mask;

                if (p == gapPos) {
                    // reached beginning of series (all entries has desired position after the gap)
//                    log.debug("final x=lastPos={}", gapPos);
                    data[gapPos << 1] = HashingUtils.NOT_ALLOWED_KEY;
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
        data[gapPos << 1] = HashingUtils.NOT_ALLOWED_KEY;
        data[(gapPos << 1) + 1] = 0;

    }

    private void resize() {

        log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (data.length * 2L > Integer.MAX_VALUE) {
            log.warn("WARN: Can not upsize hashtable - performance will degrade gradually");
            upsizeThreshold = Integer.MAX_VALUE;
            return;
        }

        final HashtableResizer hashtableResizer = new HashtableResizer(data);

        log.debug("Sync resizing...");
        final long[] data2 = hashtableResizer.resizeSync();

        switchToNewArray(data2);
        log.debug("RESIZE done, upsizeThreshold=" + upsizeThreshold);
    }

    private void switchToNewArray(long[] data2) {
        this.data = data2;
        mask = mask * 2 + 1;
        upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongStream keysStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongStream valuesStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEach(LongLongConsumer consumer) {
        throw new UnsupportedOperationException();
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
        log.info("Copying data to new table destBits={}, destMask={} ...",
            String.format("%08X", destBits), String.format("%08X", destMask));

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != HashingUtils.NOT_ALLOWED_KEY) {
                final int hash = Hashing.hash(key);
                final boolean match = (hash & destMask) == destBits;
//                log.debug("key={} hash={} pos={} match={}", key, String.format("%08X", hash), i >> 1, match);
                if (match) {
                    dest.put(key, data[i + 1]);
                    data[i] = HashingUtils.NOT_ALLOWED_KEY;
                    data[i + 1] = 0;
                    size--;
                }
            }
        }
        log.info("Copying completed, removing gaps...");

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != HashingUtils.NOT_ALLOWED_KEY) {
                final int hash = Hashing.hash(key);
                int j = (hash & mask) << 1;

//                log.debug("key={} hash={} curPos={} minPos={}", key, String.format("%08X", hash), i >> 1, j >> 1);

                while (j != i) {
                    if (data[j] == HashingUtils.NOT_ALLOWED_KEY) {
                        // found gap
                        data[j] = data[i];
                        data[j + 1] = data[i + 1];
                        data[i] = HashingUtils.NOT_ALLOWED_KEY;
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

        log.info("Gaps removal completed");
    }

    void integrityCheck() {
        // TODO check all keys are reachable
        // TODO check size is correct
        // TODO check load factor is correct


    }

//    private int desiredPosition(long key) {
//        final int hash = Hashing.hash(key);
//        return (hash & mask) << 1;
//    }


    static boolean canFillGapAndFinish(int k, int h, int g, int mask) {
        return ((k - h) & mask) >= ((g - h) & mask);
    }
}
