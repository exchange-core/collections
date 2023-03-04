package exchange.core2.collections.hashtable;

import exchange.core2.collections.orderbook.naive.OrderBookNaiveImpl;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.LongStream;

public class LongLongHashtable implements ILongLongHashtable {

    private static final Logger log = LoggerFactory.getLogger(OrderBookNaiveImpl.class);


    private final float upsizeThresholdPerc;

    // async resize configuration
    private final boolean asyncUpsize;
    private final float upsizeBackpressureThresholdPerc = 0.80f;
    private final int asyncResizeThreshold = 2000000000; // TODO temporarily disabled


    private final Queue<Long> invalidationQueue = new ConcurrentLinkedQueue<>();

    private CompletableFuture<long[]> upsizeJob = null;

    private HashingUtils.MigrationState migrationState = HashingUtils.MigrationState.STATIC;

    private long[] data = new long[32];
    private long size = 0;
    private int mask = 15;
    private long upsizeThreshold = 10;

    private  int upsizeBackpressureThreshold = 10;

    public LongLongHashtable() {
        this.upsizeThresholdPerc = 0.65f;
        this.asyncUpsize = false;

        this.mask = (this.data.length / 2) - 1;

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

        if (size >= upsizeThreshold && migrationState == HashingUtils.MigrationState.STATIC) {
            resize();
        }

        if (asyncUpsize && migrationState == HashingUtils.MigrationState.MIGRATING) {
            invalidateSegment(offset / 2);
            finalizeUpsize();
            if (size > upsizeBackpressureThreshold) {
                final long nanoTimeLocked = System.nanoTime();
                log.warn("Backpressure lock: hashtable is {}% full and invalidationQueue has {} unprocessed ranges", upsizeBackpressureThresholdPerc * 100f, invalidationQueue.size());
                upsizeJob.join();
                log.info("Backpressure release after {}us", (System.nanoTime() - nanoTimeLocked) / 1000);
            }
        }

        return prevValue;
    }


    @Override
    public long get(long key) {
        if (asyncUpsize && migrationState == HashingUtils.MigrationState.MIGRATING) {
            finalizeUpsize();
        }

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

        final int hash = Hashing.hash(key);
        int lastPos = (hash & mask);

//        if(key == 7944361780287070016L || key == 2944695948955075277L) {
//            log.debug("REMOVE key:{} hash:{}(0x{})->lastPos:{}(0x{})", key, hash, String.format("%x", hash), lastPos, String.format("%x", lastPos));
//        }

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
        }

        // doing cleanup starting from last entry (pos)
//        log.debug("last pos={}", lastPos);

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

                    if (asyncUpsize && migrationState == HashingUtils.MigrationState.MIGRATING) {
                        invalidateSegment(gapPos);
                        finalizeUpsize();
                    }

                    return oldValue;
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


        if (asyncUpsize && migrationState == HashingUtils.MigrationState.MIGRATING) {
            invalidateSegment(gapPos);
            finalizeUpsize();
        }


//        log.debug("return oldValue={}", oldValue);
        return oldValue;

    }

    private void invalidateSegment(int lastPos) {
        // invalidating whole segment, including lastPos position
        int i = lastPos;
        do {
            i = (i - 1) & mask;
        } while (data[i << 1] != HashingUtils.NOT_ALLOWED_KEY);

        invalidationQueue.add(((long) i << 32) + lastPos);
    }

    private void finalizeUpsize() {
        if (upsizeJob.isDone()) {

            log.debug("Migration is almost done! invalidationQueue.size={}", invalidationQueue.size());

            final long[] data2 = upsizeJob.join();

            HashtableResizer.replicate(data, data2, invalidationQueue);

            switchToNewArray(data2);
            migrationState = HashingUtils.MigrationState.STATIC;

            log.debug("Async RESIZE done, upsizeThreshold=" + upsizeThreshold);
        }
    }

    private void resize() {

        log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (data.length * 2L > Integer.MAX_VALUE) {
            log.warn("WARN: Can not upsize hashtable - performance will degrade gradually");
            upsizeThreshold = Integer.MAX_VALUE;
            upsizeBackpressureThreshold = Integer.MAX_VALUE;
            return;
        }

        final HashtableResizer hashtableResizer = new HashtableResizer(data, invalidationQueue);

        if (data.length < asyncResizeThreshold) {

            log.debug("Sync resizing...");
            final long[] data2 = hashtableResizer.resizeSync();

            switchToNewArray(data2);
            log.debug("RESIZE done, upsizeThreshold=" + upsizeThreshold);
        } else {

            log.debug("Async resizing...");
            migrationState = HashingUtils.MigrationState.MIGRATING;

            upsizeJob = hashtableResizer.resizeAsync();
        }
    }

    private void switchToNewArray(long[] data2) {
        this.data = data2;
        mask = mask * 2 + 1;
        upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
        upsizeBackpressureThreshold = (int) ((mask + 1) * upsizeBackpressureThresholdPerc);
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
