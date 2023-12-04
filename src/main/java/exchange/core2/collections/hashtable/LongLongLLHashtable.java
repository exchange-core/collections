package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class LongLongLLHashtable implements ILongLongHashtable {

    private static final Logger log = LoggerFactory.getLogger(LongLongLLHashtable.class);

    public static final int DEFAULT_ARRAY_SIZE = 16;

    private final float upsizeThresholdPerc;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;

    private HashtableAsyncResizer resizer;

    public LongLongLLHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongLongLLHashtable(int arraySize) {

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
        if (resizer != null && resizer.isFinished()) {
            log.debug("PUT {}: finished can switch to new array", key);
            switchToNewArray();
        }

        if (key == NOT_ALLOWED_KEY) throw new IllegalArgumentException("Not allowed key " + NOT_ALLOWED_KEY);

        if (resizer == null) {

            //        log.debug("PUT key:{} val:{}", key, value);
            final int offset = HashingUtils.findFreeOffset(key, data, mask);

            final long prevValue = data[offset + 1];
            if (data[offset] != key) {
                size++;
            }

            data[offset] = key;
            data[offset + 1] = value;

            if (size >= upsizeThreshold && resizer == null) {
                resize();
            }

            return prevValue;
        }

        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;

        int gp = resizer.getGp();
        int g0 = resizer.getG0();

        if (HashtableAsyncResizer.isInMigratedSegment(pos, g0, gp)) {
            log.debug("PUT: {} put {} In MigratedSegment {} {}", key, pos, g0, gp);

            final int offset = HashingUtils.findFreeOffset(key, resizer.getNewDataArray(), resizer.getNewMask());

            // rare scenario: offset reached gp (stays always empty)
            // just re-read gp
            // assume nothing can be inserted at gp offsets
            while ((offset & mask) == gp) { // TODO incorrect ??? (mask)
                Thread.yield();
                if (resizer.isFinished()) {
                    break;
                }
                log.debug("PUT {}: RARE Rereading gp before = {} (offset:{} +mask:{})", key, gp, offset, offset & mask);
                gp = resizer.getGp();
                log.debug("PUT {}: RARE Rereading gp after = {}", key, gp);
            }

            return putInternalInsert(resizer.getNewDataArray(), offset, key, value);

        }

        log.debug("PUT {}: Requesting pause...", key);
        resizer.pause();
        log.debug("PUT {}: Pause granted, processing", key);


        gp = resizer.getGp();
        if (HashtableAsyncResizer.isInMigratedSegment(pos, g0, gp)) {
            // migrated now
            log.debug("PUT {}: insert into new array", key);
            final int offset = HashingUtils.findFreeOffset(key, resizer.getNewDataArray(), resizer.getNewMask());

            // rare scenario: offset reached gp when on pause
            // resume-pause-read gp (g0 can not be changed by migrator)
            while ((offset & mask) == gp) { // TODO incorrect ??? (mask)
                log.debug("PUT {}: RARE gp {} == (offset:{} +mask:{}), resuming and then requesting pause", key, gp, offset, offset & mask);
                resizer.resume();
                Thread.yield();
                resizer.pause();
                if (resizer.isFinished()) {
                    break;
                }
                gp = resizer.getGp();
                log.debug("PUT {}: Rereading gp after pause = {}", key, gp);
            }

            final long prevVal = putInternalInsert(resizer.getNewDataArray(), offset, key, value);
            resizer.resume();
            return prevVal;
        }

        // not migrated yet (TODO NOTE: wont work for PUT because of possible g0/gp extension)
        final int offset = HashingUtils.findFreeOffset(key, data, mask);

        log.debug("PUT {}: insert into old array, pos={} (gp={})", key, offset, gp);

        // TODO  offset == gp possible RARE ^^^^^



        // rare scenario: offset reached g0 (stays always empty)
        // have to extend g0 (surely g0 != gp now)
        if (offset == g0) {
            log.debug("PUT {}: RARE Extending backwards section g0={}", key, g0);
            do {
                if (g0 == 0) {
                    g0 = data.length;
                }
                g0 -= 2;
                log.debug("PUT {}: check {} if busy..", key, g0);
            } while (data[g0] != NOT_ALLOWED_KEY);

            // note: it is possible g0=gp now

            log.debug("Found new g0={}, migrating from old g0={} (gp={})", g0, offset, gp);
            final long[] data2 = resizer.getNewDataArray();
            int pos2 = (g0 + 2);
            pos2 = pos2 == data.length ? 0: pos2;
            do {
                final long key2 = data[pos2];
                final int offset2 = HashingUtils.findFreeOffset(key2, data2, resizer.getNewMask());
                log.debug("move pos2={}(old) offset2={}(new) k={} v={}", pos2, offset2, key2, data[pos2 + 1]);
                data2[offset2] = key2;
                data2[offset2 + 1] = data[pos2 + 1];
                pos2 += 2;
                pos2 = pos2 == data.length ? 0: pos2;
            } while (pos2 != offset);

            // offset will be different for data2
            final int offsetFinal = HashingUtils.findFreeOffset(key, data2, resizer.getNewMask());

            resizer.setG0(g0);
            final long prevVal = putInternalInsert(data2, offsetFinal, key, value);
            log.debug("DONE RARE: offsetFinal={} prevVal={}", offsetFinal, prevVal);

            resizer.resume();
            return prevVal;
        }

        // can proceed with old section and resume

        final long prevVal = putInternalInsert(data, offset, key, value);
        resizer.resume();
        return prevVal;
    }

    private long putInternalInsert(long[] datax, int offset, long key, long value) {
        if (datax[offset] != key) {
            size++;
        }
        datax[offset] = key;

        final long prevValue = datax[offset + 1];
        datax[offset + 1] = value;
        return prevValue;
    }


    @Override
    public long get(long key) {

        if (resizer != null && resizer.isFinished()) {
            log.debug("GET {}: finished can switch to new array", key);
            switchToNewArray();
        }

        if (resizer == null) {
            final int offset = HashingUtils.findFreeOffset(key, data, mask);
            return data[offset + 1];
        }

        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;

        if (resizer.isInMigratedSegment(pos)) {
            log.debug("GET {}: migrated, get from new array", key);
            return getFromNewDataArray(key);
        } else {
            log.debug("GET {}: Requesting pause...", key);
            resizer.pause();
            log.debug("GET {}: Pause granted, processing", key);
            final long val;
            if (resizer.isInMigratedSegment(pos)) {
                log.debug("GET {}: migrated, taking from new", key);
                // migrated now
                val = getFromNewDataArray(key);
            } else {
                log.debug("GET {}: not migrated, taking from old", key);
                // not migrated yet (TODO NOTE: wont work for PUT because of possible g0/gp extension)
                final int offset = HashingUtils.findFreeOffset(key, data, mask);
                val = data[offset + 1];
            }
            resizer.resume();
            log.debug("GET {}: Returning val={}", key, val);
            return val;
        }

    }

    private long getFromNewDataArray(long key) {
        final long[] data2 = resizer.getNewDataArray();
        final int offset = HashingUtils.findFreeOffset(key, data2, resizer.getNewMask());
        return data2[offset + 1];
    }

    @Override
    public boolean containsKey(long key) {
        return get(key) != NOT_ALLOWED_KEY;
    }

    @Override
    public long remove(long key) {
        return remove(key, Hashing.hash(key));
    }

    public long remove(long key, int hash) {

        // blockOnResizing();

        if (resizer != null && resizer.isFinished()) {
            log.debug("REMOVE {}: migration finished can switch to new array", key);
            switchToNewArray();
        }
        if (resizer == null) {
            return removeInternal(key, hash, data, mask);
        }

        int pos = (hash & mask) << 1;
        if (resizer.isInMigratedSegment(pos)) {
            log.debug("REMOVE {}: in migrated range, can remove from new array", key);
            return removeInternal(key, hash, resizer.getNewDataArray(), resizer.getNewMask());
        }

        log.debug("REMOVE {}: Requesting pause...", key);
        resizer.pause();
        log.debug("REMOVE {}: Pause granted, processing", key);
        final long val;
        if (resizer.isInMigratedSegment(pos)) {
            log.debug("REMOVE {}: migrated, removing from new", key);
            // migrated now
            val = removeInternal(key, hash, resizer.getNewDataArray(), resizer.getNewMask());
        } else {
            log.debug("REMOVE {}: not migrated, taking from old", key);
            // not migrated yet
            val = removeInternal(key, hash, data, mask);
        }
        resizer.resume();
        log.debug("REMOVE {}: Returning val={}", key, val);
        return val;
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

        log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (data.length * 2L > Integer.MAX_VALUE) {
            log.warn("WARN: Can not upsize hashtable - performance will degrade gradually");
            upsizeThreshold = Integer.MAX_VALUE;
            return;
        }

        final boolean useSync = size < 12;

        if (useSync) {
            final HashtableResizer hashtableResizer = new HashtableResizer(data);
            log.debug("Sync resizing...");
            this.data = hashtableResizer.resizeSync();
            mask = mask * 2 + 1;
            upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
            log.debug("SYNC RESIZE done, upsizeThreshold=" + upsizeThreshold);
        } else {
            printLayout("BEFORE async RESIZE");
            final HashtableAsyncResizer hashtableResizer = new HashtableAsyncResizer(data);
            log.debug("Async resizing...");
            hashtableResizer.resizeAsync();
            resizer = hashtableResizer;
        }
    }

    private void blockOnResizing() {
        if (resizer != null) {
            log.debug("Active resizing found");

            resizer.waitCompletion();
            switchToNewArray();
            log.debug("ASYNC RESIZE done, upsizeThreshold=" + upsizeThreshold);
            printLayout("AFTER async RESIZE");
        }
    }


    private void switchToNewArray() {
        this.data = resizer.getNewDataArray();
        mask = mask * 2 + 1;
        upsizeThreshold = (int) ((mask + 1) * upsizeThresholdPerc);
        resizer = null;
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
    public void extractMatching(LongLongLLHashtable dest, int destBits, int destMask) {

        blockOnResizing();

        log.info("Copying data to new table destBits={}, destMask={} ...",
            String.format("%08X", destBits), String.format("%08X", destMask));

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                final int hash = Hashing.hash(key);
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
        log.info("Copying completed, removing gaps...");

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                final int hash = Hashing.hash(key);
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

        log.info("Gaps removal completed");
    }

    void integrityCheck() {
        // TODO check all keys are reachable
        // TODO check size is correct
        // TODO check load factor is correct


    }

    public void printLayout(String comment) {

        log.debug("---- {} start --- size:{} --- capacity:{} --- ", comment, size, data.length / 2);

        blockOnResizing();

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                final int hash = Hashing.hash(key);
                final int targetPos = hash & mask;
                log.debug("{}. T:{} H:{} {}={}", i >> 1, targetPos, String.format("%08X", hash), key, data[i + 1]);
            } else {
                log.debug("{}. ---", i >> 1);
            }

            if (i > 256) {
                log.debug("... skip ...");
                break;
            }
        }
        log.debug("---- {} end --- ", comment);
    }

//    private int desiredPosition(long key) {
//        final int hash = Hashing.hash(key);
//        return (hash & mask) << 1;
//    }


    static boolean canFillGapAndFinish(int k, int h, int g, int mask) {
        return ((k - h) & mask) >= ((g - h) & mask);
    }
}
