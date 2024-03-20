package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class LongLongLL2Hashtable implements ILongLongHashtable {


    // TODO remove
    // private final Long2LongHashMap actions = new Long2LongHashMap(0L);


//    private void setAction(long key, int flag) {
//        actions.put(key, actions.get(key) | (1L << flag));
//    }
//
//    public String getActions(long key) {
//        String res = "";
//        final long a = actions.get(key);
//        for (int i = 0; i < 63; i++) {
//            if ((a & (1L << i)) != 0) {
//                res += (" " + i);
//            }
//        }
//        return res;
//    }

    private static final Logger log = LoggerFactory.getLogger(LongLongLL2Hashtable.class);

    public static final int DEFAULT_ARRAY_SIZE = 32;

    private final float upsizeThresholdPerc;
    private final float blockThresholdPerc;

    private final Executor executor;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;
    private long blockThreshold;
    private CompletableFuture<long[]> arrayFeature = null;
    private CompletableFuture<Void> copyingProcess = null;

    private HashtableAsync2Resizer resizer = null;

    private int allowedPosition = -1;
    private int knownProgressCached = -1;

    public LongLongLL2Hashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongLongLL2Hashtable(int size) {
        this(size, job -> new Thread(job).start());
    }

    public LongLongLL2Hashtable(Executor executor) {
        this(DEFAULT_ARRAY_SIZE, executor);
    }

    public LongLongLL2Hashtable(int size, Executor executor) {
        this.executor = executor;
        this.upsizeThresholdPerc = 0.65f;
        final int arraySize = BitUtil.findNextPositivePowerOfTwo((int) (size / upsizeThresholdPerc));

        this.data = new long[arraySize * 2];
        //this.blockThresholdPerc = 0.6501f;
        this.blockThresholdPerc = 0.90f;
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = calculateUpsizeThreshold();
        this.blockThreshold = calculateBlockThreshold();
    }

    @Override
    public long put(long key, long value) {


//        setAction(key, 1);

        if (arrayFeature != null) {
            if (size >= blockThreshold) {
                log.warn("BLOCKED: blockThreshold={} wait initialization of array...", blockThreshold);
                arrayFeature.join();
                log.warn("UNBLOCKED");
            }
            startAsyncCopyingIfDone(key, 2);
        }

//        if (resizer != null){
//            log.warn("size={} blockThreshold={}", size, blockThreshold);
//        }


        if (key == NOT_ALLOWED_KEY) throw new IllegalArgumentException("Not allowed key " + NOT_ALLOWED_KEY);

        if (resizer != null) {

//            setAction(key, 3);
            // TODO slow ?
            knownProgressCached = resizer.getProcessedPosition();

            if (size >= blockThreshold) {

                log.warn("BLOCKED: blockThreshold={} allow copy till {}", blockThreshold, resizer.getStartingPosition());
                allowedPosition = resizer.getStartingPosition();
                resizer.setAllowedPosition(allowedPosition);
                // log.debug("A knownProgressCached = {} (allowedPosition={})", knownProgressCached, allowedPosition);

                copyingProcess.join();
//                while (knownProgressCached != allowedPosition) {
//                    knownProgressCached = resizer.getProcessedPosition();
//                    // log.debug("B knownProgressCached = {} (allowedPosition={})", knownProgressCached,allowedPosition);
//                    Thread.onSpinWait();
//                }

                switchToNewArray();
                log.warn("UNBLOCKED");

            } else if (knownProgressCached == allowedPosition) {

//                setAction(key, 4);

                enableNextMigrationSegmentOrFinishMigration();
            }
        }


        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;

        if (resizer == null) {

            // no resizing in progress, default behavior - always put into "data"

//            setAction(key, 6);

            //        log.debug("PUT key:{} val:{}", key, value);
            final int offset = HashingUtils.findFreeOffset(key, pos, data);

            final long prevValue = data[offset + 1];
            if (data[offset] != key) {
                size++;
            }

            data[offset] = key;
            data[offset + 1] = value;

            if (arrayFeature == null && size >= upsizeThreshold) {
                resize();
            }

            return prevValue;
        }


        int g0 = resizer.getStartingPosition();

        if (resizer.isInOldData(pos, allowedPosition) || pos == g0) {

//            setAction(key, 8);


            // TODO check ??
//            if (g0 == allowedPosition) {
//                //throw new IllegalStateException("should be migrated");
//                log.error("########## should be migrated !!! pos={} A=S={}", pos, g0);
//                LockSupport.parkNanos(111);
//            }


            // is in old data (or at starting position)
            final int offset = HashingUtils.findFreeOffset(key, pos, data);
            if (offset != g0) {

//                setAction(key, 10);


                // is in old data - can safely insert
                return putInternalInsert(data, offset, key, value);

            } else {

//                setAction(key, 13);


                if (g0 == allowedPosition) {
                    log.debug("PUT {}: SUPER_RARE can not extend migrated cluster backwards, have to wait for completion" +
                            " pos={} offset={} A=S={}", key, pos, offset, g0);

                    // can not extend backwards, need to wait for completion (TODO extract method)
                    while (knownProgressCached != allowedPosition) {
                        knownProgressCached = resizer.getProcessedPosition();
                        // log.debug("B knownProgressCached = {} (allowedPosition={})", knownProgressCached,allowedPosition);
                        Thread.onSpinWait();
                    }
                    switchToNewArray();
                    // can safely insert
                    int pos2 = (hash & mask) << 1;
                    final int offset2 = HashingUtils.findFreeOffset(key, pos2, data);
                    log.debug("SUPER_RARE pos={} pos2={} offset={}, offset2={}", pos, pos2, offset, offset2);
                    return putInternalInsert(data, offset2, key, value);
                }

                // rare scenario: offset reached startPos, which always stay empty (gap)
                // have to extend migrated area and shift starting position back
                // assuming it can only reach allowedPosition, but not before

                //log.debug("PUT {}: RARE Extending backwards section g0={}", key, g0);
                do {
                    if (g0 == 0) {
                        g0 = data.length;
                    }
                    g0 -= 2;
                    //log.debug("PUT {}: check {} if busy..", key, g0);
                } while (data[g0] != NOT_ALLOWED_KEY);

                // note: it is possible g0=allowedPosition now
                //log.debug("Found new g0={}, migrating from old g0={}", g0, offset);
                final long[] data2 = resizer.getNewDataArray();
                int pos2 = (g0 + 2);
                pos2 = pos2 == data.length ? 0 : pos2;
                while (pos2 != offset) {
                    final long key2 = data[pos2];
                    if (key2 == NOT_ALLOWED_KEY) {
                        throw new IllegalStateException("should not migrate empty key");
                    }

                    final int offset2 = HashingUtils.findFreeOffset(key2, data2, resizer.getNewMask());
                    //log.debug("move pos2={}(old) offset2={}(new) k={} v={}", pos2, offset2, key2, data[pos2 + 1]);
                    data2[offset2] = key2;
                    data2[offset2 + 1] = data[pos2 + 1];
                    pos2 += 2;
                    pos2 = pos2 == data.length ? 0 : pos2;
                }

                // offset will be different for data2
                final int offsetFinal = HashingUtils.findFreeOffset(key, data2, resizer.getNewMask());

                resizer.setStartingPosition(g0);
                final long prevVal = putInternalInsert(data2, offsetFinal, key, value);
                //log.debug("DONE RARE: offsetFinal={} prevVal={}", offsetFinal, prevVal);
                return prevVal;
            }
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {

//            setAction(key, 15);


            knownProgressCached = resizer.getProcessedPosition();
        }


        while (resizer.notInNewData(pos, knownProgressCached)) {
            knownProgressCached = resizer.getProcessedPosition();
            if (knownProgressCached == allowedPosition) {
                break;
            }
            Thread.onSpinWait();
        }

        // pos is not applicable for migrated array (2x larger), calculating offsetNew
        final long[] dataNew = resizer.getNewDataArray();
        final int offsetNew = HashingUtils.findFreeOffset(key, dataNew, resizer.getNewMask());


   //     boolean logLastRes = false;

        // rare scenario: offset reached gp - can not just put there -  must stay always empty
        // just re-read gp
        // assume nothing can be inserted at gp offsets
//        while ((offsetNew & mask) == knownProgressCached) { // TODO incorrect ??? check assumption (mask)
//            logLastRes = true;
//            log.debug("PUT {}: RARE offset offsetNew ({}) & mask == knownProgressCached ({})", key, offsetNew, knownProgressCached);
//            Thread.yield();
//            knownProgressCached = resizer.getProcessedPosition();
//
//            if (knownProgressCached == allowedPosition) {
//                log.debug("PUT {}: RARE knownProgressCached {} = allowedPosition = {}", key, knownProgressCached, allowedPosition);
//
//                enableNextMigrationSegmentOrFinishMigration();
//                if (resizer == null) {
//                    final int offset1 = HashingUtils.findFreeOffset(key, data, mask);
//                    long prevVal = putInternalInsert(data, offset1, key, value);
//                    log.debug("PUT {}: RARE migration finished, put normally offset1={}, prevVal={}", key, offset1, prevVal);
//                    return prevVal;
//                    //break;
//                }
//
//                // TODO this case more complicated???
//            }
//            // log.debug("PUT {}: RARE finished resizer={}", key, resizer);
//        }

//        if ((offsetNew & mask) == knownProgressCached) {
//            try {
//                Thread.sleep(10000000000L);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }

//        setAction(key, 17);


        long prevVal = putInternalInsert(dataNew, offsetNew, key, value);

        // TODO how is it possible to read non 0 value from GP ?
      //  if (logLastRes) log.debug("PUT {}: RARE proceeded normally offsetNew={}, prevVal={} newVal={}", key, offsetNew, prevVal, value);
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

      //  boolean migStated = false;
        if (arrayFeature != null) {
           startAsyncCopyingIfDone(key, 34);
        }

//        if (resizer != null) {
//            if (resizer.isFinished()) {
//                // log.debug("GET {}: finished can switch to new array", key);
//                switchToNewArray();
//            }
//        }

        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;

        if (resizer == null || resizer.isInOldData(pos, allowedPosition)) {
            // in old data
//            setAction(key, 36);

            final int offset = HashingUtils.findFreeOffset(key, pos, data);
            return data[offset + 1];
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
//            setAction(key, 37);
            knownProgressCached = resizer.getProcessedPosition();
        }


        while (resizer.notInNewData(pos, knownProgressCached)) {
//            setAction(key, 39);
            knownProgressCached = resizer.getProcessedPosition();
            if (knownProgressCached == allowedPosition) {
                break;
            }
            Thread.onSpinWait();
        }

        //      log.debug("GET {}: migrated, get from new array", key);

//        setAction(key, 42);

        final long[] newData = resizer.getNewDataArray();
        pos = (hash & resizer.getNewMask()) << 1;
        final int offset = HashingUtils.findFreeOffset(key, pos, newData);
        final long val = newData[offset + 1];

        knownProgressCached = resizer.getProcessedPosition();
        if (knownProgressCached == allowedPosition) {
            enableNextMigrationSegmentOrFinishMigration();
        }

        return val;
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

        if (arrayFeature != null) {
            startAsyncCopyingIfDone(key, 52);
        }

        if (resizer == null) {

            // in old data
//            setAction(key, 54);

            return removeInternal(key, hash, data, mask);
        }

        // TODO examine carefully scope of removal !!

        int pos = (hash & mask) << 1;

        if (resizer.isInOldData(pos, allowedPosition)) {

            // in old data 34
//            setAction(key, 56);

            return removeInternal(key, hash, data, mask);
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
            knownProgressCached = resizer.getProcessedPosition();
        }
        while (resizer.notInNewData(pos, knownProgressCached)) {
            knownProgressCached = resizer.getProcessedPosition();
            if (knownProgressCached == allowedPosition) {
                break;
            }
            Thread.onSpinWait();
        }

//        setAction(key, 59);

        // log.debug("REMOVE {}: migrated, removing from new", key);
        final long val = removeInternal(key, hash, resizer.getNewDataArray(), resizer.getNewMask());
        //    log.debug("REMOVE {}: Returning val={}", key, val);
        // migrated now

        knownProgressCached = resizer.getProcessedPosition();
        if (knownProgressCached == allowedPosition) {
            enableNextMigrationSegmentOrFinishMigration();
        }

        return val;
    }

    private long removeInternal(long key, int hash, long[] datax, int maskx) {
        int lastPos = (hash & maskx);

        // try all keys until either gap (NOT_ALLOWED_KEY)
        long existingKey = datax[lastPos << 1];
        int gapPos = -1;
        long oldValue = 0L;

        int iteration = 0;
        boolean found = false;

        while (true) {

            if (existingKey == key) {
                // desired key found
                gapPos = lastPos;
                oldValue = datax[(lastPos << 1) + 1];
                size--;
                found = true;
            }

            // try next element
            final int posNext = (lastPos + 1) & maskx;
            if (datax[posNext << 1] == NOT_ALLOWED_KEY) {
                break;
            } else {
                existingKey = datax[posNext << 1];
                lastPos = posNext;
            }

//            // TODO remove?
//            if (iteration++ > 10000) {
//                throw new IllegalStateException("TMP: 10000 iterations to remove element "
//                        + " key=" + key + " found=" + found + " size=" + size + " capaciy=" + data.length / 2
//                        + " blockThreshold=" + blockThreshold);
//            }
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

        //log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (data.length * 2L > Integer.MAX_VALUE) {
            log.warn("WARN: Can not upsize hashtable - performance will degrade gradually");
            upsizeThreshold = Integer.MAX_VALUE;
            return;
        }

        final boolean useSync = size < 8200;

        if (useSync) {
            final HashtableResizer hashtableResizer = new HashtableResizer(data);
            //log.debug("Sync resizing...");
            this.data = hashtableResizer.resizeSync();
            mask = mask * 2 + 1;
            upsizeThreshold = calculateUpsizeThreshold();
            blockThreshold = calculateBlockThreshold();
            //log.debug("SYNC RESIZE done, upsizeThreshold=" + upsizeThreshold);
        } else {
            //printLayout("BEFORE async RESIZE");
          //  log.info("(A) Allocating array async: long[{}] ...", data.length * 2);
            final String threadName = Thread.currentThread().getName();
            arrayFeature = CompletableFuture.supplyAsync(
                    () -> {
                      //  log.debug("(Allocating array for {}: long[{}] ...)", threadName, data.length * 2);
                        return new long[data.length * 2];
                    },
                    executor);

        }
    }


    private boolean startAsyncCopyingIfDone(long key, int action) {
        if (arrayFeature.isDone()) {

//            setAction(key, action);

            final int initialPos = HashtableAsync2Resizer.findNextGapPos(data, 0);

            knownProgressCached = initialPos;

            allowedPosition = HashtableAsync2Resizer.findNextGapPos(data, (initialPos + 1234) & (data.length - 1)); // TODO fix
          //  log.debug("initialPos={} allowedPosition={}", initialPos, allowedPosition);
            resizer = new HashtableAsync2Resizer(data, arrayFeature.join(), initialPos, allowedPosition);
            arrayFeature = null;

            final String threadName = Thread.currentThread().getName();
            copyingProcess = CompletableFuture.runAsync(
                    () -> {
                        final String prevThreadName = Thread.currentThread().getName(); // TODO remove
                        Thread.currentThread().setName(threadName + "COPY");
                        resizer.copy();
                        Thread.currentThread().setName(prevThreadName);
                    },
                    executor);
            return true;
        }

        return false;
    }


    private void enableNextMigrationSegmentOrFinishMigration() {
        final int startingPosition = resizer.getStartingPosition();

        //log.debug("enableNextMigrationSegmentOrFinishMigration: allowedPosition={} startingPosition={}", allowedPosition, startingPosition);

        if (allowedPosition == startingPosition) {
            // migration completed
            switchToNewArray();
        } else {


            //log.debug("findNextGapPos after {}", (allowedPosition + 3114) & (data.length - 1));

            int newAllowedPosition = HashtableAsync2Resizer.findNextGapPos(data, (allowedPosition + 2114) & (data.length - 1));

            if (!resizer.isInOldData(newAllowedPosition, allowedPosition)) { // TODO check correctness
                //log.debug("Override newAllowedPosition={} with startingPosition={}", newAllowedPosition, startingPosition);
                newAllowedPosition = startingPosition;
            }

            //log.debug("new allowedPosition: {} (data_len={})", newAllowedPosition, data.length);

            allowedPosition = newAllowedPosition;
            resizer.setAllowedPosition(allowedPosition);
        }
    }

    private void blockOnResizing() {
        throw new UnsupportedOperationException();
        // TODO will not work
//        if (resizer != null) {
//            log.debug("Active resizing found");
//            copyingProcess.join();
//            switchToNewArray();
//            log.debug("ASYNC RESIZE done, upsizeThreshold=" + upsizeThreshold);
//            // printLayout("AFTER async RESIZE");
//        }
    }


    private void switchToNewArray() {
        //log.debug("switchToNewArray");

        // can finalize migration
        resizer.setAllowedPosition(-1);
        knownProgressCached = -1;

        this.data = resizer.getNewDataArray();
        mask = mask * 2 + 1;
        upsizeThreshold = calculateUpsizeThreshold();
        blockThreshold = calculateBlockThreshold();
        resizer = null;
        copyingProcess = null;
    }

    private long calculateUpsizeThreshold() {
        return (int) ((mask + 1) * upsizeThresholdPerc);
    }

    private long calculateBlockThreshold() {
        return (int) ((mask + 1) * blockThresholdPerc);
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

    @Deprecated
    public int mask() {
        return mask;
    }

    public long upsizeThreshold() {
        return upsizeThreshold;
    }

    //public void extractMatching(long[] destArray, int destBits, int destMask) {
    public void extractMatching(LongLongLL2Hashtable dest, int destBits, int destMask) {

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
