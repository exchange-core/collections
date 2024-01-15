package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class LongLongLL2Hashtable implements ILongLongHashtable {

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

        if (arrayFeature != null) {
            startAsyncCopying();
        }

//        if (resizer != null){
//            log.warn("size={} blockThreshold={}", size, blockThreshold);
//        }


        if (key == NOT_ALLOWED_KEY) throw new IllegalArgumentException("Not allowed key " + NOT_ALLOWED_KEY);

        if (resizer != null) {

            // TODO slow ?
            knownProgressCached = resizer.getProcessedPosition();

            if (size >= blockThreshold) {

                log.warn("BLOCKED: blockThreshold={} allow copy till {}", blockThreshold, resizer.getStartingPosition());
                allowedPosition = resizer.getStartingPosition();
                resizer.setAllowedPosition(allowedPosition);
                log.debug("A knownProgressCached = {} (allowedPosition={})", knownProgressCached, allowedPosition);
                while (knownProgressCached != allowedPosition) {
                    knownProgressCached = resizer.getProcessedPosition();
                    // log.debug("B knownProgressCached = {} (allowedPosition={})", knownProgressCached,allowedPosition);
                    Thread.onSpinWait();
                }

                switchToNewArray();
                log.warn("UNBLOCKED");

            } else if (knownProgressCached == allowedPosition) {

                enableNextMigrationSegmentOrFinishMigration();
            }
        }


        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;

        if (resizer == null) {
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

            // TODO check ??
            if(g0 == allowedPosition){
                /*
22:55:55.614 [main] INFO  e.c.c.h.LongLongHashtableAbstractTest - DONE iteration: 92
22:55:55.731 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[65536] ...
22:55:55.731 [Thread-1116] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - (Allocating array: long[65536] ...)
22:55:55.731 [Thread-1117] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 16384->32768 -----------------
22:55:55.731 [Thread-1117] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
22:55:55.735 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[131072] ...
22:55:55.735 [Thread-1118] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - (Allocating array: long[131072] ...)
22:55:55.735 [Thread-1119] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 32768->65536 -----------------
22:55:55.736 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT -689037631452771687: RARE Extending backwards section g0=4
22:55:55.736 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - Found new g0=65450, migrating from old g0=4
22:55:55.736 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - DONE RARE: offsetFinal=131028 prevVal=0
22:55:55.736 [Thread-1119] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
22:55:55.742 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[262144] ...
22:55:55.742 [Thread-1120] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - (Allocating array: long[262144] ...)
22:55:55.743 [Thread-1121] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 65536->131072 -----------------
22:55:55.745 [Thread-1121] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
22:55:55.757 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[524288] ...
22:55:55.757 [Thread-1122] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - (Allocating array: long[524288] ...)
22:55:55.760 [Thread-1123] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 131072->262144 -----------------

java.lang.IllegalStateException: should be migrated


23:20:36.587 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[524288] ...
23:20:36.587 [Thread-1122] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - (Allocating array: long[524288] ...)
23:20:36.589 [Thread-1123] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 131072->262144 -----------------
23:20:36.592 [main] ERROR e.c.c.hashtable.LongLongLL2Hashtable - ########## should be migrated !!! pos=0 A=S=0
23:20:36.592 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT -1268289157630876716: RARE Extending backwards section g0=0
23:20:36.592 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - Found new g0=262142, migrating from old g0=0
23:20:36.592 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - DONE RARE: offsetFinal=262144 prevVal=0
23:20:36.593 [Thread-1123] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------

21:34:29.849 [Thread-5916] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - (Allocating array: long[65536] ...)
21:34:29.850 [Thread-5917] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 16384->32768 -----------------
21:34:29.850 [main] ERROR e.c.c.hashtable.LongLongLL2Hashtable - ########## should be migrated !!! pos=4 A=S=4
21:34:29.850 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT 8112868677862138312: RARE Extending backwards section g0=4
21:34:29.850 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - Found new g0=32766, migrating from old g0=4
21:34:29.850 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - DONE RARE: offsetFinal=4 prevVal=0
21:34:29.850 [Thread-5917] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------

21:35:45.631 [Thread-8685] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 262144->524288 -----------------
21:35:45.635 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT 8353484598072239349: RARE Extending backwards section g0=16
21:35:45.635 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - Found new g0=524284, migrating from old g0=16
21:35:45.635 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - DONE RARE: offsetFinal=524296 prevVal=0
21:35:45.637 [main] ERROR e.c.c.hashtable.LongLongLL2Hashtable - ########## should be migrated !!! pos=524284 A=S=524284
21:35:45.637 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT 4163035467544519845: RARE Extending backwards section g0=524284
21:35:45.637 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - Found new g0=524274, migrating from old g0=524284
21:35:45.637 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - DONE RARE: offsetFinal=524284 prevVal=0
21:35:45.639 [Thread-8685] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------

                 */
                //throw new IllegalStateException("should be migrated");
                log.error("########## should be migrated !!! pos={} A=S={}", pos, g0);
                LockSupport.parkNanos(111);
            }



            // is in old data (or at starting position)
            final int offset = HashingUtils.findFreeOffset(key, pos, data);
            if (offset != g0) {

                // is in old data - can safely insert
                return putInternalInsert(data, offset, key, value);

            } else {


                if(g0 == allowedPosition){
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

                log.debug("PUT {}: RARE Extending backwards section g0={}", key, g0);
                do {
                    if (g0 == 0) {
                        g0 = data.length;
                    }
                    g0 -= 2;
                    //log.debug("PUT {}: check {} if busy..", key, g0);
                } while (data[g0] != NOT_ALLOWED_KEY);

                // note: it is possible g0=allowedPosition now
                log.debug("Found new g0={}, migrating from old g0={}", g0, offset);
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
                log.debug("DONE RARE: offsetFinal={} prevVal={}", offsetFinal, prevVal);
                return prevVal;
            }
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
            knownProgressCached = resizer.getProcessedPosition();
        }
        while (resizer.notInNewData(pos, knownProgressCached)) {
            knownProgressCached = resizer.getProcessedPosition();
            Thread.onSpinWait();
        }

        final long[] dataNew = resizer.getNewDataArray();
        final int offsetNew = HashingUtils.findFreeOffset(key, dataNew, resizer.getNewMask());

        // rare scenario: offset reached gp (stays always empty)
        // just re-read gp
        // assume nothing can be inserted at gp offsets
        while ((offsetNew & mask) == knownProgressCached) { // TODO incorrect ??? check assumption (mask)
            Thread.yield();

            log.debug("PUT {}: RARE offset & mask == knownProgressCached ({})", key, knownProgressCached);
            knownProgressCached = resizer.getProcessedPosition();

            if (knownProgressCached == allowedPosition) {
                enableNextMigrationSegmentOrFinishMigration();
                if (resizer == null) {
                    break;
                }
            }
            log.debug("PUT {}: RARE finished resizer={}", key, resizer);
        }

        return putInternalInsert(dataNew, offsetNew, key, value);
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

        if (arrayFeature != null) {
            startAsyncCopying();
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
            final int offset = HashingUtils.findFreeOffset(key, pos, data);
            return data[offset + 1];
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
            knownProgressCached = resizer.getProcessedPosition();
        }
        while (resizer.notInNewData(pos, knownProgressCached)) {
            knownProgressCached = resizer.getProcessedPosition();
            Thread.onSpinWait();
        }

        //      log.debug("GET {}: migrated, get from new array", key);


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
            startAsyncCopying();
        }

        if (resizer == null) {
            return removeInternal(key, hash, data, mask);
        }

        // TODO examine carefully scope of removal !!

        int pos = (hash & mask) << 1;

        if (resizer.isInOldData(pos, allowedPosition)) {
            return removeInternal(key, hash, data, mask);
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
            knownProgressCached = resizer.getProcessedPosition();
        }
        while (resizer.notInNewData(pos, knownProgressCached)) {
            knownProgressCached = resizer.getProcessedPosition();
            Thread.onSpinWait();
        }


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
            log.info("(A) Allocating array: long[{}] ...", data.length * 2);
            arrayFeature = CompletableFuture.supplyAsync(
                    () -> {
                        log.debug("(Allocating array: long[{}] ...)", data.length * 2);
                        return new long[data.length * 2];
                    },
                    executor);

        }
    }


    private void startAsyncCopying() {
        if (arrayFeature.isDone()) {

            final int initialPos = HashtableAsync2Resizer.findNextGapPos(data, 0);

            knownProgressCached = HashtableAsync2Resizer.findNextGapPos(data, (initialPos + 32) & (data.length - 1)); // TODO fix

            allowedPosition = HashtableAsync2Resizer.findNextGapPos(data, (knownProgressCached + 1234) & (data.length - 1)); // TODO fix
            resizer = new HashtableAsync2Resizer(data, arrayFeature.join(), initialPos, knownProgressCached, allowedPosition);
            arrayFeature = null;

            resizer.copyInterval(initialPos, knownProgressCached);

            //final String threadName = Thread.currentThread().getName();
            copyingProcess = CompletableFuture.runAsync(
                    () -> {
                        //Thread.currentThread().setName(threadName);
                        resizer.copy();
                    },
                    executor);
        }
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

            if (!resizer.isInOldData(newAllowedPosition, allowedPosition)) {
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
