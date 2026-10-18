package exchange.core2.collections.hashtable;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

/**
 * One migration of a {@link LongLongLL2Hashtable}: the old array copied into the new one, a segment at a time. The
 * table authorizes a segment (allowedPosition); whoever gets to it first copies it - the worker of the
 * {@link MigrationService} ({@link #doWork()}), or the table itself where an operation needs the segment
 * ({@link #copyIfIdle()}). The state makes that exclusive: a CAS from IDLE takes the segment, the copier sets IDLE
 * again, with the progress.
 * <p>
 * The table does not wait for a worker that stopped in the middle of its portion - preempted, say: the worker copies
 * in batches of {@value #BATCH_LONGS} longs and starts each with a CAS, so the state always tells where its batch
 * begins. A table that sees no new batch for a while takes the portion over ({@link #takeOverIfStalled}): copies the
 * worker's batch again and the rest of what is authorized. Copying the batch again gives the same new array: the
 * entries go in in the same order, each to the first free slot of its probe - or to the one where the worker has
 * put it already. The worker, when it runs again, may still write the rest of its batch - the same entries to the
 * same slots - and then finds its CAS failed and stops. Until then the table does not change those entries
 * ({@link #frozen}, {@link #frozenWithin}): a removal or a new value there would be overwritten.
 * <p>
 * A new instance per migration - it doubles as its generation: a worker late on an old migration touches only the old
 * instance. Holds the arrays, never the table.
 */
final class HashtableAsync2Resizer {

    /**
     * allowedPosition value meaning "stop now" - published by the hashtable in switchToNewArray().
     */
    public static final int FINISH_SIGNAL = -1;

    /*
     * The state: progress << 32 | worker's batch start / 2 << 3 | ALL_MIGRATED | owner. The progress is an exclusive
     * bound: everything in [initial progress, progress) is in the new array, the progress itself is NOT copied yet -
     * it is always a free slot of the old array, a boundary no cluster crosses. ALL_MIGRATED: the whole table is in the
     * new array - needed because progress == startingPosition alone reads both "nothing copied" and "all copied".
     * The batch start is there while the worker owns the state.
     */
    private static final int IDLE = 0;
    private static final int WORKER = 1;
    private static final int TABLE = 2;
    private static final int FAILED = 3;
    private static final long OWNER = 3;
    private static final long ALL_MIGRATED = 4;
    private static final long BATCH = 0xFFFF_FFF8L;
    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(HashtableAsync2Resizer.class, "state", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * The worker's portion: ~256 slots, then on to the next free slot - a boundary no cluster crosses, like a segment
     * end. When the table authorizes more at once - the rest of the migration, at its block threshold or at
     * finishMigration() - the worker takes it portion by portion, the other tables get their turns in between, and the
     * table can take the rest itself.
     */
    static final int WORKER_PORTION_LONGS = 512;

    /**
     * The worker's batch: a CAS each, 32 slots - at most 32 entries the table may have to leave alone after a
     * take-over, until the worker runs again.
     */
    static final int BATCH_LONGS = 64;

    /**
     * Spins of a wait without a new batch of the worker before the table takes its portion over: ~2us at Skylake's
     * pause, a batch takes ~0.2us. A shorter pause takes over sooner - harmless, the worker stops at its next batch.
     */
    static final int TAKE_OVER_SPINS = 64;

    private final long[] srcData;
    private final long[] dstData;
    private final int newMask;

    /**
     * Written by the application (backwards cluster extension), read by the copier - the end of the
     * migration.
     */
    private volatile int startingPosition;

    private volatile int allowedPosition;

    private volatile long state;

    /** what the worker failed with - the table rethrows it */
    private volatile Throwable failure;

    /** the start of the last batch the worker left after a take-over - written once it has written all of it */
    private volatile int released = -1;

    /* the table's: a stalled worker, the batch taken over from it */
    private long watchedState;
    private long watchedSince;
    private int frozenFrom = -1;
    private int[] frozenSlots;
    private int frozenCount;


    HashtableAsync2Resizer(long[] srcData, long[] dstData, int startingPosition, int allowedPosition) {
        this.srcData = srcData;
        this.dstData = dstData;
        this.newMask = srcData.length - 1;
        this.startingPosition = startingPosition;
        // startingPosition is a gap and is never copied - start right after it (wrapping around)
        this.state = (long) ((startingPosition + 2) & (srcData.length - 1)) << 32;
        this.allowedPosition = allowedPosition;
    }

    /**
     * Check if position is surely non migrated
     * Range considered as exclusive - always preserving gaps
     *
     * @param pos             - requested position
     * @param allowedPosition - allowed position (from hashtable's prospective, not migrator's)
     * @return true if it is safe to access old data array, false if maybe in migrated (may need further checks on recent progress)
     */
    public boolean isInOldData(int pos, int allowedPosition) {
        if (startingPosition < allowedPosition) {
            return pos < startingPosition || pos > allowedPosition;
        } else {
            return pos < startingPosition && pos > allowedPosition;
        }
    }

    /**
     * Checked after {@link #isInOldData} has already ruled the old array out.
     * Returns true while the position is still waiting to be copied, false once the new array can
     * be accessed safely (unless it requires put-extension).
     */
    public boolean notInNewData(int pos, int lasKnownProgress) {

        if (isAllMigrated()) {
            // whole table is in the new array - no position is pending any more
            return false;
        }

        // lasKnownProgress is an EXCLUSIVE bound: the migrator has copied
        // [initial progress, lasKnownProgress) and lasKnownProgress itself is still
        // pending. Treating it as migrated sends lookups to the new array one position too early -
        // at migration start that position is startingPosition+2, which holds a real entry, so
        // every key hashing there reads 0 while its value still sits in the old array.
        if (startingPosition <= lasKnownProgress) {
            return pos < startingPosition || pos >= lasKnownProgress;
        } else {
            return pos < startingPosition && pos >= lasKnownProgress;
        }
    }

    /**
     * true once the whole table is copied into the new array - or the rest of it by the table: a put that reached the
     * start of the migration extends it backwards, onto the progress at most
     */
    public boolean isAllMigrated() {
        final long s = state;
        return (s & ALL_MIGRATED) != 0 || progress(s) == startingPosition;
    }


    public long[] getNewDataArray() {
        return dstData;
    }

    public int getNewMask() {
        return newMask;
    }

    /**
     * Authorizes the copying up to allowedPosition. The table wakes the worker after it (Slot.authorized()).
     */
    public void setAllowedPosition(int allowedPosition) {
        this.allowedPosition = allowedPosition;
    }

    /** the progress: always a gap */
    public int getProcessedPosition() {
        return progress(state);
    }

    public int getStartingPosition() {
        return startingPosition;
    }

    public void setStartingPosition(int startingPosition) {
        this.startingPosition = startingPosition;
    }

    /**
     * The worker's turn: copies a portion of what is authorized, unless there is nothing or the table is copying it.
     * Batch by batch, each started with a CAS that tells the table where it begins; the last one publishes the
     * progress. A failed CAS means the table has taken the portion over: the worker stops - after the batch it was
     * writing, which the table copied too.
     *
     * @return true if it copied
     */
    boolean doWork() {
        final long s = state;
        final int processed = progress(s);
        final int allowed = allowedPosition;
        if ((s & OWNER) != IDLE || allowed == FINISH_SIGNAL || allowed == processed) {
            return false;
        }
        long held = workerAt(processed, processed);
        if (!STATE.compareAndSet(this, s, held)) {
            return false;
        }
        try {
            final int to = portionEnd(processed, allowed);
            checkReachable(to);
            int from = processed;
            while (true) {
                final int end = batchEnd(from, to);
                copy(srcData, dstData, newMask, from, end);
                final long next = end == to ? idleAt(to) : workerAt(processed, end);
                if (!STATE.compareAndSet(this, held, next)) {
                    // taken over: the table copied this batch as well, and leaves its entries alone until now
                    released = from;
                    return true;
                }
                if (end == to) {
                    return true;
                }
                held = next;
                from = end;
            }
        } catch (Throwable e) {
            // nobody copies any more - the table rethrows it instead of waiting for the hang timeout
            failure = e;
            if (!STATE.compareAndSet(this, held, (held & ~(OWNER | BATCH)) | FAILED)) {
                released = batchStart(held); // taken over in the meantime: the table goes on without the worker
            }
            return true;
        }
    }

    /**
     * The table's turn, where it would wait for the worker: copies what is authorized itself, unless the worker is
     * copying it right now.
     *
     * @return false if there is nothing to copy, the worker is copying (or has failed) - the table waits
     */
    boolean copyIfIdle() {
        final long s = state;
        if ((s & OWNER) != IDLE || progress(s) == allowedPosition || !STATE.compareAndSet(this, s, s | TABLE)) {
            return false;
        }
        copyAsTable(s, progress(s));
        return true;
    }

    /**
     * The table's turn while the worker is copying: takes the worker's portion over if its batch has not changed for
     * {@value #TAKE_OVER_SPINS} spins of this wait. Copies the batch again, recording where its entries are -
     * {@link #frozen} until the worker has left the batch - and the rest of what is authorized.
     *
     * @param spins of the table's wait, from 1
     * @return true if it took over
     */
    boolean takeOverIfStalled(long spins) {
        final long s = state;
        if ((s & OWNER) != WORKER) {
            return false;
        }
        if (spins == 1 || s != watchedState) {
            watchedState = s;
            watchedSince = spins;
            return false;
        }
        if (spins - watchedSince < TAKE_OVER_SPINS || !STATE.compareAndSet(this, s, (s & ~(OWNER | BATCH)) | TABLE)) {
            return false;
        }
        final int from = batchStart(s);
        // covers the worker's batch: the batch ends at the end of its portion or earlier, and that is authorized
        final int frozenEnd = batchEnd(from, allowedPosition);
        if (frozenSlots == null) {
            frozenSlots = new int[BATCH_LONGS / 2];
        }
        frozenCount = 0;
        int pos = from;
        while (pos != frozenEnd) {
            final long key = srcData[pos];
            if (key != NOT_ALLOWED_KEY) {
                final int offset = HashingUtils.findFreeOffset(key, dstData, newMask);
                dstData[offset] = key;
                dstData[offset + 1] = srcData[pos + 1];
                frozenSlots[frozenCount++] = offset;
            }
            pos = (pos + 2) & (srcData.length - 1);
        }
        frozenFrom = from;
        copyAsTable(s, frozenEnd);
        return true;
    }

    /**
     * Holding the state as the table: copies [from, allowedPosition) and publishes the progress. If the copying fails,
     * the state is given back as it was taken: the progress stays.
     */
    private void copyAsTable(long taken, int from) {
        final int allowed = allowedPosition;
        boolean copied = false;
        try {
            if (from != allowed) {
                copyInterval(from, allowed);
            }
            copied = true;
        } finally {
            state = copied ? idleAt(allowed) : (taken & ~(OWNER | BATCH));
        }
    }

    /**
     * The table: true while the worker may still write entries of the batch the table took over from it - with the
     * values they have, into the slots they have. Until then a removal or a new value there would be overwritten.
     */
    boolean frozenPending() {
        if (frozenFrom < 0) {
            return false;
        }
        if (released == frozenFrom) {
            frozenFrom = -1;
            return false;
        }
        return true;
    }

    /** the table: true if the worker may still write the entry at this offset of the new array */
    boolean frozen(int offset) {
        if (!frozenPending()) {
            return false;
        }
        for (int i = 0; i < frozenCount; i++) {
            if (frozenSlots[i] == offset) {
                return true;
            }
        }
        return false;
    }

    /** the table: true if the worker may still write an entry within [from, to] of the new array (wrapping around) */
    boolean frozenWithin(int from, int to) {
        if (!frozenPending()) {
            return false;
        }
        final int ring = dstData.length - 1;
        final int length = (to - from) & ring;
        for (int i = 0; i < frozenCount; i++) {
            if (((frozenSlots[i] - from) & ring) <= length) {
                return true;
            }
        }
        return false;
    }

    /** the failure of the worker, null if none - or if the table took over and goes on without it */
    Throwable failure() {
        return (state & OWNER) == FAILED ? failure : null;
    }

    /**
     * Tests: claims the state as the worker, its batch starting at the progress.
     *
     * @return the state held, 0 if the state is not idle
     */
    long claimAsWorkerForTest() {
        final long s = state;
        final long held = workerAt(progress(s), progress(s));
        return (s & OWNER) == IDLE && STATE.compareAndSet(this, s, held) ? held : 0;
    }

    /**
     * Tests: ends the claim as the worker ends its portion - publishes the progress (a gap), or, taken over, leaves the
     * batch.
     *
     * @param progress -1: as it was
     * @return false if taken over
     */
    boolean endAsWorkerForTest(long held, int progress) {
        if (STATE.compareAndSet(this, held, idleAt(progress < 0 ? progress(held) : progress))) {
            return true;
        }
        released = batchStart(held);
        return false;
    }

    /** the table ended the migration: the worker does nothing more here */
    void finish() {
        allowedPosition = FINISH_SIGNAL;
    }

    String stateName() {
        final long s = state;
        final int owner = (int) (s & OWNER);
        return (owner == IDLE ? "idle" : owner == WORKER ? "worker copying from " + batchStart(s)
                : owner == TABLE ? "table copying" : "worker failed")
                + (frozenFrom >= 0 ? ", worker's batch " + frozenFrom + " taken over, released " + released : "");
    }

    /**
     * The worker's portion: what is authorized, or - given more than {@value #WORKER_PORTION_LONGS} longs - up to the
     * first free slot past that. The authorized range is old data no one writes while it waits for the copying: its
     * gaps stay put.
     */
    private int portionEnd(int processed, int allowed) {
        final int ring = srcData.length - 1;
        if (((allowed - processed) & ring) > WORKER_PORTION_LONGS) {
            final int gap = findNextGapPos(srcData, (processed + WORKER_PORTION_LONGS) & ring);
            if (((gap - processed) & ring) < ((allowed - processed) & ring)) {
                return gap;
            }
        }
        return allowed;
    }

    private int batchEnd(int from, int to) {
        final int ring = srcData.length - 1;
        return ((to - from) & ring) <= BATCH_LONGS ? to : (from + BATCH_LONGS) & ring;
    }

    private static int progress(long state) {
        return (int) (state >>> 32);
    }

    private static int batchStart(long state) {
        return ((int) state >>> 3) << 1;
    }

    private static long workerAt(int progress, int batchStart) {
        return (long) progress << 32 | (long) (batchStart >>> 1) << 3 | WORKER;
    }

    /** the state after copying up to 'to': the whole table is copied once it wraps back to the start */
    private long idleAt(int to) {
        return (long) to << 32 | (to == startingPosition ? ALL_MIGRATED : 0);
    }

    public void copyInterval(final int from, int to) {
        checkReachable(to);
        copy(srcData, dstData, newMask, from, to);
    }

    /**
     * 'to' is only ever reached by even wrapping increments - anything else (notably the FINISH_SIGNAL) would make the
     * copy loop run forever
     */
    private void checkReachable(int to) {
        if (to < 0 || to >= srcData.length || (to & 1) != 0) {
            throw new IllegalStateException("copyInterval: unreachable 'to'=" + to
                    + " (srcData.length=" + srcData.length + ")");
        }
    }

    /**
     * The copy loop: a static method of its own, so that it compiles the same wherever it is inlined - inlined into
     * the worker's batch loop with the fields read inside, it ran ~20% slower.
     */
    private static void copy(long[] src, long[] dst, int dstMask, int from, int to) {
        int pos = from;
        do {
            final long key = src[pos];
            if (key != NOT_ALLOWED_KEY) {
                final int offset = HashingUtils.findFreeOffset(key, dst, dstMask);
                dst[offset] = key;
                dst[offset + 1] = src[pos + 1];
            }

            pos += 2;
            if (pos == src.length) {
                pos = 0;
            }
        } while (pos != to);
    }


    public static int findNextGapPos(final long[] data, final int initialPos) {

        int pos = initialPos;

        long existingKey = data[pos];
        while (existingKey != NOT_ALLOWED_KEY) {
            pos += 2;
            if (pos == data.length) {
                pos = 0;
            }
            if (pos == initialPos) {
                throw new IllegalStateException("No gap found, can not perform async migration");
            }
            existingKey = data[pos];
        }

        return pos;
    }

}
