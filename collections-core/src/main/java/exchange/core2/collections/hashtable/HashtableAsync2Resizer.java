package exchange.core2.collections.hashtable;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

/**
 * One migration of a {@link LongLongLL2Hashtable}: the old array copied into the new one, a segment at a time. The
 * table authorizes a segment (allowedPosition); whoever gets to it first copies it - the worker of the
 * {@link MigrationService} ({@link #doWork()}), or the table itself where an operation needs the segment
 * ({@link #copyIfIdle()}). The state makes that exclusive: a CAS from IDLE takes the segment, the copier sets IDLE
 * again. The table waits only for a segment the worker is copying at that moment.
 * <p>
 * A new instance per migration - it doubles as its generation: a worker late on an old migration touches only the old
 * instance. Holds the arrays, never the table.
 */
final class HashtableAsync2Resizer {

    /**
     * allowedPosition value meaning "stop now" - published by the hashtable in switchToNewArray().
     */
    public static final int FINISH_SIGNAL = -1;

    private static final int IDLE = 0;
    private static final int WORKER = 1;
    private static final int TABLE = 2;
    private static final int FAILED = 3;
    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(HashtableAsync2Resizer.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long[] srcData;
    private final long[] dstData;
    private final int newMask;

    /**
     * Written by the application (backwards cluster extension), read by the copier - the end of the
     * migration.
     */
    private volatile int startingPosition;

    /**
     * Exclusive upper bound of the copied range: everything in [initial toProcessPosition,
     * toProcessPosition) is in the new array, and toProcessPosition itself is NOT yet copied.
     * Note the name - this is the next position to process, not the last processed one.
     */
    private volatile int toProcessPosition;

    private volatile int allowedPosition;

    /**
     * Explicit end-of-migration marker. Needed because {@code toProcessPosition == startingPosition}
     * is ambiguous on its own - it reads as both "nothing copied yet" and "everything copied" - and
     * relying on that coincidence made the range checks below silently wrong at the boundary.
     * Published BEFORE the final toProcessPosition, so a reader that sees the final progress always
     * sees this flag too.
     */
    private volatile boolean allMigrated = false;

    /** who copies right now: IDLE, WORKER, TABLE - or FAILED, for good */
    private volatile int state = IDLE;

    /** what the worker failed with - the table rethrows it */
    private volatile Throwable failure;


    HashtableAsync2Resizer(long[] srcData, long[] dstData, int startingPosition, int allowedPosition) {
        this.srcData = srcData;
        this.dstData = dstData;
        this.newMask = srcData.length - 1;
        this.startingPosition = startingPosition;
        // startingPosition is a gap and is never copied - start right after it (wrapping around)
        this.toProcessPosition = (startingPosition + 2) & (srcData.length - 1);
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

        if (allMigrated) {
            // whole table is in the new array - no position is pending any more
            return false;
        }

        // lasKnownProgress is an EXCLUSIVE bound: the migrator has copied
        // [initial toProcessPosition, lasKnownProgress) and lasKnownProgress itself is still
        // pending. Treating it as migrated sends lookups to the new array one position too early -
        // at migration start that position is startingPosition+2, which holds a real entry, so
        // every key hashing there reads 0 while its value still sits in the old array.
        if (startingPosition <= lasKnownProgress) {
            return pos < startingPosition || pos >= lasKnownProgress;
        } else {
            return pos < startingPosition && pos >= lasKnownProgress;
        }
    }

    /** true once the migrator has copied the whole table into the new array */
    public boolean isAllMigrated() {
        return allMigrated;
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

    // only gaps
    public int getProcessedPosition() {
        return toProcessPosition;
    }

    public int getStartingPosition() {
        return startingPosition;
    }

    public void setStartingPosition(int startingPosition) {
        this.startingPosition = startingPosition;
    }

    /**
     * The worker's turn: copies the authorized segment, unless there is none or the table is copying it.
     *
     * @return true if it copied
     */
    boolean doWork() {
        final int allowed = allowedPosition;
        if (allowed == FINISH_SIGNAL || allowed == toProcessPosition || state != IDLE
                || !STATE.compareAndSet(this, IDLE, WORKER)) {
            return false;
        }
        final boolean copied;
        try {
            copied = copyAuthorized();
        } catch (Throwable e) {
            // nobody copies any more - the table rethrows it instead of waiting for the hang timeout
            failure = e;
            state = FAILED;
            return true;
        }
        state = IDLE;
        return copied;
    }

    /**
     * The table's turn, where it would wait for the worker: copies the authorized segment itself, unless the worker is
     * copying it right now.
     *
     * @return false if the worker is copying (or has failed) - the table waits
     */
    boolean copyIfIdle() {
        if (state != IDLE || !STATE.compareAndSet(this, IDLE, TABLE)) {
            return false;
        }
        try {
            copyAuthorized();
        } finally {
            state = IDLE;
        }
        return true;
    }

    /**
     * Copies [toProcessPosition, allowedPosition) and publishes the progress - by the one that holds the state. Reads
     * the positions only now: before the CAS the other one may have copied the segment, and the table may have ended
     * the migration.
     */
    private boolean copyAuthorized() {
        final int allowed = allowedPosition;
        final int processed = toProcessPosition;
        if (allowed == FINISH_SIGNAL || processed == allowed) {
            return false;
        }
        copySegment(processed, allowed);
        return true;
    }

    /** the failure of the worker, null if none */
    Throwable failure() {
        return failure;
    }

    /** the table ended the migration: the worker does nothing more here */
    void finish() {
        allowedPosition = FINISH_SIGNAL;
    }

    String stateName() {
        final int s = state;
        return s == IDLE ? "idle" : s == WORKER ? "worker copying" : s == TABLE ? "table copying" : "worker failed";
    }

    /**
     * Copies [from, to) and publishes the progress.
     *
     * @return true if the whole table is copied
     */
    private boolean copySegment(int from, int to) {
        copyInterval(from, to);
        if (to == startingPosition) {
            // wrapped back to the start - the whole table is copied. Publish the flag BEFORE the progress, so nobody
            // can observe the final (otherwise ambiguous) toProcessPosition == startingPosition without also seeing
            // allMigrated.
            allMigrated = true;
            toProcessPosition = to;
            return true;
        }
        toProcessPosition = to;
        return false;
    }

    public void copyInterval(final int from, int to) {

        // 'to' is only ever reached by even wrapping increments - anything else (notably the
        // FINISH_SIGNAL) would make the loop below run forever
        if (to < 0 || to >= srcData.length || (to & 1) != 0) {
            throw new IllegalStateException("copyInterval: unreachable 'to'=" + to
                    + " (from=" + from + " srcData.length=" + srcData.length + ")");
        }

        int pos = from;
        do {
            final long key = srcData[pos];
            if (key != NOT_ALLOWED_KEY) {
                final int offset = HashingUtils.findFreeOffset(key, dstData, newMask);
                dstData[offset] = key;
                dstData[offset + 1] = srcData[pos + 1];
            }

            pos += 2;
            if (pos == srcData.length) {
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
