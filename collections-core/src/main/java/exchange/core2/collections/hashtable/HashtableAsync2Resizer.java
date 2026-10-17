package exchange.core2.collections.hashtable;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

class HashtableAsync2Resizer {


    /**
     * allowedPosition value meaning "stop now" - published by the hashtable in switchToNewArray().
     */
    public static final int FINISH_SIGNAL = -1;

    /**
     * Spin budget before the migrator starts backing off. Long enough to cover the usual gap
     * between two consecutive setAllowedPosition() calls, so a busy table never parks the migrator.
     */
    private static final int SPINS_BEFORE_PARK = 64 * 1024;
    private static final int YIELD_SPIN_MASK = 1023;
    private static final long PARK_INITIAL_NANOS = 1_000L;
    private static final long PARK_BACKOFF_FACTOR = 100L;
    private static final long PARK_MAX_NANOS = 1_000_000L; // 1ms - migration progress only

    private final long[] srcData;
    private final long[] dstData;
    private final int newMask;

    /**
     * Written by the application (backwards cluster extension), read by the migrator - it is the
     * termination condition of {@link #copy()}, so a non-published write means the migrator would
     * never stop.
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

    /** the thread currently running {@link #copy()}, null before it starts and after it returns */
    private volatile Thread migratorThread;

    /** the migrator was interrupted while backing off - set again when copy() returns (migrator thread only) */
    private boolean interrupted;

    /** the migrator is parked, or about to park - setAllowedPosition() wakes it */
    private volatile boolean parked;

    /*
     * Who copies. Until the copying task starts (NEW) the table copies the authorized segments itself, one at a time
     * (TABLE), where it would otherwise wait for the migrator: the task may never start - the executor rejected it,
     * runs it on the table's thread, or queues it behind busy threads. A task that starts (RUNNING) copies the rest.
     */
    private static final int NEW = 0;
    private static final int RUNNING = 1;
    private static final int TABLE = 2;
    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(HashtableAsync2Resizer.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile int state = NEW;

    /** the thread that started the migration - the copying task must not run there, see copy() */
    private final Thread starter;


    public HashtableAsync2Resizer(long[] srcData, long[] dstData, int startingPosition, int allowedPosition,
                                  Thread starter) {
        this.srcData = srcData;
        this.dstData = dstData;
        this.newMask = srcData.length - 1;
        this.startingPosition = startingPosition;
        // startingPosition is a gap and is never copied - start right after it (wrapping around)
        this.toProcessPosition = (startingPosition + 2) & (srcData.length - 1);
        this.allowedPosition = allowedPosition;
        this.starter = starter;
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
     * Authorizes the migrator up to allowedPosition - and wakes it if it is backing off: the operations after this one
     * may need the segment right away. Only a parked migrator is unparked, a busy table pays one volatile read.
     */
    public void setAllowedPosition(int allowedPosition) {
        this.allowedPosition = allowedPosition;
        // Dekker with doCopy(): the migrator publishes parked, then reads allowedPosition - one of the two threads sees
        // the other's write, so the authorization is not missed (an unpark before the park makes it return at once)
        if (parked) {
            final Thread thread = migratorThread;
            if (thread != null) {
                LockSupport.unpark(thread);
            }
        }
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

    /** true once the copying task has started - it copies the rest, and leaves copy() once signalled to finish */
    public boolean isMigratorRunning() {
        return state == RUNNING;
    }

    /**
     * Called by the table where it would wait for the migrator: if the copying task has not started, the table copies
     * what it has authorized itself, as the task would. The task, if it starts later, continues from there.
     *
     * @return false if the task has started - it copies
     */
    public boolean copyIfNotStarted() {
        if (state != NEW || !STATE.compareAndSet(this, NEW, TABLE)) {
            return false;
        }
        try {
            final int allowed = allowedPosition;
            final int processed = toProcessPosition;
            if (processed != allowed) {
                copySegment(processed, allowed);
            }
        } finally {
            state = NEW;
        }
        return true;
    }

    String stateName() {
        final int s = state;
        return s == NEW ? "not started" : s == RUNNING ? "running" : "copied by the table";
    }

    public void copy() {
        if (Thread.currentThread() == starter) {
            // the executor runs the task on the table's thread - right away (Runnable::run, CallerRunsPolicy) or later
            // (the application's own event loop): waiting there for the next authorization would wait for itself.
            // The table copies where it needs to.
            return;
        }
        // the table may be copying a segment right now - it does until the task starts
        while (!STATE.compareAndSet(this, NEW, RUNNING)) {
            Thread.onSpinWait();
        }

        migratorThread = Thread.currentThread();
        try {
            doCopy();
        } finally {
            migratorThread = null;
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void doCopy() {

        int allowedLocal;
        int processedLocal = toProcessPosition;

      //  log.info("allowedPosition={} toProcessPosition={}", allowedPosition, toProcessPosition);

        int spins = 0;
        long parkNanos = 0;

        while (true) {

            allowedLocal = allowedPosition;

            if (allowedLocal == FINISH_SIGNAL) {
                // switchToNewArray() gave up on us - the table no longer reads dstData through
                // this resizer, so stop immediately instead of copying into a detached array
                return;
            }

            if (processedLocal != allowedLocal) {
                if (copySegment(processedLocal, allowedLocal)) {
                    return;
                }
                processedLocal = allowedLocal;

                spins = 0;
                parkNanos = 0;
                continue;
            }

            // Nothing authorized yet. Spin first - the hand-over is normally sub-microsecond, and
            // the migrator usually runs on its own pinned core, so spinning is the cheap case.
            if (spins < SPINS_BEFORE_PARK) {
                spins++;
                Thread.onSpinWait();
                if ((spins & YIELD_SPIN_MASK) == 0) {
                    // the application may share this core (threads started by a pinned application inherit its
                    // core): it can authorize the next segment only when it gets the core back. On a core of its own
                    // the yield returns at once - a syscall every ~1024 spins
                    Thread.yield();
                }
                continue;
            }

            // The application went quiet. Back off instead of holding a core forever: a live but
            // idle table used to pin its migrator (and, with an affinity-locked executor, its core)
            // for as long as the table existed.
            //
            // The table unparks a parked migrator when it authorizes the next segment (setAllowedPosition): the
            // operations after the authorizing one may need that segment, and used to wait out the rest of a park of
            // up to 1ms. The unpark syscall is paid once after a quiet period, a busy table never parks the migrator.
            parkNanos = (parkNanos == 0)
                    ? PARK_INITIAL_NANOS
                    : Math.min(parkNanos * PARK_BACKOFF_FACTOR, PARK_MAX_NANOS);

            // An interrupt does not stop the migration: entries put into the new array exist only there, the
            // table needs it complete (the hang detection on the application side covers a migrator that never
            // gets authorized). But with the interrupt status set parkNanos returns at once - a busy loop. Clear
            // it here, set it again when copy() returns.
            if (Thread.interrupted()) {
                interrupted = true;
            }
            parked = true;
            if (allowedPosition == allowedLocal) { // not authorized since - see setAllowedPosition
                LockSupport.parkNanos(this, parkNanos);
            }
            parked = false;
        }
    }

    /**
     * Tells the migrator to stop and wakes it if it is backing off, so a shutdown does not have to
     * wait out the current park interval. Safe to call repeatedly and after the migrator finished.
     */
    public void signalFinish() {
        allowedPosition = FINISH_SIGNAL;
        final Thread thread = migratorThread;
        if (thread != null) {
            LockSupport.unpark(thread);
        }
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
       // log.debug("(A) Copying range {}..{} ...", from, to);

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
                // TODO // TODO non epmty gp piblem - should not put anything into FROM ??

                dstData[offset] = key;
                dstData[offset + 1] = srcData[pos + 1];
            }

            pos += 2;
            if (pos == srcData.length) {
                pos = 0;
            }
        } while (pos != to);
        // log.debug("(A) Copied range {}..{}", from, to);
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
