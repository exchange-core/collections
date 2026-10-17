package exchange.core2.collections.hashtable;


import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;
import static exchange.core2.collections.hashtable.HashingUtils.SEED;

/**
 * The {@link LongLongHashtable} layout with an asynchronous resize: a migrator thread copies the old array into the
 * new one segment by segment while the application keeps using the table - no stop-the-world pause.
 * <ul>
 *     <li>One application thread. Every operation, {@link #get} included, may advance the migration and switch
 *     arrays, so the table is not safe even for concurrent readers.</li>
 *     <li>An operation on a key whose home is in the segment being copied waits for that segment: 256 slots, ~2us to
 *     copy - and up to the end of the cluster there, a segment ends at a free slot. A put whose probe reaches the
 *     start of the migration copies the cluster before it on the calling thread. A put that brings the old array to
 *     90% load before the migration ends waits for all of it. {@link Builder#fixedCapacity} makes a table that
 *     never resizes, with none of this.</li>
 *     <li>The application drives the migration: every operation authorizes the next segment once the migrator has
 *     copied the current one, whatever its key - and wakes the migrator if it went to sleep while the application
 *     was quiet. A table nobody uses keeps its migration where it is; {@link #migrationInProgress()} tells, and
 *     {@link #finishMigration()} completes it.</li>
 *     <li>During a migration both arrays are alive: three times the memory of the old one.</li>
 *     <li>Tables with fewer than {@code syncResizeBelow} entries (default {@value #DEFAULT_SYNC_RESIZE_BELOW}) resize
 *     synchronously, like {@link LongLongHashtable}.</li>
 *     <li>The executor runs the allocation of the new array and the copying, which spins until its migration ends:
 *     it must not be bounded ({@link Builder#executor}). A task the executor rejects, runs on the thread that started
 *     the migration ({@code Runnable::run}, {@code CallerRunsPolicy}, the application's own event loop) or has not
 *     started yet is not waited for: the table does that work itself, on the calling thread - it resizes
 *     synchronously, allocates the new array at the block threshold, or copies the segment an operation needs. No
 *     operation fails because of the executor.</li>
 *     <li>{@link #finishMigration()} finishes an in-flight migration; {@link #close()} too, and makes the later
 *     resizes synchronous. {@link #clear()}, {@link #forEach}, {@link #keysStream()} and {@link #valuesStream()}
 *     finish an in-flight migration first - a pause, for the set-up, snapshots or tests.</li>
 *     <li>Waits for the migrator are bounded: past the hang timeout ({@link Builder#hangTimeout}, default 10s) an
 *     operation fails with {@link IllegalStateException} and the state of the migration, instead of hanging; a
 *     migrator that died is reported right away. The table stays consistent after such a failure. Interrupts do
 *     not fail an operation: the waits are short, they go on, and the interrupt status is kept for the caller's
 *     own blocking calls.</li>
 *     <li>Any key, {@code missingValue} ({@link Builder}), the hash seed of the JVM and the maximum capacity as in
 *     {@link LongLongHashtable}. The entry stored as key 0 is kept outside the arrays, migrations do not touch
 *     it.</li>
 * </ul>
 */
public final class LongLongLL2Hashtable implements ILongLongHashtable, AutoCloseable {

    public static final int DEFAULT_ARRAY_SIZE = 32;
    public static final int DEFAULT_SYNC_RESIZE_BELOW = 8200;

    private final float upsizeThresholdPerc;
    private final float blockThresholdPerc;

    private final Executor executor;
    private final int syncResizeBelow;
    private final int maxCapacity;
    private final long missingValue;

    // the arrays hold key ^ SEED; the entry stored as key 0 - the gap marker - is kept here (counted in size): the key
    // equal to the seed
    private boolean hasZeroKey;
    private long zeroKeyValue;

    private long[] data;
    private long size = 0;
    private int mask;
    private long upsizeThreshold;
    private long blockThreshold;
    private CompletableFuture<long[]> arrayFeature = null;
    private AtomicBoolean allocationStarted;
    private CompletableFuture<Void> copyingProcess = null;

    private HashtableAsync2Resizer resizer = null;
    private Cleaner.Cleanable resizerCleanable = null;

    /** set by close(): the resizes are synchronous from then on */
    private boolean closed;

    // started at class init - see newMigratorExecutor() for affinity-pinned callers
    private static final Cleaner CLEANER = Cleaner.create(r -> daemon(r, "ll2-cleaner"));

    /**
     * Must not hold a reference to the hashtable - only to the resizer - otherwise the table stays
     * reachable and the cleanup never runs.
     */
    private static final class MigratorShutdown implements Runnable {

        private final HashtableAsync2Resizer resizer;

        private MigratorShutdown(HashtableAsync2Resizer resizer) {
            this.resizer = resizer;
        }

        @Override
        public void run() {
            resizer.signalFinish();
        }
    }

    private int allowedPosition = -1;
    private int knownProgressCached = -1;

    /**
     * Every wait in here is a spin on progress published by the migrator. A migration segment is
     * copied in microseconds, so anything past this bound means the migrator is stuck or dead -
     * fail with the full state instead of hanging, which is what makes these bugs debuggable.
     */
    public static final Duration DEFAULT_HANG_TIMEOUT = Duration.ofSeconds(10);
    private static final long HANG_CHECK_SPIN_MASK = 0x3FF; // check the clock every 1024 spins (~10-50us)

    private final long hangTimeoutNanos;
    /** of the wait for the migrator in progress, see waitStep() */
    private long waitDeadline;

    /**
     * Runs {@link #integrityCheck()} right after every completed migration. Enable with
     * -Dexchange.hashtable.verify=true when hunting migration corruption; being a static final
     * read of a system property, the branch folds away when it is off.
     */
    private static final boolean VERIFY_AFTER_MIGRATION = Boolean.getBoolean("exchange.hashtable.verify");

    public LongLongLL2Hashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongLongLL2Hashtable(int size) {
        this(size, DAEMON_EXECUTOR);
    }

    /**
     * Migration threads must be daemons: {@link HashtableAsync2Resizer#copy()} spins until the
     * table authorizes the next segment, so a table abandoned mid-migration leaves a thread that
     * never terminates. As a non-daemon thread it also prevents the JVM from shutting down.
     */
    private static final Executor DAEMON_EXECUTOR = job -> daemon(job, "ll2-migrator").start();

    /**
     * Thread-per-task daemon executor for callers pinned to a CPU (collections-affinity, taskset): a thread
     * inherits the affinity of its creator, so DAEMON_EXECUTOR started from a pinned thread puts the
     * migrator on that same core, time-sliced with the application. Here the migrators are started
     * by a helper thread created now - call this before pinning. Initializes the class (and its
     * Cleaner thread) on the calling thread as well.
     * <p>
     * Close it once the tables using it are closed: that ends the helper thread. A closed executor rejects tasks, so
     * a table still using it resizes synchronously.
     */
    public static MigratorExecutor newMigratorExecutor() {
        return new MigratorExecutor();
    }

    /**
     * See {@link #newMigratorExecutor()}.
     */
    public static final class MigratorExecutor implements Executor, AutoCloseable {

        private final ThreadPoolExecutor spawner = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), r -> daemon(r, "ll2-spawner"));

        private MigratorExecutor() {
            spawner.prestartAllCoreThreads(); // helper thread created here, not lazily by the first (pinned) caller
        }

        @Override
        public void execute(Runnable job) {
            spawner.execute(() -> daemon(job, "ll2-migrator").start());
        }

        /**
         * Ends the helper thread once it has started the migrators already submitted; later tasks are rejected.
         */
        @Override
        public void close() {
            spawner.shutdown();
        }
    }

    private static Thread daemon(Runnable job, String name) {
        final Thread thread = new Thread(job, name);
        thread.setDaemon(true);
        return thread;
    }

    public LongLongLL2Hashtable(Executor executor) {
        this(DEFAULT_ARRAY_SIZE, executor);
    }

    /**
     * WARNING: the executor must not be bounded - see {@link Builder#executor}.
     */
    public LongLongLL2Hashtable(int size, Executor executor) {
        this(size, executor, DEFAULT_SYNC_RESIZE_BELOW);
    }

    /**
     * @param syncResizeBelow see {@link Builder#syncResizeBelow}
     */
    public LongLongLL2Hashtable(int size, Executor executor, int syncResizeBelow) {
        this(builder().expectedSize(size).executor(executor).syncResizeBelow(syncResizeBelow));
    }

    private LongLongLL2Hashtable(Builder builder) {
        this.missingValue = builder.missingValue;
        this.executor = builder.executor;
        this.syncResizeBelow = builder.syncResizeBelow;
        this.hangTimeoutNanos = builder.hangTimeout.toNanos();
        this.upsizeThresholdPerc = 0.65f;
        final int arraySize = HashingUtils.capacityFor(builder.expectedSize, upsizeThresholdPerc, builder.maxCapacity);
        this.maxCapacity = builder.fixedCapacity ? arraySize : builder.maxCapacity;

        this.data = LinearProbing.newArray(arraySize, missingValue);
        //this.blockThresholdPerc = 0.6501f;
        this.blockThresholdPerc = 0.90f;
        this.mask = (this.data.length / 2) - 1;
        this.upsizeThreshold = calculateUpsizeThreshold();
        this.blockThreshold = calculateBlockThreshold();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link LongLongLL2Hashtable}, all optional.
     */
    public static final class Builder {

        private int expectedSize = DEFAULT_ARRAY_SIZE;
        private long missingValue = 0L;
        private Executor executor = DAEMON_EXECUTOR;
        private int syncResizeBelow = DEFAULT_SYNC_RESIZE_BELOW;
        private Duration hangTimeout = DEFAULT_HANG_TIMEOUT;
        private int maxCapacity = HashingUtils.MAX_CAPACITY;
        private boolean fixedCapacity;

        private Builder() {
        }

        /**
         * Entries that fit without a resize (default {@value LongLongLL2Hashtable#DEFAULT_ARRAY_SIZE}).
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * What get, put and remove return for a missing key (default 0) - see
         * {@link LongLongHashtable.Builder#missingValue}. The pass over a new array runs on the executor.
         */
        public Builder missingValue(long missingValue) {
            this.missingValue = missingValue;
            return this;
        }

        /**
         * Runs the allocation of the new array and the copying task (default: a new daemon thread per task).
         * <p>
         * WARNING: the copying task spins while waiting for the application to authorize the next segment, until its
         * migration ends. Pass an unbounded / thread-per-task executor, or see {@link #newMigratorExecutor()} for
         * threads pinned to a CPU. In a bounded one, once every thread is occupied by a migrator, new tasks wait in
         * the queue - and the application thread does their work.
         * <p>
         * A task the executor rejects, runs on the thread that started the migration, or has not started yet is not
         * waited for: the table does that work itself, on the calling thread - a synchronous resize, the allocation of
         * the new array at the block threshold, the copying of the segment an operation needs. A copying task that
         * starts late continues from there.
         */
        public Builder executor(Executor executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
            return this;
        }

        /**
         * Tables with fewer entries resize synchronously on the caller (~7.5ns per slot, 62us at 8192 slots; default
         * {@value LongLongLL2Hashtable#DEFAULT_SYNC_RESIZE_BELOW}). An async resize must get its array allocated
         * within the upsize-to-block window (25% of the capacity in puts), so keep the default unless the executor
         * starts a task within microseconds (a dedicated spinning thread): a thread-per-task executor needs
         * 50-100us, more if each new thread pins itself to a core.
         */
        public Builder syncResizeBelow(int syncResizeBelow) {
            if (syncResizeBelow < 0) {
                throw new IllegalArgumentException("syncResizeBelow must not be negative: " + syncResizeBelow);
            }
            this.syncResizeBelow = syncResizeBelow;
            return this;
        }

        /**
         * How long an operation waits for the migrator before it fails with IllegalStateException (default 10s).
         * A migration segment is copied in microseconds and the array allocated in milliseconds, so the bound only
         * matters for a migrator that is stuck - or descheduled for long: a debugger, an overloaded machine. A task
         * the executor has not started is not waited for at all, see {@link #executor}; an allocation that started
         * and does not end by the hang timeout is not either - the table allocates the array itself.
         */
        public Builder hangTimeout(Duration hangTimeout) {
            if (hangTimeout.isNegative() || hangTimeout.isZero()) {
                throw new IllegalArgumentException("hang timeout must be positive: " + hangTimeout);
            }
            this.hangTimeout = hangTimeout;
            return this;
        }

        /**
         * Capacity in slots the table grows up to - see {@link LongLongHashtable.Builder#maxCapacity}.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = HashingUtils.checkMaxCapacity(maxCapacity);
            return this;
        }

        /**
         * No resize, ever - see {@link LongLongHashtable.Builder#fixedCapacity}: no migration, no background task,
         * no wait for the migrator.
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        public LongLongLL2Hashtable build() {
            return new LongLongLL2Hashtable(this);
        }
    }

    /*
     * put, lookup and remove keep the path without a resize in flight small, so that it inlines into the caller
     * (FreqInlineSize is 325 bytes of bytecode, put used to be 625); everything about migration is in *Resizing().
     */

    @Override
    public long put(long key, long value) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return putZeroKey(value);

        if (arrayFeature != null || resizer != null) {
            return putResizing(k, value);
        }

        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        if (data[offset] != k) {
            data[offset] = k;
            data[offset + 1] = value;
            if (++size >= upsizeThreshold) {
                resize(offset);
            }
            return missingValue;
        }

        final long prevValue = data[offset + 1];
        data[offset + 1] = value;
        return prevValue;
    }

    private long putResizing(long key, long value) {

//        setAction(key, 1);

        if (arrayFeature != null) {
            if (size >= blockThreshold) {
                awaitArray();
            }
            startAsyncCopyingIfDone();
        }

//        if (resizer != null){
//            log.warn("size={} blockThreshold={}", size, blockThreshold);
//        }

        if (resizer != null) {

//            setAction(key, 3);
            // TODO slow ?
            knownProgressCached = resizer.getProcessedPosition();

            if (size >= blockThreshold) {

                // The application outran the migrator: authorize the rest and wait until it is copied. Not by
                // joining the copy task - after a backward extension (below) the migrator can already sit at the
                // starting position, waiting for an authorization, and then it never returns on its own.
                // switchToNewArray() stops it.
                allowedPosition = resizer.getStartingPosition();
                resizer.setAllowedPosition(allowedPosition);
                awaitAllowedPositionReached();
                switchToNewArray();

            } else if (knownProgressCached == allowedPosition) {

//                setAction(key, 4);

                enableNextMigrationSegmentOrFinishMigration();
            }
        }


        final int hash = HashingUtils.hash(key);
        int pos = (hash & mask) << 1;

        if (resizer == null) {

            // the new array is still being allocated (or the migration just finished) - put into "data"

//            setAction(key, 6);

            //        log.debug("PUT key:{} val:{}", key, value);
            final int offset = HashingUtils.findFreeOffset(key, pos, data);

            if (data[offset] != key) {
                data[offset] = key;
                data[offset + 1] = value;
                if (++size >= upsizeThreshold && arrayFeature == null) {
                    resize(offset);
                }
                return missingValue;
            }

            final long prevValue = data[offset + 1];
            data[offset + 1] = value;
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

                    // can not extend backwards, need to wait for completion
                    awaitAllowedPositionReached();
                    switchToNewArray();
                    // can safely insert
                    int pos2 = (hash & mask) << 1;
                    final int offset2 = HashingUtils.findFreeOffset(key, pos2, data);
                    return putInternalInsert(data, offset2, key, value);
                }

                // rare scenario: offset reached startPos, which always stay empty (gap)
                // have to extend migrated area and shift starting position back
                // assuming it can only reach allowedPosition, but not before

                // the entries are copied into the new array by this thread - not while the migrator writes there
                awaitAllowedPositionReached();

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


        awaitMigrated(pos);

        // pos is not applicable for migrated array (2x larger), calculating offsetNew
        final long[] dataNew = resizer.getNewDataArray();
        final int newHome = (hash & resizer.getNewMask()) << 1;
        int offsetNew = HashingUtils.findFreeOffset(key, newHome, dataNew);

        if (dataNew[offsetNew] != key && knownProgressCached != allowedPosition && reachesFrontier(newHome, offsetNew)) {
            // a new entry past the frontier would go where the migrator is writing right now, and neither thread
            // sees the other's writes - let it finish the segment first (bounded by the segment size)
            awaitAllowedPositionReached();
            offsetNew = HashingUtils.findFreeOffset(key, newHome, dataNew);
        }

        return putInternalInsert(dataNew, offsetNew, key, value);
    }

    /**
     * While a segment is in flight the migrator writes the new array from the frontier on: its entries come from old
     * positions past knownProgressCached, so in both halves of the new array their homes are at least frontier + 2.
     * Everything before that belongs to the application. True if the probe from home to end (inclusive) gets there.
     */
    private boolean reachesFrontier(int home, int end) {
        final int ringMask = (data.length << 1) - 1;
        final int distance = (end - home) & ringMask;
        final int frontier = knownProgressCached + 2;
        return ((frontier - home) & ringMask) <= distance
                || ((frontier + data.length - home) & ringMask) <= distance;
    }


    private long putInternalInsert(long[] datax, int offset, long key, long value) {
        if (datax[offset] != key) {
            size++;
            datax[offset] = key;
            datax[offset + 1] = value;
            return missingValue;
        }

        final long prevValue = datax[offset + 1];
        datax[offset + 1] = value;
        return prevValue;
    }


    @Override
    public long get(long key) {
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : missingValue;
        return lookup(k, VALUE);
    }

    /** what lookup returns from the slot the probe stopped at: its key, or its value (missingValue for a gap) */
    private static final int KEY = 0;
    private static final int VALUE = 1;

    /**
     * Probes for the key - in whichever array currently holds its home position - and returns one long of the slot
     * the probe stopped at: KEY is equal to the requested key only if it is present (0 for a gap), VALUE is its value
     * (missingValue for a gap - free slots hold it).
     */
    private long lookup(long key, int cell) {
        if (arrayFeature != null || resizer != null) {
            return lookupResizing(key, cell);
        }
        return data[HashingUtils.findFreeOffset(key, data, mask) + cell];
    }

    private long lookupResizing(long key, int cell) {

      //  boolean migStated = false;
        if (arrayFeature != null) {
            startAsyncCopyingIfDone();
        }

//        if (resizer != null) {
//            if (resizer.isFinished()) {
//                // log.debug("GET {}: finished can switch to new array", key);
//                switchToNewArray();
//            }
//        }

        final int hash = HashingUtils.hash(key);
        int pos = (hash & mask) << 1;

        if (resizer == null || resizer.isInOldData(pos, allowedPosition)) {
            // in old data
            final long value = data[HashingUtils.findFreeOffset(key, pos, data) + cell];
            if (resizer != null) {
                advanceMigration();
            }
            return value;
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
//            setAction(key, 37);
            knownProgressCached = resizer.getProcessedPosition();
        }


        awaitMigrated(pos);

        //      log.debug("GET {}: migrated, get from new array", key);

//        setAction(key, 42);

        final long[] newData = resizer.getNewDataArray();
        pos = (hash & resizer.getNewMask()) << 1;
        final int offset = HashingUtils.findFreeOffset(key, pos, newData);
        final long val = newData[offset + cell];
        advanceMigration();
        return val;
    }

    /**
     * Authorizes the next segment once the migrator has copied the current one. Every operation does it during a
     * migration, whatever its key: when only the operations that reached the migrated area did, a stream of reads and
     * removals of keys in the old array stalled the migration - with both arrays and the migrator kept alive.
     */
    private void advanceMigration() {
        knownProgressCached = resizer.getProcessedPosition();
        if (knownProgressCached == allowedPosition) {
            enableNextMigrationSegmentOrFinishMigration();
        }
    }

    @Override
    public boolean containsKey(long key) {
        // the probe stops either at the key or at a gap, which holds key 0 - that key is kept outside the array
        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey;
        return lookup(k, KEY) == k;
    }

    @Override
    public long remove(long key) {
        final long k = key ^ SEED;
        return remove(k, HashingUtils.hash(k));
    }

    private long remove(long key, int hash) {

        if (key == NOT_ALLOWED_KEY) {
            return removeZeroKey();
        }

        if (arrayFeature != null || resizer != null) {
            return removeResizing(key, hash);
        }
        return removeInternal(key, hash, data, mask);
    }

    private long removeResizing(long key, int hash) {

        if (arrayFeature != null) {
            startAsyncCopyingIfDone();
        }

        if (resizer == null) {

            // in old data
//            setAction(key, 54);

            return removeInternal(key, hash, data, mask);
        }

        // TODO examine carefully scope of removal !!

        int pos = (hash & mask) << 1;

        if (resizer.isInOldData(pos, allowedPosition)) {
            // in old data - authorized after the removal: that publishes it to the migrator, which may copy here next
            final long value = removeInternal(key, hash, data, mask);
            advanceMigration();
            return value;
        }

        // not in old data - either migrated already or still under processing
        if (knownProgressCached == -1) {
            knownProgressCached = resizer.getProcessedPosition();
        }
        awaitMigrated(pos);

//        setAction(key, 59);

        final long[] dataNew = resizer.getNewDataArray();
        final int newHome = (hash & resizer.getNewMask()) << 1;
        if (knownProgressCached != allowedPosition
                && reachesFrontier(newHome, HashtableAsync2Resizer.findNextGapPos(dataNew, newHome))) {
            // the removal shifts entries back along the whole cluster - not while the migrator writes its end
            awaitAllowedPositionReached();
        }

        // log.debug("REMOVE {}: migrated, removing from new", key);
        final long val = removeInternal(key, hash, dataNew, resizer.getNewMask());
        advanceMigration();
        return val;
    }

    private long putZeroKey(long value) {
        final long prev = hasZeroKey ? zeroKeyValue : missingValue;
        if (!hasZeroKey) {
            hasZeroKey = true;
            size++;
        }
        zeroKeyValue = value;
        return prev;
    }

    private long removeZeroKey() {
        if (!hasZeroKey) {
            return missingValue;
        }
        hasZeroKey = false;
        size--;
        return zeroKeyValue;
    }

    private long removeInternal(long key, int hash, long[] datax, int maskx) {
        final int offset = HashingUtils.findFreeOffset(key, (hash & maskx) << 1, datax);
        if (datax[offset] != key) {
            return missingValue;
        }
        final long value = datax[offset + 1];
        LinearProbing.deleteAt(datax, offset, missingValue);
        size--;
        return value;
    }

    /**
     * @param insertedOffset where the put that crossed the threshold stored its new key - taken back if the table is
     *                       full (no migration is in flight here, data is the only array)
     */
    private void resize(int insertedOffset) {

        //log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2L, size);

        if (mask + 1 >= maxCapacity) {
            // can not grow any more: take new keys up to FULL_LOAD, then refuse them - beyond that the probes, and
            // with them every operation, get longer and longer
            final long limit = (long) ((mask + 1) * HashingUtils.FULL_LOAD);
            if (size <= limit) {
                upsizeThreshold = limit + 1;
                return;
            }
            LinearProbing.deleteAt(data, insertedOffset, missingValue);
            size--;
            throw new IllegalStateException("table is full: " + size + " entries at the maximum capacity of "
                    + (mask + 1) + " slots");
        }

        if (closed || size < syncResizeBelow) {
            resizeSync();
            return;
        }

        // the value cells are filled with missingValue on the executor, not by the application; locals - the task
        // must not keep the table reachable
        final int newCapacity = data.length;
        final long missing = missingValue;
        final AtomicBoolean started = new AtomicBoolean();
        try {
            arrayFeature = CompletableFuture.supplyAsync(() -> {
                started.set(true);
                return LinearProbing.newArray(newCapacity, missing);
            }, executor);
            allocationStarted = started;
        } catch (RuntimeException rejected) {
            // RejectedExecutionException, or whatever else the executor throws: the put has stored its key already -
            // resize here instead of failing it
            resizeSync();
        }
    }

    /**
     * Synchronous copy on the calling thread: ~7.5ns per slot, 62us at 8192 slots.
     */
    private void resizeSync() {
        data = new HashtableResizer(data, missingValue).resizeSync();
        mask = mask * 2 + 1;
        upsizeThreshold = calculateUpsizeThreshold();
        blockThreshold = calculateBlockThreshold();
    }

    private void startAsyncCopyingIfDone() {
        if (!arrayFeature.isDone()) {
            return;
        }

        final long[] newArray;
        try {
            newArray = arrayFeature.join();
        } catch (CompletionException | CancellationException failed) {
            // the allocation failed on the executor (out of memory there?) - resize here
            arrayFeature = null;
            resizeSync();
            return;
        }
        arrayFeature = null;

        final int initialPos = HashtableAsync2Resizer.findNextGapPos(data, 0);
        knownProgressCached = initialPos;
        allowedPosition = nextSegmentEnd(initialPos, initialPos);

        // NOTE: keep this in a local and never touch the 'resizer' field from the lambda below.
        // Reading the field would capture 'this', and the migrator thread would then keep the
        // whole hashtable (and both arrays) reachable - an abandoned table could never be
        // collected, and the Cleaner safety net below would never fire.
        final HashtableAsync2Resizer newResizer =
                new HashtableAsync2Resizer(data, newArray, initialPos, allowedPosition, Thread.currentThread());
        resizer = newResizer;

        try {
            copyingProcess = CompletableFuture.runAsync(newResizer::copy, executor);
        } catch (RuntimeException rejected) {
            // the table copies itself, segment by segment, where it would wait for the migrator (waitStep)
            return;
        }

        // safety net for a forgotten close(): stop the migrator once the table is unreachable
        resizerCleanable = CLEANER.register(this, new MigratorShutdown(newResizer));
    }


    /**
     * Waits until the migrator has copied the segment covering pos, updating knownProgressCached.
     */
    private void awaitMigrated(int pos) {

        long spins = 0;

        while (resizer.notInNewData(pos, knownProgressCached)) {
            knownProgressCached = resizer.getProcessedPosition();
            if (knownProgressCached == allowedPosition) {
                // The migrator copied everything it was authorized to and is now waiting for the
                // next authorization, which only this thread can give - so waiting further would
                // deadlock. The only position that can still be pending here is allowedPosition
                // itself, and that one is a gap in the old array (findNextGapPos picked it), so no
                // entry with that home was ever stored there: the new array is safe to access.
                break;
            }
            if (waitStep(++spins)) {
                throw new IllegalStateException("stuck waiting for migration of pos=" + pos
                        + " after " + spins + " spins, " + migrationState());
            }
        }
    }

    /**
     * One step of a wait for the migrator. A copying task that has not started is not waited for: the table copies the
     * authorized segment itself. Otherwise spins, and every 1024 spins rethrows a failure of the migrator and checks
     * the hang timeout. The clock starts at the first check: the usual wait - a segment, a microsecond or two - does
     * not read it, and the timeout starts at most 1024 spins late.
     *
     * @param spins of this wait, from 1
     * @return true if the migrator is stuck: the hang timeout is over
     */
    private boolean waitStep(long spins) {
        final HashtableAsync2Resizer r = resizer;
        if (r.copyIfNotStarted()) {
            knownProgressCached = r.getProcessedPosition();
            return false;
        }
        Thread.onSpinWait();
        if ((spins & HANG_CHECK_SPIN_MASK) == 0) {
            rethrowMigratorFailure();
            final long now = System.nanoTime();
            if (spins == HANG_CHECK_SPIN_MASK + 1) {
                waitDeadline = now + hangTimeoutNanos;
                return false;
            }
            return now - waitDeadline > 0;
        }
        return false;
    }

    /**
     * A migrator that died never makes progress: fail with its exception right away instead of at the hang detection.
     */
    private void rethrowMigratorFailure() {
        if (copyingProcess != null && copyingProcess.isCompletedExceptionally()) {
            awaitFuture(copyingProcess, "migration");
        }
    }

    /**
     * Waits until the migrator has copied everything up to the currently allowed position.
     */
    private void awaitAllowedPositionReached() {

        long spins = 0;

        // refresh first - a stale cached value could otherwise match the freshly published
        // allowedPosition and skip the wait entirely
        knownProgressCached = resizer.getProcessedPosition();

        while (knownProgressCached != allowedPosition) {
            if (waitStep(++spins)) {
                throw new IllegalStateException("stuck waiting for allowedPosition after "
                        + spins + " spins, " + migrationState());
            }
            knownProgressCached = resizer.getProcessedPosition();
        }
    }

    /**
     * At the block threshold a put needs the new array. If the executor has not started the allocation - it may never
     * (a queue behind busy threads, the application's own event loop) - the put allocates the array itself, and the
     * task skips it if it runs late. A started allocation is waited for, at most the hang timeout.
     */
    private void awaitArray() {
        if (!allocationStarted.get() || !awaitDone(arrayFeature)) {
            arrayFeature.complete(LinearProbing.newArray(data.length, missingValue));
        }
    }

    /**
     * Waits for a background task to end, at most the hang timeout, and fails with the state past it or if the task
     * failed.
     */
    private void awaitFuture(CompletableFuture<?> future, String what) {
        if (!awaitDone(future)) {
            throw new IllegalStateException("stuck waiting for " + what + ", " + migrationState());
        }
        if (future.isCompletedExceptionally()) {
            try {
                future.join();
            } catch (CompletionException | CancellationException e) {
                throw new IllegalStateException(what + " failed, " + migrationState(),
                        e.getCause() != null ? e.getCause() : e);
            }
        }
    }

    /**
     * Same idea as the spin waits: a blocking join hides both a stuck migrator and an executor that never scheduled
     * the task. Bounded by the hang timeout.
     * <p>
     * Not interruptible, like the spin waits: an interrupt used to fail the put (or close) waiting here, though
     * nothing was wrong. The wait goes on, and the interrupt status is set again.
     *
     * @return false if the future is still not done at the hang timeout
     */
    private boolean awaitDone(CompletableFuture<?> future) {
        final long deadline = System.nanoTime() + hangTimeoutNanos;
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    future.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    return true;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (TimeoutException e) {
                    return false;
                } catch (ExecutionException | CancellationException e) {
                    return true; // done - the caller looks at the outcome
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Diagnostic: where a key physically is, and how each array is classified for it right now.
     * Scans both arrays, so it is only for failure reporting.
     */
    String debugLocate(long originalKey) {

        final long key = originalKey ^ SEED;
        final HashtableAsync2Resizer r = resizer;
        final int hash = HashingUtils.hash(key);
        final int pos = (hash & mask) << 1;

        final StringBuilder sb = new StringBuilder("key=").append(key)
                .append(" homeOld=").append(pos)
                .append(" mask=").append(mask)
                .append(" oldLen=").append(data.length);

        sb.append(" foundInOld=").append(scanFor(key, data));

        if (r == null) {
            sb.append(" resizer=null");
        } else {
            final long[] dn = r.getNewDataArray();
            sb.append(" S=").append(r.getStartingPosition())
                    .append(" A=").append(allowedPosition)
                    .append(" P=").append(r.getProcessedPosition())
                    .append(" isInOldData=").append(r.isInOldData(pos, allowedPosition))
                    .append(" posEqS=").append(pos == r.getStartingPosition())
                    .append(" notInNewData=").append(r.notInNewData(pos, r.getProcessedPosition()))
                    .append(" homeNew=").append((hash & r.getNewMask()) << 1)
                    .append(" foundInNew=").append(scanFor(key, dn))
                    .append(" newLen=").append(dn.length);
        }
        return sb.toString();
    }

    private static int scanFor(long key, long[] arr) {
        for (int i = 0; i < arr.length; i += 2) {
            if (arr[i] == key) {
                return i;
            }
        }
        return -1;
    }

    private String migrationState() {
        final HashtableAsync2Resizer r = resizer;
        return "size=" + size + " mask=" + mask + " capacity=" + (data.length >> 1)
                + " allowedPosition=" + allowedPosition + " knownProgressCached=" + knownProgressCached
                + (r == null
                ? " resizer=null"
                : " startingPosition=" + r.getStartingPosition() + " processedPosition=" + r.getProcessedPosition()
                + " migrator: " + r.stateName())
                + " copyingProcessDone=" + (copyingProcess != null && copyingProcess.isDone());
    }

    /**
     * Segment = 1/16 of the table, clamped: small tables get small segments (async migration works for any size),
     * large ones a bounded step - an access to a position authorized but not yet copied waits for the migrator to
     * copy up to it, so the segment size bounds that wait. The step is not a hard bound: the segment goes on to the
     * next free slot, a cluster across the step extends it (no cluster may cross a boundary - the migrator and the
     * application would both write it). Clusters stay short at load 0.65 with keys nobody picked to collide - see
     * the hash seed. At 256 slots (was 1024) the put benchmark's
     * p99.99 went from 4.6us to 1.0us. The price is a hand-over per segment (~0.3-0.5us of migrator idle time):
     * copying 2^23 slots takes 74ms instead of 58ms, still ~3x faster than one thread fills the table
     * (PerfLL2MigrationThroughput).
     */
    private static final int SEGMENTS_PER_MIGRATION = 16;
    private static final int MIN_SEGMENT_LONGS = 32;
    private static final int MAX_SEGMENT_LONGS = 512;

    /**
     * Next migration boundary: the first gap at least one segment after 'from' (boundaries must be gaps - no
     * cluster crosses them). Returns the starting position (= last segment) if that gap would wrap past it.
     */
    private int nextSegmentEnd(int from, int startingPosition) {
        final int len = data.length;
        final int step = Math.min(MAX_SEGMENT_LONGS, Math.max(MIN_SEGMENT_LONGS, (len / SEGMENTS_PER_MIGRATION) & ~1));
        final int gap = HashtableAsync2Resizer.findNextGapPos(data, (from + step) & (len - 1));
        // ring distance from the start: a valid boundary lies further than 'from'
        return ((gap - startingPosition) & (len - 1)) > ((from - startingPosition) & (len - 1)) ? gap : startingPosition;
    }

    private void enableNextMigrationSegmentOrFinishMigration() {
        final int startingPosition = resizer.getStartingPosition();

        //log.debug("enableNextMigrationSegmentOrFinishMigration: allowedPosition={} startingPosition={}", allowedPosition, startingPosition);

        if (allowedPosition == startingPosition) {
            // migration completed
            switchToNewArray();
        } else {


            final int newAllowedPosition = nextSegmentEnd(allowedPosition, startingPosition);

            //log.debug("new allowedPosition: {} (data_len={})", newAllowedPosition, data.length);

            allowedPosition = newAllowedPosition;
            resizer.setAllowedPosition(allowedPosition);
        }
    }

    /**
     * Drives an in-flight migration to completion and switches to the new array, so that callers
     * can work with a single consistent array.
     * <p>
     * Note the order: the migrator only stops once it has copied everything up to startingPosition,
     * so the remaining work has to be authorized FIRST and awaited, and only then can the table
     * switch. Waiting for the copying task before authorizing it is a self-deadlock.
     */
    private void blockOnResizing() {

        if (arrayFeature != null) {
            // allocation in flight, no migration started yet - drop it (a task not run yet skips the allocation);
            // resize() is triggered again by the next put of a new key
            arrayFeature.cancel(false);
            arrayFeature = null;
        }

        if (resizer != null) {

            // switchToNewArray() clears the field, so keep the task to join it afterwards
            final CompletableFuture<Void> pendingCopy = copyingProcess;

            allowedPosition = resizer.getStartingPosition();
            resizer.setAllowedPosition(allowedPosition);
            awaitAllowedPositionReached();
            final boolean migratorRunning = resizer.isMigratorRunning();
            switchToNewArray();

            if (migratorRunning) {
                // the migrator has been signalled and unparked - wait for it to actually leave copy() (a task the
                // table took over has nothing to leave, and may never run)
                awaitFuture(pendingCopy, "migrator shutdown");
            }
        }
    }

    /**
     * Finishes an in-flight migration and switches to the new array - a pause: the rest is copied while the caller
     * waits. The table then has one array and no background work, until its next resize.
     */
    public void finishMigration() {
        blockOnResizing();
    }

    /**
     * Finishes an in-flight migration, as {@link #finishMigration()}, and releases the migration thread. The later
     * resizes are synchronous, on the calling thread, as in {@link LongLongHashtable}: the table stays fully usable,
     * and starts no background work any more.
     * <p>
     * The migration is finished rather than aborted on purpose: while it is running, entries are
     * inserted into the NEW array for already-migrated positions, so those entries exist only
     * there - dropping the new array would silently lose them.
     * <p>
     * A forgotten close() is not fatal - the Cleaner stops the migrator once the table becomes
     * unreachable - but it leaves the thread (and, with an affinity-locked executor, its core)
     * occupied until the next GC cycle notices.
     */
    @Override
    public void close() {
        closed = true;
        blockOnResizing();
    }


    private void switchToNewArray() {
        //log.debug("switchToNewArray");

        // can finalize migration - signalFinish also unparks a backing-off migrator so it does not
        // linger for up to PARK_MAX_NANOS after the table already moved on
        resizer.signalFinish();
        knownProgressCached = -1;

        if (resizerCleanable != null) {
            // deregister the safety net (running the action on a finished resizer is a no-op)
            resizerCleanable.clean();
            resizerCleanable = null;
        }

        this.data = resizer.getNewDataArray();
        mask = mask * 2 + 1;
        upsizeThreshold = calculateUpsizeThreshold();
        blockThreshold = calculateBlockThreshold();
        resizer = null;
        copyingProcess = null;

        if (VERIFY_AFTER_MIGRATION) {
            integrityCheck();
        }
    }

    private long calculateUpsizeThreshold() {
        return (int) ((mask + 1) * upsizeThresholdPerc);
    }

    private long calculateBlockThreshold() {
        return (int) ((mask + 1) * blockThresholdPerc);
    }

    /*
     * Iteration and clear work on one array: an in-flight migration is finished first (blockOnResizing, as in close).
     */

    /**
     * Removes all entries, keeping the current capacity. Finishes an in-flight migration first.
     */
    @Override
    public void clear() {
        blockOnResizing();
        LinearProbing.clear(data, missingValue);
        size = 0;
        hasZeroKey = false;
        zeroKeyValue = 0L;
    }

    /**
     * Keys in the table order, the key equal to the seed first. Finishes an in-flight migration first. The stream reads the table lazily -
     * do not modify the table until it is consumed.
     */
    @Override
    public LongStream keysStream() {
        blockOnResizing();
        return LinearProbing.keysStream(hasZeroKey, data, SEED);
    }

    /**
     * Values in the same order as {@link #keysStream()}, with the same restrictions.
     */
    @Override
    public LongStream valuesStream() {
        blockOnResizing();
        return LinearProbing.valuesStream(hasZeroKey, zeroKeyValue, data);
    }

    /**
     * Allocation-free iteration over all entries, in the order of {@link #keysStream()}. Finishes an in-flight
     * migration first. The consumer must not modify the table.
     */
    @Override
    public void forEach(LongLongConsumer consumer) {
        blockOnResizing();
        LinearProbing.forEach(hasZeroKey, zeroKeyValue, data, SEED, consumer);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long missingValue() {
        return missingValue;
    }

    /**
     * @return the hash seed - one for all tables in the JVM, see {@link LongLongHashtable#seed()}
     */
    public long seed() {
        return SEED;
    }

    /*
     * The compound operations take one probe, like put, when no resize is in flight. During a resize they are made of
     * the basic operations, which deal with the migration.
     */

    @Override
    public long getOrDefault(long key, long defaultValue) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : defaultValue;

        if (arrayFeature != null || resizer != null) {
            return containsKey(key) ? get(key) : defaultValue;
        }

        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        return data[offset] == k ? data[offset + 1] : defaultValue;
    }

    @Override
    public long putIfAbsent(long key, long value) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) return hasZeroKey ? zeroKeyValue : putZeroKey(value);

        if (arrayFeature != null || resizer != null) {
            return containsKey(key) ? get(key) : put(key, value);
        }

        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        if (data[offset] == k) {
            return data[offset + 1];
        }
        insertAt(offset, k, value);
        return missingValue;
    }

    @Override
    public long addAndGet(long key, long delta) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY) {
            final long value = (hasZeroKey ? zeroKeyValue : 0L) + delta;
            putZeroKey(value);
            return value;
        }

        if (arrayFeature != null || resizer != null) {
            final long value = (containsKey(key) ? get(key) : 0L) + delta;
            put(key, value);
            return value;
        }

        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        if (data[offset] == k) {
            return data[offset + 1] += delta;
        }
        insertAt(offset, k, delta);
        return delta;
    }

    @Override
    public long computeIfAbsent(long key, LongUnaryOperator mappingFunction) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY || arrayFeature != null || resizer != null) {
            if (containsKey(key)) {
                return get(key);
            }
        } else {
            final int offset = HashingUtils.findFreeOffset(k, data, mask);
            if (data[offset] == k) {
                return data[offset + 1];
            }
        }
        final long value = mappingFunction.applyAsLong(key);
        // a new probe through put: the function may have changed the table
        put(key, value);
        return value;
    }

    @Override
    public boolean remove(long key, long value) {

        final long k = key ^ SEED;
        if (k == NOT_ALLOWED_KEY || arrayFeature != null || resizer != null) {
            if (containsKey(key) && get(key) == value) {
                remove(key);
                return true;
            }
            return false;
        }

        final int offset = HashingUtils.findFreeOffset(k, data, mask);
        if (data[offset] != k || data[offset + 1] != value) {
            return false;
        }
        LinearProbing.deleteAt(data, offset, missingValue);
        size--;
        return true;
    }

    /**
     * Stores a new (seeded) key in the gap its probe stopped at - no resize in flight.
     */
    private void insertAt(int offset, long k, long value) {
        data[offset] = k;
        data[offset + 1] = value;
        if (++size >= upsizeThreshold) {
            resize(offset);
        }
    }

    /**
     * @return true while a resize is in flight: the new array is being allocated, or the entries copied into it. Then
     * the operations may wait for the migrator, and the old array is alive next to the new one. Two field reads.
     */
    public boolean migrationInProgress() {
        return arrayFeature != null || resizer != null;
    }

    /**
     * @return the number of slots of the current array - during a migration the old one, the new one has twice as many
     */
    @Override
    public int capacity() {
        return mask + 1;
    }

    /**
     * A resize in flight already counts with its new capacity. If more is needed, the resize is finished first
     * ({@link #finishMigration()}), then the table is rehashed synchronously.
     */
    @Override
    public void ensureCapacity(int expectedSize) {
        final int capacity = HashingUtils.capacityFor(expectedSize, upsizeThresholdPerc, maxCapacity);
        final int targetCapacity = arrayFeature != null || resizer != null ? (mask + 1) * 2 : mask + 1;
        if (capacity > targetCapacity) {
            blockOnResizing();
            if (capacity > mask + 1) {
                data = HashtableResizer.rehash(data, capacity, missingValue);
                mask = capacity - 1;
                upsizeThreshold = calculateUpsizeThreshold();
                blockThreshold = calculateBlockThreshold();
            }
        }
    }

    /**
     * All entries as {@code {key=value, ...}} - for tests and debugging, it walks the whole table. During a migration
     * only the size: the entries are split between two arrays, and the migrator is writing one of them.
     */
    @Override
    public String toString() {
        return resizer == null
                ? LinearProbing.entriesToString(hasZeroKey, zeroKeyValue, data, SEED)
                : "LongLongLL2Hashtable[size=" + size + ", migration in progress]";
    }

    /**
     * Verifies the linear-probing invariants of the current array. Must not be called while a
     * migration is in flight - the two arrays are only consistent as a whole in between.
     * <p>
     * The single check {@code findFreeOffset(key) == storedPosition} covers both failure modes a
     * broken concurrent migration produces: a duplicated key (the probe stops at the earlier copy)
     * and an unreachable key (the probe stops at a gap that appeared in the middle of its cluster).
     */
    void integrityCheck() {

        if (resizer != null) {
            throw new IllegalStateException("integrityCheck: migration in progress, " + migrationState());
        }

        final int capacity = data.length >> 1;
        long occupied = 0;

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key == NOT_ALLOWED_KEY) {
                if (data[i + 1] != missingValue) {
                    throw new IllegalStateException("integrityCheck: free slot " + i + " holds value " + data[i + 1]);
                }
                continue;
            }
            occupied++;

            final int reachableAt = HashingUtils.findFreeOffset(key, data, mask);
            if (reachableAt != i) {
                throw new IllegalStateException("integrityCheck: key=" + key + " stored at " + i
                        + " but probing from its home reaches " + reachableAt
                        + " (" + (data[reachableAt] == key ? "duplicate" : "unreachable")
                        + ", capacity=" + capacity + " size=" + size + ")");
            }
        }

        if (occupied + (hasZeroKey ? 1 : 0) != size) {
            throw new IllegalStateException("integrityCheck: size=" + size
                    + " but " + occupied + " occupied cells" + (hasZeroKey ? " and key 0" : "")
                    + " (capacity=" + capacity + ")");
        }

        if (occupied >= capacity) {
            throw new IllegalStateException("integrityCheck: no gap left, occupied=" + occupied
                    + " capacity=" + capacity);
        }
    }

//    private int desiredPosition(long key) {
//        final int hash = HashingUtils.hash(key);
//        return (hash & mask) << 1;
//    }
}
