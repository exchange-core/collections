package exchange.core2.collections.hashtable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * The thread that resizes {@link LongLongLL2Hashtable}s: it allocates their new arrays and copies the old ones into
 * them, a segment at a time, for any number of tables in turn. A table registers when it is built and leaves at its
 * {@link LongLongLL2Hashtable#close()}.
 * <pre>
 * try (MigrationService service = MigrationService.builder().start()) {
 *     final LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(service).build();
 *     ...
 *     table.close();
 * }
 * </pre>
 * <ul>
 *     <li>One worker, a daemon thread, started by {@link Builder#start()}, which returns once the worker runs - after
 *     {@link Builder#threadInit}, which pins it to a core, for one. Without that the worker inherits the CPU affinity
 *     of the thread calling start(): start the service before that thread pins itself. Not from a static
 *     initializer of a class threadInit uses - see {@link Builder#start()}.</li>
 *     <li>Waiting for work: {@link WaitStrategy#SPIN_THEN_PARK} (the default) spins ~1-3ms, then parks - the table
 *     that brings work wakes it, an unpark on the table's thread, only after such a quiet period;
 *     {@link WaitStrategy#BUSY_SPIN} spins all the time - for a core of its own, the shortest hand-over.</li>
 *     <li>Work goes in turns: one segment of one table (256 slots, ~2us), or one new array. A large allocation keeps
 *     the other tables waiting for its duration - they copy the segments they need themselves meanwhile.</li>
 *     <li>The tables are closed first: {@link #close()} refuses while a table is registered - built, not closed and
 *     not garbage-collected (a forgotten table is dropped once the GC has collected it).</li>
 * </ul>
 */
public final class MigrationService implements AutoCloseable {

    /**
     * How the worker waits for work.
     */
    public enum WaitStrategy {
        /** spins ~1-3ms, then parks until a table brings work - an unpark syscall on that table's thread */
        SPIN_THEN_PARK,
        /** spins all the time: one core, busy, and the shortest hand-over */
        BUSY_SPIN
    }

    private static final int SPINS_BEFORE_PARK = 64 * 1024;
    /** the worker yields its core every 1024 spins: the application may share it */
    private static final int YIELD_SPIN_MASK = 1023;
    /** parks are woken by the tables - the timeout only bounds a missed wake-up */
    private static final long PARK_NANOS = 100_000_000L;

    private final WaitStrategy waitStrategy;
    /** null for a service without a worker - the tests run serveOnce() themselves */
    private final Thread worker;
    private final CountDownLatch ready = new CountDownLatch(1);
    private volatile Throwable initFailure;

    /** copy-on-write, changed under the lock of 'registry' */
    private volatile Slot[] slots = new Slot[0];
    private final Object registry = new Object();

    private volatile boolean stopping;
    /** the worker is parked, or about to park - see wake() */
    private volatile boolean parked;
    /** the worker was interrupted while parking - set again when it ends (worker thread only) */
    private boolean interrupted;

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link MigrationService}, all optional.
     */
    public static final class Builder {

        private WaitStrategy waitStrategy = WaitStrategy.SPIN_THEN_PARK;
        private Supplier<? extends AutoCloseable> threadInit;
        private String threadName = "ll2-migration";
        private long startTimeoutMillis = 10_000;

        private Builder() {
        }

        /**
         * How the worker waits for work (default {@link WaitStrategy#SPIN_THEN_PARK}).
         */
        public Builder waitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = Objects.requireNonNull(waitStrategy, "waitStrategy");
            return this;
        }

        /**
         * Runs on the worker before {@link #start()} returns - e.g. {@code CpuAffinity::acquireCore} of
         * collections-affinity; what it returns is closed when the worker ends. A failure fails start().
         */
        public Builder threadInit(Supplier<? extends AutoCloseable> threadInit) {
            this.threadInit = Objects.requireNonNull(threadInit, "threadInit");
            return this;
        }

        /**
         * Name of the worker thread (default "ll2-migration").
         */
        public Builder threadName(String threadName) {
            this.threadName = Objects.requireNonNull(threadName, "threadName");
            return this;
        }

        /** how long start() waits for the worker - lower only in tests */
        Builder startTimeoutMillis(long startTimeoutMillis) {
            this.startTimeoutMillis = startTimeoutMillis;
            return this;
        }

        /**
         * Starts the worker and waits until it runs - at most 10s.
         * <p>
         * Not from a static initializer of a class that {@link #threadInit} uses (a lambda in that class, say): the
         * worker would wait for the class, the class for the worker - start() fails after the 10s.
         *
         * @throws IllegalStateException if {@link #threadInit} failed, or the worker has not run within 10s
         */
        public MigrationService start() {
            final MigrationService service = new MigrationService(waitStrategy, threadInit, threadName);
            service.worker.start();
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(startTimeoutMillis);
            boolean interrupted = false;
            boolean ready = false;
            while (true) {
                try {
                    ready = service.ready.await(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (!ready) {
                service.stopping = true; // the worker ends as soon as it gets past threadInit
                throw new IllegalStateException("the migration thread has not started within " + startTimeoutMillis
                        + "ms: threadInit has not returned - is start() called from a static initializer of a class"
                        + " that threadInit uses? The worker waits for that class, the class for the worker");
            }
            if (service.initFailure != null) {
                throw new IllegalStateException("the migration thread failed to start", service.initFailure);
            }
            return service;
        }
    }

    private MigrationService(WaitStrategy waitStrategy, Supplier<? extends AutoCloseable> threadInit, String name) {
        this.waitStrategy = waitStrategy;
        this.worker = new Thread(() -> run(threadInit), name);
        this.worker.setDaemon(true);
    }

    /**
     * A service without a worker: nothing is served unless {@link #serveOnce()} is called - for the tests, which
     * play the worker step by step.
     */
    MigrationService() {
        this.waitStrategy = WaitStrategy.SPIN_THEN_PARK;
        this.worker = null;
        this.ready.countDown();
    }

    private void run(Supplier<? extends AutoCloseable> threadInit) {
        final AutoCloseable init;
        try {
            init = threadInit == null ? null : threadInit.get();
        } catch (Throwable e) {
            initFailure = e;
            ready.countDown();
            return;
        }
        ready.countDown();
        try {
            int idle = 0;
            while (!stopping) {
                idle = serveOnce() ? 0 : idle(idle + 1);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (init != null) {
                try {
                    init.close();
                } catch (Exception ignored) {
                    // the worker ends anyway
                }
            }
        }
    }

    /**
     * One round: each registered table once - an array to allocate, a segment to copy. Drops the tables the GC has
     * collected.
     *
     * @return true if there was work
     */
    boolean serveOnce() {
        boolean worked = false;
        boolean collected = false;
        for (Slot slot : slots) {
            if (slot.table.refersTo(null)) {
                collected = true;
            } else if (slot.serve()) {
                worked = true;
            }
        }
        if (collected) {
            synchronized (registry) {
                slots = Arrays.stream(slots).filter(s -> !s.table.refersTo(null)).toArray(Slot[]::new);
            }
        }
        return worked;
    }

    /**
     * No work in the last round: spin, and yield every 1024 spins - the application may share this core, and it brings
     * the work. SPIN_THEN_PARK parks after a while: it publishes 'parked', looks for work once more - a table that
     * brought work before it read 'parked' is served here, one that brings it later unparks the worker.
     *
     * @return the spins so far
     */
    private int idle(int spins) {
        if (waitStrategy == WaitStrategy.BUSY_SPIN || spins < SPINS_BEFORE_PARK) {
            Thread.onSpinWait();
            if ((spins & YIELD_SPIN_MASK) == 0) {
                Thread.yield();
            }
            return spins;
        }
        parked = true;
        if (!stopping && !serveOnce()) {
            // an interrupt does not stop the service - close() does - but with the status set parkNanos returns at once
            if (Thread.interrupted()) {
                interrupted = true;
            }
            LockSupport.parkNanos(this, PARK_NANOS);
        }
        parked = false;
        return spins;
    }

    /**
     * Called by a table after it published work (a volatile write): unparks a parked worker. A busy worker costs one
     * volatile read. Dekker with idle(): one of the two sees the other's write.
     */
    void wake() {
        if (parked) {
            LockSupport.unpark(worker);
        }
    }

    Slot register(LongLongLL2Hashtable table, long missingValue) {
        synchronized (registry) {
            if (stopping) {
                throw new IllegalStateException("the migration service is closed");
            }
            final Slot slot = new Slot(this, table, missingValue);
            final Slot[] grown = Arrays.copyOf(slots, slots.length + 1);
            grown[slots.length] = slot;
            slots = grown;
            return slot;
        }
    }

    private void deregister(Slot slot) {
        synchronized (registry) {
            slots = Arrays.stream(slots).filter(s -> s != slot).toArray(Slot[]::new);
        }
    }

    /**
     * @return the tables registered - built, not closed, not collected by the GC yet
     */
    int registeredTables() {
        int n = 0;
        for (Slot slot : slots) {
            if (!slot.table.refersTo(null)) {
                n++;
            }
        }
        return n;
    }

    /** the requested arrays only, no copying - for the tests, which play the worker step by step */
    boolean serveAllocations() {
        boolean worked = false;
        for (Slot slot : slots) {
            worked |= slot.serveAllocation();
        }
        return worked;
    }

    /** the slots, collected tables included until the worker drops them - for the tests */
    Slot[] slots() {
        return slots;
    }

    int slotCount() {
        return slots.length;
    }

    /**
     * Stops the worker and waits until it ends. The tables must be closed first.
     *
     * @throws IllegalStateException if a table is still registered - built, not closed, not garbage-collected
     */
    @Override
    public void close() {
        synchronized (registry) {
            final int registered = registeredTables();
            if (registered > 0) {
                throw new IllegalStateException(registered + " tables still use the migration service - close them"
                        + " first");
            }
            stopping = true;
        }
        if (worker != null && worker.isAlive() && worker != Thread.currentThread()) {
            LockSupport.unpark(worker);
            boolean interrupted = false;
            while (worker.isAlive()) {
                try {
                    worker.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return "MigrationService[" + waitStrategy + ", tables=" + registeredTables()
                + (worker == null ? ", no worker" : ", worker " + worker.getName() + (worker.isAlive() ? "" : " ended"))
                + (stopping ? ", closed" : "") + "]";
    }

    /**
     * What a table and the worker share: the request of a new array, and the migration in progress. Created once, when
     * the table is built - starting and ending a resize only publish into it. Holds the table weakly: nothing here, nor
     * in the migration, refers to the table, so a forgotten one can be collected.
     * <p>
     * The array request is one word, generation and state: the table posts REQUESTED for a new generation; the worker
     * takes it (ALLOCATING) and publishes the array (DONE); the table takes the array, or gives the request up (NONE)
     * - then the worker drops an array it allocates late, and a late result never reaches a later request.
     */
    static final class Slot {

        private static final int NONE = 0;
        private static final int REQUESTED = 1;
        private static final int ALLOCATING = 2;
        private static final int DONE = 3;
        private static final VarHandle ALLOC;

        static {
            try {
                ALLOC = MethodHandles.lookup().findVarHandle(Slot.class, "alloc", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final MigrationService service;
        private final WeakReference<LongLongLL2Hashtable> table;
        private final long missingValue;

        /** generation << 2 | state */
        private volatile long alloc;
        /** of the request - written before it is published */
        private int allocCapacity;
        /** the array - written before DONE is published */
        private long[] allocated;

        /** the migration in progress, null between migrations */
        private volatile HashtableAsync2Resizer resizer;

        private Slot(MigrationService service, LongLongLL2Hashtable table, long missingValue) {
            this.service = service;
            this.table = new WeakReference<>(table);
            this.missingValue = missingValue;
        }

        /* ---- the table's side ---- */

        /**
         * Asks for an array of the given capacity, in slots.
         *
         * @return the generation of the request
         */
        int requestArray(int capacity) {
            final int generation = (int) (alloc >>> 2) + 1;
            allocCapacity = capacity;
            alloc = ((long) generation << 2) | REQUESTED;
            service.wake();
            return generation;
        }

        /**
         * @return the array of the request, or null if it is not allocated yet
         */
        long[] takeArray(int generation) {
            if (alloc != (((long) generation << 2) | DONE)) {
                return null;
            }
            final long[] array = allocated;
            allocated = null;
            alloc = (long) generation << 2;
            return array;
        }

        /**
         * @return true if the worker has started allocating the array of the request (or has done it)
         */
        boolean allocationStarted(int generation) {
            final long w = alloc;
            return (int) (w >>> 2) == generation && (w & 3) >= ALLOCATING;
        }

        /**
         * Gives the request up: the table allocates the array itself, or finishes without it. An array allocated
         * already, or later, is dropped.
         */
        void abandonArray(int generation) {
            final long previous = (long) ALLOC.getAndSet(this, (long) generation << 2);
            if ((previous & 3) == DONE) {
                allocated = null;
            }
        }

        /** Publishes the migration to the worker - and wakes it. */
        void startCopying(HashtableAsync2Resizer resizer) {
            this.resizer = resizer;
            service.wake();
        }

        /** The table authorized the next segment: wakes the worker. */
        void authorized() {
            service.wake();
        }

        void endCopying() {
            resizer = null;
        }

        void deregister() {
            service.deregister(this);
        }

        /* ---- the worker's side ---- */

        /**
         * One turn: the requested array, if any, and the authorized segment, if any.
         *
         * @return true if there was work
         */
        boolean serve() {
            boolean worked = serveAllocation();
            final HashtableAsync2Resizer r = resizer;
            if (r != null && r.doWork()) {
                worked = true;
            }
            return worked;
        }

        boolean serveAllocation() {
            final long w = alloc;
            return (w & 3) == REQUESTED && allocate(w);
        }

        private boolean allocate(long requested) {
            // the capacity is of this request: the table writes the next one only after it gave this one up, and then
            // the CAS below fails
            final int capacity = allocCapacity;
            final long claimed = (requested & ~3L) | ALLOCATING;
            if (!ALLOC.compareAndSet(this, requested, claimed)) {
                return false;
            }
            final long[] array;
            try {
                array = LinearProbing.newArray(capacity, missingValue);
            } catch (Throwable e) {
                // out of memory, likely: give the request back - the table allocates at its block threshold
                ALLOC.compareAndSet(this, claimed, requested & ~3L);
                return true;
            }
            allocated = array;
            if (!ALLOC.compareAndSet(this, claimed, (requested & ~3L) | DONE)) {
                allocated = null; // the table gave the request up meanwhile
            }
            return true;
        }

        /** the array allocated and not taken - for the tests */
        long[] allocated() {
            return allocated;
        }

        String state() {
            final long w = alloc;
            return "array request #" + (w >>> 2) + " " + new String[]{"none", "requested", "allocating", "done"}[(int) (w & 3)];
        }
    }
}
