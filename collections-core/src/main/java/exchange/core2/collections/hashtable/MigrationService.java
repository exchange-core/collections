package exchange.core2.collections.hashtable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * The threads that resize {@link LongLongLL2Hashtable}s: an allocator allocates their new arrays, a worker copies the
 * old ones into them, a portion at a time, for any number of tables in turn. A table registers when it is built and
 * leaves at its {@link LongLongLL2Hashtable#close()}.
 * <pre>
 * try (MigrationService service = MigrationService.builder().start()) {
 *     final LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(service).build();
 *     ...
 *     table.close();
 * }
 * </pre>
 * <ul>
 *     <li>Two daemon threads, started by {@link Builder#start()}, which returns once they run - after
 *     {@link Builder#threadInit} and {@link Builder#allocatorThreadInit}, which pin them to cores, for one. Without
 *     that a thread inherits the CPU affinity of the thread calling start(): start the service before that thread
 *     pins itself. Not from a static initializer of a class threadInit uses - see {@link Builder#start()}.</li>
 *     <li>The worker copies. Waiting for work: {@link WaitStrategy#SPIN_THEN_PARK} (the default) spins ~1-3ms, then
 *     parks - the table that brings work wakes it, an unpark on the table's thread, only after such a quiet period;
 *     {@link WaitStrategy#BUSY_SPIN} spins all the time - for a core of its own, the shortest hand-over.</li>
 *     <li>Work goes in turns, over the tables with a migration in flight only: a portion of one table - ~256 slots
 *     (~2us), to the end of the cluster there - then the next table. A table that has authorized more, the rest of its
 *     migration at once, gets it portion by portion.</li>
 *     <li>The allocator only allocates the large arrays - above 2^18 slots, 4MB - and parks in between: zeroing 256MB
 *     takes tens of milliseconds, and the copying for the other tables does not wait for it, nor the small arrays -
 *     the worker allocates those between portions (~0.2ms at most). {@link Builder#allocatorThread(boolean)} false
 *     allocates all on the worker: one thread, and the copying waits for each allocation.</li>
 *     <li>The tables are closed first: {@link #close()} refuses while a table is registered - built, not closed and
 *     not garbage-collected (a forgotten table is dropped once the GC has collected it, with its arrays).</li>
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
    /**
     * Arrays up to 2^18 slots (4MB, ~0.2ms to zero) the worker allocates itself, between portions; larger ones the
     * allocator thread - a large allocation then holds up only the large ones after it, not a small table's next array.
     */
    static final int SMALL_ARRAY_SLOTS = 1 << 18;

    /** an idle worker looks for collected tables every 2^20 spins (~30ms), and before it parks */
    private static final int PRUNE_SPIN_MASK = (1 << 20) - 1;
    /** parks are woken by the tables - the timeout only bounds a missed wake-up */
    private static final long PARK_NANOS = 100_000_000L;

    private static final VarHandle COPY_QUEUE;
    private static final VarHandle ALLOC_QUEUE;

    static {
        try {
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            COPY_QUEUE = lookup.findVarHandle(MigrationService.class, "copyQueue", Slot.class);
            ALLOC_QUEUE = lookup.findVarHandle(MigrationService.class, "allocQueue", Slot.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final WaitStrategy waitStrategy;
    /** null for a service without threads - the tests run serveOnce() themselves */
    private final Thread worker;
    /** null: the worker allocates */
    private final Thread allocator;
    private final CountDownLatch ready;
    private volatile Throwable initFailure;

    /** all the tables (their slots), guarded by itself - O(1) to register and to leave, read by close() */
    private final Set<Slot> registry = new HashSet<>();
    /** where the GC puts the references of the tables it collected - see pruneCollected() */
    private final ReferenceQueue<LongLongLL2Hashtable> collectedTables = new ReferenceQueue<>();

    /** slots the tables gave the worker work since its last round - a stack, linked through Slot.nextCopy */
    private volatile Slot copyQueue;
    /** the worker's round: the slots with a migration in flight (worker only) */
    private final ArrayList<Slot> active = new ArrayList<>();
    /** slots with an array request for the allocator - a stack, linked through Slot.nextAlloc */
    private volatile Slot allocQueue;

    private volatile boolean stopping;
    /** the worker / the allocator is parked, or about to park - see wakeWorker(), wakeAllocator() */
    private volatile boolean workerParked;
    private volatile boolean allocatorParked;
    /** interrupted while parking - set again when the thread ends (each field confined to its thread) */
    private boolean workerInterrupted;
    private boolean allocatorInterrupted;

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link MigrationService}, all optional.
     */
    public static final class Builder {

        private WaitStrategy waitStrategy = WaitStrategy.SPIN_THEN_PARK;
        private Supplier<? extends AutoCloseable> threadInit;
        private Supplier<? extends AutoCloseable> allocatorThreadInit;
        private boolean allocatorThread = true;
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
         * The same for the allocator thread. Its work is memory zeroing, now and then: a housekeeping core is a good
         * place for it, not the application's.
         */
        public Builder allocatorThreadInit(Supplier<? extends AutoCloseable> allocatorThreadInit) {
            this.allocatorThreadInit = Objects.requireNonNull(allocatorThreadInit, "allocatorThreadInit");
            return this;
        }

        /**
         * A thread of its own for the allocations (default true). False: the worker allocates - one thread, and the
         * copying for all the tables waits for each allocation.
         */
        public Builder allocatorThread(boolean allocatorThread) {
            this.allocatorThread = allocatorThread;
            return this;
        }

        /**
         * Name of the worker thread (default "ll2-migration"); the allocator gets "-allocator" appended.
         */
        public Builder threadName(String threadName) {
            this.threadName = Objects.requireNonNull(threadName, "threadName");
            return this;
        }

        /** how long start() waits for the threads - lower only in tests */
        Builder startTimeoutMillis(long startTimeoutMillis) {
            this.startTimeoutMillis = startTimeoutMillis;
            return this;
        }

        /**
         * Starts the threads and waits until they run - at most 10s.
         * <p>
         * Not from a static initializer of a class that {@link #threadInit} uses (a lambda in that class, say): the
         * worker would wait for the class, the class for the worker - start() fails after the 10s.
         *
         * @throws IllegalStateException if a thread init failed, or the threads have not run within 10s
         */
        public MigrationService start() {
            final MigrationService service = new MigrationService(this);
            service.worker.start();
            if (service.allocator != null) {
                service.allocator.start();
            }
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
                service.stopping = true; // the threads end as soon as they get past their init
                throw new IllegalStateException("the migration threads have not started within " + startTimeoutMillis
                        + "ms: a thread init has not returned - is start() called from a static initializer of a class"
                        + " that the init uses? The thread waits for that class, the class for the thread");
            }
            if (service.initFailure != null) {
                service.stopping = true;
                service.wakeWorker();
                service.wakeAllocator();
                throw new IllegalStateException("a migration thread failed to start", service.initFailure);
            }
            return service;
        }
    }

    private MigrationService(Builder builder) {
        this.waitStrategy = builder.waitStrategy;
        this.worker = new Thread(() -> runWorker(builder.threadInit), builder.threadName);
        this.worker.setDaemon(true);
        if (builder.allocatorThread) {
            this.allocator = new Thread(() -> runAllocator(builder.allocatorThreadInit),
                    builder.threadName + "-allocator");
            this.allocator.setDaemon(true);
        } else {
            this.allocator = null;
        }
        this.ready = new CountDownLatch(builder.allocatorThread ? 2 : 1);
    }

    /**
     * A service without threads: nothing is served unless {@link #serveOnce()} is called - for the tests, which play
     * the worker (the allocations included) step by step.
     */
    MigrationService() {
        this.waitStrategy = WaitStrategy.SPIN_THEN_PARK;
        this.worker = null;
        this.allocator = null;
        this.ready = new CountDownLatch(0);
    }

    /** runs the init, counts down 'ready'; @return what the init returned, or a failure marker */
    private AutoCloseable init(Supplier<? extends AutoCloseable> threadInit) throws InitFailed {
        try {
            final AutoCloseable init = threadInit == null ? null : threadInit.get();
            ready.countDown();
            return init;
        } catch (Throwable e) {
            initFailure = e;
            ready.countDown();
            throw new InitFailed();
        }
    }

    private static final class InitFailed extends Exception {
        InitFailed() {
            super(null, null, false, false);
        }
    }

    private static void close(AutoCloseable init) {
        if (init != null) {
            try {
                init.close();
            } catch (Exception ignored) {
                // the thread ends anyway
            }
        }
    }

    private void runWorker(Supplier<? extends AutoCloseable> threadInit) {
        final AutoCloseable init;
        try {
            init = init(threadInit);
        } catch (InitFailed e) {
            return;
        }
        try {
            int idle = 0;
            while (!stopping) {
                idle = serveOnce() ? 0 : idle(idle + 1);
            }
        } finally {
            active.clear(); // the collected tables' migrations, if any - the arrays go with them
            if (workerInterrupted) {
                Thread.currentThread().interrupt();
            }
            close(init);
        }
    }

    private void runAllocator(Supplier<? extends AutoCloseable> threadInit) {
        final AutoCloseable init;
        try {
            init = init(threadInit);
        } catch (InitFailed e) {
            return;
        }
        try {
            while (!stopping) {
                Slot slot = (Slot) ALLOC_QUEUE.getAndSet(this, null);
                if (slot == null) {
                    allocatorIdle();
                    continue;
                }
                while (slot != null) {
                    final Slot next = slot.nextAlloc;
                    slot.nextAlloc = null;
                    // cleared first: a request posted from now on queues the slot again
                    slot.allocQueued = false;
                    if (!slot.table.refersTo(null)) {
                        slot.serveAllocation();
                    }
                    slot = next;
                }
            }
        } finally {
            if (allocatorInterrupted) {
                Thread.currentThread().interrupt();
            }
            close(init);
        }
    }

    /**
     * One round of the worker: the slots the tables brought since the last round join the active ones; each active
     * slot once - a portion to copy (and the requested array, if the worker allocates). A slot whose migration has
     * ended leaves - the table brings it back with its next work - and so does one whose table the GC has collected.
     *
     * @return true if there was work
     */
    boolean serveOnce() {
        if (copyQueue != null) {
            Slot slot = (Slot) COPY_QUEUE.getAndSet(this, null);
            while (slot != null) {
                final Slot next = slot.nextCopy;
                slot.nextCopy = null;
                active.add(slot);
                slot = next;
            }
        }
        final boolean allocates = allocator == null;
        boolean worked = false;
        boolean collected = false;
        for (int i = 0; i < active.size(); ) {
            final Slot slot = active.get(i);
            if (slot.table.refersTo(null)) {
                collected = true;
                removeActive(i);
                continue;
            }
            if (slot.serve(allocates)) {
                worked = true;
                i++;
                continue;
            }
            if (slot.hasWork(allocates)) {
                i++; // a migration in flight, nothing authorized right now: the table authorizes more later
                continue;
            }
            // Leaves. Dekker with Slot.activate(): the table publishes its work, then reads 'active'; the worker clears
            // 'active', then looks for work. If the table saw 'active' set, the worker sees the work - and keeps the
            // slot, unless the table queued it meanwhile (then it comes back with the queue).
            slot.active = false;
            if (slot.hasWork(allocates) && Slot.ACTIVE.compareAndSet(slot, false, true)) {
                i++;
                continue;
            }
            removeActive(i);
        }
        if (collected) {
            pruneCollected();
        }
        return worked;
    }

    private void removeActive(int i) {
        final int last = active.size() - 1;
        active.set(i, active.get(last));
        active.remove(last);
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
                if ((spins & PRUNE_SPIN_MASK) == 0) {
                    pruneCollected();
                }
            }
            return spins;
        }
        pruneCollected();
        workerParked = true;
        if (!stopping && !serveOnce()) {
            // an interrupt does not stop the service - close() does - but with the status set parkNanos returns at once
            if (Thread.interrupted()) {
                workerInterrupted = true;
            }
            LockSupport.parkNanos(this, PARK_NANOS);
        }
        workerParked = false;
        return spins;
    }

    /** the allocator has nothing to do: parks - the same handshake as the worker's, over the allocation queue */
    private void allocatorIdle() {
        allocatorParked = true;
        if (!stopping && allocQueue == null) {
            if (Thread.interrupted()) {
                allocatorInterrupted = true;
            }
            LockSupport.parkNanos(this, PARK_NANOS);
        }
        allocatorParked = false;
    }

    /**
     * Called by a table after it published work for the worker (a volatile write): unparks a parked worker. A busy
     * worker costs one volatile read. Dekker with idle(): one of the two sees the other's write.
     */
    void wakeWorker() {
        if (workerParked) {
            LockSupport.unpark(worker);
        }
    }

    private void wakeAllocator() {
        if (allocatorParked) {
            LockSupport.unpark(allocator);
        }
    }

    /** puts a slot on the worker's queue - a CAS push, no allocation */
    private void queueCopy(Slot slot) {
        Slot head;
        do {
            head = copyQueue;
            slot.nextCopy = head;
        } while (!COPY_QUEUE.compareAndSet(this, head, slot));
    }

    /** an array request: a small one, or any without an allocator thread, for the worker; a large one for the allocator */
    private void arrayRequested(Slot slot, int capacity) {
        if (allocator == null || capacity <= SMALL_ARRAY_SLOTS) {
            slot.activate();
            return;
        }
        if (!slot.allocQueued && Slot.ALLOC_QUEUED.compareAndSet(slot, false, true)) {
            Slot head;
            do {
                head = allocQueue;
                slot.nextAlloc = head;
            } while (!ALLOC_QUEUE.compareAndSet(this, head, slot));
        }
        wakeAllocator();
    }

    Slot register(LongLongLL2Hashtable table, long missingValue) {
        synchronized (registry) {
            if (stopping) {
                throw new IllegalStateException("the migration service is closed");
            }
            final Slot slot = new Slot(this, table, missingValue, collectedTables);
            registry.add(slot);
            return slot;
        }
    }

    private void deregister(Slot slot) {
        synchronized (registry) {
            registry.remove(slot);
        }
    }

    /**
     * Drops the slots of the tables the GC has collected - and with them an array allocated for one and not taken, or
     * the arrays of a migration in flight. The GC queues their references: nothing to do costs one poll. The worker
     * does it while idle - a table forgotten while its array was allocated never reaches its round - and close().
     */
    private void pruneCollected() {
        Reference<? extends LongLongLL2Hashtable> ref;
        while ((ref = collectedTables.poll()) != null) {
            final Slot slot = ((TableRef) ref).slot;
            synchronized (registry) {
                registry.remove(slot);
            }
        }
    }

    /**
     * @return the tables registered - built, not closed, not collected by the GC yet
     */
    int registeredTables() {
        synchronized (registry) {
            int n = 0;
            for (Slot slot : registry) {
                if (!slot.table.refersTo(null)) {
                    n++;
                }
            }
            return n;
        }
    }

    /** the requested arrays only, no copying - for the tests, which play the worker step by step */
    boolean serveAllocations() {
        boolean worked = false;
        for (Slot slot : slots()) {
            worked |= slot.serveAllocation();
        }
        return worked;
    }

    /** the registered slots, collected tables included until pruned - for the tests */
    Slot[] slots() {
        synchronized (registry) {
            return registry.toArray(new Slot[0]);
        }
    }

    int slotCount() {
        synchronized (registry) {
            return registry.size();
        }
    }

    /** the worker's round - for the tests */
    int activeSlots() {
        return active.size();
    }

    /** an unpark of a parked worker - for the tests */
    void wake() {
        wakeWorker();
    }

    /**
     * Stops the threads and waits until they end. The tables must be closed first. Forgets the tables the GC has
     * collected, with the arrays of a migration they had in flight.
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
            registry.clear();
        }
        while (collectedTables.poll() != null) {
            // the slots are dropped already
        }
        join(worker);
        join(allocator);
        copyQueue = null;
        allocQueue = null;
        if (worker == null) {
            active.clear(); // no worker thread: the caller played it, the round is its
        }
    }

    private static void join(Thread thread) {
        if (thread == null || !thread.isAlive() || thread == Thread.currentThread()) {
            return;
        }
        LockSupport.unpark(thread);
        boolean interrupted = false;
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "MigrationService[" + waitStrategy + ", tables=" + registeredTables()
                + (worker == null ? ", no threads" : ", worker " + worker.getName() + (worker.isAlive() ? "" : " ended"))
                + (allocator == null ? "" : ", allocator" + (allocator.isAlive() ? "" : " ended"))
                + (stopping ? ", closed" : "") + "]";
    }

    /** the weak reference of a slot to its table: queued by the GC once the table is collected, the slot with it */
    private static final class TableRef extends WeakReference<LongLongLL2Hashtable> {

        private final Slot slot;

        private TableRef(LongLongLL2Hashtable table, Slot slot, ReferenceQueue<LongLongLL2Hashtable> queue) {
            super(table, queue);
            this.slot = slot;
        }
    }

    /**
     * What a table and the service share: the request of a new array, and the migration in progress. Created once, when
     * the table is built - starting and ending a resize only publish into it. Holds the table weakly: nothing here, nor
     * in the migration, refers to the table, so a forgotten one can be collected.
     * <p>
     * The array request is one word, generation and state: the table posts REQUESTED for a new generation; the
     * allocator takes it (ALLOCATING) and publishes the array (DONE); the table takes the array, or gives the request
     * up (NONE) - then the allocator drops an array it allocates late, and a late result never reaches a later request.
     */
    static final class Slot {

        private static final int NONE = 0;
        private static final int REQUESTED = 1;
        private static final int ALLOCATING = 2;
        private static final int DONE = 3;
        private static final VarHandle ALLOC;
        static final VarHandle ACTIVE;
        static final VarHandle ALLOC_QUEUED;

        static {
            try {
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                ALLOC = lookup.findVarHandle(Slot.class, "alloc", long.class);
                ACTIVE = lookup.findVarHandle(Slot.class, "active", boolean.class);
                ALLOC_QUEUED = lookup.findVarHandle(Slot.class, "allocQueued", boolean.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final MigrationService service;
        private final TableRef table;
        private final long missingValue;

        /** generation << 2 | state */
        private volatile long alloc;
        /** of the request - written before it is published */
        private int allocCapacity;
        /** the array - written before DONE is published */
        private long[] allocated;

        /** the migration in progress, null between migrations */
        private volatile HashtableAsync2Resizer resizer;

        /** on the worker's round or queue for it - set by the table (CAS), cleared by the worker */
        private volatile boolean active;
        private Slot nextCopy;
        /** on the allocator's queue - set by the table (CAS), cleared by the allocator */
        private volatile boolean allocQueued;
        private Slot nextAlloc;

        private Slot(MigrationService service, LongLongLL2Hashtable table, long missingValue,
                     ReferenceQueue<LongLongLL2Hashtable> collected) {
            this.service = service;
            this.table = new TableRef(table, this, collected);
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
            service.arrayRequested(this, capacity);
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
         * @return true if the allocation of the array of the request has started (or is done)
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

        /** Publishes the migration to the worker - and puts the slot on its round. */
        void startCopying(HashtableAsync2Resizer resizer) {
            this.resizer = resizer;
            activate();
        }

        /** The table authorized more of its migration: wakes the worker. The slot is on its round already. */
        void authorized() {
            activate();
        }

        void endCopying() {
            resizer = null;
        }

        void deregister() {
            service.deregister(this);
        }

        /**
         * After a volatile write of the work: queues the slot for the worker unless it is on the round already, and
         * wakes the worker. One volatile read each when it is - per segment.
         */
        private void activate() {
            if (!active && ACTIVE.compareAndSet(this, false, true)) {
                service.queueCopy(this);
            }
            service.wakeWorker();
        }

        /* ---- the service's side ---- */

        /**
         * The worker's turn: a portion of the migration, and a requested array the worker allocates - a small one, any
         * without an allocator thread.
         *
         * @return true if there was work
         */
        boolean serve(boolean allocatesAll) {
            boolean worked = serveAllocation(allocatesAll ? Integer.MAX_VALUE : SMALL_ARRAY_SLOTS);
            final HashtableAsync2Resizer r = resizer;
            if (r != null && r.doWork()) {
                worked = true;
            }
            return worked;
        }

        /** a migration in flight - or a requested array the worker allocates */
        boolean hasWork(boolean allocatesAll) {
            return resizer != null
                    || ((alloc & 3) == REQUESTED && (allocatesAll || allocCapacity <= SMALL_ARRAY_SLOTS));
        }

        boolean serveAllocation() {
            return serveAllocation(Integer.MAX_VALUE);
        }

        /** the requested array, if its capacity is within the bound - read after the request, so it is the request's */
        private boolean serveAllocation(int maxCapacity) {
            final long w = alloc;
            return (w & 3) == REQUESTED && allocCapacity <= maxCapacity && allocate(w);
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
            return "array request #" + (w >>> 2) + " " + new String[]{"none", "requested", "allocating", "done"}[(int) (w & 3)]
                    + (active ? ", on the worker's round" : "");
        }
    }
}
