package exchange.core2.collections.hashtable;

import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The MigrationService and the tables using it: the work shared between the worker and the tables, the
 * registrations, the array requests, the worker thread.
 */
public class LongLongLL2HashtableServiceTest {

    /** 1M keys, ~16 asynchronous migrations from tiny tables on (sync below 100 entries), both wait strategies. */
    @Test(timeout = 60_000)
    public void should_keep_all_entries_with_a_parking_worker() {
        try (MigrationService service = MigrationService.builder().start()) {
            checkAgainstReference(table(service, 16, 100), 1_000_000, 1L);
        }
    }

    @Test(timeout = 60_000)
    public void should_keep_all_entries_with_a_spinning_worker() {
        try (MigrationService service = MigrationService.builder()
                .waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN).start()) {
            checkAgainstReference(table(service, 16, 100), 1_000_000, 2L);
        }
    }

    /** A worker that never comes: the tables do all the work - allocations at the block threshold, the copying. */
    @Test(timeout = 60_000)
    public void table_does_the_work_the_worker_does_not() {
        final MigrationService noWorker = new MigrationService();
        checkAgainstReference(table(noWorker, 16, 0), 300_000, 3L);
        assertThat(noWorker.registeredTables(), is(0));
        noWorker.close();
    }

    /** One worker for four tables growing at once, puts round-robin. */
    @Test(timeout = 60_000)
    public void tables_share_one_worker() {
        try (MigrationService service = MigrationService.builder().start()) {
            final LongLongLL2Hashtable[] tables = new LongLongLL2Hashtable[4];
            final Long2LongHashMap[] refs = new Long2LongHashMap[4];
            for (int t = 0; t < 4; t++) {
                tables[t] = table(service, 16, 0);
                refs[t] = new Long2LongHashMap(0L);
            }
            assertThat(service.registeredTables(), is(4));
            final Random rand = new Random(4L);
            for (int i = 0; i < 400_000; i++) {
                final long key = rand.nextLong() | 1L;
                final long value = rand.nextLong();
                assertEquals(refs[i & 3].put(key, value), tables[i & 3].put(key, value));
            }
            for (int t = 0; t < 4; t++) {
                tables[t].finishMigration();
                assertEquals(refs[t].size(), tables[t].size());
                final Long2LongHashMap.KeyIterator it = refs[t].keySet().iterator();
                while (it.hasNext()) {
                    final long key = it.nextValue();
                    assertEquals(refs[t].get(key), tables[t].get(key));
                }
                tables[t].integrityCheck();
                tables[t].close();
            }
            assertThat(service.registeredTables(), is(0));
        }
    }

    /** close() leaves the service: the later resizes are synchronous, the service may close. */
    @Test(timeout = 60_000)
    public void closed_table_leaves_the_service_and_resizes_synchronously() {
        final MigrationService service = MigrationService.builder().start();
        final LongLongLL2Hashtable table = table(service, 1000, 0);
        for (long key = 1; key <= 1500; key++) {
            table.put(key, key); // the asynchronous resize at 1331 entries
        }
        table.close();
        assertThat(service.registeredTables(), is(0));
        service.close();
        for (long key = 1501; key <= 20_000; key++) {
            table.put(key, key);
            assertFalse(table.migrationInProgress());
        }
        assertThat(table.capacity(), is(32768));
        for (long key = 1; key <= 20_000; key++) {
            assertThat(table.get(key), is(key));
        }
        table.integrityCheck();
    }

    /** The tables are closed first: a service with a registered table refuses to close, and a closed one to register. */
    @Test(timeout = 60_000)
    public void service_closes_only_after_its_tables() {
        final MigrationService service = MigrationService.builder().start();
        final LongLongLL2Hashtable table = table(service, 16, 0);
        final IllegalStateException e = assertThrows(IllegalStateException.class, service::close);
        assertTrue(e.getMessage(), e.getMessage().contains("1 tables"));
        table.put(1L, 1L); // still served
        table.close();
        service.close();
        service.close(); // again: nothing to do
        assertThrows(IllegalStateException.class, () -> table(service, 16, 0));
    }

    /** A forgotten table - never closed - is dropped once the GC has collected it, with the arrays of its migration. */
    @Test(timeout = 60_000)
    public void forgotten_table_is_dropped_once_collected() throws Exception {
        try (MigrationService service = MigrationService.builder().start()) {
            forgetTableInTheMiddleOfAMigration(service);
            assertThat(service.registeredTables(), is(0)); // collected, though maybe not dropped yet
            for (int i = 0; i < 100 && service.slotCount() > 0; i++) {
                System.gc();
                Thread.sleep(20);
                service.wake();
            }
            assertThat(service.slotCount(), is(0));
        }
    }

    private static void forgetTableInTheMiddleOfAMigration(MigrationService service) throws InterruptedException {
        final java.lang.ref.WeakReference<LongLongLL2Hashtable> ref = new java.lang.ref.WeakReference<>(
                fillUpToAMigration(table(service, 1000, 0)));
        for (int i = 0; i < 100 && !ref.refersTo(null); i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertTrue("the table was collected", ref.refersTo(null));
    }

    private static LongLongLL2Hashtable fillUpToAMigration(LongLongLL2Hashtable table) {
        for (long key = 1; key <= 1400; key++) {
            table.put(key, key);
        }
        assertTrue(table.migrationInProgress());
        return table;
    }

    /**
     * A table forgotten right after it requested its array: the allocator allocates it, nobody takes it. Once the GC
     * has collected the table, the idle worker drops its slot - and the array.
     */
    @Test(timeout = 60_000)
    public void forgotten_table_with_an_allocated_array_is_dropped() throws Exception {
        try (MigrationService service = MigrationService.builder().start()) {
            final java.lang.ref.WeakReference<LongLongLL2Hashtable> ref = new java.lang.ref.WeakReference<>(
                    requestAnArray(table(service, 1000, 0)));
            for (int i = 0; i < 100 && (!ref.refersTo(null) || service.slotCount() > 0); i++) {
                System.gc();
                Thread.sleep(20);
                service.wake();
            }
            assertTrue("the table was collected", ref.refersTo(null));
            assertThat(service.slotCount(), is(0));
        }
    }

    /** puts up to the resize threshold of 2048 slots: the array requested, nothing more */
    private static LongLongLL2Hashtable requestAnArray(LongLongLL2Hashtable table) {
        for (long key = 1; !table.migrationInProgress(); key++) {
            table.put(key, key);
        }
        return table;
    }

    /**
     * close() forgets the tables the GC collected before the worker got to them - their slots held the arrays of their
     * migrations, kept as long as the closed service was reachable.
     */
    @Test(timeout = 60_000)
    public void close_forgets_collected_tables() throws Exception {
        final MigrationService noWorker = new MigrationService();
        final java.lang.ref.WeakReference<LongLongLL2Hashtable> ref = new java.lang.ref.WeakReference<>(
                requestAnArray(table(noWorker, 1000, 0)));
        noWorker.serveOnce(); // allocated: the slot holds the array
        for (int i = 0; i < 100 && !ref.refersTo(null); i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertTrue("the table was collected", ref.refersTo(null));
        assertThat(noWorker.slotCount(), is(1));
        assertThat(noWorker.registeredTables(), is(0));
        noWorker.close();
        assertThat(noWorker.slotCount(), is(0));
        assertThat(noWorker.activeSlots(), is(0));
    }

    /**
     * When a table authorizes the whole rest of its migration at once, the worker still takes ~256 slots per turn (to
     * the next free slot), so the other tables get theirs in between; the table can copy the rest itself.
     */
    @Test
    public void worker_takes_a_long_authorization_portion_by_portion() {
        final int slots = 1 << 15;
        final long[] src = new long[slots * 2];
        final Random rand = new Random(7L);
        for (int n = 0; n < slots * 6 / 10; n++) {
            final long key = rand.nextLong() | 1L;
            src[HashingUtils.findFreeOffset(key, src, slots - 1)] = key;
        }
        final int start = HashtableAsync2Resizer.findNextGapPos(src, 0);
        final HashtableAsync2Resizer r = new HashtableAsync2Resizer(src, new long[slots * 4], start, start);
        int portions = 0;
        int longest = 0;
        int processed = r.getProcessedPosition();
        while (!r.isAllMigrated()) {
            assertTrue(r.doWork());
            final int now = r.getProcessedPosition();
            longest = Math.max(longest, (now - processed) & (src.length - 1));
            processed = now;
            portions++;
        }
        assertTrue(portions + " portions", portions >= src.length / 1024);
        assertTrue("longest portion " + longest + " longs", longest < 2048);

        // the table, by contrast, copies all that is authorized at once
        final HashtableAsync2Resizer t = new HashtableAsync2Resizer(src, new long[slots * 4], start, start);
        assertTrue(t.copyIfIdle());
        assertTrue(t.isAllMigrated());
    }

    /** The worker's round holds the tables with a migration in flight, not every registered one. */
    @Test(timeout = 60_000)
    public void only_migrating_tables_are_on_the_round() {
        final MigrationService noWorker = new MigrationService();
        final LongLongLL2Hashtable[] idle = new LongLongLL2Hashtable[100];
        for (int i = 0; i < idle.length; i++) {
            idle[i] = table(noWorker, 16, 0);
            idle[i].put(i + 1L, i);
        }
        final LongLongLL2Hashtable migrating = requestAnArray(table(noWorker, 1000, 0));
        noWorker.serveOnce(); // allocates
        assertThat(noWorker.activeSlots(), is(1));
        migrating.get(1L); // starts the copying
        noWorker.serveOnce();
        assertThat(noWorker.activeSlots(), is(1));
        migrating.finishMigration();
        noWorker.serveOnce(); // nothing left: the slot leaves the round
        assertThat(noWorker.activeSlots(), is(0));
        assertThat(noWorker.registeredTables(), is(101));
        migrating.close();
        for (LongLongLL2Hashtable t : idle) {
            t.close();
        }
        noWorker.close();
    }

    /**
     * Registering and closing a table is O(1): 10 000 of them allocate a few MB - their arrays and slots - not the
     * hundreds of MB a copy of the registry per registration came to.
     */
    @Test(timeout = 60_000)
    public void registering_many_tables_costs_linear_memory() {
        final MigrationService noWorker = new MigrationService();
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long allocated0 = threads.getCurrentThreadAllocatedBytes();
        final LongLongLL2Hashtable[] tables = new LongLongLL2Hashtable[10_000];
        for (int i = 0; i < tables.length; i++) {
            tables[i] = table(noWorker, 1, 0);
        }
        assertThat(noWorker.registeredTables(), is(10_000));
        for (LongLongLL2Hashtable t : tables) {
            t.close();
        }
        final long mb = (threads.getCurrentThreadAllocatedBytes() - allocated0) >> 20;
        assertTrue(mb + "MB allocated", mb < 50);
        assertThat(noWorker.slotCount(), is(0));
        noWorker.close();
    }

    /** One thread: the worker allocates too. */
    @Test(timeout = 60_000)
    public void should_keep_all_entries_with_a_single_thread_service() {
        try (MigrationService service = MigrationService.builder().allocatorThread(false).start()) {
            checkAgainstReference(table(service, 16, 0), 300_000, 6L);
        }
    }

    /** allocatorThreadInit runs on the allocator thread before start() returns, and is closed when it ends. */
    @Test(timeout = 60_000)
    public void allocator_thread_init_runs_on_the_allocator() {
        final AtomicReference<Thread> initThread = new AtomicReference<>();
        final AtomicBoolean closed = new AtomicBoolean();
        final MigrationService service = MigrationService.builder().threadName("alloc-test").allocatorThreadInit(() -> {
            initThread.set(Thread.currentThread());
            return () -> closed.set(true);
        }).start();
        assertThat(initThread.get().getName(), is("alloc-test-allocator"));
        checkAgainstReference(table(service, 16, 0), 100_000, 8L);
        service.close();
        assertTrue(closed.get());
        assertFalse(initThread.get().isAlive());
    }

    /**
     * The array request: taken by the table, given up - an array allocated before or after that is dropped - and a
     * late result never reaches the next request.
     */
    @Test
    public void array_requests_hand_over_by_generation() {
        final MigrationService noWorker = new MigrationService();
        final LongLongLL2Hashtable table = table(noWorker, 16, 0);
        final MigrationService.Slot slot = noWorker.slots()[0];

        // done and taken
        final int g1 = slot.requestArray(64);
        assertNull(slot.takeArray(g1));
        assertFalse(slot.allocationStarted(g1));
        assertTrue(noWorker.serveOnce());
        assertTrue(slot.allocationStarted(g1));
        final long[] a1 = slot.takeArray(g1);
        assertNotNull(a1);
        assertThat(a1.length, is(128));
        assertNull("taken once", slot.takeArray(g1));

        // done, then given up: dropped
        final int g2 = slot.requestArray(256);
        noWorker.serveOnce();
        slot.abandonArray(g2);
        assertNull(slot.takeArray(g2));
        assertNull("dropped", slot.allocated());

        // given up before the worker came: not allocated at all
        final int g3 = slot.requestArray(512);
        slot.abandonArray(g3);
        assertFalse(noWorker.serveOnce());
        assertNull(slot.allocated());

        // a request after a given-up one gets its own array, of its size
        final int g4 = slot.requestArray(1024);
        assertTrue(g4 > g3);
        assertNull(slot.takeArray(g3));
        noWorker.serveOnce();
        assertThat(slot.takeArray(g4).length, is(2048));
        table.close();
    }

    /** threadInit runs on the worker before start() returns; what it returns is closed when the worker ends. */
    @Test(timeout = 60_000)
    public void thread_init_runs_on_the_worker_before_start_returns() {
        final AtomicReference<Thread> initThread = new AtomicReference<>();
        final AtomicBoolean closed = new AtomicBoolean();
        final MigrationService service = MigrationService.builder().threadName("init-test").threadInit(() -> {
            initThread.set(Thread.currentThread());
            return () -> closed.set(true);
        }).start();
        assertNotNull("ran before start() returned", initThread.get());
        assertThat(initThread.get().getName(), is("init-test"));
        assertTrue(initThread.get().isDaemon());
        assertFalse(closed.get());
        service.close();
        assertTrue(closed.get());
        assertFalse(initThread.get().isAlive());

        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> MigrationService.builder()
                .threadInit(() -> {
                    throw new IllegalArgumentException("no core");
                }).start());
        assertThat(e.getCause().getMessage(), is("no core"));
    }

    /**
     * start() in a static initializer of the class whose method threadInit is: the worker waits for the class, the
     * class for the worker. start() fails after its timeout instead of hanging - and the worker, once past threadInit,
     * ends.
     */
    @Test(timeout = 60_000)
    public void start_from_a_static_initializer_threadInit_needs_fails_instead_of_hanging() throws Exception {
        final IllegalStateException e = StartedInStaticInitializer.FAILURE;
        assertNotNull("start() failed", e);
        assertTrue(e.getMessage(), e.getMessage().contains("static initializer"));
        for (int i = 0; i < 1000 && StartedInStaticInitializer.WORKER.get() == null; i++) {
            Thread.sleep(10);
        }
        final Thread worker = StartedInStaticInitializer.WORKER.get();
        assertNotNull("the worker got through threadInit once the class was initialized", worker);
        worker.join(10_000);
        assertFalse("the worker ended", worker.isAlive());
    }

    static final class StartedInStaticInitializer {

        static final AtomicReference<Thread> WORKER = new AtomicReference<>();
        static final IllegalStateException FAILURE;

        static {
            IllegalStateException failure = null;
            try {
                MigrationService.builder().startTimeoutMillis(200)
                        .threadInit(StartedInStaticInitializer::init).start();
            } catch (IllegalStateException e) {
                failure = e;
            }
            FAILURE = failure;
        }

        private static AutoCloseable init() {
            WORKER.set(Thread.currentThread());
            return null;
        }
    }

    /**
     * An interrupt does not stop the worker - close() does: it goes on serving, does not spin while parked
     * (parkNanos returns at once while the status is set), and has the status set again when it ends.
     */
    @Test(timeout = 60_000)
    public void interrupted_worker_goes_on_without_spinning() throws Exception {
        final AtomicReference<Thread> worker = new AtomicReference<>();
        final AtomicReference<Boolean> interruptedAtEnd = new AtomicReference<>();
        final MigrationService service = MigrationService.builder().threadInit(() -> {
            worker.set(Thread.currentThread());
            return () -> interruptedAtEnd.set(Thread.currentThread().isInterrupted());
        }).start();
        Thread.sleep(50); // idle: parked by now
        worker.get().interrupt();

        // its CPU time shows it: a parked thread looks TIMED_WAITING even when every park returns at once
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        Thread.sleep(10);
        final long cpu0 = threads.getThreadCpuTime(worker.get().threadId());
        Thread.sleep(200);
        final long cpuMs = (threads.getThreadCpuTime(worker.get().threadId()) - cpu0) / 1_000_000;
        assertTrue("worker used " + cpuMs + "ms of CPU in 200ms", cpuMs < 100);

        checkAgainstReference(table(service, 16, 0), 100_000, 5L); // still served
        service.close();
        assertSame(Boolean.TRUE, interruptedAtEnd.get());
    }

    private static LongLongLL2Hashtable table(MigrationService service, int expectedSize, int syncResizeBelow) {
        return LongLongLL2Hashtable.builder().migrationService(service).expectedSize(expectedSize)
                .syncResizeBelow(syncResizeBelow).build();
    }

    /** puts, removals and lookups against a reference map, then the contents and the integrity; closes the table */
    private static void checkAgainstReference(LongLongLL2Hashtable table, int operations, long seed) {
        final Random rand = new Random(seed);
        final Long2LongHashMap ref = new Long2LongHashMap(0L);
        final long[] keys = new long[operations / 2];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rand.nextLong() | 1L;
        }
        try (LongLongLL2Hashtable t = table) {
            for (int op = 0; op < operations; op++) {
                final long key = keys[rand.nextInt(Math.min(keys.length, 16 + op / 2))];
                final int kind = rand.nextInt(10);
                if (kind < 6) {
                    final long value = rand.nextLong();
                    assertEquals("put", ref.put(key, value), t.put(key, value));
                } else if (kind < 8) {
                    assertEquals("remove", ref.remove(key), t.remove(key));
                } else {
                    assertEquals("get", ref.get(key), t.get(key));
                }
            }
            t.finishMigration();
            assertEquals(ref.size(), t.size());
            for (long key : keys) {
                assertEquals(ref.get(key), t.get(key));
            }
            t.integrityCheck();
        }
    }
}
