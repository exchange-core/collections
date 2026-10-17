package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ObjLongConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The contract tests against LongLongLL2Hashtable. With the default executor and sync-resize bound, the tables
 * here resize synchronously up to 8200 entries and asynchronously past that (should_upsize goes to 50000).
 */
public class LongLongLL2HashtableTest extends LongLongHashtableAbstractTest {

    private static final Executor THREAD_PER_TASK = task -> {
        final Thread thread = new Thread(task, "test-migrator");
        thread.setDaemon(true);
        thread.start();
    };

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return new LongLongLL2Hashtable(expectedSize);
    }

    @Override
    protected ILongLongHashtable create(int expectedSize, long missingValue) {
        return LongLongLL2Hashtable.builder().expectedSize(expectedSize).missingValue(missingValue).build();
    }

    @Override
    protected ILongLongHashtable createLimited(int expectedSize, int maxCapacity) {
        return LongLongLL2Hashtable.builder().expectedSize(expectedSize).executor(THREAD_PER_TASK)
                .maxCapacity(maxCapacity).build();
    }

    @Override
    protected ILongLongHashtable createFixed(int expectedSize) {
        return LongLongLL2Hashtable.builder().expectedSize(expectedSize).executor(NO_TASKS).syncResizeBelow(0)
                .fixedCapacity().build();
    }

    /** the tables of fixed capacity must never start a migration */
    private static final Executor NO_TASKS = task -> {
        throw new AssertionError("a table of fixed capacity started a task");
    };

    /** Large enough for asynchronous migrations (syncResizeBelow 0): no task, no migration, up to the full load. */
    @Test
    public void fixed_capacity_starts_no_migration() {
        try (LongLongLL2Hashtable table = (LongLongLL2Hashtable) createFixed(10_000)) {
            final int limit = (int) (table.capacity() * HashingUtils.FULL_LOAD);
            for (long key = 1; key <= limit; key++) {
                table.put(key, key);
                assertFalse(table.migrationInProgress());
            }
            assertThrows(IllegalStateException.class, () -> table.put(-1L, 1L));
            assertThat(table.capacity(), is(16384));
            checkIntegrity(table);
        }
    }

    @Test
    public void builder_should_validate_the_max_capacity() {
        for (int invalid : new int[]{0, 1, 3, 100, -64, HashingUtils.MAX_CAPACITY * 2}) {
            assertThrows(String.valueOf(invalid), IllegalArgumentException.class,
                    () -> LongLongLL2Hashtable.builder().maxCapacity(invalid));
        }
        assertThrows(IllegalArgumentException.class,
                () -> LongLongLL2Hashtable.builder().expectedSize(100).maxCapacity(128).build());
    }

    /**
     * A put at the block threshold needs the new array. The executor here starts the allocation a second late: the
     * put does not wait for it, it allocates the array itself - the late task then skips the allocation. An interrupt
     * does not fail the put, and its status stays set.
     */
    @Test(timeout = 60_000)
    public void put_at_the_block_threshold_does_not_wait_for_an_allocation_not_started() {

        final AtomicInteger tasks = new AtomicInteger();
        final AtomicBoolean allocationStarted = new AtomicBoolean();
        final Executor lateAllocation = task -> {
            final boolean allocation = tasks.getAndIncrement() == 0;
            final Thread thread = new Thread(() -> {
                if (allocation) {
                    LockSupport.parkNanos(1_000_000_000L);
                    allocationStarted.set(true);
                }
                task.run();
            }, "test-migrator");
            thread.setDaemon(true);
            thread.start();
        };
        // 256 slots: the resize starts at 166 entries, a put finding 230 needs the new array
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(100)
                .executor(lateAllocation).syncResizeBelow(0).build()) {
            assertThat(table.capacity(), is(256));
            for (long key = 1; key <= 230; key++) {
                table.put(key, key);
            }
            Thread.currentThread().interrupt();
            try {
                table.put(231L, 231L); // at the block threshold: allocates, migrates
                assertTrue("interrupt status kept", Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            assertFalse("did not wait for the allocation", allocationStarted.get());
            assertThat(table.capacity(), is(512));
            for (long key = 1; key <= 231; key++) {
                assertThat(table.get(key), is(key));
            }
            checkIntegrity(table);
        }
    }

    /**
     * An interrupted migrator goes on with its migration (the table needs it complete), backs off while idle
     * instead of spinning - parkNanos returns at once while the interrupt status is set - and has the status set
     * again when its task ends.
     */
    @Test(timeout = 60_000)
    public void interrupted_migrator_backs_off_and_keeps_the_interrupt() throws Exception {

        final AtomicReference<Thread> copier = new AtomicReference<>();
        final AtomicReference<Boolean> interruptedAfterCopy = new AtomicReference<>();
        final AtomicInteger tasks = new AtomicInteger();
        final Executor executor = task -> {
            final boolean copy = tasks.getAndIncrement() % 2 == 1;
            final Thread thread = new Thread(() -> {
                if (copy) {
                    copier.set(Thread.currentThread());
                }
                task.run();
                if (copy) {
                    interruptedAfterCopy.set(Thread.currentThread().isInterrupted());
                }
            }, "test-migrator");
            thread.setDaemon(true);
            thread.start();
        };
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(1000)
                .executor(executor).syncResizeBelow(0).build()) {
            // past the upsize threshold the puts start the migration - and stop there: further operations would
            // authorize the next segments and could finish it
            for (long key = 1; !table.toString().contains("migration in progress"); key++) {
                table.put(key, key);
            }
            while (copier.get() == null) {
                Thread.onSpinWait();
            }
            // the application is quiet: the migrator copies its first segment, then waits for the next one
            Thread.sleep(50);
            final Thread migrator = copier.get();
            assertTrue("migrator still waiting", migrator.isAlive());
            migrator.interrupt();

            // its CPU time shows it: a parked thread looks TIMED_WAITING even when every park returns at once
            final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            Thread.sleep(10);
            final long cpu0 = threads.getThreadCpuTime(migrator.threadId());
            Thread.sleep(200);
            final long cpuMs = (threads.getThreadCpuTime(migrator.threadId()) - cpu0) / 1_000_000;
            assertTrue("migrator used " + cpuMs + "ms of CPU in 200ms", cpuMs < 100);

            table.close(); // authorizes the rest: the migration completes
            for (long key = 1; key <= table.size(); key++) {
                assertThat(table.get(key), is(key));
            }
            migrator.join(10_000);
            assertThat(interruptedAfterCopy.get(), is(true));
        }
    }

    /**
     * Lookups of a key that stays in the old array until the last segment - homed in the last eighth, the migration
     * starts at the beginning - advance the migration too. They used to stall it: only the operations that reached the
     * migrated area authorized the next segment.
     */
    @Test(timeout = 60_000)
    public void reads_of_keys_in_the_old_array_finish_the_migration() throws Exception {
        checkMigrationFinishesUnder(LongLongLL2Hashtable::get);
    }

    /** The same for removals (of a missing key - in a table where it would be present, the same path). */
    @Test(timeout = 60_000)
    public void removals_of_keys_in_the_old_array_finish_the_migration() throws Exception {
        checkMigrationFinishesUnder(LongLongLL2Hashtable::remove);
    }

    private static void checkMigrationFinishesUnder(ObjLongConsumer<LongLongLL2Hashtable> operation) throws Exception {
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(1000)
                .executor(THREAD_PER_TASK).syncResizeBelow(0).build()) {
            final int slots = table.capacity();
            final long farKey = keyWithSlot(1L << 40, slots * 7 / 8, slots - 1);
            for (long key = 1; !table.migrationInProgress(); key++) {
                table.put(key, key);
            }
            Thread.sleep(20); // the new array is allocated: the next operation starts the copying
            int operations = 0;
            while (table.migrationInProgress() && operations < 1_000_000) {
                operation.accept(table, farKey);
                operations++;
            }
            assertFalse("migration still in progress after " + operations + " operations",
                    table.migrationInProgress());
            assertThat(table.capacity(), is(slots * 2));
            checkIntegrity(table);
        }
    }

    /**
     * After a quiet period the migrator sleeps - parks, up to 1ms at a time. The operation that authorizes the next
     * segment wakes it: one that needs the segment right after waits for the copying, not for the rest of a park.
     */
    @Test(timeout = 60_000)
    public void sleeping_migrator_is_woken_by_the_next_authorization() throws Exception {

        final Field allowedPosition = LongLongLL2Hashtable.class.getDeclaredField("allowedPosition");
        allowedPosition.setAccessible(true);
        // 32768 slots: 128 segments of 256 slots
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(20_000)
                .executor(THREAD_PER_TASK).syncResizeBelow(0).build()) {
            final int slotMask = table.capacity() - 1;
            for (long key = 1; !table.migrationInProgress(); key++) {
                table.put(key, key);
            }
            Thread.sleep(20);
            table.get(1L); // starts the copying

            final long[] waits = new long[15];
            for (int t = 0; t < waits.length; t++) {
                Thread.sleep(30); // the migrator copies its segment, spins, then parks
                final int nextSegment = allowedPosition.getInt(table);
                table.get(1L); // authorizes the next segment
                final long key = keyWithSlot(1L << 40, ((nextSegment + 2) >> 1) & slotMask, slotMask);
                final long t0 = System.nanoTime();
                table.get(key); // needs that segment: waits until it is copied
                waits[t] = System.nanoTime() - t0;
            }
            Arrays.sort(waits);
            final long medianUs = waits[waits.length / 2] / 1000;
            assertTrue("median wait " + medianUs + "us, all: " + Arrays.toString(waits), medianUs < 300);
            assertTrue(table.migrationInProgress());
        }
    }

    private static long keyWithSlot(long from, int slot, int slotMask) {
        long key = from;
        while ((key ^ HashingUtils.SEED) == 0 || (HashingUtils.hash(key ^ HashingUtils.SEED) & slotMask) != slot) {
            key++;
        }
        return key;
    }

    /**
     * Builder defaults are those of the constructors; the seed is the one of the JVM.
     */
    @Test
    public void builder_should_set_the_parameters() {

        try (LongLongLL2Hashtable plain = new LongLongLL2Hashtable();
             LongLongLL2Hashtable built = LongLongLL2Hashtable.builder().expectedSize(1000).missingValue(-7L)
                     .executor(THREAD_PER_TASK).syncResizeBelow(0).build()) {

            assertThat(plain.missingValue(), is(0L));
            assertThat(plain.capacity(), is(64));
            assertThat(built.missingValue(), is(-7L));
            assertThat(built.capacity(), is(2048));
            assertThat(built.get(1L), is(-7L));
            assertThat(built.seed(), is(HashingUtils.SEED));
        }
    }

    /**
     * The same limit when the table got there by asynchronous migrations: the last one ends inside a put, which then
     * inserts into the new array and checks the threshold on its own path.
     */
    @Test
    public void should_refuse_new_keys_at_full_load_after_async_migrations() {

        final int limit = (int) (64 * HashingUtils.FULL_LOAD);
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(10)
                .executor(THREAD_PER_TASK).syncResizeBelow(0).maxCapacity(64).build()) {
            for (long key = 1; key <= limit; key++) {
                table.put(key, key);
            }
            assertThat(table.size(), is((long) limit));
            assertThrows(IllegalStateException.class, () -> table.put(limit + 1L, 1L));
            assertThat(table.size(), is((long) limit));
            for (long key = 1; key <= limit; key++) {
                assertThat(table.get(key), is(key));
            }
            checkIntegrity(table);
        }
    }
}
