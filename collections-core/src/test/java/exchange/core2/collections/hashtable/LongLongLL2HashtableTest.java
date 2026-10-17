package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.function.ObjLongConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The contract tests against LongLongLL2Hashtable. With the default sync-resize bound, the tables here resize
 * synchronously up to 8200 entries and asynchronously past that (should_upsize goes to 50000) - one migration service
 * for all of them, as an application would have.
 */
public class LongLongLL2HashtableTest extends LongLongHashtableAbstractTest {

    static final MigrationService SERVICE = MigrationService.builder().threadName("test-migration").start();

    /** a service whose worker never comes: the tables do all the work themselves */
    static final MigrationService NO_WORKER = new MigrationService();

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return LongLongLL2Hashtable.builder().migrationService(SERVICE).expectedSize(expectedSize).build();
    }

    @Override
    protected ILongLongHashtable create(int expectedSize, long missingValue) {
        return LongLongLL2Hashtable.builder().migrationService(SERVICE).expectedSize(expectedSize)
                .missingValue(missingValue).build();
    }

    @Override
    protected ILongLongHashtable createLimited(int expectedSize, int maxCapacity) {
        return LongLongLL2Hashtable.builder().migrationService(SERVICE).expectedSize(expectedSize)
                .maxCapacity(maxCapacity).build();
    }

    @Override
    protected ILongLongHashtable createFixed(int expectedSize) {
        return LongLongLL2Hashtable.builder().migrationService(NO_WORKER).expectedSize(expectedSize).syncResizeBelow(0)
                .fixedCapacity().build();
    }

    /** Large enough for asynchronous migrations (syncResizeBelow 0): no request, no migration, up to the full load. */
    @Test
    public void fixed_capacity_starts_no_migration() {
        try (LongLongLL2Hashtable table = (LongLongLL2Hashtable) createFixed(10_000)) {
            final int limit = (int) HashingUtils.maxEntries(table.capacity(), HashingUtils.DEFAULT_MAX_LOAD);
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
    public void builder_should_validate_the_parameters() {
        for (int invalid : new int[]{0, 1, 3, 100, -64, HashingUtils.MAX_CAPACITY * 2}) {
            assertThrows(String.valueOf(invalid), IllegalArgumentException.class,
                    () -> LongLongLL2Hashtable.builder().maxCapacity(invalid));
        }
        assertThrows(IllegalArgumentException.class, () -> LongLongLL2Hashtable.builder().migrationService(SERVICE)
                .expectedSize(100).maxCapacity(128).build());
        assertThrows(IllegalArgumentException.class, () -> LongLongLL2Hashtable.builder().maxLoad(1.0));
        assertThrows(IllegalArgumentException.class, () -> LongLongLL2Hashtable.builder().syncResizeBelow(-1));
        assertThrows(NullPointerException.class, () -> LongLongLL2Hashtable.builder().migrationService(null));
        assertThrows(IllegalStateException.class, () -> LongLongLL2Hashtable.builder().build());
    }

    @Test
    public void should_take_new_keys_up_to_the_max_load() {
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(NO_WORKER).expectedSize(9)
                .fixedCapacity().maxLoad(0.75).build()) {
            LongLongHashtableTest.checkMaxLoad(table, 12);
        }
    }

    /**
     * A put at the block threshold needs the new array. The worker never comes here: the put allocates the array
     * itself and does the migration - the request is dropped.
     */
    @Test(timeout = 60_000)
    public void put_at_the_block_threshold_allocates_the_array_the_worker_has_not() {
        // 256 slots: the resize starts at 166 entries, a put finding 230 needs the new array
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(NO_WORKER)
                .expectedSize(100).syncResizeBelow(0).build()) {
            assertThat(table.capacity(), is(256));
            for (long key = 1; key <= 230; key++) {
                table.put(key, key);
            }
            assertTrue(table.migrationInProgress());
            table.put(231L, 231L); // at the block threshold: allocates, migrates
            assertFalse(table.migrationInProgress());
            assertThat(table.capacity(), is(512));
            for (long key = 1; key <= 231; key++) {
                assertThat(table.get(key), is(key));
            }
            checkIntegrity(table);
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
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(SERVICE).expectedSize(1000)
                .syncResizeBelow(0).build()) {
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
     * After a quiet period the worker sleeps (SPIN_THEN_PARK, parks of up to 100ms). The operation that authorizes the
     * next segment wakes it: the worker copies the segment right away. (An operation that needs the segment does not
     * wait for the worker anyway - it copies the segment itself if the worker has not taken it.)
     */
    @Test(timeout = 60_000)
    public void sleeping_worker_is_woken_by_the_next_authorization() throws Exception {

        final Field allowedPosition = LongLongLL2Hashtable.class.getDeclaredField("allowedPosition");
        allowedPosition.setAccessible(true);
        final Field resizerField = LongLongLL2Hashtable.class.getDeclaredField("resizer");
        resizerField.setAccessible(true);
        try (MigrationService service = MigrationService.builder().start()) {
            // 32768 slots: 128 segments of 256 slots
            try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(service)
                    .expectedSize(20_000).syncResizeBelow(0).build()) {
                for (long key = 1; !table.migrationInProgress(); key++) {
                    table.put(key, key);
                }
                Thread.sleep(20);
                table.get(1L); // starts the copying
                final HashtableAsync2Resizer resizer = (HashtableAsync2Resizer) resizerField.get(table);

                final long[] waits = new long[15];
                for (int t = 0; t < waits.length; t++) {
                    Thread.sleep(30); // the worker copies its segment, spins, then parks
                    table.get(1L); // authorizes the next segment - before or after its own lookup, not needing it
                    final int segmentEnd = allowedPosition.getInt(table);
                    final long t0 = System.nanoTime();
                    while (resizer.getProcessedPosition() != segmentEnd && System.nanoTime() - t0 < 1_000_000_000L) {
                        Thread.onSpinWait();
                    }
                    waits[t] = System.nanoTime() - t0;
                }
                Arrays.sort(waits);
                final long medianUs = waits[waits.length / 2] / 1000;
                assertTrue("median until copied " + medianUs + "us, all: " + Arrays.toString(waits), medianUs < 300);
                assertTrue(table.migrationInProgress());
            }
        }
    }

    static long keyWithSlot(long from, int slot, int slotMask) {
        long key = from;
        while ((key ^ HashingUtils.SEED) == 0 || (HashingUtils.hash(key ^ HashingUtils.SEED) & slotMask) != slot) {
            key++;
        }
        return key;
    }

    /**
     * The builder's defaults; the seed is the one of the JVM.
     */
    @Test
    public void builder_should_set_the_parameters() {

        try (LongLongLL2Hashtable plain = LongLongLL2Hashtable.builder().migrationService(SERVICE).build();
             LongLongLL2Hashtable built = LongLongLL2Hashtable.builder().migrationService(SERVICE).expectedSize(1000)
                     .missingValue(-7L).syncResizeBelow(0).build()) {

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

        final int limit = (int) HashingUtils.maxEntries(64, HashingUtils.DEFAULT_MAX_LOAD);
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(SERVICE).expectedSize(10)
                .syncResizeBelow(0).maxCapacity(64).build()) {
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
