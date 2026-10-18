package exchange.core2.collections.hashtable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Interleavings of the application thread with the migrator, set up step by step: the table's migration service has
 * no worker, the test plays it - copies segments itself (as {@link HashtableAsync2Resizer#copyInterval} does) in the
 * worker's state, and hands over to the service's own serving when the scenario is set up. Where the migrator
 * pauses, the real worker can be preempted - or its stores can still sit in the store buffer. A table that waits
 * for a paused migrator takes its portion over; the migrator's writes after that are the ones it had started:
 * the same entries, into the same slots.
 * <p>
 * The same interleavings with real threads: LongLongLL2HashtableFrontierRaceStressTest in tests-stress.
 */
public class LongLongLL2HashtableMigrationInterleavingTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongLL2HashtableMigrationInterleavingTest.class);

    private static final Field RESIZER = field("resizer");
    private static final Field ALLOCATING = field("allocating");
    private static final Field DATA = field("data");

    private static final VarHandle ALLOWED = resizerField("allowedPosition");

    /** the migrator's steps did not copy anything they publish */
    private static final int KEEP = -1;

    /** how long a paused migrator waits for the application before it carries on anyway */
    private static final long MIGRATOR_PAUSE_MS = 100;

    /**
     * Keys whose home is migrated go straight into the new array. Inserted homed one slot below the frontier, they
     * grow a cluster across it - up to the slot the migrator has already chosen for the first entry of its segment
     * but not written yet. The migrator then overwrote the application's key.
     */
    @Test
    public void app_insert_across_migration_frontier_must_not_be_lost() throws Exception {
        int scenarios = 0;
        int corrupted = 0;
        for (long seed = 1; seed <= 20; seed++) {
            final Boolean ok = insertAcrossFrontier(seed);
            if (ok != null) {
                scenarios++;
                if (!ok) {
                    corrupted++;
                }
            }
        }
        log.info("scenarios={} corrupted={}", scenarios, corrupted);
        assertTrue("no seed produced the scenario", scenarios > 0);
        assertEquals("keys lost in " + corrupted + " of " + scenarios + " scenarios", 0, corrupted);
    }

    /**
     * A put reaching the starting position extends the migrated area backwards and moves the starting position -
     * possibly onto the migrator's position, after the migrator checked it. That migrator now waits for an
     * authorization, and a put at the block threshold used to join its task: it never ended, the put failed with
     * IllegalStateException after the 10s hang detection.
     * <p>
     * 64 slots, upsize at 41 entries, block at 57. Old array: slot 0 = starting position and slot 16 = end of the
     * first segment are gaps; slots 17..63 are filled into one cluster, so the next key homed there wraps to slot 0.
     */
    @Test(timeout = 60_000)
    public void put_at_block_threshold_after_backward_extension_must_not_hang() throws Exception {

        final Harness h = new Harness(40);
        final Map<Long, Long> ref = new HashMap<>();
        long nextKey = 1;

        // 9 entries in slots 1..9, 32 in slots 17..48 - each home is its slot; the 41st put allocates the new array
        for (int slot = 1; slot <= 48; slot++) {
            if (slot <= 9 || slot >= 17) {
                final long key = keyWithSlot(nextKey, slot, 63);
                nextKey = key + 1;
                h.table.put(key, slot);
                ref.put(key, (long) slot);
            }
        }
        assertTrue(h.allocationRequested());
        h.allocate();

        // the first put starts the migration; 15 keys homed at slot 17 fill slots 49..63 of the old array
        final long[] fill = new long[16];
        for (int i = 0; i < fill.length; i++) {
            fill[i] = keyWithSlot(nextKey, 17, 63);
            nextKey = fill[i] + 1;
        }
        for (int i = 0; i < 15; i++) {
            h.table.put(fill[i], 1000 + i);
            ref.put(fill[i], 1000L + i);
        }
        final HashtableAsync2Resizer r = h.resizer();
        assertEquals("starting position", 0, r.getStartingPosition());
        assertEquals("end of the first segment", 32, (int) ALLOWED.getVolatile(r));
        assertEquals(56, h.table.size());

        // migrator: copies the first segment and checks the starting position (0, not done) - then publishes the
        // progress only once the extension has happened, or after the pause
        final CountDownLatch extended = new CountDownLatch(1);
        final Thread migrator = h.startMigrator(() -> {
            r.copyInterval(2, 32);
            extended.await(MIGRATOR_PAUSE_MS, TimeUnit.MILLISECONDS);
            return 32;
        });

        // homed at slot 17, wraps to the starting position: extension back to slot 16 - the migrator's position
        h.table.put(fill[15], 2000);
        ref.put(fill[15], 2000L);
        extended.countDown();
        assertEquals("starting position moved onto the migrator", 32, r.getStartingPosition());
        assertEquals(57, h.table.size());

        // at the block threshold
        final long key = keyWithSlot(nextKey, 5, 63);
        h.table.put(key, 3000);
        ref.put(key, 3000L);

        h.close(migrator);
        assertEquals(0, h.verify(ref, "extension"));
    }

    /**
     * A migrator that dies leaves the application spinning on its progress. That used to end only at the 10s hang
     * detection, without the migrator's exception; now the failure is rethrown, with its cause, within milliseconds.
     */
    @Test(timeout = 60_000)
    public void migrator_failure_is_rethrown_without_waiting_for_the_hang_detection() throws Exception {

        final Harness h = new Harness(1000);
        final Random rand = new Random(5L);
        long migratedKey = 0;
        while (!h.allocationRequested()) {
            migratedKey = rand.nextLong() | 1L;
            h.table.put(migratedKey, 1L);
        }
        h.allocate();
        final long[] old = (long[]) DATA.get(h.table);
        final long farKey = keyWithSlot(1, (old.length >> 1) * 3 / 4, (old.length >> 1) - 1);
        h.table.get(farKey); // starts the migration
        final HashtableAsync2Resizer r = h.resizer();

        // a key homed in the first segment: its lookup waits for the migrator
        final int s = r.getStartingPosition();
        final long firstSegmentKey = keyWithSlot(1, ((s + 2) & (old.length - 1)) >> 1, (old.length >> 1) - 1);

        // an odd allowed position, within one portion of the worker: copyInterval rejects it, the worker fails (the test
        // plays it here)
        ALLOWED.setVolatile(r, r.getProcessedPosition() + 5);
        h.service.serveOnce();

        final long t0 = System.nanoTime();
        try {
            h.table.get(firstSegmentKey);
            throw new AssertionError("expected the migrator failure");
        } catch (IllegalStateException e) {
            final long ms = (System.nanoTime() - t0) / 1_000_000;
            log.info("failed after {}ms: {}", ms, e.getMessage());
            assertNotNull("cause", e.getCause());
            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().contains("copyInterval"));
            assertTrue("took " + ms + "ms", ms < 5_000);
        }
    }

    /**
     * During a migration the compound operations are made of the basic ones. Keys homed in the old array, while the
     * copying task is held back - a wait for the migrator would hang the test; then ensureCapacity beyond the new
     * array, which finishes the migration and rehashes.
     */
    @Test(timeout = 60_000)
    public void compound_operations_and_ensure_capacity_during_migration() throws Exception {

        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final Random rand = new Random(9L);
        while (!h.allocationRequested()) {
            final long key = rand.nextLong() | 1L;
            h.table.put(key, key);
            ref.put(key, key);
        }
        h.allocate();

        // keys homed in the last quarter of the old array - the migration starts at its beginning
        final long[] old = (long[]) DATA.get(h.table);
        final int oldSlotMask = (old.length >> 1) - 1;
        final long[] far = new long[40];
        long nextKey = 1;
        for (int i = 0; i < far.length; i++) {
            far[i] = keyWithSlot(nextKey, (old.length >> 1) * 3 / 4 + i, oldSlotMask);
            nextKey = far[i] + 1;
        }
        h.table.get(far[0]); // starts the migration
        h.resizer();
        assertTrue(h.table.toString(), h.table.toString().contains("migration in progress"));

        for (int i = 0; i < far.length; i++) {
            final long key = far[i];
            final long value = i;
            assertEquals(-9L, h.table.getOrDefault(key, -9L));
            assertEquals(0L, h.table.putIfAbsent(key, value));
            assertEquals(value, h.table.getOrDefault(key, -9L));
            assertEquals(value, h.table.putIfAbsent(key, -1L));
            assertEquals(value + 5, h.table.addAndGet(key, 5L));
            assertEquals(value + 5, h.table.computeIfAbsent(key, k -> -1L));
            assertFalse(h.table.remove(key, -1L));
            if (i % 2 == 0) {
                assertTrue(h.table.remove(key, value + 5));
            } else {
                ref.put(key, value + 5);
            }
        }
        final long computed = keyWithSlot(nextKey, (old.length >> 1) * 3 / 4 + far.length, oldSlotMask);
        assertEquals(computed * 3, h.table.computeIfAbsent(computed, k -> k * 3));
        ref.put(computed, computed * 3);
        assertEquals(ref.size(), h.table.size());

        // within the capacity the migration is heading to: nothing to do, the migration goes on
        h.table.ensureCapacity(2000);
        assertEquals(2048, h.table.capacity());
        assertTrue(h.table.toString().contains("migration in progress"));

        // beyond it: the migration is finished first (the migrator runs from here on), then one rehash
        final Thread migrator = h.startMigrator(() -> KEEP);
        h.table.ensureCapacity(10_000);
        assertEquals(16384, h.table.capacity());
        assertTrue(h.table.toString().startsWith("{"));

        h.close(migrator);
        assertEquals(0, h.verify(ref, "compound"));
    }

    /**
     * forEach, the streams and clear in the middle of a migration finish it first - and then see each entry once,
     * whichever array it was in.
     */
    @Test(timeout = 60_000)
    public void iteration_and_clear_finish_the_migration() throws Exception {

        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final Random rand = new Random(11L);
        while (!h.allocationRequested()) {
            final long key = rand.nextLong() | 1L;
            h.table.put(key, key);
            ref.put(key, key);
        }
        h.allocate();
        h.table.put(0L, 5L);
        ref.put(0L, 5L);
        final long[] old = (long[]) DATA.get(h.table);
        h.table.get(keyWithSlot(1, (old.length >> 1) * 3 / 4, (old.length >> 1) - 1)); // starts the migration
        h.resizer();
        final Thread migrator = h.startMigrator(() -> KEEP);

        final Map<Long, Long> visited = new HashMap<>();
        h.table.forEach((k, v) -> assertEquals("duplicate key " + k, null, visited.put(k, v)));
        assertEquals(ref, visited);
        assertEquals(null, RESIZER.get(h.table));
        assertEquals(4096, h.table.capacity());
        assertEquals(ref.size(), h.table.keysStream().count());
        assertEquals(ref.values().stream().mapToLong(Long::longValue).sum(), h.table.valuesStream().sum());

        h.close(migrator);
        assertEquals(0, h.verify(ref, "iteration"));

        h.table.clear();
        assertEquals(0, h.table.size());
        assertFalse(h.table.containsKey(0L));
        for (long key : ref.keySet()) {
            assertFalse(h.table.containsKey(key));
        }
        assertEquals(0, h.verify(Map.of(), "clear"));
    }

    /**
     * The hang detection is configurable. A migrator stuck in its first batch is not waited for - the lookup takes its
     * portion over - but the end of the migration waits until the migrator has left that batch, and fails after the
     * hang timeout, not after the default 10s. The table is consistent afterwards: the migrator, going on late, leaves
     * its batch, and the migration ends.
     */
    @Test(timeout = 60_000)
    public void hang_timeout_fails_a_wait_for_a_stuck_migrator() throws Exception {

        final Harness h = new Harness(1000, Duration.ofMillis(200));
        final Map<Long, Long> ref = new HashMap<>();
        final long firstSegmentKey = startMigrationHeldBack(h, ref);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread migrator = h.startMigrator(() -> { // stuck in its first batch
            release.await();
            return KEEP;
        });

        assertEquals(0L, h.table.get(firstSegmentKey));

        final long t0 = System.nanoTime();
        final IllegalStateException e = assertThrows(IllegalStateException.class, h.table::finishMigration);
        final long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("failed after {}ms: {}", ms, e.getMessage());
        assertTrue(e.getMessage(), e.getMessage().startsWith("stuck waiting for the worker"));
        assertTrue("took " + ms + "ms", ms >= 200 && ms < 3_000);

        release.countDown(); // the migrator goes on
        h.close(migrator);
        assertEquals(0, h.verify(ref, "hang timeout"));
    }

    /**
     * A migrator that stops in the middle of its batch - preempted, say - does not hold the table up: the lookup that
     * needs its portion takes it over, the puts go on, the one at the block threshold copies the rest. The table
     * switches arrays only once the migrator has left its batch - which it then writes to the end, entries the table
     * has copied too. Nothing is lost.
     */
    @Test(timeout = 60_000)
    public void migrator_stalled_in_its_batch_is_taken_over() throws Exception {

        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final long firstSegmentKey = startMigrationHeldBack(h, ref);
        final HashtableAsync2Resizer r = h.resizer();
        final int from = r.getProcessedPosition();
        final int batchEnd = from + HashtableAsync2Resizer.BATCH_LONGS;
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        final Thread migrator = h.startMigrator(() -> {
            r.copyInterval(from, from + 16);
            started.countDown();
            resume.await();
            r.copyInterval(from + 16, batchEnd);
            return KEEP;
        });
        started.await();

        final long t0 = System.nanoTime();
        assertEquals(0L, h.table.get(firstSegmentKey));
        final Random rand = new Random(17L);
        for (int i = 0; i < 2000 && !r.isAllMigrated(); i++) {
            final long key = rand.nextLong() | 1L;
            h.table.put(key, i);
            ref.put(key, (long) i);
            assertEquals(i, h.table.get(key));
        }
        final long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue("copied by the table", r.isAllMigrated());
        assertTrue("took " + ms + "ms", ms < 1_000);
        assertTrue("both arrays until the migrator leaves its batch", h.table.migrationInProgress());
        assertEquals(0L, h.table.get(firstSegmentKey));
        for (Map.Entry<Long, Long> e : ref.entrySet()) {
            assertEquals("while the migrator is stalled, key " + e.getKey(), (long) e.getValue(), h.table.get(e.getKey()));
        }

        resume.countDown();
        h.close(migrator);
        assertEquals(4096, h.table.capacity());
        assertEquals(0, h.verify(ref, "taken over"));
    }

    /**
     * After a take-over the migrator may still write the entries of its batch - with the values they had. A new value
     * for one of them waits until it has: otherwise the migrator would bring the old value back. Here the table has
     * copied everything already, and stays with both arrays until then.
     */
    @Test(timeout = 60_000)
    public void new_value_in_the_taken_over_batch_waits_for_the_migrator() throws Exception {
        changeEntryOfTakenOverBatch(false);
    }

    /** The same for a removal: the migrator would bring the entry back - or overwrite one shifted into its slot. */
    @Test(timeout = 60_000)
    public void removal_in_the_taken_over_batch_waits_for_the_migrator() throws Exception {
        changeEntryOfTakenOverBatch(true);
    }

    private static void changeEntryOfTakenOverBatch(boolean remove) throws Exception {

        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final long firstSegmentKey = startMigrationHeldBack(h, ref);
        final HashtableAsync2Resizer r = h.resizer();
        final long[] old = (long[]) DATA.get(h.table);
        final int from = r.getProcessedPosition();
        final int batchEnd = from + HashtableAsync2Resizer.BATCH_LONGS;
        int p = from;
        while (old[p] == HashingUtils.NOT_ALLOWED_KEY) {
            p += 2;
        }
        assertTrue("an entry in the batch", p < batchEnd);
        final long key = old[p] ^ HashingUtils.SEED;

        // the migrator writes its batch only after the take-over
        final CountDownLatch resume = new CountDownLatch(1);
        final Thread migrator = h.startMigrator(() -> {
            resume.await();
            r.copyInterval(from, batchEnd);
            return KEEP;
        });
        assertEquals(0L, h.table.get(firstSegmentKey));
        final Random rand = new Random(19L);
        for (int i = 0; i < 2000 && !r.isAllMigrated(); i++) {
            final long k = rand.nextLong() | 1L;
            h.table.put(k, i);
            ref.put(k, (long) i);
        }
        assertTrue("copied by the table", r.isAllMigrated());

        final Thread resumer = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            resume.countDown();
        });
        resumer.start();
        final long t0 = System.nanoTime();
        if (remove) {
            assertEquals((long) ref.remove(key), h.table.remove(key));
        } else {
            h.table.put(key, -7L);
            ref.put(key, -7L);
        }
        final long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("{} waited {}ms", remove ? "removal" : "new value", ms);
        assertTrue("waited for the migrator: " + ms + "ms", ms >= 150);
        resumer.join();

        h.close(migrator);
        assertEquals(remove ? 0L : -7L, h.table.get(key));
        assertEquals(!remove, h.table.containsKey(key));
        assertEquals(0, h.verify(ref, remove ? "removal" : "new value"));
    }

    /**
     * A segment the worker has not taken - here it does not come at all - is not waited for: a lookup that needs the
     * first segment copies it itself, and so do the operations after it. The worker, coming late, continues from
     * there.
     */
    @Test(timeout = 60_000)
    public void segment_the_worker_has_not_taken_is_copied_by_the_table() throws Exception {

        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final long firstSegmentKey = startMigrationHeldBack(h, ref);
        final HashtableAsync2Resizer r = h.resizer();
        final int firstSegmentEnd = (int) ALLOWED.getVolatile(r);

        final long t0 = System.nanoTime();
        assertEquals(0L, h.table.get(firstSegmentKey));
        final long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue("took " + ms + "ms", ms < 1_000);
        assertEquals("first segment copied by the table", firstSegmentEnd, r.getProcessedPosition());

        // the migration goes on, with segments copied where operations need them
        final Random rand = new Random(13L);
        for (int i = 0; i < 300; i++) {
            final long key = rand.nextLong() | 1L;
            h.table.put(key, key);
            ref.put(key, key);
        }

        h.close(null); // the worker comes late: it continues from there, or finds the migration finished
        assertEquals(4096, h.table.capacity());
        assertEquals(0, h.verify(ref, "copied by the table"));
    }

    /**
     * An interrupt does not fail an operation that waits for the migrator: the lookup returns once the migrator
     * (started 100ms late) has copied the segment, and the interrupt status is still set for the caller.
     */
    @Test(timeout = 60_000)
    public void interrupt_does_not_fail_a_wait_for_the_migrator() throws Exception {

        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final long firstSegmentKey = startMigrationHeldBack(h, ref);

        final Thread migrator = h.startMigrator(() -> {
            Thread.sleep(100);
            return KEEP;
        });
        Thread.currentThread().interrupt();
        try {
            assertEquals(0L, h.table.get(firstSegmentKey)); // missing - known only once its segment is copied
            assertTrue("interrupt status kept", Thread.currentThread().isInterrupted());
            h.table.close(); // finishes the migration
            assertTrue("interrupt status kept", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        h.close(migrator);
        assertEquals(0, h.verify(ref, "interrupt"));
    }

    /**
     * Fills the harness table up to the resize, then starts the migration (no worker serving it).
     *
     * @return a key homed in the first segment of the migration - its lookup waits for the migrator
     */
    private static long startMigrationHeldBack(Harness h, Map<Long, Long> ref) throws Exception {
        final Random rand = new Random(5L);
        while (!h.allocationRequested()) {
            final long key = rand.nextLong() | 1L;
            h.table.put(key, 1L);
            ref.put(key, 1L);
        }
        h.allocate();
        final long[] old = (long[]) DATA.get(h.table);
        final int slotMask = (old.length >> 1) - 1;
        h.table.get(keyWithSlot(1, (old.length >> 1) * 3 / 4, slotMask)); // starts the migration
        final HashtableAsync2Resizer r = h.resizer();
        final int s = r.getStartingPosition();
        return keyWithSlot(1_000_000_007L, ((s + 2) & (old.length - 1)) >> 1, slotMask);
    }

    /** @return null if the seed does not produce the scenario, otherwise whether no key was lost */
    private static Boolean insertAcrossFrontier(long seed) throws Exception {

        // 2048 slots, the first resize (at 1331 entries) is already asynchronous
        final Harness h = new Harness(1000);
        final Map<Long, Long> ref = new HashMap<>();
        final Random rand = new Random(seed);

        while (!h.allocationRequested()) {
            final long key = rand.nextLong() | 1L;
            final long value = rand.nextLong();
            h.table.put(key, value);
            ref.put(key, value);
        }
        h.allocate();

        final long[] old = (long[]) DATA.get(h.table);
        final int len = old.length;              // longs in the old array
        final int oldSlotMask = (len >> 1) - 1;
        final int newSlotMask = len - 1;         // the new array has twice the slots

        // keys homed in the last quarter stay in the old array for the first segments
        long nextKey = seed * 1_000_000_007L;
        final long farKey1 = keyWithSlot(nextKey, (len >> 1) * 3 / 4, oldSlotMask);
        final long farKey2 = keyWithSlot(farKey1 + 1, (len >> 1) * 3 / 4 + 1, oldSlotMask);
        nextKey = farKey2 + 1;

        // a lookup starts the migration: resizer created, first segment authorized, copying task submitted
        h.table.get(farKey1);
        final HashtableAsync2Resizer r = h.resizer();
        final int s = r.getStartingPosition();
        final int a1 = (int) ALLOWED.getVolatile(r);
        final long[] dst = r.getNewDataArray();

        // migrator: the first segment, published like the worker does
        final long held = r.claimAsWorkerForTest();
        r.copyInterval((s + 2) & (len - 1), a1);
        assertTrue(r.endAsWorkerForTest(held, a1));

        // application: the next put sees the segment done and authorizes the second one (its key stays in old data)
        h.table.put(farKey2, 22L);
        ref.put(farKey2, 22L);
        final int a2 = (int) ALLOWED.getVolatile(r);
        if (a2 == s || a1 - 2 <= s) {
            log.info("seed={} not applicable: s={} a1={} a2={}", seed, s, a1, a2);
            h.close(null);
            return null;
        }

        // first entry of the second segment
        int q = a1;
        while (q != a2 && old[q] == HashingUtils.NOT_ALLOWED_KEY) {
            q = (q + 2) & (len - 1);
        }
        if (q == a2) {
            log.info("seed={} not applicable: empty segment {}..{}", seed, a1, a2);
            h.close(null);
            return null;
        }
        final long mKey = old[q];
        final long mValue = old[q + 1];
        final int qFinal = q;

        final AtomicInteger chosen = new AtomicInteger(-1);
        final CountDownLatch paused = new CountDownLatch(1);
        final CountDownLatch go = new CountDownLatch(1);
        final Thread migrator = h.startMigrator(() -> {
            // as in copyInterval: find a free slot ...
            final int o = HashingUtils.findFreeOffset(mKey, dst, newSlotMask);
            chosen.set(o);
            paused.countDown();
            go.await(MIGRATOR_PAUSE_MS, TimeUnit.MILLISECONDS);
            // ... and write the entry there
            dst[o] = mKey;
            dst[o + 1] = mValue;
            final int next = (qFinal + 2) & (len - 1);
            if (next != a2) {
                r.copyInterval(next, a2);
            }
            return a2;
        });

        paused.await();
        final int o = chosen.get();

        // application: keys homed one slot below the frontier, in the half of the new array where o is
        final int half = o >= len ? len : 0;
        final int homeSlot = (a1 - 2 + half) >> 1;
        long placedAtO = 0;
        int inserted = 0;
        while (r.getProcessedPosition() != a2 && inserted < 64) {
            final long key = keyWithSlot(nextKey, homeSlot, newSlotMask);
            nextKey = key + 1;
            h.table.put(key, key * 31);
            ref.put(key, key * 31);
            inserted++;
            if (dst[o] == (key ^ HashingUtils.SEED)) {
                placedAtO = key;
                break;
            }
        }
        go.countDown();

        h.close(migrator);
        final int lost = h.verify(ref, "seed=" + seed + " s=" + s + " a1=" + a1 + " a2=" + a2 + " q=" + qFinal
                + " o=" + o + " appInserts=" + inserted + " appKeyAtMigratorSlot=" + (placedAtO != 0));
        return lost == 0;
    }

    /** A table whose migration service has no worker - the test plays it - plus the helpers to drive and check it. */
    private static final class Harness {

        final MigrationService service = new MigrationService();
        final AtomicReference<Throwable> migratorError = new AtomicReference<>();
        final LongLongLL2Hashtable table;
        private volatile boolean stopMigrator;

        Harness(int expectedSize) {
            this(expectedSize, LongLongLL2Hashtable.DEFAULT_HANG_TIMEOUT);
        }

        Harness(int expectedSize, Duration hangTimeout) {
            table = LongLongLL2Hashtable.builder().migrationService(service).expectedSize(expectedSize)
                    .syncResizeBelow(0).hangTimeout(hangTimeout).build();
        }

        boolean allocationRequested() throws IllegalAccessException {
            return ALLOCATING.getBoolean(table);
        }

        /** the worker's allocation of the requested array - without copying anything */
        void allocate() {
            assertTrue("allocated", service.serveAllocations());
        }

        HashtableAsync2Resizer resizer() throws IllegalAccessException {
            final HashtableAsync2Resizer r = (HashtableAsync2Resizer) RESIZER.get(table);
            assertNotNull("migration not started", r);
            return r;
        }

        /**
         * Runs the steps as the worker in its first batch - the table waits for them, then takes them over - and
         * publishes the progress they return, unless taken over; then serves the table as the worker does,
         * continuing from the published progress, until close().
         */
        Thread startMigrator(Steps steps) throws IllegalAccessException {
            final HashtableAsync2Resizer r = (HashtableAsync2Resizer) RESIZER.get(table);
            final long held = r != null ? r.claimAsWorkerForTest() : 0;
            final Thread thread = new Thread(() -> {
                try {
                    final int progress = steps.run();
                    if (held != 0) {
                        r.endAsWorkerForTest(held, progress);
                    }
                    while (!stopMigrator) {
                        if (!service.serveOnce()) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (Throwable e) {
                    migratorError.set(e);
                }
            }, "test-migrator");
            thread.setDaemon(true);
            thread.start();
            return thread;
        }

        /** completes the migration; without a migrator thread one is started */
        void close(Thread migrator) throws InterruptedException, IllegalAccessException {
            final Thread m = migrator != null ? migrator : startMigrator(() -> KEEP);
            table.close();
            stopMigrator = true;
            m.join(TimeUnit.SECONDS.toMillis(20));
            if (migratorError.get() != null) {
                throw new AssertionError("migrator failed", migratorError.get());
            }
        }

        /** @return number of keys missing or with a wrong value; integrity problems count as one */
        int verify(Map<Long, Long> ref, String scenario) {
            int lost = 0;
            for (Map.Entry<Long, Long> e : ref.entrySet()) {
                final long actual = table.get(e.getKey());
                if (actual != e.getValue()) {
                    lost++;
                    log.warn("{}: key={} expected={} actual={}", scenario, e.getKey(), e.getValue(), actual);
                }
            }
            String integrity = "ok";
            try {
                table.integrityCheck();
            } catch (IllegalStateException e) {
                integrity = e.getMessage();
                lost = Math.max(lost, 1);
            }
            log.info("{}: lost={} integrity: {}", scenario, lost, integrity);
            return lost;
        }
    }

    @FunctionalInterface
    private interface Steps {
        /** @return the progress to publish - a gap - or KEEP */
        int run() throws Exception;
    }

    private static long keyWithSlot(long from, int slot, int slotMask) {
        long key = from;
        // the arrays hold key ^ SEED, and the key stored as 0 is not in them
        while ((key ^ HashingUtils.SEED) == 0 || (HashingUtils.hash(key ^ HashingUtils.SEED) & slotMask) != slot) {
            key++;
        }
        return key;
    }

    private static Field field(String name) {
        try {
            final Field f = LongLongLL2Hashtable.class.getDeclaredField(name);
            f.setAccessible(true);
            return f;
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static VarHandle resizerField(String name) {
        try {
            return MethodHandles.privateLookupIn(HashtableAsync2Resizer.class, MethodHandles.lookup())
                    .findVarHandle(HashtableAsync2Resizer.class, name, int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
