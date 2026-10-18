package tests.stress;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import exchange.core2.collections.hashtable.MigrationService;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * The migration frontier race with real threads, no emulation (the deterministic version is
 * LongLongLL2HashtableFrontierRaceTest in collections-core): the application inserts keys whose home is migrated
 * straight into the new array, and a cluster growing across the frontier reaches slots the migrator is writing
 * at the same time.
 * <ul>
 *     <li>{@code crafted} - right after a segment is authorized, the application inserts keys homed one slot below
 *     the frontier, so its cluster crosses the frontier while the migrator starts copying there</li>
 *     <li>{@code random} - random puts/removes/gets, as an application would do them</li>
 * </ul>
 * Every table is verified after its migrations: all keys with their values, plus integrityCheck(). A "crossing"
 * is an application write past the frontier while the segment was in flight - the precondition of the race.
 * <pre>
 * mvn test -Pstress -pl tests-stress -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=LongLongLL2HashtableFrontierRaceStressTest
 * </pre>
 * {@code -Dstress.frontier.mode=crafted|random|both}, {@code -Dstress.frontier.tables} (crafted, default 20000),
 * {@code -Dstress.frontier.randomTables} (default 200), {@code -Dstress.frontier.syncResizeBelow} (random, default 0),
 * {@code -Dstress.cpu.app} / {@code -Dstress.cpu.migrator} pin the two threads.
 */
public class LongLongLL2HashtableFrontierRaceStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongLL2HashtableFrontierRaceStressTest.class);

    private static final String MODE = System.getProperty("stress.frontier.mode", "both");
    private static final int CRAFTED_TABLES = Integer.getInteger("stress.frontier.tables", 20_000);
    private static final int RANDOM_TABLES = Integer.getInteger("stress.frontier.randomTables", 200);
    /** random tables: smaller ones resize synchronously - 0 makes every resize a migration, the table default is 8200 */
    private static final int RANDOM_SYNC_RESIZE_BELOW = Integer.getInteger("stress.frontier.syncResizeBelow", 0);

    /** crafted tables: 2048 slots, the first resize (at 1331 entries) is asynchronous, 4096 slots afterwards */
    private static final int CRAFTED_EXPECTED_SIZE = 1000;
    private static final int CRAFTED_NEW_SLOTS = 4096;
    private static final int KEYS_PER_SLOT = 16;
    /** keys inserted below each frontier: about 3 make the cluster reach past it */
    private static final int CRAFTED_PER_FRONTIER = 4;

    /** the resizer class is package-private: its state is read like the private fields of the table */
    private static final Class<?> RESIZER_CLASS = resizerClass();
    private static final VarHandle RESIZER = ll2Field("resizer", RESIZER_CLASS);
    /** the resizer's state: the progress in its high half */
    private static final VarHandle STATE = resizerField("state", long.class);
    private static final VarHandle STARTING = resizerField("startingPosition", int.class);
    private static final VarHandle DST = resizerField("dstData", long[].class);
    private static final VarHandle ALLOCATING = ll2Field("allocating", boolean.class);
    private static final VarHandle DATA = ll2Field("data", long[].class);
    private static final VarHandle ALLOWED = ll2Field("allowedPosition", int.class);
    private static final VarHandle BLOCK_THRESHOLD = ll2Field("blockThreshold", long.class);
    private static final Method INTEGRITY_CHECK = integrityCheckMethod();

    private static final class Stats {
        long tables;
        long appWritesDuringMigration;
        long crossings;
        long corruptedTables;
        long lostKeys;
        long wrongResults;

        @Override
        public String toString() {
            return "tables=" + tables + " appWritesDuringMigration=" + appWritesDuringMigration
                    + " crossings=" + crossings + " corruptedTables=" + corruptedTables
                    + " lostKeys=" + lostKeys + " wrongResults=" + wrongResults;
        }
    }

    @Test
    public void should_not_lose_keys_at_migration_frontier() {

        // a worker spinning on its core, so a migration starts within microseconds - started before the application
        // thread pins itself (a thread inherits its creator's affinity), and pinned by its own threadInit
        try (MigrationService migrator = MigrationService.builder()
                .waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN).threadName("frontier-migrator")
                .threadInit(() -> pin("migrator")).start();
             CpuAffinity ignore = pin("app")) {
            long corrupted = 0;
            if (!MODE.equals("random")) {
                final Stats crafted = crafted(migrator);
                log.info("CRAFTED: {}", crafted);
                corrupted += crafted.corruptedTables;
            }
            if (!MODE.equals("crafted")) {
                final Stats random = random(migrator);
                log.info("RANDOM: {}", random);
                corrupted += random.corruptedTables;
            }
            assertEquals("corrupted tables", 0, corrupted);
        }
    }

    private static Stats crafted(MigrationService migrator) {

        final Stats stats = new Stats();
        final Random rand = new Random(1L);
        final KeyPool pool = new KeyPool(CRAFTED_NEW_SLOTS, KEYS_PER_SLOT, 0x5DEECE66DL << 16);
        final long t0 = System.nanoTime();

        for (int t = 0; t < CRAFTED_TABLES; t++) {

            final LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(migrator)
                    .expectedSize(CRAFTED_EXPECTED_SIZE).syncResizeBelow(0).build();
            final Long2LongHashMap ref = new Long2LongHashMap(0L);
            pool.reset();

            while (!(boolean) ALLOCATING.get(table)) {
                final long key = rand.nextLong() | 1L;
                final long value = rand.nextLong() | 1L;
                table.put(key, value);
                ref.put(key, value);
            }

            final int len = ((long[]) DATA.get(table)).length;      // longs in the old array
            final long farKey = pool.take(CRAFTED_NEW_SLOTS * 3 / 8); // old home in the last quarter
            long budget = (long) BLOCK_THRESHOLD.get(table) - table.size() - 8;

            int lastP = -1;
            int perFrontier = 0;
            boolean upper = false;
            long lastCrafted = 0;

            while (true) {
                final Object r = RESIZER.get(table);
                if (r == null) {
                    if (!(boolean) ALLOCATING.get(table)) {
                        break; // migration finished
                    }
                    table.get(farKey); // starts the copying once the new array is allocated
                    continue;
                }
                if (budget <= 0) {
                    break;
                }

                final int p = processed(r);
                final int s = (int) STARTING.getVolatile(r);
                if (p != lastP) {
                    lastP = p;
                    perFrontier = 0;
                    upper = !upper;
                }

                final int homeOld = (p - 2) & (len - 1);
                if (perFrontier < CRAFTED_PER_FRONTIER && homeOld != s && p != ((s + 2) & (len - 1))) {
                    final int slot = (homeOld >> 1) + (upper ? len >> 1 : 0);
                    final long key = pool.take(slot);
                    if (key != 0) {
                        final long value = rand.nextLong() | 1L;
                        put(table, key, value, ref, stats);
                        lastCrafted = key;
                        budget--;
                    }
                    perFrontier++;
                } else {
                    // drives the migration: a lookup in the migrated area authorizes the next segment
                    table.get(lastCrafted != 0 ? lastCrafted : farKey);
                }
            }

            table.close();
            verify(table, ref, stats);
        }

        log.info("crafted: {} tables in {}ms", stats.tables, (System.nanoTime() - t0) / 1_000_000);
        return stats;
    }

    private static Stats random(MigrationService migrator) {

        final Stats stats = new Stats();
        final Random rand = new Random(2L);
        final long t0 = System.nanoTime();

        for (int t = 0; t < RANDOM_TABLES; t++) {

            final LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().migrationService(migrator)
                    .expectedSize(16).syncResizeBelow(RANDOM_SYNC_RESIZE_BELOW).build();
            final Long2LongHashMap ref = new Long2LongHashMap(0L);
            final long[] keys = new long[60_000];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = rand.nextLong() | 1L;
            }

            for (int op = 0; op < 300_000; op++) {
                final long key = keys[rand.nextInt(keys.length)];
                final int kind = rand.nextInt(10);
                if (kind < 6) {
                    put(table, key, rand.nextLong() | 1L, ref, stats);
                } else if (kind < 8) {
                    if (table.remove(key) != ref.remove(key)) {
                        stats.wrongResults++;
                    }
                } else if (table.get(key) != ref.get(key)) {
                    stats.wrongResults++;
                }
            }

            table.close();
            verify(table, ref, stats);
        }

        log.info("random: {} tables in {}ms", stats.tables, (System.nanoTime() - t0) / 1_000_000);
        return stats;
    }

    /**
     * put, and if it went into the new array during a migration: did it land past the frontier while the segment
     * was in flight (the frontier read before the put, the authorization read after it - a put can authorize the
     * next segment itself, before inserting)?
     */
    private static void put(LongLongLL2Hashtable table, long key, long value, Long2LongHashMap ref, Stats stats) {

        final Object r = RESIZER.get(table);
        final int pBefore = r != null ? processed(r) : -1;

        if (table.put(key, value) != ref.put(key, value)) {
            stats.wrongResults++;
        }

        if (r == null || RESIZER.get(table) != r) {
            return;
        }
        final long[] dst = (long[]) DST.get(r);
        final int ring = dst.length;
        final int home = (TableHash.hash(key) & ((ring >> 1) - 1)) << 1;
        int x = home;
        while (dst[x] != TableHash.stored(key)) {
            if (dst[x] == 0) {
                return; // in the old array
            }
            x = (x + 2) & (ring - 1);
        }
        stats.appWritesDuringMigration++;

        final boolean inFlight = pBefore != (int) ALLOWED.get(table);
        final int half = ring >> 1;
        if (inFlight && (crossed(home, x, pBefore + 2, ring) || crossed(home, x, pBefore + half + 2, ring))) {
            stats.crossings++;
        }
    }

    /** the probe home..x passed position f */
    private static boolean crossed(int home, int x, int f, int ring) {
        return ((f - home) & (ring - 1)) <= ((x - home) & (ring - 1));
    }

    private static void verify(LongLongLL2Hashtable table, Long2LongHashMap ref, Stats stats) {
        stats.tables++;
        int lost = 0;
        final Long2LongHashMap.KeyIterator it = ref.keySet().iterator();
        while (it.hasNext()) {
            final long key = it.nextValue();
            if (table.get(key) != ref.get(key)) {
                lost++;
            }
        }
        String integrity = null;
        try {
            INTEGRITY_CHECK.invoke(table);
        } catch (InvocationTargetException e) {
            integrity = e.getCause().getMessage();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        if (lost != 0 || integrity != null) {
            stats.corruptedTables++;
            stats.lostKeys += lost;
            if (stats.corruptedTables <= 10) {
                log.warn("table #{} corrupted: lost={} integrity={}", stats.tables, lost, integrity);
            }
        }
    }

    /** keys grouped by their slot in the new array */
    private static final class KeyPool {
        private final long[] keys;
        private final int[] used;
        private final int perSlot;

        KeyPool(int slots, int perSlot, long base) {
            this.keys = new long[slots * perSlot];
            this.used = new int[slots];
            this.perSlot = perSlot;
            final int[] filled = new int[slots];
            int remaining = slots;
            for (long key = base; remaining > 0; key++) {
                final int slot = TableHash.hash(key) & (slots - 1);
                if (TableHash.stored(key) != 0 && filled[slot] < perSlot) {
                    keys[slot * perSlot + filled[slot]++] = key;
                    if (filled[slot] == perSlot) {
                        remaining--;
                    }
                }
            }
        }

        void reset() {
            Arrays.fill(used, 0);
        }

        /** a key not taken since reset(), 0 if the slot ran out */
        long take(int slot) {
            return used[slot] < perSlot ? keys[slot * perSlot + used[slot]++] : 0;
        }
    }

    /** -Dstress.cpu.app / -Dstress.cpu.migrator, not pinned (null) otherwise */
    private static CpuAffinity pin(String role) {
        final Integer cpu = Integer.getInteger("stress.cpu." + role);
        if (cpu == null) {
            return null;
        }
        final CpuAffinity affinity = CpuAffinity.acquireCore(cpu);
        log.info("{}: {}", role, affinity);
        return affinity;
    }

    private static int processed(Object resizer) {
        return (int) ((long) STATE.getVolatile(resizer) >>> 32);
    }

    private static VarHandle ll2Field(String name, Class<?> type) {
        try {
            return MethodHandles.privateLookupIn(LongLongLL2Hashtable.class, MethodHandles.lookup())
                    .findVarHandle(LongLongLL2Hashtable.class, name, type);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Class<?> resizerClass() {
        try {
            return Class.forName("exchange.core2.collections.hashtable.HashtableAsync2Resizer");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static VarHandle resizerField(String name, Class<?> type) {
        try {
            return MethodHandles.privateLookupIn(RESIZER_CLASS, MethodHandles.lookup())
                    .findVarHandle(RESIZER_CLASS, name, type);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Method integrityCheckMethod() {
        try {
            final Method m = LongLongLL2Hashtable.class.getDeclaredMethod("integrityCheck");
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
