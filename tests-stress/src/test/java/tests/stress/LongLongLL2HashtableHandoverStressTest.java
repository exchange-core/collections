package tests.stress;

import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import exchange.core2.collections.hashtable.MigrationService;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * The work of the migrations shared between the tables and the worker of a {@link MigrationService}, with real
 * threads: each segment is copied by whichever of the two takes it first - a table where an operation needs it, the
 * worker in its turn - and each new array allocated by the worker, or by the table at its block threshold if the
 * worker has not started it. Here four tables grow at once on one worker, so it is often busy with another one; every
 * resize is a migration (syncResizeBelow 0), from 32 slots on, with segments of 16 to 256 slots. The rounds alternate
 * a parking and a spinning worker.
 * <p>
 * Every table: random puts/removes/gets checked against a reference map, then all keys, the size and
 * integrityCheck().
 * <pre>
 * mvn test -Pstress -pl tests-stress -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=LongLongLL2HashtableHandoverStressTest
 * </pre>
 * {@code -Dstress.handover.rounds} (default 1000).
 */
public class LongLongLL2HashtableHandoverStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongLL2HashtableHandoverStressTest.class);

    private static final int ROUNDS = Integer.getInteger("stress.handover.rounds", 1000);
    private static final int TABLES = 4;
    private static final int OPS = 80_000;

    private static final Method INTEGRITY_CHECK = integrityCheckMethod();

    @Test
    public void should_share_the_migrations_without_losing_keys() {

        final Random rand = new Random(4L);
        long wrongResults = 0;
        long corruptedTables = 0;
        final long t0 = System.nanoTime();

        for (int round = 0; round < ROUNDS; round++) {
            final MigrationService.WaitStrategy wait = round % 2 == 0
                    ? MigrationService.WaitStrategy.SPIN_THEN_PARK : MigrationService.WaitStrategy.BUSY_SPIN;
            try (MigrationService service = MigrationService.builder().waitStrategy(wait).start()) {

                final LongLongLL2Hashtable[] tables = new LongLongLL2Hashtable[TABLES];
                final Long2LongHashMap[] refs = new Long2LongHashMap[TABLES];
                for (int t = 0; t < TABLES; t++) {
                    tables[t] = LongLongLL2Hashtable.builder().migrationService(service).expectedSize(16)
                            .syncResizeBelow(0).build();
                    refs[t] = new Long2LongHashMap(0L);
                }
                final long[] keys = new long[OPS / 2];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = rand.nextLong() | 1L;
                }

                for (int op = 0; op < OPS; op++) {
                    final LongLongLL2Hashtable table = tables[op % TABLES];
                    final Long2LongHashMap ref = refs[op % TABLES];
                    final long key = keys[rand.nextInt(Math.min(keys.length, 16 + op / 2))];
                    final int kind = rand.nextInt(10);
                    if (kind < 6) {
                        final long value = rand.nextLong();
                        if (table.put(key, value) != ref.put(key, value)) {
                            wrongResults++;
                        }
                    } else if (kind < 8) {
                        if (table.remove(key) != ref.remove(key)) {
                            wrongResults++;
                        }
                    } else if (table.get(key) != ref.get(key)) {
                        wrongResults++;
                    }
                }

                for (int t = 0; t < TABLES; t++) {
                    tables[t].close();
                    if (!verify(tables[t], refs[t])) {
                        corruptedTables++;
                    }
                }
            }
        }

        log.info("HANDOVER: rounds={} tables={} wrongResults={} corruptedTables={} in {}ms", ROUNDS, ROUNDS * TABLES,
                wrongResults, corruptedTables, (System.nanoTime() - t0) / 1_000_000);
        assertEquals("wrong results", 0, wrongResults);
        assertEquals("corrupted tables", 0, corruptedTables);
    }

    private static boolean verify(LongLongLL2Hashtable table, Long2LongHashMap ref) {
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
        if (lost != 0 || integrity != null || table.size() != ref.size()) {
            log.warn("corrupted: lost={} size={} expected={} integrity={}", lost, table.size(), ref.size(), integrity);
            return false;
        }
        return true;
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
