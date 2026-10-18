package tests.stress;

import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import exchange.core2.collections.hashtable.MigrationService;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A worker the scheduler takes off its core in the middle of its work, with real threads: a thread per CPU burns
 * them all, so the spinning worker - and the applications - run in time slices; four application threads with four
 * growing tables each keep the worker copying most of the time, so it is often preempted in the middle of a batch. A
 * table that needs the portion of a stalled worker takes it over; the worker, back on a core, finishes the batch it
 * was writing - entries the table has copied too - and the table changes those entries only after that. Every resize
 * is a migration (syncResizeBelow 0), from 32 slots on.
 * <p>
 * Every table: random puts (new keys and new values), removes and gets checked against a reference map, then all
 * keys, the size and integrityCheck(); at the end the number of take-overs, which must not be 0.
 * <pre>
 * mvn test -Pstress -pl tests-stress -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=LongLongLL2HashtablePreemptedWorkerStressTest
 * </pre>
 * {@code -Dstress.preempted.rounds} (default 100).
 */
public class LongLongLL2HashtablePreemptedWorkerStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongLL2HashtablePreemptedWorkerStressTest.class);

    private static final int ROUNDS = Integer.getInteger("stress.preempted.rounds", 100);
    private static final int APPS = 4;
    private static final int TABLES = 4;
    private static final int OPS = 400_000;

    private static final Method INTEGRITY_CHECK = integrityCheckMethod();
    private static final Field TAKE_OVERS = takeOversField();

    private volatile boolean stopBurning;

    @Test
    public void should_take_over_from_a_preempted_worker_without_losing_keys() throws Exception {

        final List<Thread> burners = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            final Thread burner = new Thread(() -> {
                while (!stopBurning) {
                    Thread.onSpinWait();
                }
            }, "burner-" + i);
            burner.setDaemon(true);
            burner.start();
            burners.add(burner);
        }

        final AtomicLong wrongResults = new AtomicLong();
        final AtomicLong corruptedTables = new AtomicLong();
        final AtomicLong takeOvers = new AtomicLong();
        final long t0 = System.nanoTime();
        try {
            for (int round = 0; round < ROUNDS; round++) {
                try (MigrationService service = MigrationService.builder()
                        .waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN).start()) {
                    final Thread[] apps = new Thread[APPS];
                    for (int a = 0; a < APPS; a++) {
                        final long seed = round * 31L + a;
                        apps[a] = new Thread(() -> run(service, seed, wrongResults, corruptedTables, takeOvers),
                                "app-" + a);
                        apps[a].start();
                    }
                    for (Thread app : apps) {
                        app.join();
                    }
                }
            }
        } finally {
            stopBurning = true;
            for (Thread burner : burners) {
                burner.join();
            }
        }

        log.info("PREEMPTED WORKER: rounds={} tables={} takeOvers={} wrongResults={} corruptedTables={} in {}ms",
                ROUNDS, ROUNDS * APPS * TABLES, takeOvers, wrongResults, corruptedTables,
                (System.nanoTime() - t0) / 1_000_000);
        assertEquals("wrong results", 0, wrongResults.get());
        assertEquals("corrupted tables", 0, corruptedTables.get());
        assertTrue("no take-overs", takeOvers.get() > 0);
    }

    /** one application thread: its tables, random operations against the reference maps, then the checks */
    private static void run(MigrationService service, long seed, AtomicLong wrongResults, AtomicLong corruptedTables,
                            AtomicLong takeOvers) {
        final Random rand = new Random(seed);
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

        long wrong = 0;
        for (int op = 0; op < OPS; op++) {
            final LongLongLL2Hashtable table = tables[op % TABLES];
            final Long2LongHashMap ref = refs[op % TABLES];
            final long key = keys[rand.nextInt(Math.min(keys.length, 16 + op / 2))];
            final int kind = rand.nextInt(10);
            if (kind < 6) {
                final long value = rand.nextLong();
                if (table.put(key, value) != ref.put(key, value)) {
                    wrong++;
                }
            } else if (kind < 8) {
                if (table.remove(key) != ref.remove(key)) {
                    wrong++;
                }
            } else if (table.get(key) != ref.get(key)) {
                wrong++;
            }
        }
        wrongResults.addAndGet(wrong);

        for (int t = 0; t < TABLES; t++) {
            try {
                takeOvers.addAndGet(TAKE_OVERS.getLong(tables[t]));
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
            tables[t].close();
            if (!verify(tables[t], refs[t])) {
                corruptedTables.incrementAndGet();
            }
        }
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

    private static Field takeOversField() {
        try {
            final Field f = LongLongLL2Hashtable.class.getDeclaredField("takeOvers");
            f.setAccessible(true);
            return f;
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
