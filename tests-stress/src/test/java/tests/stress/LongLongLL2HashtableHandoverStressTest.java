package tests.stress;

import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * The hand-over of the copying from the table to the migrator, with real threads: until the copying task starts, the
 * table copies the segments it needs itself, and the task, once started, continues from the table's progress. Here
 * each task starts after a random delay (up to {@code maxDelayUs}, plus the thread start), so that the start falls
 * anywhere in a migration - and the allocation may come too late as well: the table then allocates the array itself.
 * Every resize is a migration (syncResizeBelow 0), from 32 slots on.
 * <p>
 * Every table: random puts/removes/gets checked against a reference map, then all keys and integrityCheck().
 * A "late start" is a task that found segments copied by the table when it started (approximate - read racily).
 * <pre>
 * mvn test -Pstress -pl tests-stress -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=LongLongLL2HashtableHandoverStressTest
 * </pre>
 * {@code -Dstress.handover.tables} (default 2000), {@code -Dstress.handover.maxDelayUs} (default 1000).
 */
public class LongLongLL2HashtableHandoverStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongLL2HashtableHandoverStressTest.class);

    private static final int TABLES = Integer.getInteger("stress.handover.tables", 2000);
    private static final long MAX_DELAY_NS = Integer.getInteger("stress.handover.maxDelayUs", 1000) * 1000L;
    private static final int OPS = 20_000;

    private static final Class<?> RESIZER_CLASS = resizerClass();
    private static final VarHandle RESIZER = ll2Field("resizer", RESIZER_CLASS);
    private static final VarHandle PROCESSED = resizerField("toProcessPosition");
    private static final VarHandle STARTING = resizerField("startingPosition");
    private static final VarHandle DATA = ll2Field("data", long[].class);
    private static final Method INTEGRITY_CHECK = integrityCheckMethod();

    @Test
    public void should_hand_the_copying_over_without_losing_keys() {

        final Random rand = new Random(4L);
        final AtomicLong tasks = new AtomicLong();
        final AtomicLong lateStarts = new AtomicLong();
        long wrongResults = 0;
        long corruptedTables = 0;
        final long t0 = System.nanoTime();

        for (int t = 0; t < TABLES; t++) {

            final AtomicReference<LongLongLL2Hashtable> tableRef = new AtomicReference<>();
            final Executor delayed = task -> {
                final long delayNs = ThreadLocalRandom.current().nextLong(MAX_DELAY_NS + 1);
                final Thread thread = new Thread(() -> {
                    final long until = System.nanoTime() + delayNs;
                    while (System.nanoTime() < until) {
                        Thread.onSpinWait();
                    }
                    tasks.incrementAndGet();
                    if (copiedByTheTable(tableRef.get())) {
                        lateStarts.incrementAndGet();
                    }
                    task.run();
                }, "handover-migrator");
                thread.setDaemon(true);
                thread.start();
            };
            final LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(16).executor(delayed)
                    .syncResizeBelow(0).build();
            tableRef.set(table);

            final Long2LongHashMap ref = new Long2LongHashMap(0L);
            final long[] keys = new long[OPS / 2];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = rand.nextLong() | 1L;
            }
            for (int op = 0; op < OPS; op++) {
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

            table.close();
            if (!verify(table, ref)) {
                corruptedTables++;
            }
        }

        log.info("HANDOVER: tables={} tasks={} lateStarts={} wrongResults={} corruptedTables={} in {}ms", TABLES,
                tasks.get(), lateStarts.get(), wrongResults, corruptedTables, (System.nanoTime() - t0) / 1_000_000);
        assertEquals("wrong results", 0, wrongResults);
        assertEquals("corrupted tables", 0, corruptedTables);
    }

    /** a migration in flight whose progress is past its first position: the table copied before the task started */
    private static boolean copiedByTheTable(LongLongLL2Hashtable table) {
        final Object r = RESIZER.getVolatile(table);
        if (r == null) {
            return false; // an allocation, or a task that comes after its migration
        }
        final int len = ((long[]) DATA.getVolatile(table)).length;
        return (int) PROCESSED.getVolatile(r) != (((int) STARTING.getVolatile(r) + 2) & (len - 1));
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

    private static VarHandle resizerField(String name) {
        try {
            return MethodHandles.privateLookupIn(RESIZER_CLASS, MethodHandles.lookup())
                    .findVarHandle(RESIZER_CLASS, name, int.class);
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
