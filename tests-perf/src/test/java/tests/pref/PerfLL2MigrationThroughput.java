package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.HashtableAsync2Resizer;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Random;
import java.util.concurrent.Executor;

/**
 * How fast can a large LongLongLL2Hashtable be inserted into while it migrates? One migration per trial: a table
 * of 2^23 slots is filled up to its resize threshold (5.45M entries), then new keys are put at a fixed rate until
 * the migration is over. If the inserts reach the block threshold (90% = 7.55M entries) first, the put that gets
 * there waits for the whole rest of the migration.
 * <ul>
 *     <li>alloc - from the put that triggers the resize until copying starts (the new array is allocated and
 *     zeroed on the migrator thread)</li>
 *     <li>copy - from the start of copying until the table switches to the new array</li>
 * </ul>
 * Rates in M puts/s with {@code -Dmig.rates=1,2,5,max}, {@code -Dmig.repeats}, {@code -Dmig.warmup} (unthrottled
 * trials before measuring). The application thread and the migrator pin themselves like in CollectionsLatencySamples.
 * <pre>
 * mvn test -Pbenchmarks -pl tests-perf -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=PerfLL2MigrationThroughput
 * </pre>
 * Segment size 2048 / 1024 / 512 / 256 / 128 longs (2026-10, Xeon Gold 6244, the old array's 2^23 slots): copying
 * took 58 / 65 / 74 / 85 / 104ms with puts back to back, longer at 1M puts/s (60 / 69 / 90 / 123 / 127ms) - every
 * segment waits for an operation to authorize it. Allocating the new array: 35ms regardless. One thread inserted
 * at most ~6M puts/s here, the migrations would have kept up with 22 / 21 / 19 / 17 / 15M.
 */
public class PerfLL2MigrationThroughput {

    private static final Logger log = LoggerFactory.getLogger(PerfLL2MigrationThroughput.class);

    /** 2^23 slots */
    private static final int EXPECTED = 5_400_000;
    private static final int CAPACITY = 1 << 23;
    private static final int FILL = (int) (CAPACITY * 0.65f) - 1;
    private static final int BLOCK = (int) (CAPACITY * 0.9f);
    private static final int EXTRA = BLOCK - FILL + 1000;

    private static final String[] RATES = System.getProperty("mig.rates", "1,2,5,max").split(",");
    private static final int REPEATS = Integer.getInteger("mig.repeats", 3);
    private static final int WARMUP = Integer.getInteger("mig.warmup", 4);

    private static final VarHandle RESIZER;

    static {
        try {
            // LL2 starts its Cleaner thread at class init - before this thread pins itself, or it inherits the pin
            MethodHandles.lookup().ensureInitialized(LongLongLL2Hashtable.class);
            RESIZER = MethodHandles.privateLookupIn(LongLongLL2Hashtable.class, MethodHandles.lookup())
                    .findVarHandle(LongLongLL2Hashtable.class, "resizer", HashtableAsync2Resizer.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Executor MIGRATOR = spinningExecutorOnOwnCore("ll2-migrator");

    private long sink;

    @Test
    public void migrationThroughput() {
        final long[] fill = keys(FILL, 1);
        final long[] extra = keys(EXTRA, 2);
        log.info("capacity={} fill={} block at {} ({} puts of headroom)", CAPACITY, FILL, BLOCK, BLOCK - FILL);

        try (CpuAffinity ignore = pinCurrentThread()) {
            for (int i = 0; i < WARMUP; i++) {
                trial(fill, extra, 0, "warmup");
            }
            for (String rate : RATES) {
                final double mops = rate.equals("max") ? 0 : Double.parseDouble(rate);
                for (int i = 0; i < REPEATS; i++) {
                    trial(fill, extra, mops, "RESULT");
                }
            }
        }
        log.info("done ({})", sink);
    }

    /** @param mops puts per microsecond, 0 = back to back */
    private void trial(long[] fill, long[] extra, double mops, String tag) {
        System.gc();
        final LongLongLL2Hashtable table = new LongLongLL2Hashtable(EXPECTED, MIGRATOR, 1000);
        for (long k : fill) table.put(k, k);
        final long threshold = table.upsizeThreshold();

        final double nsPerOp = mops == 0 ? 0 : 1000.0 / mops;
        final long start = System.nanoTime();
        long copyStart = 0;
        int putsBeforeCopy = 0;
        long maxPutNs = 0;
        long end = start;
        int i = 0;
        while (i < extra.length) {
            if (nsPerOp > 0) {
                final long planned = start + (long) (i * nsPerOp);
                while (System.nanoTime() < planned) {
                    Thread.onSpinWait();
                }
            }
            final long t0 = System.nanoTime();
            sink += table.put(extra[i], i);
            end = System.nanoTime();
            maxPutNs = Math.max(maxPutNs, end - t0);
            i++;
            if (copyStart == 0 && RESIZER.get(table) != null) {
                copyStart = end;
                putsBeforeCopy = i;
            }
            if (table.upsizeThreshold() != threshold) {
                break; // switched to the new array
            }
        }
        final boolean blocked = table.size() >= BLOCK;
        final double allocMs = copyStart == 0 ? 0 : (copyStart - start) / 1e6;
        final double copyMs = copyStart == 0 ? 0 : (end - copyStart) / 1e6;
        final double totalMs = (end - start) / 1e6;
        log.info("{} rate={} achieved={} Mput/s alloc={}ms copy={}ms total={}ms puts={} (before copy {}) blocked={} maxPut={}us copySpeed={} slots/us",
                tag, mops == 0 ? "max" : String.valueOf(mops), String.format("%.2f", i / totalMs / 1000), String.format("%.1f", allocMs),
                String.format("%.1f", copyMs), String.format("%.1f", totalMs), i, putsBeforeCopy, blocked,
                String.format("%.1f", maxPutNs / 1000.0), String.format("%.0f", copyMs == 0 ? 0 : (CAPACITY / 1000.0) / copyMs));
        table.close();
    }

    private static long[] keys(int n, long seed) {
        final Random rand = new Random(seed);
        final long[] keys = new long[n];
        for (int i = 0; i < n; i++) {
            keys[i] = (rand.nextLong() & Long.MAX_VALUE) | 1;
        }
        return keys;
    }

    /** One daemon thread on its own core, spinning on a single-task slot. */
    private static Executor spinningExecutorOnOwnCore(String name) {
        final Slot slot = new Slot();
        final Thread thread = new Thread(() -> {
            try (CpuAffinity ignore = pinCurrentThread()) {
                while (true) {
                    final Runnable task = slot.task;
                    if (task != null) {
                        slot.task = null;
                        task.run();
                    } else {
                        Thread.onSpinWait();
                    }
                }
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        return task -> {
            while (slot.task != null) {
                Thread.onSpinWait();
            }
            slot.task = task;
        };
    }

    private static final class Slot {
        volatile Runnable task;
    }

    private static CpuAffinity pinCurrentThread() {
        final String name = Thread.currentThread().getName();
        final Integer cpu = Integer.getInteger("samples.cpu." + name);
        final CpuAffinity affinity = cpu != null ? CpuAffinity.acquireCore(cpu) : CpuAffinity.acquireCore();
        log.info("{}: {}", name, affinity);
        return affinity;
    }
}
