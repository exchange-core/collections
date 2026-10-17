package tests.pref;

import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Lookups right after the application was quiet in the middle of a migration. The migrator, done with its segment,
 * backs off - parks, up to 1 ms at a time; the lookups after the quiet period authorize the next segments and wait
 * for the migrator whenever they need one of them.
 * <p>
 * A trial: a table of 4096 slots (segments of 1/16) in its first migration, the default executor (a daemon thread per
 * task), quiet for {@code idleMs}, then a burst of lookups of random present keys, each one timed. Reported: the
 * percentiles over all burst lookups, and those of the slowest lookup per burst.
 * <pre>
 * -Dburst.trials (default 300), -Dburst.idleMs (default 20), -Dburst.size (default 200)
 * </pre>
 * Uses no API newer than the builder, so it runs against older collections-core builds too.
 */
public class PerfLL2IdleBurst {

    private static final Logger log = LoggerFactory.getLogger(PerfLL2IdleBurst.class);

    private static final int TRIALS = Integer.getInteger("burst.trials", 300);
    private static final int WARMUP_TRIALS = 30;
    private static final long IDLE_MS = Integer.getInteger("burst.idleMs", 20);
    private static final int BURST = Integer.getInteger("burst.size", 200);

    private static final VarHandle RESIZER = field("resizer", resizerClass());
    private static final VarHandle ARRAY_FEATURE = field("arrayFeature", CompletableFuture.class);

    private long sink;

    @Test
    public void lookups_after_a_quiet_period_during_a_migration() throws Exception {

        final long[] all = new long[TRIALS * BURST];
        final long[] slowestPerBurst = new long[TRIALS];
        final Random rand = new Random(7L);

        for (int t = -WARMUP_TRIALS; t < TRIALS; t++) {
            try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(2000).syncResizeBelow(0)
                    .build()) {
                final long[] keys = new long[4096];
                int n = 0;
                while (ARRAY_FEATURE.get(table) == null && RESIZER.get(table) == null) {
                    final long key = rand.nextLong() | 1L;
                    table.put(key, key);
                    keys[n++] = key;
                }
                // the next operation after the allocation starts the copying
                while (RESIZER.get(table) == null) {
                    Thread.sleep(1);
                    sink += table.get(keys[0]);
                }
                Thread.sleep(IDLE_MS);

                long slowest = 0;
                for (int i = 0; i < BURST; i++) {
                    final long key = keys[rand.nextInt(n)];
                    final long t0 = System.nanoTime();
                    sink += table.get(key);
                    final long ns = System.nanoTime() - t0;
                    if (t >= 0) {
                        all[t * BURST + i] = ns;
                    }
                    slowest = Math.max(slowest, ns);
                }
                if (t >= 0) {
                    slowestPerBurst[t] = slowest;
                }
            }
        }

        Arrays.sort(all);
        Arrays.sort(slowestPerBurst);
        log.info("idle {}ms, {} trials x {} lookups (sink {})", IDLE_MS, TRIALS, BURST, sink);
        log.info("all lookups (us):      p50 {} p90 {} p99 {} p99.9 {} max {}", us(all, .5), us(all, .9),
                us(all, .99), us(all, .999), us(all, 1));
        log.info("slowest per burst (us): p10 {} p50 {} p90 {} max {}", us(slowestPerBurst, .1),
                us(slowestPerBurst, .5), us(slowestPerBurst, .9), us(slowestPerBurst, 1));
    }

    private static String us(long[] sorted, double p) {
        return String.format("%.1f", sorted[Math.min(sorted.length - 1, (int) (p * sorted.length))] / 1000.0);
    }

    private static Class<?> resizerClass() {
        try {
            return Class.forName("exchange.core2.collections.hashtable.HashtableAsync2Resizer");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static VarHandle field(String name, Class<?> type) {
        try {
            return MethodHandles.privateLookupIn(LongLongLL2Hashtable.class, MethodHandles.lookup())
                    .findVarHandle(LongLongLL2Hashtable.class, name, type);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
