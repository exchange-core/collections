package tests.pref;

import androidx.collection.MutableLongLongMap;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.hash.HashLongLongMap;
import com.koloboke.collect.map.hash.HashLongLongMaps;
import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import exchange.core2.collections.hashtable.MigrationService;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Raw latency samples for exchange-charts: one CSV per scenario with a single {@code latency_ps}
 * column (one sample per row), one directory per comparison group.
 * <pre>
 * mvn test -Pbenchmarks -pl tests-perf -am -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dtest=CollectionsLatencySamples -Dsamples.dir=../../exchange-charts/samples-collections
 * </pre>
 * <ul>
 *     <li>{@code -Dsamples.dir} - output directory, relative to tests-perf (default target/samples-collections)</li>
 *     <li>{@code -Dsamples.count} - samples per CSV (default 100000)</li>
 *     <li>{@code -Dsamples.batch} - operations per service-time sample (default: 1, or enough to make a coarse
 *     clock tick worth under 1ns per operation - 128 on Windows, where nanoTime ticks every 100ns)</li>
 *     <li>{@code -Dsamples.groups} - comma-separated subset of the groups below; {@code ll2-scenarios},
 *     {@code ll2-many-tables}, {@code ll2-big-allocation}, {@code hashtable-collide} and {@code art-fill} run only
 *     when named</li>
 *     <li>{@code -Dsamples.art.label} - name of the ART series in {@code art-fill} (default "art"), to put the samples
 *     of two builds of collections-core side by side</li>
 *     <li>{@code -Dsamples.warmup.ms} - warm-up limit per service-time series (default 3000); it ends earlier,
 *     once the JIT has been quiet for 300ms</li>
 *     <li>{@code -Daffinity.reserved} - hex mask of CPUs for the pinned threads (default: the kernel's isolated CPUs),
 *     {@code -Dsamples.cpu.main} / {@code -Dsamples.cpu.ll2-migrator} - explicit CPU, see {@link CpuAffinity}</li>
 *     <li>{@code -Dsamples.seed} - offset of the data-set seeds (default 0), other keys of the same shape</li>
 *     <li>{@code -Dsamples.rewarm.puts} - hashtable-put: puts of a throwaway instance between the GC and the measured
 *     instance (default 50000, 0 = none)</li>
 * </ul>
 * Two kinds of samples:
 * <ul>
 *     <li><b>service time</b> - operations back-to-back, one sample = duration of a batch divided by its
 *     length. With batch > 1 the length is jittered within [batch, 2*batch), otherwise the clock tick turns
 *     the distribution into a comb.</li>
 *     <li><b>response time</b> ({@code hashtable-put}) - offered rate of 1M ops/s, latency counted from the time the
 *     operation was SCHEDULED, so a resize stall is charged to every operation queued behind it (coordinated
 *     omission, as in {@link PerfLatencyTests}). Every operation is timed, every N-th one is written.</li>
 * </ul>
 * With batch > 1 the operations of one batch overlap in the CPU (independent cache misses are served in
 * parallel), so a sample is closer to 1/throughput than to the latency of a lone operation. With batch = 1 a
 * sample is a single operation plus the cost of reading the clock.
 */
public class CollectionsLatencySamples {
    private static final Logger log = LoggerFactory.getLogger(CollectionsLatencySamples.class);

    private static final Path DIR = Path.of(System.getProperty("samples.dir", "target/samples-collections"));
    private static final int SAMPLES = Integer.getInteger("samples.count", 100_000);
    private static final Set<String> GROUPS = Set.of(System.getProperty("samples.groups",
            "hashtable-get,hashtable-put,hashtable-remove,hashtable-working-set,art-vs-treemap,art-keys").split(","));

    /**
     * Warm-up bounds: a tiered recompilation that lands in the measured loop shows up as a 5-20us outlier,
     * so the warm-up runs until the JIT has been quiet for a while instead of a fixed number of operations.
     */
    private static final long WARMUP_MIN_NS = 200_000_000L;
    private static final long WARMUP_QUIET_NS = 300_000_000L;
    private static final long WARMUP_MAX_NS = Long.getLong("samples.warmup.ms", 3000) * 1_000_000L;

    /** Added to every data-set seed: same shapes, other keys - e.g. for a PGO training run that must not see the measured keys. */
    private static final long SEED = Long.getLong("samples.seed", 0);

    /** the spinning threads: they burn their core all the time, ResourceUse leaves them out */
    private static final List<Thread> SPINNING = new CopyOnWriteArrayList<>();

    /**
     * LL2 migration: a worker spinning on a core of its own - started with the instance, before the measuring thread
     * is pinned (a thread inherits its creator's affinity), and pinned by its threadInit - so a migration starts
     * within microseconds. One worker for all the tables. Not in a static initializer: start() waits for threadInit,
     * which calls into this class - a deadlock while the class is being initialized.
     */
    private final MigrationService ll2Migrator = MigrationService.builder()
            .waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN).threadName("ll2-migrator")
            .threadInit(() -> {
                SPINNING.add(Thread.currentThread());
                return pinCurrentThread();
            }).start();

    /** With the spinning migrator an async resize starts within microseconds - sync only for the smallest tables. */
    private static final int LL2_SYNC_RESIZE_BELOW = 300;

    /** ll2-big-allocation: spinning workers on cores of their own, with an allocator thread and without */
    private MigrationService bigAllocationSeparate;
    private MigrationService bigAllocationOnWorker;

    /** ll2-preempted-worker: a spinning worker pinned to a core it shares with a thread that burns it */
    private MigrationService preemptedWorker;

    /** ll2-scenarios: a parking worker (the default wait), unpinned - started with the instance, before the pinning */
    private final MigrationService ll2Parking = MigrationService.builder().threadName("ll2-parking").start();

    /** hashtable-collide: HashingUtils.hash - package-private in the library - and the seed the tables use */
    private static final MethodHandle TABLE_HASH;
    private static final long TABLE_SEED;

    static {
        try {
            final Class<?> utils = Class.forName("exchange.core2.collections.hashtable.HashingUtils");
            final MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(utils, MethodHandles.lookup());
            TABLE_HASH = lookup.findStatic(utils, "hash", MethodType.methodType(int.class, long.class));
            TABLE_SEED = (long) lookup.findStaticVarHandle(utils, "SEED", long.class).get();
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Entries in every map, except the working-set and remove groups. */
    private static final int MAP_SIZE = 1_000_000;
    /** hashtable-remove: just below the resize of a 2^21-slot table at load factor 0.65 - the longest clusters. */
    private static final int REMOVE_MAP_SIZE = 1_360_000;
    /** Length of the query sequence, cycled through by the service-time loop. */
    private static final int QUERIES = 1 << 20;

    /** hashtable-put: every map grows from INITIAL_CAPACITY to PUT_KEYS entries at PUT_RATE. */
    private static final int PUT_KEYS = 4_000_000;
    /** hashtable-put: puts of the throwaway instance run between the GC and the measured one (~50ms at PUT_RATE), 0 = none. */
    private static final int REWARM_PUTS = Integer.getInteger("samples.rewarm.puts", 50_000);
    private static final long PUT_RATE = 1_000_000;
    private static final int INITIAL_CAPACITY = 16;
    private static final float LOAD_FACTOR = 0.65f;

    /** One shared value object, so that ART and TreeMap puts do not pay for boxing the value. */
    private static final Long VALUE = 7L;

    private final long clockTickNs = clockTickNs();
    /** Windows: 100ns ticks. Linux has no visible tick, the smallest step there is the cost of a call. */
    private final boolean coarseClock = clockTickNs > 50;
    private final int batch = Integer.getInteger("samples.batch",
            coarseClock ? (int) Long.highestOneBit(clockTickNs * 2 - 1) : 1);
    /**
     * A coarse clock returns the start of the tick in which the operation ended, the operation itself ends
     * half a tick later on average. Response time is counted from an exact planned time, so it is corrected.
     */
    private final long tickCorrectionPs = coarseClock ? clockTickNs * 500 : 0;

    /** Everything an operation returns ends up here, so the JIT can not drop the work. */
    private long sink;

    /**
     * A collection with one operation bound to it. undo() restores what run() changed, outside of the
     * measured interval - this keeps put and remove benchmarks at a steady size.
     */
    private interface Bench extends AutoCloseable {
        long run(long[] keys, int from, int to);

        default long undo(long[] keys, int from, int to) {
            return 0;
        }

        @Override
        default void close() {
        }
    }

    /** Entry point without JUnit, e.g. for a GraalVM native image (system properties go as -Dname=value arguments). */
    public static void main(String[] args) throws IOException {
        new CollectionsLatencySamples().writeSamples();
    }

    @Test
    public void writeSamples() throws IOException {
        log.info("clock tick {}ns, batch {}, {} samples per series -> {}", clockTickNs, batch, SAMPLES, DIR.toAbsolutePath());
        Files.createDirectories(DIR);
        writeEnvironment();
        pollutePolymorphicCalls();

        if (GROUPS.contains("ll2-big-allocation")) {
            // before the pinning: their allocator threads inherit the affinity of this one
            bigAllocationSeparate = spinningService("ll2-alloc-separate", true);
            bigAllocationOnWorker = spinningService("ll2-alloc-on-worker", false);
        }
        if (GROUPS.contains("ll2-preempted-worker")) {
            preemptedWorker = preemptedService();
        }

        try (CpuAffinity ignore = pinCurrentThread()) {
            if (GROUPS.contains("hashtable-get")) hashtableGet();
            if (GROUPS.contains("hashtable-put")) hashtablePut();
            if (GROUPS.contains("hashtable-remove")) hashtableRemove();
            if (GROUPS.contains("hashtable-working-set")) hashtableWorkingSet();
            if (GROUPS.contains("art-vs-treemap")) artVsTreeMap();
            if (GROUPS.contains("art-keys")) artKeys();
            if (GROUPS.contains("art-fill")) artFill();
            if (GROUPS.contains("ll2-scenarios")) ll2Scenarios();
            if (GROUPS.contains("hashtable-collide")) hashtableCollide();
            if (GROUPS.contains("ll2-many-tables")) ll2ManyTables();
            if (GROUPS.contains("ll2-big-allocation")) ll2BigAllocation();
            if (GROUPS.contains("ll2-preempted-worker")) ll2PreemptedWorker();
        }
        log.info("done ({})", sink);
    }

    /** Get of a present key, 1M random keys, every map grown from the same initial capacity. */
    private void hashtableGet() throws IOException {
        final String group = "hashtable-get";
        final long[] stored = randomKeys(MAP_SIZE, 1);
        final long[] hits = sample(stored, 2);

        serviceTime(group, "exchange-hashtable-get", hits, () -> {
            final LongLongHashtable m = new LongLongHashtable(INITIAL_CAPACITY);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "exchange-ll2-hashtable-get", hits, () -> {
            final LongLongLL2Hashtable m = ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW);
            for (long k : stored) m.put(k, k);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.get(keys[i]);
                    return s;
                }

                @Override
                public void close() {
                    m.close();
                }
            };
        });
        serviceTime(group, "agrona-long2long-get", hits, () -> {
            final Long2LongHashMap m = new Long2LongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, 0L);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "fastutil-long2long-get", hits, () -> {
            final Long2LongOpenHashMap m = new Long2LongOpenHashMap(INITIAL_CAPACITY, LOAD_FACTOR);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "hppc-longlong-get", hits, () -> {
            final com.carrotsearch.hppc.LongLongHashMap m = new com.carrotsearch.hppc.LongLongHashMap(INITIAL_CAPACITY, LOAD_FACTOR);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "koloboke-longlong-get", hits, () -> {
            final HashLongLongMap m = HashLongLongMaps.getDefaultFactory()
                    .withHashConfig(HashConfig.fromLoads(0.2, LOAD_FACTOR, LOAD_FACTOR))
                    .newMutableMap(INITIAL_CAPACITY);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "eclipse-longlong-get", hits, () -> {
            final org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap m =
                    new org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap(INITIAL_CAPACITY);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "androidx-longlong-get", hits, () -> {
            final MutableLongLongMap m = new MutableLongLongMap(INITIAL_CAPACITY);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.getOrDefault(keys[i], 0L);
                return s;
            };
        });
        serviceTime(group, "jdk-hashmap-get", hits, () -> {
            final Map<Long, Long> m = new HashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);
            for (long k : stored) m.put(k, k);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
    }

    /** Put of new keys at a fixed rate, from an empty map to PUT_KEYS entries - resizes included. */
    private void hashtablePut() throws IOException {
        final String group = "hashtable-put";
        final long[] keys = randomKeys(PUT_KEYS, 3);

        fixedRate(group, "exchange-hashtable-put", keys, () -> {
            final LongLongHashtable m = new LongLongHashtable(INITIAL_CAPACITY);
            return (k, from, to) -> m.put(k[from], k[from]);
        });
        fixedRate(group, "exchange-ll2-hashtable-put", keys, () -> {
            final LongLongLL2Hashtable m = ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW);
            return new Bench() {
                @Override
                public long run(long[] k, int from, int to) {
                    return m.put(k[from], k[from]);
                }

                @Override
                public void close() {
                    m.close();
                }
            };
        });
        fixedRate(group, "agrona-long2long-put", keys, () -> {
            final Long2LongHashMap m = new Long2LongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, 0L);
            return (k, from, to) -> m.put(k[from], k[from]);
        });
        fixedRate(group, "fastutil-long2long-put", keys, () -> {
            final Long2LongOpenHashMap m = new Long2LongOpenHashMap(INITIAL_CAPACITY, LOAD_FACTOR);
            return (k, from, to) -> m.put(k[from], k[from]);
        });
        fixedRate(group, "jdk-hashmap-put", keys, () -> {
            final Map<Long, Long> m = new HashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);
            return (k, from, to) -> {
                final Long prev = m.put(k[from], k[from]);
                return prev == null ? 0 : prev;
            };
        });
    }

    /**
     * LongLongLL2Hashtable configurations and situations, all at the put rate of hashtable-put (1M ops/s, response
     * time, the same 4M keys):
     * <ul>
     *     <li>{@code tuned} - the one of hashtable-put: a spinning worker on its own core, syncResizeBelow 300</li>
     *     <li>{@code parking} - a parking worker (the default wait), unpinned, the default syncResizeBelow</li>
     *     <li>{@code service-of-pinned-app} - the service started by the pinned measuring thread, without a threadInit:
     *     its worker inherits that core and shares it with the application - the pitfall</li>
     *     <li>{@code 4-tables-*} - four tables growing at once (puts round-robin, 1M keys each) on one worker,
     *     spinning or parking</li>
     *     <li>{@code mixed} - 50% puts of new keys, 30% gets and 20% removals of keys put before, LL2 vs
     *     LongLongHashtable: the table grows to ~1.2M entries</li>
     * </ul>
     * The log adds what the measuring thread allocated and the CPU of the other threads (the workers, JIT, GC - not the
     * spinning worker, which burns its core whether there is work or not).
     */
    private void ll2Scenarios() throws IOException {
        final String group = "ll2-scenarios";
        final long[] keys = randomKeys(PUT_KEYS, 3);

        fixedRate(group, "ll2-put-tuned", keys, () -> ll2Put(ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW), null));
        fixedRate(group, "ll2-put-parking", keys,
                () -> ll2Put(ll2(ll2Parking, LongLongLL2Hashtable.DEFAULT_SYNC_RESIZE_BELOW), null));
        fixedRate(group, "ll2-put-service-of-pinned-app", keys, () -> {
            final MigrationService service = MigrationService.builder().threadName("ll2-pinned-app").start();
            return ll2Put(ll2(service, LongLongLL2Hashtable.DEFAULT_SYNC_RESIZE_BELOW), service);
        });
        fixedRate(group, "ll2-put-4-tables-spinning", keys,
                () -> ll2PutTables(4, () -> ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW)));
        fixedRate(group, "ll2-put-4-tables-parking", keys,
                () -> ll2PutTables(4, () -> ll2(ll2Parking, LL2_SYNC_RESIZE_BELOW)));

        final int[] picks = mixedPicks(keys.length, 16);
        fixedRate(group, "ll2-mixed", keys, () -> mixed(ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW), picks));
        fixedRate(group, "exchange-hashtable-mixed", keys, () -> mixed(new LongLongHashtable(INITIAL_CAPACITY), picks));
    }

    /**
     * Many tables on one service - the spinning worker of ll2-put-tuned - at the put rate of hashtable-put:
     * <ul>
     *     <li>{@code 10k-idle-tables} - 10 000 small tables registered, idle, next to the measured one growing</li>
     *     <li>{@code 64-tables} - 64 tables growing at once, the keys round-robin (62 500 each)</li>
     * </ul>
     * Uses only the API of the first MigrationService, so that the versions can be compared.
     */
    private void ll2ManyTables() throws IOException {
        final String group = "ll2-many-tables";
        final long[] keys = randomKeys(PUT_KEYS, 3);

        fixedRate(group, "ll2-put-10k-idle-tables", keys, () -> {
            final LongLongLL2Hashtable[] idle = new LongLongLL2Hashtable[10_000];
            for (int t = 0; t < idle.length; t++) {
                idle[t] = ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW);
                idle[t].put(t + 1L, t);
            }
            final LongLongLL2Hashtable m = ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW);
            return new Bench() {
                @Override
                public long run(long[] k, int from, int to) {
                    return m.put(k[from], k[from]);
                }

                @Override
                public void close() {
                    m.close();
                    for (LongLongLL2Hashtable t : idle) t.close();
                }
            };
        });
        fixedRate(group, "ll2-put-64-tables", keys,
                () -> ll2PutTables(64, () -> ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW)));
    }

    /** the neighbour of ll2-big-allocation: 2^24 slots, filled to one entry below its resize */
    private static final int NEIGHBOUR_SLOTS = 1 << 24;
    private static final int NEIGHBOUR_ENTRIES = (int) (NEIGHBOUR_SLOTS * 0.65f) - 1;

    /**
     * A large allocation next to a growing table, at the put rate of hashtable-put. On the measured table's service a
     * neighbour of 2^24 slots, filled to one entry below its resize, gets that entry with the first measured put: its
     * request of a 2^25-slot array - 512MB, tens of milliseconds to zero - is served while the measured table grows
     * through its migrations. With an allocator thread the worker goes on copying; without one it allocates first.
     */
    private void ll2BigAllocation() throws IOException {
        final String group = "ll2-big-allocation";
        final long[] keys = randomKeys(PUT_KEYS, 3);
        final long[] neighbourKeys = randomKeys(NEIGHBOUR_ENTRIES + 1, 21);

        fixedRate(group, "ll2-put-neighbour-allocates-on-allocator", keys,
                () -> withBigNeighbour(bigAllocationSeparate, neighbourKeys));
        fixedRate(group, "ll2-put-neighbour-allocates-on-worker", keys,
                () -> withBigNeighbour(bigAllocationOnWorker, neighbourKeys));
    }

    private static Bench withBigNeighbour(MigrationService service, long[] neighbourKeys) {
        final LongLongLL2Hashtable neighbour = LongLongLL2Hashtable.builder().migrationService(service)
                .expectedSize(NEIGHBOUR_SLOTS / 2).build();
        for (int i = 0; i < NEIGHBOUR_ENTRIES; i++) {
            neighbour.put(neighbourKeys[i], i);
        }
        final long trigger = neighbourKeys[NEIGHBOUR_ENTRIES];
        final LongLongLL2Hashtable m = ll2(service, LL2_SYNC_RESIZE_BELOW);
        return new Bench() {
            @Override
            public long run(long[] k, int from, int to) {
                if (from == 0) {
                    neighbour.put(trigger, 0L); // its resize: the request of the large array
                }
                return m.put(k[from], k[from]);
            }

            @Override
            public void close() {
                m.close();
                neighbour.close();
            }
        };
    }

    /**
     * A worker the scheduler takes off its core for milliseconds at a time, at the put rate of hashtable-put: it is
     * pinned to a core it shares with a thread that burns it, so they get time slices in turn - also while the worker
     * is in the middle of a portion it has claimed.
     */
    private void ll2PreemptedWorker() throws IOException {
        final String group = "ll2-preempted-worker";
        final long[] keys = randomKeys(PUT_KEYS, 3);

        fixedRate(group, "ll2-put-worker-preempted", keys,
                () -> ll2Put(ll2(preemptedWorker, LL2_SYNC_RESIZE_BELOW), null));
        fixedRate(group, "ll2-put-4-tables-worker-preempted", keys,
                () -> ll2PutTables(4, () -> ll2(preemptedWorker, LL2_SYNC_RESIZE_BELOW)));
    }

    /** a spinning worker pinned by its threadInit, which starts a thread burning the same core (it inherits the pin) */
    private static MigrationService preemptedService() {
        return MigrationService.builder().waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN)
                .threadName("ll2-preempted")
                .threadInit(() -> {
                    SPINNING.add(Thread.currentThread());
                    final CpuAffinity affinity = pinCurrentThread();
                    final Thread burner = new Thread(() -> {
                        for (; ; ) Thread.onSpinWait();
                    }, "ll2-preempting");
                    burner.setDaemon(true);
                    SPINNING.add(burner);
                    burner.start();
                    return affinity;
                }).start();
    }

    /** a worker spinning on a core of its own (pinned by its threadInit), the allocator unpinned or none */
    private static MigrationService spinningService(String name, boolean allocatorThread) {
        return MigrationService.builder().waitStrategy(MigrationService.WaitStrategy.BUSY_SPIN).threadName(name)
                .allocatorThread(allocatorThread)
                .threadInit(() -> {
                    SPINNING.add(Thread.currentThread());
                    return pinCurrentThread();
                }).start();
    }

    private static LongLongLL2Hashtable ll2(MigrationService service, int syncResizeBelow) {
        return LongLongLL2Hashtable.builder().migrationService(service).expectedSize(INITIAL_CAPACITY)
                .syncResizeBelow(syncResizeBelow).build();
    }

    /** puts into m; closes it - and then the service, if one is given */
    private static Bench ll2Put(LongLongLL2Hashtable m, MigrationService service) {
        return new Bench() {
            @Override
            public long run(long[] k, int from, int to) {
                return m.put(k[from], k[from]);
            }

            @Override
            public void close() {
                m.close();
                if (service != null) service.close();
            }
        };
    }

    /** n (a power of two) tables, the keys round-robin */
    private static Bench ll2PutTables(int n, Supplier<LongLongLL2Hashtable> table) {
        final LongLongLL2Hashtable[] tables = new LongLongLL2Hashtable[n];
        for (int t = 0; t < n; t++) tables[t] = table.get();
        return new Bench() {
            @Override
            public long run(long[] k, int from, int to) {
                return tables[from & (n - 1)].put(k[from], k[from]);
            }

            @Override
            public void close() {
                for (LongLongLL2Hashtable t : tables) t.close();
            }
        };
    }

    /** operation i: put of keys[i] if i % 10 < 5, otherwise a get (5..7) or a removal (8, 9) of keys[picks[i]] */
    private static Bench mixed(ILongLongHashtable m, int[] picks) {
        return new Bench() {
            @Override
            public long run(long[] k, int from, int to) {
                final int op = from % 10;
                if (op < 5) return m.put(k[from], k[from]);
                final long key = k[picks[from]];
                return op < 8 ? m.get(key) : m.remove(key);
            }

            @Override
            public void close() {
                if (m instanceof LongLongLL2Hashtable ll2) ll2.close();
            }
        };
    }

    /** for every non-put operation of mixed(): the index of a key put before it */
    private static int[] mixedPicks(int n, long seed) {
        final Random rand = new Random(seed + SEED);
        final int[] picks = new int[n];
        for (int i = 0; i < n; i++) {
            if (i % 10 >= 5) {
                // a put index (j % 10 < 5) below i: i % 10 >= 5, so (i / 10) * 10 + 4 < i
                picks[i] = rand.nextInt(i / 10 + 1) * 10 + rand.nextInt(5);
            }
        }
        return picks;
    }

    /**
     * Keys picked to collide - as anyone who knows the hash function can, client order ids for one: 2000 keys whose
     * home slot under seed 0 is in the first 64 of the 2^21 slots a 1M-entry table has, stored with 1M random keys.
     * Service time of their gets. Under seed 0 they pile up into one cluster; run again with
     * {@code -Dexchange.hashtable.seed=random}: the same keys scatter.
     */
    private void hashtableCollide() throws IOException {
        final String group = "hashtable-collide";
        final long[] stored = randomKeys(MAP_SIZE, 1);
        final long[] colliding = collidingKeys(2000, 64, (1 << 21) - 1, 18);
        final long[] queries = sample(colliding, 19);
        final String seed = TABLE_SEED == 0 ? "seed-0" : "seed-random";

        serviceTime(group, "exchange-hashtable-get-colliding-" + seed, queries, () -> {
            final LongLongHashtable m = new LongLongHashtable(INITIAL_CAPACITY);
            for (long k : stored) m.put(k, k);
            for (long k : colliding) m.put(k, k);
            log.info("{}: capacity {}", group, m.capacity());
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "exchange-ll2-hashtable-get-colliding-" + seed, queries, () -> {
            final LongLongLL2Hashtable m = ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW);
            for (long k : stored) m.put(k, k);
            for (long k : colliding) m.put(k, k);
            m.finishMigration();
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.get(keys[i]);
                    return s;
                }

                @Override
                public void close() {
                    m.close();
                }
            };
        });
    }

    /** the hash of a key as a table with seed 0 computes it */
    private static int tableHash(long key) {
        try {
            return (int) TABLE_HASH.invokeExact(key);
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    /** n distinct keys whose home slot under seed 0 (hash & slotMask) is below window */
    private static long[] collidingKeys(int n, int window, int slotMask, long seed) {
        final Random rand = new Random(seed + SEED);
        final long[] keys = new long[n];
        int found = 0;
        while (found < n) {
            final long key = rand.nextLong() | 1L;
            if ((tableHash(key) & slotMask) < window) keys[found++] = key;
        }
        return keys;
    }

    /** Remove of a present key at load ~0.65, put back outside the measured interval. */
    private void hashtableRemove() throws IOException {
        final String group = "hashtable-remove";
        final long[] stored = randomKeys(REMOVE_MAP_SIZE, 14);
        final long[] removals = shuffled(stored, 15);

        serviceTime(group, "exchange-hashtable-remove", removals, () -> {
            final LongLongHashtable m = new LongLongHashtable(INITIAL_CAPACITY);
            for (long k : stored) m.put(k, k);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.remove(keys[i]);
                    return s;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], keys[i]);
                    return to - from;
                }
            };
        });
        serviceTime(group, "exchange-ll2-hashtable-remove", removals, () -> {
            final LongLongLL2Hashtable m = ll2(ll2Migrator, LL2_SYNC_RESIZE_BELOW);
            for (long k : stored) m.put(k, k);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.remove(keys[i]);
                    return s;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], keys[i]);
                    return to - from;
                }

                @Override
                public void close() {
                    m.close();
                }
            };
        });
        serviceTime(group, "agrona-long2long-remove", removals, () -> {
            final Long2LongHashMap m = new Long2LongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, 0L);
            for (long k : stored) m.put(k, k);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.remove(keys[i]);
                    return s;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], keys[i]);
                    return to - from;
                }
            };
        });
        serviceTime(group, "fastutil-long2long-remove", removals, () -> {
            final Long2LongOpenHashMap m = new Long2LongOpenHashMap(INITIAL_CAPACITY, LOAD_FACTOR);
            for (long k : stored) m.put(k, k);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.remove(keys[i]);
                    return s;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], keys[i]);
                    return to - from;
                }
            };
        });
    }

    /** LongLongHashtable get from L1-sized to DRAM-sized tables, plus misses. */
    private void hashtableWorkingSet() throws IOException {
        final String group = "hashtable-working-set";
        for (int size : new int[]{1_000, 64_000, 1_000_000, 16_000_000}) {
            final long[] stored = randomKeys(size, 4);
            serviceTime(group, "exchange-hashtable-get-" + sizeLabel(size), sample(stored, 5), () -> hashtableGetBench(stored));
        }
        final long[] stored = randomKeys(MAP_SIZE, 4);
        final long[] misses = randomKeys(QUERIES, 6);
        serviceTime(group, "exchange-hashtable-get-miss-1m", misses, () -> hashtableGetBench(stored));
    }

    private static Bench hashtableGetBench(long[] stored) {
        final LongLongHashtable m = new LongLongHashtable(INITIAL_CAPACITY);
        for (long k : stored) m.put(k, k);
        return (keys, from, to) -> {
            long s = 0;
            for (int i = from; i < to; i++) s += m.get(keys[i]);
            return s;
        };
    }

    /** Ordered maps with 1M random keys: put of a new key, get, higher, remove. */
    private void artVsTreeMap() throws IOException {
        final String group = "art-vs-treemap";
        final long[] stored = randomKeys(MAP_SIZE, 7);
        final long[] hits = sample(stored, 8);
        final long[] fresh = randomKeys(QUERIES, 9);
        final long[] removals = shuffled(stored, 10);

        serviceTime(group, "art-put", fresh, () -> {
            final LongAdaptiveRadixTreeMap<Long> m = art(stored);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], VALUE);
                    return to - from;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.remove(keys[i]);
                    return to - from;
                }
            };
        });
        serviceTime(group, "treemap-put", fresh, () -> {
            final TreeMap<Long, Long> m = treeMap(stored);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], VALUE);
                    return to - from;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.remove(keys[i]);
                    return to - from;
                }
            };
        });
        serviceTime(group, "art-get", hits, () -> {
            final LongAdaptiveRadixTreeMap<Long> m = art(stored);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "treemap-get", hits, () -> {
            final TreeMap<Long, Long> m = treeMap(stored);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) s += m.get(keys[i]);
                return s;
            };
        });
        serviceTime(group, "art-higher", hits, () -> {
            final LongAdaptiveRadixTreeMap<Long> m = art(stored);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) {
                    final Long v = m.getHigherValue(keys[i]);
                    if (v != null) s += v;
                }
                return s;
            };
        });
        serviceTime(group, "treemap-higher", hits, () -> {
            final TreeMap<Long, Long> m = treeMap(stored);
            return (keys, from, to) -> {
                long s = 0;
                for (int i = from; i < to; i++) {
                    final Map.Entry<Long, Long> e = m.higherEntry(keys[i]);
                    if (e != null) s += e.getValue();
                }
                return s;
            };
        });
        serviceTime(group, "art-remove", removals, () -> {
            final LongAdaptiveRadixTreeMap<Long> m = art(stored);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.remove(keys[i]);
                    return to - from;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], VALUE);
                    return to - from;
                }
            };
        });
        serviceTime(group, "treemap-remove", removals, () -> {
            final TreeMap<Long, Long> m = treeMap(stored);
            return new Bench() {
                @Override
                public long run(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.remove(keys[i]);
                    return to - from;
                }

                @Override
                public long undo(long[] keys, int from, int to) {
                    for (int i = from; i < to; i++) m.put(keys[i], VALUE);
                    return to - from;
                }
            };
        });
    }

    /** ART get with 1M keys of different density: radix tree depth and node types follow the keys. */
    private void artKeys() throws IOException {
        final String group = "art-keys";
        final Random rand = new Random(11 + SEED);
        final long[] sequential = new long[MAP_SIZE];
        final long[] sparse = new long[MAP_SIZE];
        final long[] clustered = new long[MAP_SIZE];
        final long[] clusterNext = new long[16];
        for (int c = 0; c < clusterNext.length; c++) clusterNext[c] = (rand.nextLong() & Long.MAX_VALUE) >>> 1;
        for (int i = 0; i < MAP_SIZE; i++) {
            sequential[i] = i + 1;
            sparse[i] = (i + 1) * 256L;
            clustered[i] = clusterNext[rand.nextInt(clusterNext.length)]++;
        }
        final Map<String, long[]> profiles = Map.of(
                "sequential", sequential, "sparse", sparse, "clustered", clustered, "random", randomKeys(MAP_SIZE, 12));
        for (String profile : List.of("sequential", "sparse", "clustered", "random")) {
            final long[] stored = profiles.get(profile);
            serviceTime(group, "art-get-" + profile + "-keys", sample(stored, 13), () -> {
                final LongAdaptiveRadixTreeMap<Long> m = art(stored);
                return (keys, from, to) -> {
                    long s = 0;
                    for (int i = from; i < to; i++) s += m.get(keys[i]);
                    return s;
                };
            });
        }
    }

    private static LongAdaptiveRadixTreeMap<Long> art(long[] keys) {
        final LongAdaptiveRadixTreeMap<Long> m = new LongAdaptiveRadixTreeMap<>();
        for (long k : keys) m.put(k, VALUE);
        return m;
    }

    /**
     * get / put / remove with 1M keys of different density (as in art-keys) - the ART against TreeMap and fastutil's
     * red-black tree. A put takes a key that continues the shape (the next id, the next stride, the clusters growing,
     * another random key) and is undone, a removal takes a stored key and puts it back: the size stays 1M.
     */
    private void artFill() throws IOException {
        final String group = "art-fill";
        final String art = System.getProperty("samples.art.label", "art");
        final Random rand = new Random(21 + SEED);
        final int total = MAP_SIZE + QUERIES;
        final long[] clusterNext = new long[16];
        for (int c = 0; c < clusterNext.length; c++) clusterNext[c] = (rand.nextLong() & Long.MAX_VALUE) >>> 1;
        final Map<String, long[]> shapes = new HashMap<>();
        final long[] sequential = new long[total];
        final long[] sparse = new long[total];
        final long[] clustered = new long[total];
        for (int i = 0; i < total; i++) {
            sequential[i] = i + 1;
            sparse[i] = (i + 1) * 256L;
            clustered[i] = clusterNext[rand.nextInt(clusterNext.length)]++;
        }
        shapes.put("sequential", sequential);
        shapes.put("sparse", sparse);
        shapes.put("clustered", clustered);
        shapes.put("random", randomKeys(total, 22));

        for (String profile : List.of("sequential", "sparse", "clustered", "random")) {
            final long[] all = shapes.get(profile);
            final long[] stored = Arrays.copyOf(all, MAP_SIZE);
            final long[] fresh = Arrays.copyOfRange(all, MAP_SIZE, total);
            final long[] hits = sample(stored, 23);
            final long[] removals = shuffled(stored, 24);
            for (String impl : List.of(art, "treemap", "fastutil-rbtree")) {
                serviceTime(group, impl + "-get-" + profile, hits, () -> {
                    final SortedIndex m = sortedIndex(impl, art, stored);
                    return (keys, from, to) -> {
                        long sum = 0;
                        for (int i = from; i < to; i++) sum += m.get(keys[i]);
                        return sum;
                    };
                });
                serviceTime(group, impl + "-put-" + profile, fresh, () -> {
                    final SortedIndex m = sortedIndex(impl, art, stored);
                    return new Bench() {
                        @Override
                        public long run(long[] keys, int from, int to) {
                            for (int i = from; i < to; i++) m.put(keys[i]);
                            return to - from;
                        }

                        @Override
                        public long undo(long[] keys, int from, int to) {
                            for (int i = from; i < to; i++) m.remove(keys[i]);
                            return to - from;
                        }
                    };
                });
                serviceTime(group, impl + "-remove-" + profile, removals, () -> {
                    final SortedIndex m = sortedIndex(impl, art, stored);
                    return new Bench() {
                        @Override
                        public long run(long[] keys, int from, int to) {
                            for (int i = from; i < to; i++) m.remove(keys[i]);
                            return to - from;
                        }

                        @Override
                        public long undo(long[] keys, int from, int to) {
                            for (int i = from; i < to; i++) m.put(keys[i]);
                            return to - from;
                        }
                    };
                });
            }
        }
    }

    /** the ordered maps of art-fill; each series loads one implementation, so the calls stay monomorphic */
    private interface SortedIndex {
        long get(long key);

        void put(long key);

        void remove(long key);
    }

    private static SortedIndex sortedIndex(String impl, String artLabel, long[] keys) {
        if (impl.equals(artLabel)) {
            final LongAdaptiveRadixTreeMap<Long> m = art(keys);
            return new SortedIndex() {
                @Override
                public long get(long key) {
                    return m.get(key);
                }

                @Override
                public void put(long key) {
                    m.put(key, VALUE);
                }

                @Override
                public void remove(long key) {
                    m.remove(key);
                }
            };
        }
        if (impl.equals("treemap")) {
            final TreeMap<Long, Long> m = treeMap(keys);
            return new SortedIndex() {
                @Override
                public long get(long key) {
                    return m.get(key);
                }

                @Override
                public void put(long key) {
                    m.put(key, VALUE);
                }

                @Override
                public void remove(long key) {
                    m.remove(key);
                }
            };
        }
        final Long2ObjectRBTreeMap<Long> m = new Long2ObjectRBTreeMap<>();
        for (long k : keys) m.put(k, VALUE);
        return new SortedIndex() {
            @Override
            public long get(long key) {
                return m.get(key);
            }

            @Override
            public void put(long key) {
                m.put(key, VALUE);
            }

            @Override
            public void remove(long key) {
                m.remove(key);
            }
        };
    }

    private static TreeMap<Long, Long> treeMap(long[] keys) {
        final TreeMap<Long, Long> m = new TreeMap<>();
        for (long k : keys) m.put(k, VALUE);
        return m;
    }

    private void serviceTime(String group, String name, long[] keys, Supplier<Bench> subject) throws IOException {
        log.info("{}/{}: preparing...", group, name);
        final long[] samples = new long[SAMPLES];
        final long warmupMs;
        try (Bench bench = subject.get()) {
            final Random rand = new Random(name.hashCode());
            warmupMs = warmUp(bench, keys, rand);
            measureServiceTime(bench, keys, samples, rand);
        }
        log.info("{}/{}: warm-up {}ms", group, name, warmupMs);
        write(group, name, samples);
    }

    /** Runs the bench until the JIT has been quiet for WARMUP_QUIET_NS (within the min/max bounds), returns ms spent. */
    private long warmUp(Bench bench, long[] keys, Random rand) {
        final CompilationMXBean jit = ManagementFactory.getCompilationMXBean();
        final boolean canWatch = jit != null && jit.isCompilationTimeMonitoringSupported();
        // one round = a full pass over the keys: every key of the measured run is seen (a branch first taken there would
        // hit an uncommon trap, ~40us), and the measured ones (from pos 0) are long evicted from the cache by the end
        final long[] discarded = new long[Math.max(SAMPLES, keys.length / batch)];
        final long start = System.nanoTime();
        long quietSince = start;
        long compileMs = canWatch ? jit.getTotalCompilationTime() : 0;
        while (true) {
            measureServiceTime(bench, keys, discarded, rand);
            final long now = System.nanoTime();
            if (!canWatch) return (now - start) / 1_000_000; // single round, as before
            final long c = jit.getTotalCompilationTime();
            if (c != compileMs) {
                compileMs = c;
                quietSince = now;
            }
            if (now - start > WARMUP_MAX_NS || (now - start > WARMUP_MIN_NS && now - quietSince > WARMUP_QUIET_NS)) {
                return (now - start) / 1_000_000;
            }
        }
    }

    /**
     * What the measuring thread allocated, and the CPU of the other threads - migrators, JIT, GC; not the spinning
     * executors, which burn their core all the time - over one measured instance, where the JVM reports it (HotSpot's
     * com.sun.management beans).
     */
    private static final class ResourceUse {
        private final long allocated = allocatedBytes();
        private final long othersCpu = othersCpuNanos();
        private final long gcMs = gcMillis();

        @Override
        public String toString() {
            return String.format("measuring thread allocated %.1f MB; CPU of the other threads (not spinning) %d ms,"
                            + " GC %d ms", (allocatedBytes() - allocated) / 1e6,
                    (othersCpuNanos() - othersCpu) / 1_000_000, gcMillis() - gcMs);
        }

        private static long othersCpuNanos() {
            long spinning = 0;
            for (Thread t : SPINNING) spinning += Math.max(0, ManagementFactory.getThreadMXBean().getThreadCpuTime(t.threadId()));
            return processCpuNanos() - currentThreadCpuNanos() - spinning;
        }

        private static long allocatedBytes() {
            return ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean t
                    ? t.getCurrentThreadAllocatedBytes() : 0;
        }

        private static long processCpuNanos() {
            return ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean o
                    ? o.getProcessCpuTime() : 0;
        }

        private static long currentThreadCpuNanos() {
            return ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
        }

        private static long gcMillis() {
            return ManagementFactory.getGarbageCollectorMXBeans().stream()
                    .mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
        }
    }

    /** A core from the reserved CPUs, or the one given by -Dsamples.cpu.<thread name>. */
    private static CpuAffinity pinCurrentThread() {
        final String name = Thread.currentThread().getName();
        final Integer cpu = Integer.getInteger("samples.cpu." + name);
        final CpuAffinity affinity = cpu != null ? CpuAffinity.acquireCore(cpu) : CpuAffinity.acquireCore();
        log.info("{}: {}", name, affinity);
        return affinity;
    }

    private void measureServiceTime(Bench bench, long[] keys, long[] samples, Random rand) {
        int pos = 0;
        for (int s = 0; s < samples.length; s++) {
            final int n = batch == 1 ? 1 : batch + rand.nextInt(batch);
            if (pos + n > keys.length) pos = 0;
            final long t = System.nanoTime();
            sink += bench.run(keys, pos, pos + n);
            final long ns = System.nanoTime() - t;
            sink += bench.undo(keys, pos, pos + n);
            // exchange-charts takes positive values only; 0 is possible when the whole batch fits in one tick
            samples[s] = Math.max(1, ns * 1000 / n);
            pos += n;
        }
    }

    private void fixedRate(String group, String name, long[] keys, Supplier<Bench> subject) throws IOException {
        log.info("{}/{}: warming up on throwaway instances...", group, name);
        // resize/migration code runs rarely: repeat until a whole instance passes without a JIT compilation
        // (at least 2, at most 30s), otherwise C2 versions get installed during the measured run. Full key set:
        // the largest resizes (and the code paths they reach) must be seen before the measured run too
        final CompilationMXBean jit = ManagementFactory.getCompilationMXBean();
        final boolean canWatch = jit != null && jit.isCompilationTimeMonitoringSupported();
        final long start = System.nanoTime();
        int instances = 0;
        while (true) {
            final long compileMs = canWatch ? jit.getTotalCompilationTime() : 0;
            try (Bench warmup = subject.get()) {
                measureResponseTime(warmup, keys, new long[SAMPLES]);
            }
            instances++;
            final boolean quiet = !canWatch || jit.getTotalCompilationTime() == compileMs;
            if ((instances >= 2 && quiet) || System.nanoTime() - start > 30_000_000_000L) break;
        }
        log.info("{}/{}: warm-up {} instances, {}ms", group, name, instances, (System.nanoTime() - start) / 1_000_000);
        System.gc();
        // The full GC leaves the caches cold, and the measured instance resizes most often right at its start: its first
        // LL2 migrations ran 30-80us slower than any later one. Run the start of one more throwaway instance first.
        if (REWARM_PUTS > 0) {
            try (Bench rewarm = subject.get()) {
                measureResponseTime(rewarm, Arrays.copyOf(keys, REWARM_PUTS), new long[REWARM_PUTS * SAMPLES / keys.length]);
            }
        }
        log.info("{}/{}: measuring {} puts at {} ops/s...", group, name, keys.length, PUT_RATE);
        final long[] samples = new long[SAMPLES];
        final ResourceUse use = new ResourceUse();
        try (Bench bench = subject.get()) {
            measureResponseTime(bench, keys, samples);
        }
        log.info("{}/{}: {}", group, name, use);
        write(group, name, samples);
    }

    private void measureResponseTime(Bench bench, long[] keys, long[] samples) {
        final int stride = keys.length / samples.length;
        final long psPerOp = 1_000_000_000_000L / PUT_RATE;
        final Random rand = new Random(keys.length);
        final long start = System.nanoTime();
        long planned = 0;
        long now = 0;
        for (int i = 0; i < samples.length * stride; i++) {
            // the interval is jittered within [0.5, 1.5) of the nominal one: a schedule that is a multiple
            // of the clock tick (1us vs 100ns on Windows) would leave only two values, 0 and 1 tick
            planned += psPerOp / 2 + rand.nextLong(psPerOp);
            while (now < planned) {
                Thread.onSpinWait();
                now = (System.nanoTime() - start) * 1000;
            }
            sink += bench.run(keys, i, i + 1);
            // measured from the planned start: once behind schedule the loop above is skipped and the
            // backlog drains back-to-back, each operation charged with how late it really is
            now = (System.nanoTime() - start) * 1000;
            if (i % stride == 0) samples[i / stride] = Math.max(1, now - planned + tickCorrectionPs);
        }
    }

    private void write(String group, String name, long[] samples) throws IOException {
        final Path csv = DIR.resolve(group).resolve(name + ".csv");
        Files.createDirectories(csv.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(csv)) {
            w.write("latency_ps\n");
            for (long ps : samples) {
                w.write(Long.toString(ps));
                w.write('\n');
            }
        }
        final Histogram h = new Histogram(3);
        h.setAutoResize(true);
        for (long ps : samples) h.recordValue(ps);
        log.info("{}/{}: p50={} p99={} p99.9={} max={}", group, name,
                ns(h.getValueAtPercentile(50)), ns(h.getValueAtPercentile(99)),
                ns(h.getValueAtPercentile(99.9)), ns(h.getMaxValue()));
        System.gc();
    }

    private void writeEnvironment() throws IOException {
        final Runtime rt = Runtime.getRuntime();
        final String gc = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(GarbageCollectorMXBean::getName).collect(Collectors.joining(", "));
        Files.writeString(DIR.resolve("environment.txt"), String.join("\n",
                "date: " + LocalDate.now(),
                "jvm: " + System.getProperty("java.vm.name") + " " + System.getProperty("java.runtime.version"),
                "os: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"),
                "cpus: " + rt.availableProcessors() + ", max heap: " + (rt.maxMemory() >> 20) + "MB, gc: " + gc,
                "input arguments: " + ManagementFactory.getRuntimeMXBean().getInputArguments(),
                "nanoTime tick: " + clockTickNs + "ns, service-time batch: " + batch + (batch > 1 ? ".." + (2 * batch - 1) : "")
                        + ", response-time correction: +" + tickCorrectionPs / 1000 + "ns",
                "samples per series: " + SAMPLES,
                ""));
    }

    /**
     * The measuring loops call Bench through one call site. The first scenarios would otherwise get their
     * lambda inlined there, and every later one would pay for a megamorphic call instead.
     */
    private void pollutePolymorphicCalls() {
        final long[] keys = new long[1024];
        final List<Bench> benches = List.of(
                (k, from, to) -> from,
                (k, from, to) -> to,
                (k, from, to) -> k[from],
                new Bench() {
                    @Override
                    public long run(long[] k, int from, int to) {
                        return to - from;
                    }

                    @Override
                    public long undo(long[] k, int from, int to) {
                        return k[to - 1];
                    }
                });
        for (int i = 0; i < 20; i++) {
            for (Bench b : benches) {
                measureServiceTime(b, keys, new long[1000], new Random(i));
                measureResponseTime(b, keys, new long[1]);
            }
        }
    }

    /** Smallest step of System.nanoTime(): its resolution, or the cost of a call when that is coarser. */
    private static long clockTickNs() {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < 1000; i++) {
            final long t0 = System.nanoTime();
            long t1;
            do {
                t1 = System.nanoTime();
            } while (t1 == t0);
            min = Math.min(min, t1 - t0);
        }
        return min;
    }

    /** Distinct (with overwhelming probability) positive non-zero keys. */
    private static long[] randomKeys(int n, long seed) {
        final Random rand = new Random(seed + SEED);
        final long[] keys = new long[n];
        for (int i = 0; i < n; i++) {
            final long k = rand.nextLong() & Long.MAX_VALUE;
            keys[i] = k == 0 ? 1 : k;
        }
        return keys;
    }

    /** QUERIES keys drawn from the stored ones with replacement, in random order. */
    private static long[] sample(long[] stored, long seed) {
        final Random rand = new Random(seed + SEED);
        final long[] keys = new long[QUERIES];
        for (int i = 0; i < QUERIES; i++) keys[i] = stored[rand.nextInt(stored.length)];
        return keys;
    }

    /** Each stored key exactly once, in random order - a batch never removes the same key twice. */
    private static long[] shuffled(long[] stored, long seed) {
        final Random rand = new Random(seed + SEED);
        final long[] keys = stored.clone();
        for (int i = keys.length - 1; i > 0; i--) {
            final int j = rand.nextInt(i + 1);
            final long t = keys[i];
            keys[i] = keys[j];
            keys[j] = t;
        }
        return keys;
    }

    private static String sizeLabel(int size) {
        return size >= 1_000_000 ? size / 1_000_000 + "m" : size / 1_000 + "k";
    }

    private static String ns(long ps) {
        return String.format("%.1fns", ps / 1000.0);
    }
}
