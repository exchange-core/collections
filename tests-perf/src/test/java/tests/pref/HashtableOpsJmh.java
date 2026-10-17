package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import exchange.core2.collections.hashtable.MigrationService;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Average time of the single-key operations on a pre-sized table that does not resize: hits and misses, from an
 * L1/L2-resident table to one in DRAM. For A/B runs of collections-core builds (same benchmark, different jar):
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main HashtableOpsJmh -rf json -rff out.json
 * </pre>
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class HashtableOpsJmh {

    @Param({"LongLongHashtable", "LongLongLL2Hashtable"})
    public String impl;

    /** entries: 1000 (32 KB table), 100000 (4 MB), 3000000 (128 MB) */
    @Param({"1000", "100000", "3000000"})
    public int size;

    private static final int KEYS = 1 << 16;
    private static final int KEYS_MASK = KEYS - 1;

    private ILongLongHashtable table;
    private MigrationService service;
    private final long[] present = new long[KEYS];
    private final long[] absent = new long[KEYS];
    private int i;
    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        if (impl.equals("LongLongLL2Hashtable")) {
            service = MigrationService.builder().start();
            table = LongLongLL2Hashtable.builder().migrationService(service).expectedSize(size).build();
        } else {
            table = new LongLongHashtable(size);
        }

        final SplittableRandom rand = new SplittableRandom(42L);
        final long[] keys = new long[size];
        for (int k = 0; k < size; k++) {
            keys[k] = rand.nextLong() | 1L;
            table.put(keys[k], k);
        }
        for (int k = 0; k < KEYS; k++) {
            present[k] = keys[rand.nextInt(size)];
            long key;
            do {
                key = rand.nextLong() | 1L;
            } while (table.containsKey(key));
            absent[k] = key;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (table instanceof AutoCloseable closeable) {
            closeable.close();
        }
        if (service != null) {
            service.close();
        }
        if (affinity != null) {
            affinity.close();
        }
    }

    @Benchmark
    public long getHit() {
        return table.get(present[i++ & KEYS_MASK]);
    }

    @Benchmark
    public long getMiss() {
        return table.get(absent[i++ & KEYS_MASK]);
    }

    @Benchmark
    public boolean containsKeyHit() {
        return table.containsKey(present[i++ & KEYS_MASK]);
    }

    @Benchmark
    public boolean containsKeyMiss() {
        return table.containsKey(absent[i++ & KEYS_MASK]);
    }

    @Benchmark
    public long putUpdate() {
        final long key = present[i++ & KEYS_MASK];
        return table.put(key, key);
    }

    /** removal and insertion of the same key - the size stays, no resize */
    @Benchmark
    public long removeInsert() {
        final long key = present[i++ & KEYS_MASK];
        final long value = table.remove(key);
        return table.put(key, value);
    }
}
