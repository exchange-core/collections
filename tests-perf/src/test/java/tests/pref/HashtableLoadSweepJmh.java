package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.LongObjectHashtable;
import exchange.core2.collections.hashtable.UuidObjectHashtable;
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
 * Cost of an operation against the capacity and the load of a table - the data the load factors are chosen from.
 * {@link LongObjectHashtable} (12 bytes a slot) and {@link UuidObjectHashtable} (20 bytes a slot) of exactly
 * 2^log2Capacity slots holding load * 2^log2Capacity random keys; the table never resizes. From L1 (2^10 slots) to
 * DRAM (2^24):
 * <ul>
 *     <li>{@code chase} - a present key, the next one read from its object: the latency of a hit;</li>
 *     <li>{@code getMiss} - a missing key, independent lookups: a probe to the end of a cluster;</li>
 *     <li>{@code replace} - a key removed, a new one put: the backward shift and the insert, the load unchanged.</li>
 * </ul>
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main HashtableLoadSweepJmh -rf json -rff sweep.json
 * </pre>
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class HashtableLoadSweepJmh {

    @Param({"LongObject", "UuidObject"})
    public String impl;

    @Param({"10", "12", "14", "16", "18", "20", "22", "24"})
    public int log2Capacity;

    @Param({"0.25", "0.35", "0.45", "0.55", "0.65", "0.75", "0.85"})
    public double load;

    /** what the tables hold: the next key of the cycle of chase */
    static final class Value {
        long next;
        long nextLo;
    }

    private boolean uuid;
    private LongObjectHashtable<Value> longs;
    private UuidObjectHashtable<Value> uuids;
    private int size;

    /** every key once, in a random order; two longs a key for the UUID table */
    private long[] keys;
    /** keys that are not in the table */
    private long[] missing;
    private int i;
    /** chase: the key reached */
    private long current;
    private long currentLo;
    /** replace: the new keys */
    private long nextKeySeed = 0x9e3779b97f4a7c15L;

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        final int capacity = 1 << log2Capacity;
        size = (int) (capacity * load);
        // grows a little above the load: exactly the capacity for the size, no resize in replace
        final double loadFactor = Math.min(0.9, load + 0.02);
        uuid = impl.equals("UuidObject");
        if (uuid) {
            uuids = UuidObjectHashtable.builder().expectedSize(size).loadFactor(loadFactor)
                    .smallTableLoadFactor(loadFactor).build();
            check(uuids.capacity(), capacity);
        } else if (impl.equals("LongObject")) {
            longs = LongObjectHashtable.builder().expectedSize(size).loadFactor(loadFactor)
                    .smallTableLoadFactor(loadFactor).build();
            check(longs.capacity(), capacity);
        } else {
            throw new IllegalArgumentException(impl);
        }
        final int stride = uuid ? 2 : 1;
        final SplittableRandom rand = new SplittableRandom(42L);
        keys = new long[size * stride];
        final Value[] values = new Value[size];
        for (int k = 0; k < size; k++) {
            values[k] = new Value();
            randomAbsentKey(rand, keys, k);
            if (uuid) {
                uuids.put(keys[2 * k], keys[2 * k + 1], values[k]);
            } else {
                longs.put(keys[k], values[k]);
            }
        }
        missing = new long[size * stride];
        for (int k = 0; k < size; k++) {
            randomAbsentKey(rand, missing, k);
        }
        // the cycle of chase: a random one (Sattolo's shuffle), not the order of the objects in memory
        final int[] cycle = new int[size];
        for (int k = 0; k < size; k++) {
            cycle[k] = k;
        }
        for (int k = size - 1; k > 0; k--) {
            final int j = rand.nextInt(k);
            final int t = cycle[k];
            cycle[k] = cycle[j];
            cycle[j] = t;
        }
        for (int k = 0; k < size; k++) {
            final int to = cycle[k + 1 == size ? 0 : k + 1];
            values[cycle[k]].next = keys[to * stride];
            values[cycle[k]].nextLo = uuid ? keys[to * 2 + 1] : 0;
        }
        current = keys[cycle[0] * stride];
        currentLo = uuid ? keys[cycle[0] * 2 + 1] : 0;
        shuffle(keys, stride, rand);
    }

    private static void check(int actual, int expected) {
        if (actual != expected) {
            throw new IllegalStateException("capacity " + actual + ", not " + expected);
        }
    }

    /** a random key at index k that is not in the table: a random long, or a random version 4 UUID */
    private void randomAbsentKey(SplittableRandom rand, long[] to, int k) {
        if (uuid) {
            long hi;
            long lo;
            do {
                hi = (rand.nextLong() & ~0xF000L) | 0x4000L;
                lo = (rand.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
            } while (uuids.containsKey(hi, lo));
            to[2 * k] = hi;
            to[2 * k + 1] = lo;
        } else {
            long key;
            do {
                key = rand.nextLong();
            } while (longs.containsKey(key));
            to[k] = key;
        }
    }

    private static void shuffle(long[] a, int stride, SplittableRandom rand) {
        for (int k = a.length / stride - 1; k > 0; k--) {
            final int j = rand.nextInt(k + 1);
            for (int s = 0; s < stride; s++) {
                final long t = a[k * stride + s];
                a[k * stride + s] = a[j * stride + s];
                a[j * stride + s] = t;
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (affinity != null) {
            affinity.close();
        }
    }

    private int next() {
        final int k = i;
        i = k + 1 == size ? 0 : k + 1;
        return k;
    }

    /** the next key of the cycle, read from the object: the latency of a hit and of the read of its value */
    @Benchmark
    public long chase() {
        final Value v = uuid ? uuids.get(current, currentLo) : longs.get(current);
        currentLo = v.nextLo;
        return current = v.next;
    }

    /** a missing key: a probe to the free slot that ends its cluster */
    @Benchmark
    public Value getMiss() {
        final int k = next();
        return uuid ? uuids.get(missing[2 * k], missing[2 * k + 1]) : longs.get(missing[k]);
    }

    /** a key removed, a new one added with its object - the size stays */
    @Benchmark
    public Value replace() {
        final int k = next();
        long z = nextKeySeed += 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        final long key = z ^ (z >>> 27);
        if (uuid) {
            final Value value = uuids.remove(keys[2 * k], keys[2 * k + 1]);
            final long lo = z * 0x94d049bb133111ebL;
            keys[2 * k] = key;
            keys[2 * k + 1] = lo;
            return uuids.put(key, lo, value);
        }
        final Value value = longs.remove(keys[k]);
        keys[k] = key;
        return longs.put(key, value);
    }
}
