package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.LongObjectHashtable;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
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
 * {@code long} keys to objects: {@link LongObjectHashtable} against the primitive maps of the same layout - the keys in
 * a {@code long[]}, the values in an {@code Object[]} beside it:
 * <ul>
 *     <li>{@code LongObjectHashtable} - linear probing, load 0.65, backward-shift removal, a free slot holds null;</li>
 *     <li>{@code LongObjectHashtable-2x} - the same pre-sized for twice the keys: the load of Eclipse's map;</li>
 *     <li>{@code LongObjectHashMap} - Eclipse Collections (as in exchange-core): load 0.5, tombstones;</li>
 *     <li>{@code Long2ObjectHashMap} - agrona: load 0.65, a null value marks a free slot, removal compacts the chain;</li>
 *     <li>{@code Long2ObjectOpenHashMap} - fastutil: load 0.75, backward-shift removal.</li>
 * </ul>
 * Each pre-sized for the keys, the others with their default load factor, {@code LongObjectHashtable} with
 * {@link #loadFactor}. The keys are random, reached in a random order through a shuffled array read in sequence: the
 * lookups are independent and their misses overlap - except in {@code chase}, which follows a cycle through the keys
 * (a value holds the next key), each lookup and the read of the object waiting for the one before: the latency of a
 * lookup. From L2 (10k) to DRAM (10M):
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main LongObjectHashtableJmh -rf json -rff out.json
 * java -cp test-classes:lib/* org.openjdk.jmh.Main LongObjectHashtableJmh -p loadFactor=default ...
 * </pre>
 * The setup prints the capacity the table got. With -Daffinity.reserved (isolated CPUs) the benchmark thread pins
 * itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class LongObjectHashtableJmh {

    @Param({"LongObjectHashtable", "LongObjectHashtable-2x", "LongObjectHashMap", "Long2ObjectHashMap",
            "Long2ObjectOpenHashMap"})
    public String impl;

    @Param({"10000", "1000000", "10000000"})
    public int size;

    /**
     * The load {@code LongObjectHashtable} grows at: a number - one load factor for every size, as
     * {@code loadFactor(x).smallTableLoadFactor(x)}; 0.65 is the load of the results in docs/hashtables.md - or
     * {@code default}, the defaults of the builder (0.5 below 2^16 slots, 0.65 from there on).
     */
    @Param({"0.65"})
    public String loadFactor;

    private static final int OURS = 0;
    private static final int ECLIPSE = 1;
    private static final int AGRONA = 2;
    private static final int FASTUTIL = 3;

    /** what the maps hold: the next key of the cycle of chase */
    static final class Value {
        long next;
    }

    private int kind;

    private LongObjectHashtable<Value> ours;
    private LongObjectHashMap<Value> eclipse;
    private Long2ObjectHashMap<Value> agrona;
    private Long2ObjectOpenHashMap<Value> fastutil;

    /** every key once, in a random order */
    private long[] keys;
    /** keys that are not in the map */
    private long[] missing;
    private int i;
    /** chase: the key reached */
    private long current;
    /** replace: the new keys */
    private long nextKeySeed = 0x9e3779b97f4a7c15L;

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        final SplittableRandom rand = new SplittableRandom(42L);
        switch (impl) {
            case "LongObjectHashtable" -> {
                kind = OURS;
                ours = newTable(size);
            }
            case "LongObjectHashtable-2x" -> {
                kind = OURS;
                ours = newTable(2 * size);
            }
            case "LongObjectHashMap" -> {
                kind = ECLIPSE;
                eclipse = new LongObjectHashMap<>(size);
            }
            case "Long2ObjectHashMap" -> {
                kind = AGRONA;
                agrona = new Long2ObjectHashMap<>(size, 0.65f);
            }
            case "Long2ObjectOpenHashMap" -> {
                kind = FASTUTIL;
                fastutil = new Long2ObjectOpenHashMap<>(size);
            }
            default -> throw new IllegalArgumentException(impl);
        }
        keys = new long[size];
        final Value[] values = new Value[size];
        for (int k = 0; k < size; k++) {
            long key;
            do {
                key = rand.nextLong();
            } while (get(key) != null);
            keys[k] = key;
            values[k] = new Value();
            put(key, values[k]);
        }
        missing = new long[size];
        for (int k = 0; k < size; k++) {
            long key;
            do {
                key = rand.nextLong();
            } while (get(key) != null);
            missing[k] = key;
        }
        // the cycle of chase: a random one (Sattolo's shuffle) - not the order of the keys, which is the order of the
        // objects in memory
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
            values[cycle[k]].next = keys[cycle[k + 1 == size ? 0 : k + 1]];
        }
        current = keys[cycle[0]];
        shuffle(keys, rand);
        if (kind == OURS) {
            System.out.printf("%s, load factor %s: %d keys in %d slots (load %.2f)%n",
                    impl, loadFactor, size, ours.capacity(), (double) size / ours.capacity());
        }
    }

    private LongObjectHashtable<Value> newTable(int expectedSize) {
        final LongObjectHashtable.Builder builder = LongObjectHashtable.builder().expectedSize(expectedSize);
        if (!loadFactor.equals("default")) {
            final double load = Double.parseDouble(loadFactor);
            builder.loadFactor(load).smallTableLoadFactor(load);
        }
        return builder.build();
    }

    private static void shuffle(long[] a, SplittableRandom rand) {
        for (int k = a.length - 1; k > 0; k--) {
            final int j = rand.nextInt(k + 1);
            final long t = a[k];
            a[k] = a[j];
            a[j] = t;
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

    private Value get(long key) {
        return switch (kind) {
            case OURS -> ours.get(key);
            case ECLIPSE -> eclipse.get(key);
            case AGRONA -> agrona.get(key);
            default -> fastutil.get(key);
        };
    }

    private Value put(long key, Value value) {
        return switch (kind) {
            case OURS -> ours.put(key, value);
            case ECLIPSE -> eclipse.put(key, value);
            case AGRONA -> agrona.put(key, value);
            default -> fastutil.put(key, value);
        };
    }

    private Value remove(long key) {
        return switch (kind) {
            case OURS -> ours.remove(key);
            case ECLIPSE -> eclipse.remove(key);
            case AGRONA -> agrona.remove(key);
            default -> fastutil.remove(key);
        };
    }

    /** a present key: the reference, the object not read */
    @Benchmark
    public Value get() {
        return get(keys[next()]);
    }

    /** a missing key: a probe to the free slot that ends its cluster */
    @Benchmark
    public Value getMiss() {
        return get(missing[next()]);
    }

    /** the next key of the cycle, read from the object: the latency of a lookup and of the read of its value */
    @Benchmark
    public long chase() {
        return current = get(current).next;
    }

    /** a key removed, a new one added with its object - the size stays */
    @Benchmark
    public Value replace() {
        final int k = next();
        long z = nextKeySeed += 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        final long key = z ^ (z >>> 27);
        final Value value = remove(keys[k]);
        keys[k] = key;
        return put(key, value);
    }
}
