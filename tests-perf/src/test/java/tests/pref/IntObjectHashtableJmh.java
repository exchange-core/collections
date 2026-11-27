package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.IntObjectHashtable;
import exchange.core2.collections.hashtable.LongObjectHashtable;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
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
 * {@code int} keys to objects: {@link IntObjectHashtable} against the primitive maps of the same layout - the keys in
 * an {@code int[]}, the values in an {@code Object[]} beside it - and against {@link LongObjectHashtable} under the
 * same keys widened, the layout of 8 keys a cache line instead of 16:
 * <ul>
 *     <li>{@code IntObjectHashtable} - linear probing, load 0.65, backward-shift removal, a free slot holds null;</li>
 *     <li>{@code LongObjectHashtable} - the same with {@code long} keys: 12 bytes a slot, not 8;</li>
 *     <li>{@code IntObjectHashMap} - Eclipse Collections: load 0.5, tombstones;</li>
 *     <li>{@code Int2ObjectHashMap} - agrona: load 0.65, a null value marks a free slot, removal compacts the chain;</li>
 *     <li>{@code Int2ObjectOpenHashMap} - fastutil: load 0.75, backward-shift removal.</li>
 * </ul>
 * Each pre-sized for the keys, the others with their default load factor, the two tables of this library with
 * {@link #loadFactor}. The operations and the order of the keys as in {@link LongObjectHashtableJmh}: distinct keys
 * that look random ({@link #key}), reached in a random order, {@code chase} along a cycle through the keys (a value
 * holds the next key). From L2 (10k) to DRAM (10M):
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main IntObjectHashtableJmh -rf json -rff out.json
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
public class IntObjectHashtableJmh {

    @Param({"IntObjectHashtable", "LongObjectHashtable", "IntObjectHashMap", "Int2ObjectHashMap",
            "Int2ObjectOpenHashMap"})
    public String impl;

    @Param({"10000", "1000000", "10000000"})
    public int size;

    /**
     * The load the two tables of this library grow at: a number - one load factor for every size, as
     * {@code loadFactor(x).smallTableLoadFactor(x)}; 0.65 is the load of the results in docs/hashtables.md - or
     * {@code default}, the defaults of the builders (0.5 below 2^16 slots, 0.65 from there on).
     */
    @Param({"0.65"})
    public String loadFactor;

    private static final int OURS = 0;
    private static final int OURS_LONG = 1;
    private static final int ECLIPSE = 2;
    private static final int AGRONA = 3;
    private static final int FASTUTIL = 4;

    /** what the maps hold: the next key of the cycle of chase */
    static final class Value {
        int next;
    }

    private int kind;

    private IntObjectHashtable<Value> ours;
    private LongObjectHashtable<Value> oursLong;
    private IntObjectHashMap<Value> eclipse;
    private Int2ObjectHashMap<Value> agrona;
    private Int2ObjectOpenHashMap<Value> fastutil;

    /** every key once, in a random order */
    private int[] keys;
    /** keys that are not in the map */
    private int[] missing;
    private int i;
    /** chase: the key reached */
    private int current;
    /** n of the next key, see {@link #key} */
    private int keyCounter;

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        final SplittableRandom rand = new SplittableRandom(42L);
        switch (impl) {
            case "IntObjectHashtable" -> {
                kind = OURS;
                final IntObjectHashtable.Builder builder = IntObjectHashtable.builder().expectedSize(size);
                if (!loadFactor.equals("default")) {
                    final double load = Double.parseDouble(loadFactor);
                    builder.loadFactor(load).smallTableLoadFactor(load);
                }
                ours = builder.build();
            }
            case "LongObjectHashtable" -> {
                kind = OURS_LONG;
                final LongObjectHashtable.Builder builder = LongObjectHashtable.builder().expectedSize(size);
                if (!loadFactor.equals("default")) {
                    final double load = Double.parseDouble(loadFactor);
                    builder.loadFactor(load).smallTableLoadFactor(load);
                }
                oursLong = builder.build();
            }
            case "IntObjectHashMap" -> {
                kind = ECLIPSE;
                eclipse = new IntObjectHashMap<>(size);
            }
            case "Int2ObjectHashMap" -> {
                kind = AGRONA;
                agrona = new Int2ObjectHashMap<>(size, 0.65f);
            }
            case "Int2ObjectOpenHashMap" -> {
                kind = FASTUTIL;
                fastutil = new Int2ObjectOpenHashMap<>(size);
            }
            default -> throw new IllegalArgumentException(impl);
        }
        // keys k(n) of a bijection, n counting from 0: the keys, then the missing ones, then the new keys of replace
        keys = new int[size];
        final Value[] values = new Value[size];
        for (int k = 0; k < size; k++) {
            keys[k] = key(keyCounter++);
            values[k] = new Value();
            put(keys[k], values[k]);
        }
        missing = new int[size];
        for (int k = 0; k < size; k++) {
            missing[k] = key(keyCounter++);
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
        } else if (kind == OURS_LONG) {
            System.out.printf("%s, load factor %s: %d keys in %d slots (load %.2f)%n",
                    impl, loadFactor, size, oursLong.capacity(), (double) size / oursLong.capacity());
        }
    }

    private static void shuffle(int[] a, SplittableRandom rand) {
        for (int k = a.length - 1; k > 0; k--) {
            final int j = rand.nextInt(k + 1);
            final int t = a[k];
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

    private Value get(int key) {
        return switch (kind) {
            case OURS -> ours.get(key);
            case OURS_LONG -> oursLong.get(key);
            case ECLIPSE -> eclipse.get(key);
            case AGRONA -> agrona.get(key);
            default -> fastutil.get(key);
        };
    }

    private Value put(int key, Value value) {
        return switch (kind) {
            case OURS -> ours.put(key, value);
            case OURS_LONG -> oursLong.put(key, value);
            case ECLIPSE -> eclipse.put(key, value);
            case AGRONA -> agrona.put(key, value);
            default -> fastutil.put(key, value);
        };
    }

    private Value remove(int key) {
        return switch (kind) {
            case OURS -> ours.remove(key);
            case OURS_LONG -> oursLong.remove(key);
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
    public int chase() {
        return current = get(current).next;
    }

    /**
     * A key removed, a new one added with its object - the size stays. Random 32-bit keys would come back: one in
     * ~400 at 10M keys would already be in the map. The keys are a bijection of a counter instead, so none repeats
     * before 2^32 of them.
     */
    @Benchmark
    public Value replace() {
        final int k = next();
        final int key = key(keyCounter++);
        final Value value = remove(keys[k]);
        keys[k] = key;
        return put(key, value);
    }

    /**
     * The n-th key: the MurmurHash3 finalizer of n plus a constant - a bijection on 32 bits, so distinct n give
     * distinct keys, which look random; another mixer than the hash of any of the maps.
     */
    private static int key(int n) {
        int h = n + 0x632be5ab;
        h = (h ^ (h >>> 16)) * 0x85ebca6b;
        h = (h ^ (h >>> 13)) * 0xc2b2ae35;
        return h ^ (h >>> 16);
    }
}
