package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
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

import java.util.HashMap;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * UUID keys to objects: {@link UuidObjectHashtable} against {@code HashMap<UUID, V>}. The keys arrive as two longs (a
 * decoded message) - in a {@code long[]}, two a key:
 * <ul>
 *     <li>{@code UuidObjectHashtable} - the halves passed as they are;</li>
 *     <li>{@code UuidObjectHashtable-2x} - the same pre-sized for twice the keys: load ~0.3 instead of ~0.6;</li>
 *     <li>{@code UuidObjectHashtable-UUID} - a {@code new UUID(hi, lo)} passed: what the allocation and the UUID
 *     methods cost, if anything;</li>
 *     <li>{@code HashMap} - a {@code new UUID(hi, lo)} per operation, as a caller of a {@code Map<UUID, V>} does.</li>
 * </ul>
 * The keys are random (UUIDv4), reached in a random order; {@code chase} follows a cycle through them, a value holding
 * the next key - the latency of a lookup and of the read of its object. See {@link LongObjectHashtableJmh}.
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main UuidObjectHashtableJmh -rf json -rff out.json
 * java -cp test-classes:lib/* org.openjdk.jmh.Main UuidObjectHashtableJmh -p loadFactor=default ...
 * </pre>
 * The table grows at {@link #loadFactor}; the setup prints the capacity it got.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class UuidObjectHashtableJmh {

    @Param({"UuidObjectHashtable", "UuidObjectHashtable-2x", "UuidObjectHashtable-UUID", "HashMap"})
    public String impl;

    @Param({"10000", "1000000", "10000000"})
    public int size;

    /**
     * The load {@code UuidObjectHashtable} grows at: a number - one load factor for every size; 0.65 is the load of
     * the results in docs/hashtables.md - or {@code default}, the defaults of the builder. See
     * {@link LongObjectHashtableJmh#loadFactor}.
     */
    @Param({"0.65"})
    public String loadFactor;

    private static final int HALVES = 0;
    private static final int UUIDS = 1;
    private static final int HASH_MAP = 2;

    /** what the maps hold: the next key of the cycle of chase */
    static final class Value {
        long nextHi;
        long nextLo;
    }

    private int kind;

    private UuidObjectHashtable<Value> table;
    private HashMap<UUID, Value> map;

    /** every key once, in a random order: key k at 2k, 2k + 1 */
    private long[] keys;
    /** keys that are not in the map */
    private long[] missing;
    private int i;
    /** chase: the key reached */
    private long currentHi;
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
        final SplittableRandom rand = new SplittableRandom(42L);
        switch (impl) {
            case "UuidObjectHashtable", "UuidObjectHashtable-2x" -> kind = HALVES;
            case "UuidObjectHashtable-UUID" -> kind = UUIDS;
            case "HashMap" -> kind = HASH_MAP;
            default -> throw new IllegalArgumentException(impl);
        }
        if (kind == HASH_MAP) {
            map = new HashMap<>((int) (size / 0.75f) + 1);
        } else {
            final UuidObjectHashtable.Builder builder = UuidObjectHashtable.builder()
                    .expectedSize(impl.endsWith("-2x") ? 2 * size : size);
            if (!loadFactor.equals("default")) {
                final double load = Double.parseDouble(loadFactor);
                builder.loadFactor(load).smallTableLoadFactor(load);
            }
            table = builder.build();
        }
        keys = new long[size * 2];
        final Value[] values = new Value[size];
        for (int k = 0; k < size; k++) {
            long hi;
            long lo;
            do {
                hi = rand.nextLong() & ~0xF000L | 0x4000L;
                lo = rand.nextLong() & ~(3L << 62) | (2L << 62);
            } while (get(hi, lo) != null);
            keys[2 * k] = hi;
            keys[2 * k + 1] = lo;
            values[k] = new Value();
            put(hi, lo, values[k]);
        }
        missing = new long[size * 2];
        for (int k = 0; k < size; k++) {
            long hi;
            long lo;
            do {
                hi = rand.nextLong();
                lo = rand.nextLong();
            } while (get(hi, lo) != null);
            missing[2 * k] = hi;
            missing[2 * k + 1] = lo;
        }
        // the cycle of chase: a random one (Sattolo's shuffle)
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
            final int n = cycle[k + 1 == size ? 0 : k + 1];
            values[cycle[k]].nextHi = keys[2 * n];
            values[cycle[k]].nextLo = keys[2 * n + 1];
        }
        currentHi = keys[2 * cycle[0]];
        currentLo = keys[2 * cycle[0] + 1];
        for (int k = size - 1; k > 0; k--) {
            final int j = rand.nextInt(k + 1);
            final long hi = keys[2 * k];
            final long lo = keys[2 * k + 1];
            keys[2 * k] = keys[2 * j];
            keys[2 * k + 1] = keys[2 * j + 1];
            keys[2 * j] = hi;
            keys[2 * j + 1] = lo;
        }
        if (table != null) {
            System.out.printf("%s, load factor %s: %d keys in %d slots (load %.2f)%n",
                    impl, loadFactor, size, table.capacity(), (double) size / table.capacity());
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

    private Value get(long hi, long lo) {
        return switch (kind) {
            case HALVES -> table.get(hi, lo);
            case UUIDS -> table.get(new UUID(hi, lo));
            default -> map.get(new UUID(hi, lo));
        };
    }

    private Value put(long hi, long lo, Value value) {
        return switch (kind) {
            case HALVES -> table.put(hi, lo, value);
            case UUIDS -> table.put(new UUID(hi, lo), value);
            default -> map.put(new UUID(hi, lo), value);
        };
    }

    private Value remove(long hi, long lo) {
        return switch (kind) {
            case HALVES -> table.remove(hi, lo);
            case UUIDS -> table.remove(new UUID(hi, lo));
            default -> map.remove(new UUID(hi, lo));
        };
    }

    /** a present key: the reference, the object not read */
    @Benchmark
    public Value get() {
        final int k = next();
        return get(keys[2 * k], keys[2 * k + 1]);
    }

    /** a missing key: a probe to the free slot that ends its cluster */
    @Benchmark
    public Value getMiss() {
        final int k = next();
        return get(missing[2 * k], missing[2 * k + 1]);
    }

    /** the next key of the cycle, read from the object: the latency of a lookup and of the read of its value */
    @Benchmark
    public long chase() {
        final Value v = get(currentHi, currentLo);
        currentHi = v.nextHi;
        return currentLo = v.nextLo;
    }

    /** a key removed, a new one added with its object - the size stays */
    @Benchmark
    public Value replace() {
        final int k = next();
        long z = nextKeySeed += 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        final long hi = z ^ (z >>> 27);
        z = (hi ^ (hi >>> 30)) * 0xbf58476d1ce4e5b9L;
        final long lo = z ^ (z >>> 27);
        final Value value = remove(keys[2 * k], keys[2 * k + 1]);
        keys[2 * k] = hi;
        keys[2 * k + 1] = lo;
        return put(hi, lo, value);
    }
}
