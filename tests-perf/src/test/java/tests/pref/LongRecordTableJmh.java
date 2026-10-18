package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongRecordTable;
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
 * Balances (available, reserved) by key: {@link LongRecordTable} against the ways to keep them without it:
 * <ul>
 *     <li>{@code LongRecordTable} - a probe of the index, then the record;</li>
 *     <li>{@code twoHashtables} - a {@link LongLongHashtable} per field: a probe of each;</li>
 *     <li>{@code LongObjectHashMap} - Eclipse Collections, a key to an object per balance (as in exchange-core): keys
 *     and values in two arrays, then the object; freed objects go to a pool.</li>
 * </ul>
 * The keys are random, reached in a random order through a shuffled array read in sequence. The accesses are
 * independent, so their misses overlap - except in {@code chaseByKey}, which follows a cycle through the keys (a
 * balance holds the next key in its fields), each lookup waiting for the one before: the latency of one. From
 * L2-resident (10k) to DRAM (10M):
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main LongRecordTableJmh -rf json -rff out.json
 * </pre>
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class LongRecordTableJmh {

    @Param({"LongRecordTable", "twoHashtables", "LongObjectHashMap"})
    public String impl;

    /** keys: 10000, 1000000, 10000000 */
    @Param({"10000", "1000000", "10000000"})
    public int size;

    private static final int RECORD_TABLE = 0;
    private static final int TWO_HASHTABLES = 1;
    private static final int OBJECT_MAP = 2;

    private static final int AVAILABLE = 0;
    private static final int RESERVED = 1;

    static final class Balance {
        long available;
        long reserved;
    }

    private int kind;

    private LongRecordTable table;
    private LongLongHashtable available;
    private LongLongHashtable reserved;
    private LongObjectHashMap<Balance> map;
    private final Balance[] pool = new Balance[16];
    private int poolSize;

    /** every key once, in a random order */
    private long[] keys;
    private int i;
    /** chaseByKey: the key reached */
    private long current;
    /** createRemove: the new keys */
    private long nextKeySeed = 0x9e3779b97f4a7c15L;

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        final SplittableRandom rand = new SplittableRandom(42L);
        keys = new long[size];
        switch (impl) {
            case "LongRecordTable" -> {
                kind = RECORD_TABLE;
                table = LongRecordTable.builder().fields(2).expectedSize(size).build();
            }
            case "twoHashtables" -> {
                kind = TWO_HASHTABLES;
                available = new LongLongHashtable(size);
                reserved = new LongLongHashtable(size);
            }
            case "LongObjectHashMap" -> {
                kind = OBJECT_MAP;
                map = new LongObjectHashMap<>(size);
            }
            default -> throw new IllegalArgumentException(impl);
        }
        for (int k = 0; k < size; k++) {
            long key;
            do {
                key = rand.nextLong();
            } while (contains(key));
            keys[k] = key;
            create(key);
        }
        // the cycle for chaseByKey: the balance of each key holds the next key (available + reserved)
        for (int k = 0; k < size; k++) {
            final long next = keys[k + 1 == size ? 0 : k + 1];
            setBalance(keys[k], next - 1, 1);
        }
        current = keys[0];
        shuffle(keys, rand);
    }

    private boolean contains(long key) {
        return switch (kind) {
            case RECORD_TABLE -> table.containsKey(key);
            case TWO_HASHTABLES -> available.containsKey(key);
            default -> map.containsKey(key);
        };
    }

    private void create(long key) {
        switch (kind) {
            case RECORD_TABLE -> table.getOrCreate(key);
            case TWO_HASHTABLES -> {
                available.put(key, 0);
                reserved.put(key, 0);
            }
            default -> map.put(key, new Balance());
        }
    }

    private void setBalance(long key, long a, long r) {
        switch (kind) {
            case RECORD_TABLE -> {
                final long h = table.find(key);
                table.set(h, AVAILABLE, a);
                table.set(h, RESERVED, r);
            }
            case TWO_HASHTABLES -> {
                available.put(key, a);
                reserved.put(key, r);
            }
            default -> {
                final Balance b = map.get(key);
                b.available = a;
                b.reserved = r;
            }
        }
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

    /** both fields of the key */
    @Benchmark
    public long readByKey() {
        final long key = keys[next()];
        switch (kind) {
            case RECORD_TABLE: {
                final long h = table.find(key);
                return table.get(h, AVAILABLE) + table.get(h, RESERVED);
            }
            case TWO_HASHTABLES:
                return available.get(key) + reserved.get(key);
            default: {
                final Balance b = map.get(key);
                return b.available + b.reserved;
            }
        }
    }

    /** a reservation: available down, reserved up */
    @Benchmark
    public long reserveByKey() {
        final long key = keys[next()];
        switch (kind) {
            case RECORD_TABLE: {
                final long h = table.find(key);
                table.addAndGet(h, AVAILABLE, -1);
                return table.addAndGet(h, RESERVED, 1);
            }
            case TWO_HASHTABLES:
                available.addAndGet(key, -1);
                return reserved.addAndGet(key, 1);
            default: {
                final Balance b = map.get(key);
                b.available--;
                return ++b.reserved;
            }
        }
    }

    /** the next key of the cycle: the latency of a lookup by key that reads both fields */
    @Benchmark
    public long chaseByKey() {
        switch (kind) {
            case RECORD_TABLE: {
                final long h = table.find(current);
                return current = table.get(h, AVAILABLE) + table.get(h, RESERVED);
            }
            case TWO_HASHTABLES:
                return current = available.get(current) + reserved.get(current);
            default: {
                final Balance b = map.get(current);
                return current = b.available + b.reserved;
            }
        }
    }

    /** a key removed, a new one added - the size stays */
    @Benchmark
    public long createRemove() {
        final int k = next();
        final long old = keys[k];
        long z = nextKeySeed += 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        final long key = z ^ (z >>> 27);
        keys[k] = key;
        switch (kind) {
            case RECORD_TABLE: {
                table.remove(old);
                return table.getOrCreate(key);
            }
            case TWO_HASHTABLES:
                available.remove(old);
                reserved.remove(old);
                available.put(key, 0);
                return reserved.put(key, 0);
            default: {
                final Balance b = map.remove(old);
                b.available = 0;
                b.reserved = 0;
                pool[poolSize++] = b;
                final Balance n = pool[--poolSize];
                pool[poolSize] = null;
                map.put(key, n);
                return n.available;
            }
        }
    }
}
