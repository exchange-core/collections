package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.LongTupleHashtable;
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
import java.util.concurrent.TimeUnit;

/**
 * Balances (available, reserved) under a key of three longs - a user UUID (two longs), a token and a subaccount
 * (one) - in a {@link LongTupleHashtable} against a {@code HashMap} of a {@code record} key to an object per balance,
 * what a key of three longs gets without a primitive table. The HashMap lookups use key objects made in advance (no
 * allocation per lookup - its best case); a new key of createRemove is a new object, as it would be.
 * <p>
 * The keys: users with a UUIDv7-like high half (a time prefix) and a random low half, 4 tokens and 3 subaccounts
 * each; added in one random order, reached in another through an array read in sequence. From L2-resident (12k) to
 * DRAM (12M):
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main LongTupleHashtableJmh -rf json -rff out.json
 * </pre>
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class LongTupleHashtableJmh {

    @Param({"LongTupleHashtable", "HashMap"})
    public String impl;

    /** keys: 12000 (1000 users), 1200000, 12000000 */
    @Param({"12000", "1200000", "12000000"})
    public int size;

    private static final int AVAILABLE = 0;
    private static final int RESERVED = 1;

    record Key(long userHi, long userLo, long tokenSub) {
    }

    static final class Balance {
        long available;
        long reserved;
    }

    private boolean tupleImpl;
    private LongTupleHashtable tuples;
    private HashMap<Key, Balance> map;

    /** the keys in a random order: three longs each, and the same as objects for the HashMap */
    private long[] hi;
    private long[] lo;
    private long[] tokenSub;
    private Key[] keys;
    private int i;
    private long nextKeySeed = 0x9e3779b97f4a7c15L;

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        tupleImpl = switch (impl) {
            case "LongTupleHashtable" -> true;
            case "HashMap" -> false;
            default -> throw new IllegalArgumentException(impl);
        };
        final SplittableRandom rand = new SplittableRandom(42L);
        final long[] userHi = new long[size];
        final long[] userLo = new long[size];
        final long[] userTokenSub = new long[size];
        int k = 0;
        for (long user = 0; k < size; user++) {
            final long h = 0x0190_0000_0000_7000L + (user << 16);
            final long l = rand.nextLong();
            for (long token = 1; token <= 4 && k < size; token++) {
                for (long sub = 0; sub < 3 && k < size; sub++, k++) {
                    userHi[k] = h;
                    userLo[k] = l;
                    userTokenSub[k] = token << 8 | sub;
                }
            }
        }
        // added in one random order, reached in another: the objects of the HashMap lie in memory in the order they
        // were added - reached in that order, the next one would be fetched in advance
        final int[] added = permutation(rand);
        final int[] reached = permutation(rand);
        hi = new long[size];
        lo = new long[size];
        tokenSub = new long[size];
        for (int j = 0; j < size; j++) {
            hi[j] = userHi[reached[j]];
            lo[j] = userLo[reached[j]];
            tokenSub[j] = userTokenSub[reached[j]];
        }
        if (tupleImpl) {
            tuples = LongTupleHashtable.builder().keyLongs(3).valueLongs(2).expectedSize(size).build();
            for (int j : added) {
                final int pos = tuples.getOrInsert(userHi[j], userLo[j], userTokenSub[j]);
                tuples.setValue(pos, AVAILABLE, 1_000_000_000L);
            }
        } else {
            map = new HashMap<>((int) (size / 0.75f) + 1);
            for (int j : added) {
                final Balance b = new Balance();
                b.available = 1_000_000_000L;
                map.put(new Key(userHi[j], userLo[j], userTokenSub[j]), b);
            }
            // the keys of the lookups: objects of their own, made in the order of the lookups - as a request would
            // bring its key
            keys = new Key[size];
            for (int j = 0; j < size; j++) {
                keys[j] = new Key(hi[j], lo[j], tokenSub[j]);
            }
        }
    }

    private int[] permutation(SplittableRandom rand) {
        final int[] p = new int[size];
        for (int j = 0; j < size; j++) {
            p[j] = j;
        }
        for (int j = size - 1; j > 0; j--) {
            final int r = rand.nextInt(j + 1);
            final int t = p[j];
            p[j] = p[r];
            p[r] = t;
        }
        return p;
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
        final int k = next();
        if (tupleImpl) {
            final int pos = tuples.find(hi[k], lo[k], tokenSub[k]);
            return tuples.getValue(pos, AVAILABLE) + tuples.getValue(pos, RESERVED);
        }
        final Balance b = map.get(keys[k]);
        return b.available + b.reserved;
    }

    /** a reservation: available down, reserved up */
    @Benchmark
    public long reserveByKey() {
        final int k = next();
        if (tupleImpl) {
            final int pos = tuples.find(hi[k], lo[k], tokenSub[k]);
            tuples.addAndGetValue(pos, AVAILABLE, -1);
            return tuples.addAndGetValue(pos, RESERVED, 1);
        }
        final Balance b = map.get(keys[k]);
        b.available--;
        return ++b.reserved;
    }

    /** a key removed, a new one added (a new user's balance) - the size stays */
    @Benchmark
    public long createRemove() {
        final int k = next();
        long z = nextKeySeed += 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        final long newLo = z ^ (z >>> 27);
        if (tupleImpl) {
            tuples.remove(hi[k], lo[k], tokenSub[k]);
            lo[k] = newLo;
            return tuples.getOrInsert(hi[k], newLo, tokenSub[k]);
        }
        final Balance b = map.remove(keys[k]);
        b.available = 0;
        b.reserved = 0;
        keys[k] = new Key(hi[k], newLo, tokenSub[k]);
        map.put(keys[k], b);
        return b.available;
    }
}
