package exchange.core2.collections.art.testoracle;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.objpool.ObjectsPool;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.ToLongFunction;

import static org.junit.Assert.assertEquals;

/**
 * Random sequences of every mutating operation, each result compared with {@code TreeMap<Long, V>}: the previous
 * value of a put, the removed value, the neighbour of a fused put, the polled value, the size after each step.
 * <p>
 * All trials share one pool and every trial ends with a clear(), so a map keeps getting nodes another map has
 * recycled - a node that goes back to the pool dirty, or twice, breaks a later trial.
 */
@RunWith(JUnitQuickcheck.class)
public class ArtOperationsOracleTest {

    private static final int TRIALS = 200;

    private static final ObjectsPool SHARED_POOL = ObjectsPool.createDefaultTestPool();

    private static final long[] EXTREMES = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -256, -1, 0, 1, 255, 256,
            Long.MAX_VALUE - 1, Long.MAX_VALUE};

    /** a few hundred keys: leaves grow to Node256 and shrink back through every node type */
    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForNarrowKeys(long seed) {
        run(seed, r -> -300 + r.nextInt(601));
    }

    /** clusters far apart: deep path compression, branching on the sign bit and collapsing back */
    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForClusteredKeys(long seed) {
        run(seed, r -> ((long) (r.nextInt(4) - 2) << 40) + r.nextInt(3000));
    }

    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForAnyKeys(long seed) {
        run(seed, r -> r.nextInt(8) == 0 ? EXTREMES[r.nextInt(EXTREMES.length)] : r.nextLong());
    }

    private static void run(final long seed, final ToLongFunction<SplittableRandom> keys) {
        final SplittableRandom r = new SplittableRandom(seed);
        final LongAdaptiveRadixTreeMap<String> tested = new LongAdaptiveRadixTreeMap<>(SHARED_POOL);
        final TreeMap<Long, String> oracle = new TreeMap<>();

        final int operations = 1 + r.nextInt(2000);
        for (int i = 0; i < operations; i++) {
            // half of the time a stored key, so that removals and updates hit
            long key = keys.applyAsLong(r);
            if (!oracle.isEmpty() && r.nextBoolean()) {
                final Long stored = oracle.ceilingKey(key);
                key = stored != null ? stored : oracle.lastKey();
            }
            // distinct values - a wrong previous value or a wrong neighbour does not go unnoticed
            final String value = "v" + i;
            final String step = "op " + i + ", key " + key;

            final int op = r.nextInt(20);
            if (op < 6) {
                assertEquals(step + ": put", oracle.put(key, value), tested.put(key, value));
            } else if (op < 10) {
                assertEquals(step + ": remove", oracle.remove(key), tested.remove(key));
            } else if (op == 10) {
                assertEquals(step + ": putIfAbsent", oracle.putIfAbsent(key, value), tested.putIfAbsent(key, value));
            } else if (op == 11) {
                assertEquals(step + ": computeIfAbsent",
                        oracle.computeIfAbsent(key, k -> value), tested.computeIfAbsent(key, k -> value));
            } else if (op == 12) {
                oracle.put(key, value);
                assertEquals(step + ": putAndGetLowerValue",
                        valueOf(oracle.lowerEntry(key)), tested.putAndGetLowerValue(key, value));
            } else if (op == 13) {
                oracle.put(key, value);
                assertEquals(step + ": putAndGetHigherValue",
                        valueOf(oracle.higherEntry(key)), tested.putAndGetHigherValue(key, value));
            } else if (op == 14) {
                assertEquals(step + ": pollFirstValue", valueOf(oracle.pollFirstEntry()), tested.pollFirstValue());
            } else if (op == 15) {
                assertEquals(step + ": pollLastValue", valueOf(oracle.pollLastEntry()), tested.pollLastValue());
            } else if (op == 16 && r.nextInt(100) == 0) {
                oracle.clear();
                tested.clear();
            } else if (op == 17) {
                // mostly a few keys around the key, now and then a wide or an empty range
                final long span = r.nextInt(10) == 0 ? r.nextLong() >>> 1 : r.nextInt(300) - 20;
                final long to = key > Long.MAX_VALUE - span ? Long.MAX_VALUE : key + span;
                final Map<Long, String> range = key < to ? oracle.subMap(key, true, to, false) : Map.of();
                final int expected = range.size();
                if (key < to) {
                    range.clear();
                }
                assertEquals(step + ": removeRange to " + to, expected, tested.removeRange(key, to));
            } else {
                assertEquals(step + ": get", oracle.get(key), tested.get(key));
                assertEquals(step + ": getCeilingValue", valueOf(oracle.ceilingEntry(key)), tested.getCeilingValue(key));
                assertEquals(step + ": getFloorValue", valueOf(oracle.floorEntry(key)), tested.getFloorValue(key));
            }
            assertEquals(step + ": size", oracle.size(), tested.size());
            if ((i & 63) == 0) {
                tested.validateInternalState();
            }
        }

        ArtOracleAssertions.assertSameAsOracle(tested, oracle);
        tested.clear();
        tested.validateInternalState();
    }

    private static String valueOf(final Map.Entry<Long, String> entry) {
        return entry == null ? null : entry.getValue();
    }
}
