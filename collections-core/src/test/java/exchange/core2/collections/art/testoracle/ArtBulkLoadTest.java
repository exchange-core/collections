package exchange.core2.collections.art.testoracle;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.objpool.ObjectsPool;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.ToLongFunction;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * putAllSorted against TreeMap, and against a map of the same keys made by puts: the same tree, node for node - a
 * put-only map has every node of the smallest size for its children, the bulk load builds them so. Then the map has
 * to go on working: random puts and removals, compared with the oracle.
 * <p>
 * All trials share one pool and end with a clear(): the bulk load gets nodes that grew, shrank and were recycled in
 * the trials before.
 */
@RunWith(JUnitQuickcheck.class)
public class ArtBulkLoadTest {

    private static final int TRIALS = 200;

    private static final ObjectsPool SHARED_POOL = ObjectsPool.forArt(1024);

    private static final long[] EXTREMES = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -256, -1, 0, 1, 255, 256,
            Long.MAX_VALUE - 1, Long.MAX_VALUE};

    @Property(trials = TRIALS)
    public void shouldMatchPutsForNarrowKeys(long seed) {
        run(seed, r -> -300 + r.nextInt(601));
    }

    @Property(trials = TRIALS)
    public void shouldMatchPutsForClusteredKeys(long seed) {
        run(seed, r -> ((long) (r.nextInt(4) - 2) << 40) + r.nextInt(3000));
    }

    @Property(trials = TRIALS)
    public void shouldMatchPutsForAnyKeys(long seed) {
        run(seed, r -> r.nextInt(8) == 0 ? EXTREMES[r.nextInt(EXTREMES.length)] : r.nextLong());
    }

    /** 100k keys: Node256 under Node256 - the oracle compares their entries only, its full check is quadratic */
    @Test
    public void shouldBuildDenseAndSparseRuns() {
        for (final long step : new long[]{1, 3, 255, 256, 257, 1L << 40}) {
            final long[] keys = new long[100_000];
            final String[] values = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = -50_000 * step + i * step;
                values[i] = "v" + keys[i];
            }
            final LongAdaptiveRadixTreeMap<String> loaded = new LongAdaptiveRadixTreeMap<>(SHARED_POOL);
            loaded.putAllSorted(keys, values);
            loaded.validateInternalState();
            assertArrayEquals("step " + step, keys, loaded.keysStream().toArray());
            assertArrayEquals("step " + step, values, loaded.valuesStream().toArray());
            assertEquals(keys.length, loaded.size());

            final LongAdaptiveRadixTreeMap<String> byPuts = new LongAdaptiveRadixTreeMap<>();
            for (int i = 0; i < keys.length; i++) {
                byPuts.put(keys[i], values[i]);
            }
            assertEquals("the tree of the puts, step " + step, byPuts.printDiagram(), loaded.printDiagram());
            loaded.clear();
        }
    }

    @Test
    public void shouldRejectWrongInputLeavingTheMapAsItWas() {
        final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        assertThrows(IllegalArgumentException.class, () -> map.putAllSorted(new long[]{1, 3, 2}, new String[]{"a", "b", "c"}));
        assertThrows(IllegalArgumentException.class, () -> map.putAllSorted(new long[]{1, 2, 2}, new String[]{"a", "b", "c"}));
        assertThrows(IllegalArgumentException.class, () -> map.putAllSorted(new long[]{1, 2}, new String[]{"a"}));
        assertThrows(NullPointerException.class, () -> map.putAllSorted(new long[]{1, 2}, new String[]{"a", null}));
        assertTrue(map.isEmpty());

        map.putAllSorted(new long[0], new String[0]);
        assertTrue(map.isEmpty());
        map.validateInternalState();
    }

    @Test
    public void shouldPutOneByOneIntoAMapThatIsNotEmpty() {
        final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        map.put(5, "old");
        map.put(-7, "kept");
        map.putAllSorted(new long[]{-1000, 5, 1L << 50}, new String[]{"a", "new", "b"});
        assertEquals("{-1000=a, -7=kept, 5=new, 1125899906842624=b}", map.toString());
        assertEquals(4, map.size());
        map.validateInternalState();
    }

    private static void run(final long seed, final ToLongFunction<SplittableRandom> keySource) {
        final SplittableRandom r = new SplittableRandom(seed);
        final TreeMap<Long, String> oracle = new TreeMap<>();
        // up to 600 keys - the full oracle check is quadratic; the big trees are in shouldBuildDenseAndSparseRuns
        final int count = r.nextInt(8) == 0 ? r.nextInt(3) : 1 + r.nextInt(600);
        for (int i = 0; i < count; i++) {
            final long key = keySource.applyAsLong(r);
            oracle.put(key, "v" + key);
        }
        final long[] keys = oracle.keySet().stream().mapToLong(Long::longValue).toArray();
        final LongAdaptiveRadixTreeMap<String> loaded = assertBulkLoadMatches(keys, oracle);

        // the tree it built must work as one made by puts
        for (int i = 0; i < 500; i++) {
            final long key = r.nextBoolean() && keys.length > 0
                    ? keys[r.nextInt(keys.length)]
                    : keySource.applyAsLong(r);
            if (r.nextInt(3) == 0) {
                assertEquals("put " + key, oracle.put(key, "w" + key), loaded.put(key, "w" + key));
            } else {
                assertEquals("remove " + key, oracle.remove(key), loaded.remove(key));
            }
        }
        ArtOracleAssertions.assertSameAsOracle(loaded, oracle);
        loaded.clear();
    }

    private static LongAdaptiveRadixTreeMap<String> assertBulkLoadMatches(final long[] keys,
                                                                          final TreeMap<Long, String> oracle) {
        final String[] values = oracle.values().toArray(new String[0]);
        final LongAdaptiveRadixTreeMap<String> loaded = new LongAdaptiveRadixTreeMap<>(SHARED_POOL);
        loaded.putAllSorted(keys, values);
        ArtOracleAssertions.assertSameAsOracle(loaded, oracle);

        final LongAdaptiveRadixTreeMap<String> byPuts = new LongAdaptiveRadixTreeMap<>();
        for (int i = 0; i < keys.length; i++) {
            byPuts.put(keys[i], values[i]);
        }
        assertEquals("the tree of the puts", byPuts.printDiagram(), loaded.printDiagram());
        return loaded;
    }
}
