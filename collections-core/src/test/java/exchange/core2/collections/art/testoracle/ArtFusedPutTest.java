package exchange.core2.collections.art.testoracle;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;

/**
 * putAndGetLowerValue / putAndGetHigherValue find the neighbour on the way down of the put: every node type offers
 * its closest child, a leaf and a compressed path offer themselves. Each probe - a stored key, its neighbours, a
 * random key, the ends of the range - is put and compared with TreeMap, then the map is restored.
 */
public class ArtFusedPutTest {

    @Test
    public void shouldFindNeighboursForNarrowKeys() {
        final SplittableRandom random = new SplittableRandom(1);
        for (int size : new int[]{1, 2, 3, 5, 17, 49, 200}) {
            check(size, () -> -300 + random.nextInt(601), random);
        }
    }

    @Test
    public void shouldFindNeighboursForClusteredKeys() {
        final SplittableRandom random = new SplittableRandom(2);
        for (int size : new int[]{2, 10, 100, 1000}) {
            check(size, () -> ((long) (random.nextInt(4) - 2) << 40) + random.nextInt(3000), random);
        }
    }

    @Test
    public void shouldFindNeighboursForAnyKeys() {
        final SplittableRandom random = new SplittableRandom(3);
        for (int size : new int[]{1, 2, 10, 500}) {
            check(size, random::nextLong, random);
        }
    }

    private static void check(final int size, final LongSupplier keys, final SplittableRandom random) {
        final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        final TreeMap<Long, String> oracle = new TreeMap<>();
        while (oracle.size() < size) {
            final long key = keys.getAsLong();
            map.put(key, "v" + key);
            oracle.put(key, "v" + key);
        }
        final List<Long> probes = new ArrayList<>(List.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L,
                Long.MAX_VALUE - 1, Long.MAX_VALUE));
        for (long key : oracle.keySet()) {
            probes.add(key);
            probes.add(key - 1);
            probes.add(key + 1);
        }
        for (int i = 0; i < 50; i++) {
            probes.add(keys.getAsLong());
            probes.add(random.nextLong());
        }
        for (long probe : probes) {
            final String step = "size " + size + ", probe " + probe;
            final String old = oracle.get(probe);

            assertEquals(step + ": lower", valueOf(oracle.lowerEntry(probe)), map.putAndGetLowerValue(probe, "p"));
            restore(map, probe, old);
            assertEquals(step + ": higher", valueOf(oracle.higherEntry(probe)), map.putAndGetHigherValue(probe, "p"));
            restore(map, probe, old);
        }
        map.validateInternalState();
        ArtOracleAssertions.assertSameAsOracle(map, oracle);
    }

    private static void restore(final LongAdaptiveRadixTreeMap<String> map, final long key, final String old) {
        if (old == null) {
            map.remove(key);
        } else {
            map.put(key, old);
        }
    }

    private static String valueOf(final Map.Entry<Long, String> entry) {
        return entry == null ? null : entry.getValue();
    }
}
