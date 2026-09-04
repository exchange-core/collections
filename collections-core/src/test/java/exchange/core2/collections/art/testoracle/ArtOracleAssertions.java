package exchange.core2.collections.art.testoracle;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

/**
 * Compares the whole read surface of {@link LongAdaptiveRadixTreeMap} against a {@link TreeMap}.
 */
final class ArtOracleAssertions {

    private ArtOracleAssertions() {
    }

    static void assertSameAsOracle(final LongAdaptiveRadixTreeMap<String> tested,
                                   final TreeMap<Long, String> oracle) {

        // the map knows its own invariants - ask it first, it gives the sharpest error
        tested.validateInternalState();

        // 1. ordered iteration
        final List<Long> expectedKeys = new ArrayList<>(oracle.keySet());
        final List<Long> actualKeys = new ArrayList<>();
        final List<String> actualValues = new ArrayList<>();
        for (Map.Entry<Long, String> e : tested.entriesList()) {
            actualKeys.add(e.getKey());
            actualValues.add(e.getValue());
        }
        assertEquals("entriesList() key order", expectedKeys, actualKeys);
        assertEquals("entriesList() values", new ArrayList<>(oracle.values()), actualValues);

        // 2. forEach must agree with entriesList, ascending and descending
        final List<Long> viaForEach = new ArrayList<>();
        tested.forEach((k, v) -> viaForEach.add(k), Integer.MAX_VALUE);
        assertEquals("forEach() order", expectedKeys, viaForEach);

        final List<Long> viaForEachDesc = new ArrayList<>();
        tested.forEachDesc((k, v) -> viaForEachDesc.add(k), Integer.MAX_VALUE);
        final List<Long> expectedDesc = new ArrayList<>(oracle.descendingKeySet());
        assertEquals("forEachDesc() order", expectedDesc, viaForEachDesc);

        assertEquals("size()", oracle.size(), tested.size(Integer.MAX_VALUE));

        // 3. every stored key must be retrievable - ordering can be right while get() is broken
        for (Map.Entry<Long, String> e : oracle.entrySet()) {
            assertEquals("get(" + e.getKey() + ")", e.getValue(), tested.get(e.getKey()));
        }

        // 4. ordered lookups around every stored key, plus the range boundaries
        for (final Long key : probePoints(oracle)) {
            assertEquals("getHigherValue(" + key + ")",
                    valueOf(oracle.higherEntry(key)), tested.getHigherValue(key));
            assertEquals("getLowerValue(" + key + ")",
                    valueOf(oracle.lowerEntry(key)), tested.getLowerValue(key));
        }
    }

    /** stored keys and their immediate neighbours, plus the extremes of the signed range */
    private static List<Long> probePoints(final TreeMap<Long, String> oracle) {
        final List<Long> probes = new ArrayList<>();
        probes.add(Long.MIN_VALUE);
        probes.add(Long.MAX_VALUE);
        probes.add(0L);
        probes.add(-1L);
        for (final Long key : oracle.keySet()) {
            probes.add(key);
            if (key != Long.MIN_VALUE) {
                probes.add(key - 1);
            }
            if (key != Long.MAX_VALUE) {
                probes.add(key + 1);
            }
        }
        return probes;
    }

    private static String valueOf(final Map.Entry<Long, String> entry) {
        return entry == null ? null : entry.getValue();
    }
}
