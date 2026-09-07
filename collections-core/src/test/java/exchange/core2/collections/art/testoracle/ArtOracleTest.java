package exchange.core2.collections.art.testoracle;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.TreeMap;

/**
 * Property-based oracle: {@link LongAdaptiveRadixTreeMap} must behave exactly like
 * {@code TreeMap<Long, V>} - including for negative keys.
 * <p>
 * Grown out of the test oracle contributed by mpawlucz in PR #1, which found the original bug.
 * That version compared iteration order only; it would not have caught {@code get()} returning null
 * for a key that is present, nor the broken floor/ceiling lookups. So every scenario now checks the
 * whole read surface against the oracle, plus the map's own internal-state validation.
 */
@RunWith(JUnitQuickcheck.class)
public class ArtOracleTest {

    /** kept modest so a plain `mvn test` stays fast; crank it up when hunting */
    private static final int TRIALS = 200;

    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForMixedSignKeys(List<Long> keys) {
        assertBehavesLikeTreeMap(keys);
    }

    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForNegativeKeys(List<@InRange(max = "0") Long> keys) {
        assertBehavesLikeTreeMap(keys);
    }

    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForPositiveKeys(List<@InRange(min = "0") Long> keys) {
        assertBehavesLikeTreeMap(keys);
    }

    /**
     * Keys packed into a narrow band share every high byte, so the tree is forced to branch deep -
     * a different set of node splits than uniformly random keys exercise.
     */
    @Property(trials = TRIALS)
    public void shouldMatchTreeMapForKeysAroundZero(List<@InRange(min = "-64", max = "64") Long> keys) {
        assertBehavesLikeTreeMap(keys);
    }

    static void assertBehavesLikeTreeMap(List<Long> keys) {

        final LongAdaptiveRadixTreeMap<String> tested = new LongAdaptiveRadixTreeMap<>();
        final TreeMap<Long, String> oracle = new TreeMap<>();

        for (Long key : keys) {
            final String value = String.valueOf(key);
            oracle.put(key, value);
            tested.put(key, value);
        }

        ArtOracleAssertions.assertSameAsOracle(tested, oracle);
    }
}
