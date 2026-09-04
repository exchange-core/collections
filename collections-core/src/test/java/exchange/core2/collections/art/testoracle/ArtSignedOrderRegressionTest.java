package exchange.core2.collections.art.testoracle;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * Fixed regression cases for the signed-key bugs. These are deterministic, so a failure here always
 * reproduces - unlike the property-based {@link ArtOracleTest}, which needs its seed to be known.
 * <p>
 * Two separate defects were behind them:
 * <ol>
 *   <li>{@code ArtNode4.initTwoKeys} ordered the two children by comparing the FULL keys with a
 *       signed comparison, while the node array is ordered by branch index. Whenever the split
 *       happened on the sign bit the children ended up swapped, which left the node unsorted -
 *       so iteration order was wrong AND {@code get()} returned null for keys that were present,
 *       because the search gives up as soon as it passes the index it is looking for.</li>
 *   <li>The nodes then ordered keys as unsigned (as a radix tree must), so every negative key
 *       sorted after every positive one. The map now flips the sign bit at its API boundary.</li>
 * </ol>
 */
public class ArtSignedOrderRegressionTest {

    /** the case from PR #1 - one negative key among positive ones */
    @Test
    public void shouldMatchTreeMapForTheOriginalReportedCase() {
        assertSameAsTreeMap(-4429196230935817217L, 2967034165565055791L,
                3969304394476946051L, 5502623383577302445L);
    }

    @Test
    public void shouldMatchTreeMapForSmallMixedSignKeys() {
        assertSameAsTreeMap(-5, -1, 0, 1, 5);
    }

    @Test
    public void shouldMatchTreeMapForRangeExtremes() {
        assertSameAsTreeMap(Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE);
    }

    @Test
    public void shouldMatchTreeMapForNegativeKeysOnly() {
        assertSameAsTreeMap(-100, -50, -3, -2, -1);
    }

    /**
     * The children of a node are built by a split, so which keys collide in a node - and therefore
     * whether a broken split is reachable at all - depends on the insertion sequence. The original
     * bug showed up in 108 of these 120 orders and hid in the other 12.
     */
    @Test
    public void shouldNotDependOnInsertionOrder() {
        final long[] keys = {-5, -1, 0, 1, 5};
        for (long[] permutation : permutations(keys)) {
            assertSameAsTreeMap(permutation);
        }
    }

    @Test
    public void shouldMatchTreeMapForRandomKeys() {
        final Random rand = new Random(4711L);
        final long[] keys = new long[500];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rand.nextLong();
        }
        assertSameAsTreeMap(keys);
    }

    private static void assertSameAsTreeMap(final long... keys) {

        final LongAdaptiveRadixTreeMap<String> tested = new LongAdaptiveRadixTreeMap<>();
        final TreeMap<Long, String> oracle = new TreeMap<>();

        for (final long key : keys) {
            final String value = String.valueOf(key);
            tested.put(key, value);
            oracle.put(key, value);
        }

        ArtOracleAssertions.assertSameAsOracle(tested, oracle);
    }

    private static List<long[]> permutations(final long[] keys) {
        final List<long[]> result = new ArrayList<>();
        permute(Arrays.copyOf(keys, keys.length), 0, result);
        return result;
    }

    private static void permute(final long[] arr, final int k, final List<long[]> out) {
        if (k == arr.length) {
            out.add(Arrays.copyOf(arr, arr.length));
            return;
        }
        for (int i = k; i < arr.length; i++) {
            swap(arr, k, i);
            permute(arr, k + 1, out);
            swap(arr, k, i);
        }
    }

    private static void swap(final long[] arr, final int a, final int b) {
        final long t = arr[a];
        arr[a] = arr[b];
        arr[b] = t;
    }
}
