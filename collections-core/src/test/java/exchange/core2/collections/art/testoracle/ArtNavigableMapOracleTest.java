package exchange.core2.collections.art.testoracle;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.objpool.ObjectsPool;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link LongAdaptiveRadixTreeMap#asNavigableMap()} against {@code TreeMap<Long, V>}: the same chain of views
 * (descending, sub-, head-, tail-maps with random bounds, nested, out of range ones included) on both, then random
 * reads and writes through it - each result or exception compared, and the whole maps after the trial.
 * <p>
 * The keys are narrow, so the bounds cut through the nodes, plus the ends of the long range, where an exclusive bound
 * leaves nothing beyond it.
 */
@RunWith(JUnitQuickcheck.class)
public class ArtNavigableMapOracleTest {

    private static final int TRIALS = 300;

    private static final ObjectsPool SHARED_POOL = ObjectsPool.createDefaultTestPool();

    private static final long[] EXTREMES = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -256, -1, 0, 1, 255, 256,
            Long.MAX_VALUE - 1, Long.MAX_VALUE};

    @Property(trials = TRIALS)
    public void viewsShouldMatchTreeMap(long seed) {
        final SplittableRandom r = new SplittableRandom(seed);
        final LongAdaptiveRadixTreeMap<String> art = new LongAdaptiveRadixTreeMap<>(SHARED_POOL);
        final TreeMap<Long, String> oracle = new TreeMap<>();
        for (int i = r.nextInt(400); i > 0; i--) {
            final long key = key(r);
            art.put(key, "v" + key);
            oracle.put(key, "v" + key);
        }

        // the same chain of views on both; a view the oracle refuses, the tested map must refuse the same way
        NavigableMap<Long, String> tested = art.asNavigableMap();
        NavigableMap<Long, String> expected = oracle;
        final StringBuilder chain = new StringBuilder("seed " + seed + ": map");
        for (int depth = r.nextInt(5); depth > 0; depth--) {
            final Function<NavigableMap<Long, String>, NavigableMap<Long, String>> view = randomView(r, chain);
            final NavigableMap<Long, String> e = expected;
            final NavigableMap<Long, String> t = tested;
            final Object ev = outcome(() -> view.apply(e));
            final Object tv = outcome(() -> view.apply(t));
            if (ev instanceof Class<?>) {
                assertEquals(chain.toString(), ev, tv);
                chain.append(" refused");
                break;
            }
            assertTrue(chain + ": a view, not " + tv, tv instanceof NavigableMap);
            expected = cast(ev);
            tested = cast(tv);
        }

        for (int i = r.nextInt(300); i >= 0; i--) {
            step(r, expected, tested, chain + ", op " + i, i);
        }

        assertEquals(chain.toString(), oracle, art.asNavigableMap());
        assertEquals(chain.toString(), oracle.toString(), art.toString());
        art.validateInternalState();
        art.clear();
    }

    private static long key(final SplittableRandom r) {
        return r.nextInt(8) == 0 ? EXTREMES[r.nextInt(EXTREMES.length)] : -300 + r.nextInt(601);
    }

    private static Function<NavigableMap<Long, String>, NavigableMap<Long, String>> randomView(
            final SplittableRandom r, final StringBuilder chain) {

        final long a = key(r);
        final long b = key(r);
        final boolean ai = r.nextBoolean();
        final boolean bi = r.nextBoolean();
        switch (r.nextInt(7)) {
            case 0 -> {
                chain.append(".descendingMap()");
                return NavigableMap::descendingMap;
            }
            case 1 -> {
                // the bounds in the order of the view, so that more chains go deep
                final long lo = Math.min(a, b);
                final long hi = Math.max(a, b);
                chain.append(".subMap(in order: ").append(lo).append(", ").append(ai).append(", ").append(hi)
                        .append(", ").append(bi).append(")");
                return m -> m.comparator() == null ? m.subMap(lo, ai, hi, bi) : m.subMap(hi, ai, lo, bi);
            }
            case 2 -> {
                chain.append(".subMap(").append(a).append(", ").append(ai).append(", ").append(b).append(", ")
                        .append(bi).append(")");
                return m -> m.subMap(a, ai, b, bi);
            }
            case 3 -> {
                chain.append(".headMap(").append(a).append(", ").append(ai).append(")");
                return m -> m.headMap(a, ai);
            }
            case 4 -> {
                chain.append(".tailMap(").append(a).append(", ").append(ai).append(")");
                return m -> m.tailMap(a, ai);
            }
            case 5 -> {
                chain.append(".headMap(").append(a).append(")");
                return m -> cast(m.headMap(a));
            }
            default -> {
                chain.append(".tailMap(").append(a).append(")");
                return m -> cast(m.tailMap(a));
            }
        }
    }

    /** one random operation through both views, the results compared */
    private static void step(final SplittableRandom r,
                             final NavigableMap<Long, String> expected,
                             final NavigableMap<Long, String> tested,
                             final String at,
                             final int i) {

        // half of the time a key of the view, so that removals and updates hit
        long k = key(r);
        if (r.nextBoolean()) {
            final Long near = expected.isEmpty() ? null : expected.ceilingKey(k);
            if (near != null) {
                k = near;
            }
        }
        final long key = k;
        final String value = "v" + i + "_" + key;
        final int op = r.nextInt(30);
        final String step = at + ", key " + key + ", op#" + op;
        switch (op) {
            case 0 -> same(step, expected, tested, m -> m.get(key));
            case 1 -> same(step, expected, tested, m -> m.containsKey(key));
            case 2, 3, 4 -> same(step, expected, tested, m -> m.put(key, value));
            case 5, 6 -> same(step, expected, tested, m -> m.remove(key));
            case 7 -> same(step, expected, tested, m -> m.ceilingEntry(key));
            case 8 -> same(step, expected, tested, m -> m.floorKey(key));
            case 9 -> same(step, expected, tested, m -> m.higherEntry(key));
            case 10 -> same(step, expected, tested, m -> m.lowerKey(key));
            case 11 -> same(step, expected, tested, m -> m.firstEntry() + " " + m.lastEntry());
            case 12 -> same(step, expected, tested, m -> m.firstKey());
            case 13 -> same(step, expected, tested, m -> m.lastKey());
            case 14 -> same(step, expected, tested, m -> m.pollFirstEntry());
            case 15 -> same(step, expected, tested, m -> m.pollLastEntry());
            case 16 -> same(step, expected, tested, m -> m.size() + " " + m.isEmpty());
            case 17 -> same(step, expected, tested, m -> new ArrayList<>(m.entrySet()));
            case 18 -> same(step, expected, tested, m -> new ArrayList<>(m.descendingKeySet()));
            case 19 -> same(step, expected, tested, m -> new ArrayList<>(m.values()));
            case 20 -> same(step, expected, tested, m -> m.toString() + " " + m.hashCode() + " " + m.equals(expected));
            case 21 -> {
                // the iterator removes and sets values on the way
                final int every = 2 + r.nextInt(3);
                same(step, expected, tested, m -> {
                    final ArrayList<Object> seen = new ArrayList<>();
                    int n = 0;
                    for (Iterator<Map.Entry<Long, String>> it = m.entrySet().iterator(); it.hasNext(); n++) {
                        final Map.Entry<Long, String> e = it.next();
                        seen.add(e.getKey());
                        if (n % every == 0) {
                            it.remove();
                        } else if (n % every == 1) {
                            seen.add(e.setValue(value));
                        }
                    }
                    return seen;
                });
            }
            case 22 -> same(step, expected, tested, m -> m.navigableKeySet().removeIf(x -> Long.hashCode(x) % 3 == 0));
            case 23 -> same(step, expected, tested, m -> new ArrayList<>(m.navigableKeySet().headSet(key, true)));
            case 24 -> same(step, expected, tested, m -> new ArrayList<>(m.navigableKeySet().descendingSet()
                    .tailSet(key, false)));
            case 25 -> same(step, expected, tested, m -> m.merge(key, value, String::concat));
            case 26 -> same(step, expected, tested, m -> m.entrySet().remove(Map.entry(key, "v" + key)));
            case 27 -> same(step, expected, tested, m -> m.subMap(key, true, key, true).size());
            case 28 -> same(step, expected, tested, m -> m.descendingMap().firstEntry());
            default -> {
                if (r.nextInt(10) == 0) {
                    same(step, expected, tested, m -> {
                        m.clear();
                        return m.size();
                    });
                }
            }
        }
    }

    private static void same(final String step,
                             final NavigableMap<Long, String> expected,
                             final NavigableMap<Long, String> tested,
                             final Function<NavigableMap<Long, String>, Object> op) {
        assertEquals(step, outcome(() -> op.apply(expected)), outcome(() -> op.apply(tested)));
    }

    /** the result, or the class of the exception */
    private static Object outcome(final Supplier<Object> op) {
        try {
            return op.get();
        } catch (RuntimeException e) {
            return e.getClass();
        }
    }

    @SuppressWarnings("unchecked")
    private static NavigableMap<Long, String> cast(final Object o) {
        return (NavigableMap<Long, String>) o;
    }
}
