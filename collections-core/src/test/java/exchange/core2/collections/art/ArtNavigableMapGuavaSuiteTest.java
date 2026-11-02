package exchange.core2.collections.art;

import com.google.common.collect.testing.Helpers;
import com.google.common.collect.testing.NavigableMapTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestSortedMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import junit.framework.Test;
import junit.framework.TestFailure;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertTrue;

/**
 * The {@code java.util} contract of {@link LongAdaptiveRadixTreeMap#asNavigableMap()}: Guava's NavigableMap suite, with
 * the suites it derives - descending map, sub-maps with every kind of bound, key sets, entry set, values. Two sets of
 * keys: far apart (leaves under inner nodes, the bounds at the ends of the long range) and next to each other (one node
 * of the last level).
 * <p>
 * The suite is some 64 thousand tests and runs in seconds, but surefire reports each of them as an event and takes
 * minutes - hence one test that runs the suite and lists what failed.
 */
public class ArtNavigableMapGuavaSuiteTest {

    private static final int FAILURES_SHOWN = 20;

    @org.junit.Test
    public void shouldPassGuavaNavigableMapSuite() {
        final TestSuite suite = new TestSuite("LongAdaptiveRadixTreeMap.asNavigableMap");
        suite.addTest(suite("sparse keys", new Generator(
                new long[]{-1_000_000_000_000L, -1, 0, 255, 1L << 40},
                Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE - 1, Long.MAX_VALUE)));
        suite.addTest(suite("dense keys", new Generator(
                new long[]{10, 11, 12, 13, 14},
                8, 9, 15, 16)));

        final TestResult result = new TestResult();
        suite.run(result);

        final List<TestFailure> failed = new ArrayList<>(Collections.list(result.errors()));
        failed.addAll(Collections.list(result.failures()));
        final StringBuilder sb = new StringBuilder();
        failed.stream().limit(FAILURES_SHOWN).forEach(f -> sb.append('\n').append(f.failedTest()).append(": ")
                .append(f.thrownException()));
        assertTrue(failed.size() + " of " + result.runCount() + " failed:" + sb, failed.isEmpty());
        assertTrue("the suite ran " + result.runCount() + " tests", result.runCount() > 10_000);
    }

    private static Test suite(final String name, final Generator generator) {
        return NavigableMapTestSuiteBuilder.using(generator)
                .named(name)
                .withFeatures(
                        MapFeature.GENERAL_PURPOSE,
                        MapFeature.FAILS_FAST_ON_CONCURRENT_MODIFICATION,
                        CollectionFeature.SUPPORTS_ITERATOR_REMOVE,
                        CollectionFeature.KNOWN_ORDER,
                        CollectionSize.ANY)
                .createTestSuite();
    }

    private static final class Generator implements TestSortedMapGenerator<Long, String> {

        private final long[] samples;
        private final long belowLesser;
        private final long belowGreater;
        private final long aboveLesser;
        private final long aboveGreater;

        Generator(final long[] samples,
                  final long belowLesser,
                  final long belowGreater,
                  final long aboveLesser,
                  final long aboveGreater) {
            this.samples = samples;
            this.belowLesser = belowLesser;
            this.belowGreater = belowGreater;
            this.aboveLesser = aboveLesser;
            this.aboveGreater = aboveGreater;
        }

        @Override
        public SampleElements<Map.Entry<Long, String>> samples() {
            return new SampleElements<>(
                    entry(samples[0]), entry(samples[1]), entry(samples[2]), entry(samples[3]), entry(samples[4]));
        }

        @Override
        @SuppressWarnings("unchecked")
        public NavigableMap<Long, String> create(final Object... entries) {
            // a small pool of its own: the suites make thousands of maps
            final NavigableMap<Long, String> map = LongAdaptiveRadixTreeMap.builder().poolCapacity(16)
                    .<String>build()
                    .asNavigableMap();
            for (final Object o : entries) {
                final Map.Entry<Long, String> e = (Map.Entry<Long, String>) o;
                map.put(e.getKey(), e.getValue());
            }
            return map;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map.Entry<Long, String>[] createArray(final int length) {
            return new Map.Entry[length];
        }

        @Override
        public Iterable<Map.Entry<Long, String>> order(final List<Map.Entry<Long, String>> insertionOrder) {
            final List<Map.Entry<Long, String>> sorted = new ArrayList<>(insertionOrder);
            sorted.sort(Map.Entry.comparingByKey());
            return sorted;
        }

        @Override
        public Long[] createKeyArray(final int length) {
            return new Long[length];
        }

        @Override
        public String[] createValueArray(final int length) {
            return new String[length];
        }

        @Override
        public Map.Entry<Long, String> belowSamplesLesser() {
            return entry(belowLesser);
        }

        @Override
        public Map.Entry<Long, String> belowSamplesGreater() {
            return entry(belowGreater);
        }

        @Override
        public Map.Entry<Long, String> aboveSamplesLesser() {
            return entry(aboveLesser);
        }

        @Override
        public Map.Entry<Long, String> aboveSamplesGreater() {
            return entry(aboveGreater);
        }

        private static Map.Entry<Long, String> entry(final long key) {
            return Helpers.mapEntry(key, "v" + key);
        }
    }
}
