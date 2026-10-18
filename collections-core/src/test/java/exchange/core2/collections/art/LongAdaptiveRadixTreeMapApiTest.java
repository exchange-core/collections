package exchange.core2.collections.art;

import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Contracts of the single operations, and the allocations the pool is there to avoid.
 * Sequences of operations against TreeMap: {@code testoracle.ArtOperationsOracleTest}.
 */
public class LongAdaptiveRadixTreeMapApiTest {

    private final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();

    @Test
    public void putAndRemoveShouldReturnPreviousValues() {
        assertNull(map.put(5, "a"));
        assertThat(map.put(5, "b"), is("a"));
        assertThat(map.size(), is(1));
        assertThat(map.remove(5), is("b"));
        assertNull(map.remove(5));
        assertThat(map.size(), is(0));
        assertTrue(map.isEmpty());
        map.validateInternalState();
    }

    /** a missing key reads as null, so a stored null would be a key that is there and is not */
    @Test
    public void shouldRejectNullValues() {
        map.put(1, "one");
        assertThrows(NullPointerException.class, () -> map.put(2, null));
        assertThrows(NullPointerException.class, () -> map.put(1, null));
        assertThrows(NullPointerException.class, () -> map.putIfAbsent(2, null));
        assertThat(map.size(), is(1));
        assertThat(map.get(1), is("one"));
        map.validateInternalState();
    }

    @Test
    public void computeIfAbsentShouldCallTheFunctionOnlyForAnAbsentKey() {
        final AtomicInteger calls = new AtomicInteger();
        assertThat(map.computeIfAbsent(7, k -> {
            calls.incrementAndGet();
            return "k" + k;
        }), is("k7"));
        assertThat(map.computeIfAbsent(7, k -> {
            calls.incrementAndGet();
            return "other";
        }), is("k7"));
        assertThat(calls.get(), is(1));

        // null from the function: nothing is put
        assertNull(map.computeIfAbsent(8, k -> null));
        assertFalse(map.containsKey(8));
        assertThat(map.size(), is(1));
    }

    @Test
    public void putIfAbsentShouldKeepTheExistingValue() {
        assertNull(map.putIfAbsent(3, "a"));
        assertThat(map.putIfAbsent(3, "b"), is("a"));
        assertThat(map.get(3), is("a"));
    }

    @Test
    public void fusedPutsShouldReturnTheNeighbours() {
        map.put(100, "100");
        map.put(300, "300");
        assertThat(map.putAndGetLowerValue(200, "200"), is("100"));
        assertThat(map.putAndGetHigherValue(250, "250"), is("300"));
        assertNull(map.putAndGetLowerValue(50, "50"));
        assertNull(map.putAndGetHigherValue(400, "400"));
        assertThat(map.size(), is(6));
    }

    @Test
    public void endsOfAnEmptyMap() {
        assertThrows(NoSuchElementException.class, map::firstKey);
        assertThrows(NoSuchElementException.class, map::lastKey);
        assertNull(map.firstValue());
        assertNull(map.lastValue());
        assertNull(map.pollFirstValue());
        assertNull(map.pollLastValue());
    }

    @Test
    public void endsOfTheSignedRange() {
        map.put(Long.MAX_VALUE, "max");
        map.put(Long.MIN_VALUE, "min");
        map.put(0, "zero");
        assertThat(map.firstKey(), is(Long.MIN_VALUE));
        assertThat(map.lastKey(), is(Long.MAX_VALUE));
        assertThat(map.pollFirstValue(), is("min"));
        assertThat(map.pollLastValue(), is("max"));
        assertThat(map.firstKey(), is(0L));
        assertThat(map.lastKey(), is(0L));
    }

    @Test
    public void entriesShouldFollowTheMapEntryContract() {
        map.put(-1, "a");
        final Map.Entry<Long, String> entry = map.entriesList().get(0);
        assertEquals(Map.entry(-1L, "a"), entry);
        assertEquals(entry, Map.entry(-1L, "a"));
        assertEquals(new AbstractMap.SimpleEntry<>(-1L, "a"), entry);
        assertEquals(Map.entry(-1L, "a").hashCode(), entry.hashCode());
        assertNotEquals(Map.entry(-1L, "b"), entry);
        assertThat(entry.toString(), is("-1=a"));
    }

    /**
     * Key 0 stays, key 256 comes and goes: each put branches the root into a new inner node, each removal
     * collapses it. The collapsed node used to be dropped instead of pooled - 96 bytes per cycle.
     */
    @Test
    public void collapsedNodesShouldGoBackToThePool() {
        map.put(0, "zero");
        for (int i = 0; i < 20_000; i++) {
            map.put(256, "x");
            map.remove(256);
        }
        final long allocated = allocatedBytes(() -> {
            for (int i = 0; i < 10_000; i++) {
                map.put(256, "x");
                map.remove(256);
            }
        });
        assertTrue("allocated " + allocated + " bytes in 10000 cycles", allocated < 10_000);
        map.validateInternalState();
    }

    @Test
    public void clearShouldReturnTheNodesToThePool() {
        for (int i = 0; i < 1000; i++) {
            map.put(i * 3L, "v");
        }
        map.clear();
        assertTrue(map.isEmpty());
        map.validateInternalState();

        final long allocated = allocatedBytes(() -> {
            for (int i = 0; i < 1000; i++) {
                map.put(i * 3L, "v");
            }
        });
        assertTrue("allocated " + allocated + " bytes refilling the map", allocated < 1024);
        assertThat(map.size(), is(1000));
        map.validateInternalState();
    }

    /** forEach used to wrap the consumer into a key-converting lambda on every call */
    @Test
    public void forEachShouldNotAllocate() {
        for (int i = 0; i < 300; i++) {
            map.put(i * 7L - 1000, "v");
        }
        final long[] sum = new long[1];
        final LongObjConsumer<String> consumer = (k, v) -> sum[0] += k;
        for (int i = 0; i < 20_000; i++) {
            map.forEach(consumer, 10);
            map.forEachDesc(consumer, 10);
        }
        final long allocated = allocatedBytes(() -> {
            for (int i = 0; i < 10_000; i++) {
                map.forEach(consumer, 10);
                map.forEachDesc(consumer, 10);
            }
        });
        assertTrue("allocated " + allocated + " bytes in 20000 calls", allocated < 10_000);
    }

    private static long allocatedBytes(final Runnable action) {
        final java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue("per-thread allocation counter", bean instanceof com.sun.management.ThreadMXBean);
        final com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) bean;
        final long before = threads.getCurrentThreadAllocatedBytes();
        action.run();
        return threads.getCurrentThreadAllocatedBytes() - before;
    }
}
