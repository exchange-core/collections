package exchange.core2.collections.art;

import exchange.core2.collections.objpool.ObjectsPool;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.AbstractMap;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
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
    public void removeRangeShouldTakeTheKeysFromInclusiveToExclusive() {
        assertThat(map.removeRange(Long.MIN_VALUE, Long.MAX_VALUE), is(0));
        for (long key : new long[]{Long.MIN_VALUE, -300, -1, 0, 1, 255, 256, 300, Long.MAX_VALUE}) {
            map.put(key, "v" + key);
        }
        assertThat(map.removeRange(5, 5), is(0));
        assertThat(map.removeRange(10, -10), is(0));
        assertThat(map.removeRange(-1, 256), is(4));        // -1, 0, 1, 255
        assertThat(map.firstKey(), is(Long.MIN_VALUE));
        assertThat(map.higherValue(-300), is("v256"));
        assertThat(map.removeRange(Long.MIN_VALUE, Long.MAX_VALUE), is(4)); // the upper bound stays
        assertThat(map.size(), is(1));
        assertThat(map.firstKey(), is(Long.MAX_VALUE));
        map.validateInternalState();
    }

    @Test
    public void removeRangeShouldNotAllocate() {
        for (int i = 0; i < 1000; i++) {
            map.put(i * 3L, "v");
        }
        for (int i = 0; i < 20_000; i++) {
            map.removeRange(10, 13);   // takes 12
            map.put(12, "v");
        }
        final long allocated = allocatedBytes(() -> {
            for (int i = 0; i < 10_000; i++) {
                map.removeRange(10, 13);
                map.put(12, "v");
            }
        });
        assertTrue("allocated " + allocated + " bytes in 10000 calls", allocated < 10_000);
        map.validateInternalState();
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

    /**
     * A key alone is a leaf; a second key in the same 256-key block makes an ArtNode4 at level 0 and its removal
     * a leaf again; a second key elsewhere makes an inner ArtNode4 over two leaves, which collapses back into the
     * remaining leaf. validateInternalState rejects an ArtNode4 with fewer than two children.
     */
    @Test
    public void singleKeysShouldTurnIntoLeavesAndBack() {
        map.put(1000, "a");
        map.validateInternalState();
        assertThat(map.printDiagram().startsWith("─["), is(true)); // the root is a leaf
        map.put(1001, "b");            // same block - a node at level 0
        map.validateInternalState();
        assertThat(map.remove(1000), is("a"));
        map.validateInternalState();   // 1001 alone - a leaf again
        assertThat(map.firstKey(), is(1001L));
        map.put(-5_000_000L, "c");     // far away - an inner node over two leaves
        map.validateInternalState();
        assertThat(map.lowerValue(1001), is("c"));
        assertThat(map.higherValue(-5_000_000L), is("b"));
        assertThat(map.remove(1001), is("b"));
        map.validateInternalState();   // collapsed into the remaining leaf
        assertThat(map.firstKey(), is(-5_000_000L));
        assertThat(map.remove(-5_000_000L), is("c"));
        assertTrue(map.isEmpty());
        map.validateInternalState();
    }

    /**
     * A pool configured before the leaves existed - no ART_LEAF, as in exchange-core: the leaves get as many places
     * as the Node4s, so keys that come and go alone do not allocate.
     */
    @Test
    public void leavesShouldBeRecycledByAPoolConfiguredWithoutThem() {
        final Map<Integer, Integer> config = new java.util.HashMap<>();
        config.put(ObjectsPool.ART_NODE_4, 64);
        config.put(ObjectsPool.ART_NODE_16, 16);
        config.put(ObjectsPool.ART_NODE_48, 8);
        config.put(ObjectsPool.ART_NODE_256, 4);
        final LongAdaptiveRadixTreeMap<String> pooled = new LongAdaptiveRadixTreeMap<>(new ObjectsPool(config));
        for (int i = 0; i < 20; i++) {
            pooled.put(i * 1_000_000L, "far");  // twenty leaves under one branch
        }
        for (int i = 0; i < 20_000; i++) {
            pooled.put(7_777_777L, "x");
            pooled.remove(7_777_777L);
        }
        final long allocated = allocatedBytes(() -> {
            for (int i = 0; i < 10_000; i++) {
                pooled.put(7_777_777L, "x");
                pooled.remove(7_777_777L);
            }
        });
        assertTrue("allocated " + allocated + " bytes in 10000 cycles", allocated < 10_000);
        pooled.validateInternalState();

        // nothing configured at all: no recycling, but no failure either
        final LongAdaptiveRadixTreeMap<String> unpooled = new LongAdaptiveRadixTreeMap<>(new ObjectsPool(Map.of()));
        for (int i = 0; i < 1000; i++) {
            unpooled.put(i * 37L, "v");
        }
        for (int i = 0; i < 1000; i += 2) {
            unpooled.remove(i * 37L);
        }
        assertThat(unpooled.size(), is(500));
        unpooled.validateInternalState();
    }

    @Test
    public void poolCapacityShouldDecideWhetherARefillAllocates() {
        final LongAdaptiveRadixTreeMap<String> pooled = LongAdaptiveRadixTreeMap.builder().poolCapacity(1024).build();
        final LongAdaptiveRadixTreeMap<String> unpooled = LongAdaptiveRadixTreeMap.builder().poolCapacity(0).build();
        for (final LongAdaptiveRadixTreeMap<String> m : List.of(pooled, unpooled)) {
            putFarKeys(m);
            m.clear();
        }
        final long pooledBytes = allocatedBytes(() -> putFarKeys(pooled));
        final long unpooledBytes = allocatedBytes(() -> putFarKeys(unpooled));
        assertTrue("allocated " + pooledBytes + " bytes refilling the pooled map", pooledBytes < 1024);
        assertTrue("allocated " + unpooledBytes + " bytes refilling the unpooled map", unpooledBytes > 200 * 16);
        pooled.validateInternalState();
        unpooled.validateInternalState();
    }

    @Test
    public void mapsOfASharedPoolShouldTakeTheNodesOneAnotherFrees() {
        final ObjectsPool pool = ObjectsPool.forArt(1024);
        final LongAdaptiveRadixTreeMap<String> first = LongAdaptiveRadixTreeMap.builder().sharedPool(pool).build();
        final LongAdaptiveRadixTreeMap<String> second = LongAdaptiveRadixTreeMap.builder().sharedPool(pool).build();
        putFarKeys(first);
        first.clear();
        final long allocated = allocatedBytes(() -> putFarKeys(second));
        assertTrue("allocated " + allocated + " bytes filling the second map", allocated < 1024);
        assertThat(second.size(), is(200));
        second.validateInternalState();
    }

    @Test
    public void builderShouldRejectAWrongPool() {
        assertThrows(IllegalArgumentException.class, () -> LongAdaptiveRadixTreeMap.builder().poolCapacity(-1));
        assertThrows(IllegalArgumentException.class, () -> ObjectsPool.forArt(-1));
        assertThrows(NullPointerException.class, () -> LongAdaptiveRadixTreeMap.builder().sharedPool(null));
        assertThrows(IllegalStateException.class,
                () -> LongAdaptiveRadixTreeMap.builder().poolCapacity(16).sharedPool(ObjectsPool.forArt(16)).build());
    }

    /** 200 keys that differ in byte 5 only: 200 leaves under one 256-key node, reached through 4, 16 and 48 */
    private static void putFarKeys(final LongAdaptiveRadixTreeMap<String> m) {
        for (int i = 0; i < 200; i++) {
            m.put((long) i << 40, "v");
        }
    }

    /** the content of the streams and toString is checked against TreeMap in testoracle; here: when they read */
    @Test
    public void streamsShouldReadTheMapWhenConsumed() {
        assertThat(map.keysStream().count(), is(0L));
        assertThat(map.toString(), is("{}"));

        final LongStream keys = map.keysStream();
        final Stream<String> values = map.valuesStream();
        map.put(-7, "a");
        map.put(300, "b");
        assertArrayEquals(new long[]{-7, 300}, keys.toArray());
        assertThat(values.collect(Collectors.toList()), is(List.of("a", "b")));
        assertThat(map.toString(), is("{-7=a, 300=b}"));
    }

    @Test
    public void streamsShouldFailFastOnAChangeOfTheKeySet() {
        map.put(1, "a");
        map.put(2, "b");
        assertThrows(ConcurrentModificationException.class, () -> map.keysStream().forEach(k -> map.put(k + 10, "c")));
        assertThrows(ConcurrentModificationException.class, () -> map.valuesStream().forEach(v -> map.remove(2)));

        // value updates are not changes of the key set
        map.keysStream().forEach(k -> map.put(k, "d"));
        assertThat(map.valuesStream().distinct().collect(Collectors.toList()), is(List.of("d")));
    }

    /** the limit used to reach the nodes as it was: a 4- or 16-key root of the last level returned it */
    @Test
    public void walksShouldVisitNothingForALimitOfZeroOrLess() {
        final LongObjConsumer<String> fail = (k, v) -> {
            throw new AssertionError("visited " + k);
        };
        // empty, a leaf, a 4-, 16- and 256-key node of the last level, a tree of inner nodes
        for (final int keys : new int[]{0, 1, 3, 10, 100, 2000}) {
            map.clear();
            for (int i = 0; i < keys; i++) {
                map.put(keys == 2000 ? i * 1000L : i, "v");
            }
            for (final int limit : new int[]{0, -1, Integer.MIN_VALUE}) {
                final String shape = keys + " keys, limit " + limit;
                assertThat(shape, map.forEach(fail, limit), is(0));
                assertThat(shape, map.forEachDesc(fail, limit), is(0));
                assertThat(shape, map.forEach(0, fail, limit), is(0));
                assertThat(shape, map.forEachDesc(Long.MAX_VALUE, fail, limit), is(0));
            }
        }
    }

    @Test
    public void walksShouldFailFastWhenTheConsumerChangesTheKeySet() {
        for (int i = 0; i < 100; i++) {
            map.put(i, "a");
        }
        final List<LongObjConsumer<String>> changes = List.of(
                (k, v) -> map.put(k + 1000, "new"),
                (k, v) -> map.remove(k),
                (k, v) -> map.clear());
        for (final LongObjConsumer<String> change : changes) {
            assertThrows(ConcurrentModificationException.class, () -> map.forEach(change, 3));
            refill();
            assertThrows(ConcurrentModificationException.class, () -> map.forEachDesc(change, 3));
            refill();
            assertThrows(ConcurrentModificationException.class, () -> map.forEach(50, change, 3));
            refill();
            assertThrows(ConcurrentModificationException.class, () -> map.forEachDesc(50, change, 3));
            refill();
            assertThrows(ConcurrentModificationException.class, () -> map.forEach(change));
            refill();
        }

        // value updates are not changes of the key set
        map.forEach((k, v) -> map.put(k, "b"));
        map.forEachDesc((k, v) -> map.put(k, "c"), 10);
        assertThat(map.get(0), is("b"));
        assertThat(map.get(99), is("c"));
        map.validateInternalState();
    }

    /** back to the keys 0..99 after a walk that changed them */
    private void refill() {
        map.clear();
        for (int i = 0; i < 100; i++) {
            map.put(i, "a");
        }
    }

    @Test
    public void computeIfAbsentShouldRejectAFunctionThatChangesTheKeySet() {
        map.put(1, "a");
        assertThrows(ConcurrentModificationException.class, () -> map.computeIfAbsent(2, k -> {
            map.put(3, "c");
            return "b";
        }));
        assertNull("nothing put", map.get(2));
        assertThat("the change of the function stays", map.get(3), is("c"));

        // a value update is not a change of the key set; the key the function returns for is put
        assertThat(map.computeIfAbsent(4, k -> {
            map.put(1, "a2");
            return "d";
        }), is("d"));
        assertThat(map.get(1), is("a2"));
        assertThat(map.size(), is(3));
        map.validateInternalState();
    }

    @Test
    public void getOrDefaultShouldTellAMissingKey() {
        map.put(5, "a");
        assertThat(map.getOrDefault(5, "z"), is("a"));
        assertThat(map.getOrDefault(6, "z"), is("z"));
        assertNull(map.getOrDefault(6, null));
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
