package exchange.core2.collections.art.testoracle;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The cursor against TreeMap: walks in both directions, every kind of seek from stored keys, their neighbours, random
 * keys and the ends of the range, a few moves from there, removals through the cursor.
 */
public class ArtCursorTest {

    @Test
    public void shouldMatchTreeMapForNarrowKeys() {
        final SplittableRandom random = new SplittableRandom(11);
        for (int size : new int[]{0, 1, 2, 3, 5, 17, 49, 200}) {
            check(size, () -> -300 + random.nextInt(601), random);
        }
    }

    @Test
    public void shouldMatchTreeMapForClusteredKeys() {
        final SplittableRandom random = new SplittableRandom(12);
        for (int size : new int[]{1, 2, 10, 100, 1000}) {
            check(size, () -> ((long) (random.nextInt(4) - 2) << 40) + random.nextInt(3000), random);
        }
    }

    /** 40 blocks of 256 keys: an inner Node48 above them, Node4 / 16 / 48 / 256 and leaves below */
    @Test
    public void shouldMatchTreeMapForKeysUnderNode48() {
        final SplittableRandom random = new SplittableRandom(14);
        for (int size : new int[]{30, 100, 400, 3000}) {
            check(size, () -> random.nextInt(40 * 256), random);
        }
    }

    @Test
    public void shouldMatchTreeMapForAnyKeys() {
        final SplittableRandom random = new SplittableRandom(13);
        final long[] extremes = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1, 0, 1, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        for (int size : new int[]{1, 2, 10, 500}) {
            check(size, () -> random.nextInt(4) == 0 ? extremes[random.nextInt(extremes.length)] : random.nextLong(), random);
        }
    }

    @Test
    public void shouldFailFastAfterAChangeItDidNotMake() {
        final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        final LongAdaptiveRadixTreeMap.Cursor<String> cursor = map.cursor();
        assertFalse(cursor.seekFirst());
        assertThrows(NoSuchElementException.class, cursor::key);

        map.put(1, "a");
        map.put(2, "b");
        assertTrue(cursor.seekFirst());
        map.put(1, "a2");                  // a value update - not a change of the key set
        assertEquals("a2", cursor.value());
        map.put(3, "c");                   // a new key
        assertThrows(ConcurrentModificationException.class, cursor::next);
        assertTrue(cursor.seekCeiling(2)); // a seek starts over
        assertEquals(2, cursor.key());
        assertEquals("b", cursor.remove()); // a removal through the cursor keeps it usable
        assertEquals(3, cursor.key());
        assertTrue(cursor.prev());
        assertEquals(1, cursor.key());
    }

    private static void check(final int size, final LongSupplier keys, final SplittableRandom random) {
        final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        final TreeMap<Long, String> oracle = new TreeMap<>();
        while (oracle.size() < size) {
            final long key = keys.getAsLong();
            map.put(key, "v" + key);
            oracle.put(key, "v" + key);
        }
        final LongAdaptiveRadixTreeMap.Cursor<String> cursor = map.cursor();

        // whole walks
        final List<Long> forward = new ArrayList<>();
        for (boolean on = cursor.seekFirst(); on; on = cursor.next()) {
            assertEquals(oracle.get(cursor.key()), cursor.value());
            forward.add(cursor.key());
        }
        assertEquals(new ArrayList<>(oracle.keySet()), forward);
        final List<Long> backward = new ArrayList<>();
        for (boolean on = cursor.seekLast(); on; on = cursor.prev()) {
            backward.add(cursor.key());
        }
        assertEquals(new ArrayList<>(oracle.descendingKeySet()), backward);
        assertFalse(cursor.isValid());

        // seeks and a few moves from there
        final List<Long> probes = new ArrayList<>(List.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L,
                Long.MAX_VALUE - 1, Long.MAX_VALUE));
        for (long key : oracle.keySet()) {
            probes.add(key);
            probes.add(key - 1);
            probes.add(key + 1);
        }
        for (int i = 0; i < 30; i++) {
            probes.add(keys.getAsLong());
            probes.add(random.nextLong());
        }
        for (long probe : probes) {
            expect(oracle.ceilingEntry(probe), cursor.seekCeiling(probe), cursor, "ceiling " + probe);
            walk(oracle, cursor, random);
            expect(oracle.higherEntry(probe), cursor.seekHigher(probe), cursor, "higher " + probe);
            walk(oracle, cursor, random);
            expect(oracle.floorEntry(probe), cursor.seekFloor(probe), cursor, "floor " + probe);
            walk(oracle, cursor, random);
            expect(oracle.lowerEntry(probe), cursor.seekLower(probe), cursor, "lower " + probe);
            walk(oracle, cursor, random);
        }

        // removals through the cursor: it moves to the next key
        for (int i = 0; i < Math.min(size, 50); i++) {
            final long probe = probes.get(random.nextInt(probes.size()));
            if (cursor.seekCeiling(probe)) {
                final long key = cursor.key();
                assertEquals(oracle.remove(key), cursor.remove());
                expect(oracle.higherEntry(key), cursor.isValid(), cursor, "after removal of " + key);
            }
        }
        map.validateInternalState();
        ArtOracleAssertions.assertSameAsOracle(map, oracle);
    }

    private static void walk(TreeMap<Long, String> oracle, LongAdaptiveRadixTreeMap.Cursor<String> cursor, SplittableRandom random) {
        // now and then a long walk - out of the subtree the seek ended in, up and down the path
        final int steps = random.nextInt(8) == 0 ? 1000 : 4;
        for (int step = 0; step < steps && cursor.isValid(); step++) {
            final long from = cursor.key();
            if (random.nextBoolean()) {
                expect(oracle.higherEntry(from), cursor.next(), cursor, "next from " + from);
            } else {
                expect(oracle.lowerEntry(from), cursor.prev(), cursor, "prev from " + from);
            }
        }
    }

    private static void expect(Map.Entry<Long, String> expected, boolean positioned,
                               LongAdaptiveRadixTreeMap.Cursor<String> cursor, String step) {
        assertEquals(step + ": positioned", expected != null, positioned);
        assertEquals(step + ": valid", expected != null, cursor.isValid());
        if (expected != null) {
            assertEquals(step + ": key", (long) expected.getKey(), cursor.key());
            assertEquals(step + ": value", expected.getValue(), cursor.value());
        }
    }
}
