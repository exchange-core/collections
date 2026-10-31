package exchange.core2.collections.art;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

/**
 * The packed keys of ArtNode4: the zero-byte search can be fooled by a borrow (bytes 0x01, 0x80 next to a match)
 * and by the unused bytes; insertion and removal shift bytes and fields. Every 4 of the bytes where that goes
 * wrong, in every insertion order, every byte value looked up after each step, then removed in every position -
 * at level 0 (values) and at level 8 (leaves below).
 */
public class ArtNode4Test {

    private static final int[] EDGES = {0x00, 0x01, 0x7F, 0x80, 0x81, 0xFE, 0xFF};

    @Test
    public void shouldFindInsertAndRemoveAnyFourBytes() {
        for (int shift : new int[]{0, 8}) {
            for (int a = 0; a < EDGES.length; a++) {
                for (int b = a + 1; b < EDGES.length; b++) {
                    for (int c = b + 1; c < EDGES.length; c++) {
                        for (int d = c + 1; d < EDGES.length; d++) {
                            final int[] bytes = {EDGES[a], EDGES[b], EDGES[c], EDGES[d]};
                            for (int[] order : permutations(bytes)) {
                                check(order, shift);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void check(final int[] order, final int shift) {
        final LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        final TreeMap<Long, String> oracle = new TreeMap<>();
        // a common high part, so that the four keys branch on one byte
        final long base = 0x1234_5600_0000L;
        for (int b : order) {
            final long key = base + ((long) b << shift);
            map.put(key, "v" + b);
            oracle.put(key, "v" + b);
            assertAll(map, oracle, base, shift);
        }
        // remove in a rotated order, so that every position gets removed from
        for (int i = 0; i < order.length; i++) {
            final long key = base + ((long) order[(i + 1) % order.length] << shift);
            assertEquals(oracle.remove(key), map.remove(key));
            assertAll(map, oracle, base, shift);
        }
    }

    private static void assertAll(LongAdaptiveRadixTreeMap<String> map, TreeMap<Long, String> oracle, long base, int shift) {
        map.validateInternalState();
        assertEquals(oracle.size(), map.size());
        for (int b = 0; b < 256; b++) {
            final long key = base + ((long) b << shift);
            assertEquals("get " + Long.toHexString(key), oracle.get(key), map.get(key));
            final var ceiling = oracle.ceilingEntry(key);
            assertEquals("ceiling " + Long.toHexString(key), ceiling == null ? null : ceiling.getValue(), map.ceilingValue(key));
            final var floor = oracle.floorEntry(key);
            assertEquals("floor " + Long.toHexString(key), floor == null ? null : floor.getValue(), map.floorValue(key));
        }
        final List<Long> keys = new ArrayList<>();
        map.forEach((k, v) -> keys.add(k), Integer.MAX_VALUE);
        assertEquals(new ArrayList<>(oracle.keySet()), keys);
    }

    private static List<int[]> permutations(final int[] items) {
        final List<int[]> result = new ArrayList<>();
        permute(items.clone(), 0, result);
        return result;
    }

    private static void permute(final int[] a, final int k, final List<int[]> out) {
        if (k == a.length) {
            out.add(a.clone());
            return;
        }
        for (int i = k; i < a.length; i++) {
            int t = a[k];
            a[k] = a[i];
            a[i] = t;
            permute(a, k + 1, out);
            a[i] = a[k];
            a[k] = t;
        }
    }
}
