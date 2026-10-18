package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static exchange.core2.collections.hashtable.LongTupleHashtable.NO_SLOT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LongTupleHashtableTest {

    private static final int AVAILABLE = 0;
    private static final int RESERVED = 1;

    private static LongTupleHashtable table(int keyLongs, int valueLongs) {
        return LongTupleHashtable.builder().keyLongs(keyLongs).valueLongs(valueLongs).build();
    }

    /** the operations for the key length, by the arity methods or (array) through a long[] */
    private static int getOrInsert(LongTupleHashtable t, long[] k, boolean array) {
        if (array) {
            return t.getOrInsert(k, 0);
        }
        return switch (k.length) {
            case 1 -> t.getOrInsert(k[0]);
            case 2 -> t.getOrInsert(k[0], k[1]);
            case 3 -> t.getOrInsert(k[0], k[1], k[2]);
            case 4 -> t.getOrInsert(k[0], k[1], k[2], k[3]);
            default -> t.getOrInsert(k, 0);
        };
    }

    private static int find(LongTupleHashtable t, long[] k, boolean array) {
        if (array) {
            return t.find(k, 0);
        }
        return switch (k.length) {
            case 1 -> t.find(k[0]);
            case 2 -> t.find(k[0], k[1]);
            case 3 -> t.find(k[0], k[1], k[2]);
            case 4 -> t.find(k[0], k[1], k[2], k[3]);
            default -> t.find(k, 0);
        };
    }

    private static boolean remove(LongTupleHashtable t, long[] k, boolean array) {
        if (array) {
            return t.remove(k, 0);
        }
        return switch (k.length) {
            case 1 -> t.remove(k[0]);
            case 2 -> t.remove(k[0], k[1]);
            case 3 -> t.remove(k[0], k[1], k[2]);
            case 4 -> t.remove(k[0], k[1], k[2], k[3]);
            default -> t.remove(k, 0);
        };
    }

    @Test
    public void newKeyGetsZeroValues() {
        final LongTupleHashtable t = table(3, 2);
        final int pos = t.getOrInsert(7L, 8L, 9L);
        assertEquals(0, t.getValue(pos, AVAILABLE));
        assertEquals(0, t.getValue(pos, RESERVED));
        assertEquals(7L, t.getKey(pos, 0));
        assertEquals(8L, t.getKey(pos, 1));
        assertEquals(9L, t.getKey(pos, 2));
        assertEquals(pos, t.find(7L, 8L, 9L));
        assertEquals(1, t.size());
        t.integrityCheck();
    }

    @Test
    public void presentKeyKeepsItsValues() {
        final LongTupleHashtable t = table(3, 2);
        final int pos = t.getOrInsert(7L, 8L, 9L);
        t.setValue(pos, AVAILABLE, 100);
        assertEquals(-5, t.addAndGetValue(pos, RESERVED, -5));
        assertEquals(pos, t.getOrInsert(7L, 8L, 9L));
        assertEquals(100, t.getValue(pos, AVAILABLE));
        assertEquals(-5, t.getValue(pos, RESERVED));
        assertEquals(1, t.size());
    }

    @Test
    public void everyKeyLengthByItsMethodsAndThroughAnArray() {
        for (int keyLongs = 1; keyLongs <= 6; keyLongs++) {
            for (boolean array : new boolean[]{false, true}) {
                final LongTupleHashtable t = table(keyLongs, 1);
                final long[] key = new long[keyLongs];
                for (int i = 0; i < keyLongs; i++) {
                    key[i] = 100 + i;
                }
                final int pos = getOrInsert(t, key, array);
                t.setValue(pos, 0, 42);
                assertEquals(pos, find(t, key, !array));
                for (int i = 0; i < keyLongs; i++) {
                    assertEquals(key[i], t.getKey(pos, i));
                }
                // a key differing in its last long only
                final long[] other = key.clone();
                other[keyLongs - 1]++;
                assertEquals(NO_SLOT, find(t, other, array));
                assertTrue(remove(t, key, array));
                assertEquals(NO_SLOT, find(t, key, !array));
                assertFalse(remove(t, key, !array));
                t.integrityCheck();
            }
        }
    }

    @Test
    public void wrongKeyLengthIsRejected() {
        final LongTupleHashtable t = table(2, 1);
        assertThrows(IllegalArgumentException.class, () -> t.find(1L));
        assertThrows(IllegalArgumentException.class, () -> t.getOrInsert(1L, 2L, 3L));
        assertThrows(IllegalArgumentException.class, () -> t.remove(1L, 2L, 3L, 4L));
        assertThrows(IndexOutOfBoundsException.class, () -> t.find(new long[]{1L}, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> t.getOrInsert(new long[]{1L, 2L}, 1));
        assertEquals(0, t.size());
    }

    @Test
    public void anyKey() {
        final LongTupleHashtable t = table(2, 1);
        final long[][] keys = {{0, 0}, {0, 1}, {1, 0}, {-1, -1}, {Long.MIN_VALUE, Long.MAX_VALUE},
                {Long.MAX_VALUE, Long.MIN_VALUE}, {t.seed(), 0}, {0, t.seed()}, {-t.seed(), 0}};
        final List<long[]> distinct = new ArrayList<>();
        for (long[] k : keys) {
            if (distinct.stream().noneMatch(d -> Arrays.equals(d, k))) {
                distinct.add(k);
            }
            t.setValue(t.getOrInsert(k[0], k[1]), 0, k[0] * 31 + k[1]);
        }
        assertEquals(distinct.size(), t.size());
        for (long[] k : keys) {
            assertEquals(k[0] * 31 + k[1], t.getValue(t.find(k[0], k[1]), 0));
        }
        t.integrityCheck();
    }

    /**
     * Two keys with the same tag - a 63-bit hash, they do not occur by chance: the slot of one gets the key of the
     * other under its tag, and a lookup of the first must not take that slot for its own.
     */
    @Test
    public void theKeyDecidesNotTheTag() throws Exception {
        final Field dataField = LongTupleHashtable.class.getDeclaredField("data");
        dataField.setAccessible(true);
        for (boolean array : new boolean[]{false, true}) {
            final LongTupleHashtable t = table(3, 1);
            final long[] key = {1L, 2L, 3L};
            final int pos = getOrInsert(t, key, array);
            // the last long of the stored key changed, the tag kept
            ((long[]) dataField.get(t))[pos + 3] = 4L;
            assertEquals(NO_SLOT, find(t, key, array));
            assertEquals(NO_SLOT, find(t, new long[]{1L, 2L, 4L}, array));
        }
    }

    @Test
    public void noSlotIsNotAPosition() {
        final LongTupleHashtable t = table(1, 2);
        t.getOrInsert(1L);
        final int pos = t.find(2L);
        assertEquals(NO_SLOT, pos);
        assertThrows(IndexOutOfBoundsException.class, () -> t.getValue(pos, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> t.setValue(pos, 1, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> t.addAndGetValue(pos, 0, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> t.getKey(pos, 0));
        t.integrityCheck();
    }

    @Test
    public void valueAndKeyIndexesAreChecked() {
        final LongTupleHashtable t = table(2, 2);
        final int a = t.getOrInsert(1L, 2L);
        final int b = t.getOrInsert(3L, 4L);
        assertThrows(IndexOutOfBoundsException.class, () -> t.getValue(a, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> t.setValue(a, 2, 9));
        assertThrows(IndexOutOfBoundsException.class, () -> t.addAndGetValueExact(a, -1, 9));
        assertThrows(IndexOutOfBoundsException.class, () -> t.getKey(a, 2));
        assertEquals(0, t.getValue(b, 0));
        assertEquals(3L, t.getKey(b, 0));
        t.integrityCheck();
    }

    @Test
    public void addAndGetValueExactKeepsTheValueOnOverflow() {
        final LongTupleHashtable t = table(1, 1);
        final int pos = t.getOrInsert(1L);
        t.setValue(pos, 0, Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> t.addAndGetValueExact(pos, 0, 1));
        assertEquals(Long.MAX_VALUE, t.getValue(pos, 0));
        assertEquals(Long.MIN_VALUE, t.addAndGetValue(pos, 0, 1));
    }

    @Test
    public void setOfKeys() {
        final LongTupleHashtable t = table(2, 0);
        t.getOrInsert(1L, 2L);
        t.getOrInsert(1L, 2L);
        t.getOrInsert(2L, 1L);
        assertEquals(2, t.size());
        assertNotEquals(NO_SLOT, t.find(2L, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> t.getValue(t.find(1L, 2L), 0));
        assertTrue(t.remove(1L, 2L));
        assertEquals(1, t.size());
        t.integrityCheck();
    }

    @Test
    public void growsAndKeepsTheValues() {
        final LongTupleHashtable t = LongTupleHashtable.builder().keyLongs(3).valueLongs(2).expectedSize(0).build();
        for (long i = 0; i < 100_000; i++) {
            final int pos = t.getOrInsert(i, i * 7, i % 3);
            t.setValue(pos, AVAILABLE, i);
            t.setValue(pos, RESERVED, -i);
        }
        assertEquals(100_000, t.size());
        assertTrue(t.capacity() >= 100_000 / 0.65);
        for (long i = 0; i < 100_000; i++) {
            final int pos = t.find(i, i * 7, i % 3);
            assertEquals(i, t.getValue(pos, AVAILABLE));
            assertEquals(-i, t.getValue(pos, RESERVED));
        }
        t.integrityCheck();
    }

    /** 16 slots, maxLoad 0.95: 14 keys (two slots stay free), then refused */
    @Test
    public void fixedCapacityRefusesANewKeyOfAFullTable() {
        final LongTupleHashtable t = LongTupleHashtable.builder().keyLongs(2).valueLongs(1).expectedSize(9)
                .fixedCapacity().build();
        assertEquals(16, t.capacity());
        for (long i = 0; i < 14; i++) {
            t.setValue(t.getOrInsert(i, -i), 0, i);
        }
        assertThrows(IllegalStateException.class, () -> t.getOrInsert(14L, -14L));
        assertEquals(14, t.size());
        assertEquals(NO_SLOT, t.find(14L, -14L));
        assertEquals(3, t.getValue(t.getOrInsert(3L, -3L), 0));
        t.integrityCheck();
        assertTrue(t.remove(3L, -3L));
        t.getOrInsert(14L, -14L);
        t.integrityCheck();
    }

    @Test
    public void maxCapacityAndMaxLoad() {
        final LongTupleHashtable t = LongTupleHashtable.builder().keyLongs(1).valueLongs(1).expectedSize(0)
                .maxCapacity(32).maxLoad(0.75).build();
        for (long i = 0; i < 24; i++) {
            t.getOrInsert(i);
        }
        assertEquals(32, t.capacity());
        assertThrows(IllegalStateException.class, () -> t.getOrInsert(24L));
        assertEquals(24, t.size());
        t.integrityCheck();
    }

    @Test
    public void removalShiftsTheClusterBack() {
        final LongTupleHashtable t = LongTupleHashtable.builder().keyLongs(1).valueLongs(1).expectedSize(100).build();
        final Random rand = new Random(5);
        final Map<Long, Long> model = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            final long key = rand.nextInt(1000);
            t.setValue(t.getOrInsert(key), 0, key * 3);
            model.put(key, key * 3);
        }
        for (Long key : new ArrayList<>(model.keySet())) {
            if (key % 3 != 0) {
                assertTrue(t.remove(key));
                model.remove(key);
                t.integrityCheck();
            }
        }
        model.forEach((key, value) -> assertEquals((long) value, t.getValue(t.find(key), 0)));
        assertEquals(model.size(), t.size());
    }

    @Test
    public void clearAndEnsureCapacity() {
        final LongTupleHashtable t = table(2, 1);
        for (long i = 0; i < 100; i++) {
            t.getOrInsert(i, i);
        }
        final int capacity = t.capacity();
        t.clear();
        assertEquals(0, t.size());
        assertEquals(capacity, t.capacity());
        assertEquals(NO_SLOT, t.find(5L, 5L));
        t.integrityCheck();
        t.ensureCapacity(10_000);
        assertTrue(t.capacity() >= 10_000 / 0.65);
        assertThrows(IllegalArgumentException.class, () -> t.ensureCapacity(-1));
        t.integrityCheck();
    }

    @Test
    public void forEachVisitsEveryKey() {
        final LongTupleHashtable t = table(2, 1);
        for (long i = 0; i < 1000; i++) {
            t.setValue(t.getOrInsert(i, i + 1), 0, i * 2);
        }
        final long[] sums = new long[2];
        t.forEach(pos -> {
            sums[0] += t.getKey(pos, 0);
            sums[1] += t.getValue(pos, 0);
            t.addAndGetValue(pos, 0, 1);
        });
        assertEquals(999 * 1000 / 2, sums[0]);
        assertEquals(999 * 1000, sums[1]);
        assertEquals(1, t.getValue(t.find(0L, 1L), 0));
    }

    @Test
    public void toStringShowsKeysAndValues() {
        final LongTupleHashtable t = table(2, 2);
        t.setValue(t.getOrInsert(1L, 2L), 1, 9);
        assertEquals("{[1, 2]=[0, 9]}", t.toString());
    }

    @Test
    public void builderRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class, () -> LongTupleHashtable.builder().build());
        assertThrows(IllegalArgumentException.class, () -> LongTupleHashtable.builder().keyLongs(1).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongTupleHashtable.builder().keyLongs(0).valueLongs(1).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongTupleHashtable.builder().keyLongs(1).valueLongs(1).expectedSize(-1).build());
        assertThrows(IllegalArgumentException.class, () -> LongTupleHashtable.builder().maxCapacity(3));
        assertThrows(IllegalArgumentException.class, () -> LongTupleHashtable.builder().maxLoad(1.0));
        // slots of 1000 longs: at most 2^21 of them fit a long[]
        assertThrows(IllegalArgumentException.class,
                () -> LongTupleHashtable.builder().keyLongs(1).valueLongs(999).maxCapacity(1 << 22).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongTupleHashtable.builder().keyLongs(1).valueLongs(999).expectedSize(3_000_000).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongTupleHashtable.builder().keyLongs(1).valueLongs(1).expectedSize(100).maxCapacity(128)
                        .build());
    }

    /**
     * Keys built like those of a balance - a UUID (two longs: a sequential-ish high half, random low half), a token,
     * a subaccount - must not pile up: the longest cluster of a table at 65% load stays short.
     */
    @Test
    public void structuredKeysDoNotCluster() {
        final LongTupleHashtable t = LongTupleHashtable.builder().keyLongs(3).valueLongs(0).expectedSize(60_000)
                .build();
        final Random rand = new Random(1);
        int n = 0;
        for (long user = 0; user < 5000; user++) {
            final long hi = 0x0190_0000_0000_7000L + (user << 16);  // a UUIDv7-like time prefix, version nibble
            final long lo = rand.nextLong();
            for (long token = 1; token <= 4; token++) {
                for (long sub = 0; sub < 3; sub++) {
                    t.getOrInsert(hi, lo, token << 8 | sub);
                    n++;
                }
            }
        }
        assertEquals(n, t.size());
        final boolean[] occupied = new boolean[t.capacity()];
        final int stride = 1 + t.keyLongs() + t.valueLongs();
        t.forEach(pos -> occupied[pos / stride] = true);
        int longest = 0;
        int run = 0;
        for (int i = 0; i < occupied.length * 2; i++) {
            run = occupied[i % occupied.length] ? run + 1 : 0;
            longest = Math.max(longest, run);
        }
        // random keys at this load (0.46): a few dozen at most
        assertTrue("longest cluster " + longest, longest < 100);
        t.integrityCheck();
    }

    /**
     * Random operations against a model, keys of 1 to 5 longs from small ranges (so that they repeat and collide),
     * with the integrity check along the way.
     */
    @Test
    public void randomOperationsMatchTheModel() {
        for (int seed = 0; seed < 30; seed++) {
            runAgainstModel(seed);
        }
    }

    private static void runAgainstModel(long seed) {
        final Random rand = new Random(seed);
        final int keyLongs = 1 + rand.nextInt(5);
        final int valueLongs = rand.nextInt(4);
        final boolean array = keyLongs == 5 || rand.nextBoolean();
        final LongTupleHashtable t = LongTupleHashtable.builder().keyLongs(keyLongs).valueLongs(valueLongs)
                .expectedSize(rand.nextInt(100)).build();
        final Map<List<Long>, long[]> model = new HashMap<>();
        final String context = "seed=" + seed + " key=" + keyLongs + " values=" + valueLongs + " array=" + array;
        final int range = 2 + rand.nextInt(20);

        for (int op = 0; op < 20_000; op++) {
            final long[] key = new long[keyLongs];
            final List<Long> boxed = new ArrayList<>();
            for (int i = 0; i < keyLongs; i++) {
                key[i] = rand.nextInt(range) - range / 2;
                boxed.add(key[i]);
            }
            final int choice = rand.nextInt(100);
            if (choice < 40) {
                final int pos = getOrInsert(t, key, array);
                final long[] values = model.computeIfAbsent(boxed, k -> new long[valueLongs]);
                for (int i = 0; i < keyLongs; i++) {
                    assertEquals(context, key[i], t.getKey(pos, i));
                }
                if (valueLongs > 0) {
                    final int v = rand.nextInt(valueLongs);
                    final long delta = rand.nextInt(1001) - 500;
                    values[v] += delta;
                    assertEquals(context, values[v], t.addAndGetValue(pos, v, delta));
                }
            } else if (choice < 60) {
                assertEquals(context, model.remove(boxed) != null, remove(t, key, array));
            } else if (choice < 99) {
                final int pos = find(t, key, array);
                final long[] values = model.get(boxed);
                if (values == null) {
                    assertEquals(context, NO_SLOT, pos);
                } else {
                    final long[] actual = new long[valueLongs];
                    for (int v = 0; v < valueLongs; v++) {
                        actual[v] = t.getValue(pos, v);
                    }
                    assertArrayEquals(context, values, actual);
                }
            } else if (rand.nextInt(10) == 0) {
                t.clear();
                model.clear();
            }
            assertEquals(context, model.size(), t.size());
            if (op % 1000 == 0) {
                t.integrityCheck();
            }
        }
        t.integrityCheck();
        final int[] visited = {0};
        t.forEach(pos -> {
            final List<Long> boxed = new ArrayList<>();
            for (int i = 0; i < keyLongs; i++) {
                boxed.add(t.getKey(pos, i));
            }
            assertTrue(context, model.containsKey(boxed));
            visited[0]++;
        });
        assertEquals(context, model.size(), visited[0]);
    }
}
