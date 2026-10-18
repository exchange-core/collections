package exchange.core2.collections.hashtable;

import exchange.core2.collections.records.RecordStoreChecks;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.LongRecordTable.NO_RECORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LongRecordTableTest {

    private static final int AVAILABLE = 0;
    private static final int RESERVED = 1;

    private static LongRecordTable table(int fields) {
        return LongRecordTable.builder().fields(fields).build();
    }

    private static void check(LongRecordTable table) {
        table.integrityCheck();
        RecordStoreChecks.integrityCheck(table.store());
    }

    @Test
    public void newKeyGetsAZeroedRecord() {
        final LongRecordTable table = table(2);
        final long h = table.getOrCreate(42L);
        assertNotEquals(NO_RECORD, h);
        assertEquals(0, table.get(h, AVAILABLE));
        assertEquals(0, table.get(h, RESERVED));
        assertEquals(h, table.find(42L));
        assertTrue(table.containsKey(42L));
        assertEquals(42L, table.keyOf(h));
        assertEquals(1, table.size());
        check(table);
    }

    @Test
    public void presentKeyKeepsItsRecord() {
        final LongRecordTable table = table(2);
        final long h = table.getOrCreate(42L);
        table.set(h, AVAILABLE, 100);
        table.addAndGet(h, RESERVED, 7);
        assertEquals(h, table.getOrCreate(42L));
        assertEquals(100, table.get(h, AVAILABLE));
        assertEquals(7, table.get(h, RESERVED));
        assertEquals(1, table.size());
        check(table);
    }

    @Test
    public void missingKeyHasNoRecord() {
        final LongRecordTable table = table(1);
        table.getOrCreate(1L);
        assertEquals(NO_RECORD, table.find(2L));
        assertFalse(table.containsKey(2L));
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> table.get(table.find(2L), 0));
        assertTrue(e.getMessage(), e.getMessage().contains("NO_RECORD"));
        assertFalse(table.isValid(NO_RECORD));
    }

    @Test
    public void removedKeyFreesItsRecord() {
        final LongRecordTable table = table(2);
        final long h = table.getOrCreate(42L);
        table.set(h, AVAILABLE, 5);
        assertTrue(table.remove(42L));
        assertFalse(table.isValid(h));
        assertThrows(IllegalArgumentException.class, () -> table.get(h, AVAILABLE));
        assertThrows(IllegalArgumentException.class, () -> table.keyOf(h));
        assertEquals(NO_RECORD, table.find(42L));
        assertEquals(0, table.size());
        assertFalse(table.remove(42L));
        check(table);

        final long h2 = table.getOrCreate(42L);
        assertNotEquals(h, h2);
        assertEquals(0, table.get(h2, AVAILABLE));
        assertFalse(table.isValid(h));
        check(table);
    }

    @Test
    public void anyKey() {
        final LongRecordTable table = table(1);
        final long seedKey = new LongLongHashtable().seed();
        final long[] keys = {0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE, seedKey, seedKey ^ 1, 0x5bd1e995L};
        for (long key : keys) {
            table.set(table.getOrCreate(key), 0, ~key);
        }
        assertEquals(LongStream.of(keys).distinct().count(), table.size());
        for (long key : keys) {
            final long h = table.find(key);
            assertEquals(key, table.keyOf(h));
            assertEquals(~key, table.get(h, 0));
        }
        check(table);
        for (long key : keys) {
            table.remove(key);
        }
        assertEquals(0, table.size());
        check(table);
    }

    @Test
    public void theKeyIsNotAField() {
        final LongRecordTable table = table(2);
        final long h = table.getOrCreate(42L);
        assertThrows(IndexOutOfBoundsException.class, () -> table.get(h, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> table.set(h, 2, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> table.addAndGet(h, 2, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> table.addAndGetExact(h, 2, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> table.get(h, -1));
        assertEquals(42L, table.keyOf(h));
        check(table);
    }

    @Test
    public void addAndGetExactKeepsTheValueOnOverflow() {
        final LongRecordTable table = table(1);
        final long h = table.getOrCreate(1L);
        table.set(h, 0, Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> table.addAndGetExact(h, 0, 1));
        assertEquals(Long.MAX_VALUE, table.get(h, 0));
        assertEquals(Long.MIN_VALUE, table.addAndGet(h, 0, 1));
    }

    @Test
    public void handlesSurviveTheResizesOfTheIndex() {
        final LongRecordTable table = table(1);
        final Random rand = new Random(11);
        final long[] keys = new long[100_000];
        final long[] handles = new long[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rand.nextLong();
            handles[i] = table.getOrCreate(keys[i]);
            table.set(handles[i], 0, i);
        }
        for (int i = 0; i < keys.length; i++) {
            assertEquals(handles[i], table.find(keys[i]));
            assertEquals(i, table.get(handles[i], 0));
        }
        check(table);
    }

    @Test
    public void forEachInTheOrderOfTheRecords() {
        final LongRecordTable table = table(1);
        final long[] keys = {50L, -3L, 7_000_000_000L, 0L, 12L};
        for (long key : keys) {
            table.getOrCreate(key);
        }
        assertEquals(List.of(50L, -3L, 7_000_000_000L, 0L, 12L), keysInOrder(table));
        // the slot of the removed key goes to the next new one
        table.remove(-3L);
        table.getOrCreate(99L);
        assertEquals(List.of(50L, 99L, 7_000_000_000L, 0L, 12L), keysInOrder(table));
        table.forEach((key, handle) -> assertEquals(handle, table.find(key)));
    }

    private static List<Long> keysInOrder(LongRecordTable table) {
        final List<Long> keys = new ArrayList<>();
        table.forEach((key, handle) -> keys.add(key));
        return keys;
    }

    @Test
    public void forEachMayRemoveKeys() {
        final LongRecordTable table = table(1);
        for (long key = 0; key < 1000; key++) {
            table.getOrCreate(key);
        }
        final int[] visited = {0};
        table.forEach((key, handle) -> {
            visited[0]++;
            if (key % 2 == 0) {
                assertTrue(table.remove(key));
            }
        });
        assertEquals(1000, visited[0]);
        assertEquals(500, table.size());
        table.forEach((key, handle) -> assertEquals(1, key % 2));
        check(table);
    }

    @Test
    public void forEachInsideForEach() {
        final LongRecordTable table = table(1);
        for (long key = 0; key < 10; key++) {
            table.getOrCreate(key);
        }
        final int[] pairs = {0};
        final List<Long> outerKeys = new ArrayList<>();
        table.forEach((outerKey, outerHandle) -> {
            outerKeys.add(outerKey);
            table.forEach((innerKey, innerHandle) -> pairs[0]++);
        });
        assertEquals(100, pairs[0]);
        assertEquals(10, outerKeys.size());
    }

    @Test
    public void clearMakesEveryHandleStale() {
        final LongRecordTable table = table(2);
        final List<Long> handles = new ArrayList<>();
        for (long key = 0; key < 100; key++) {
            handles.add(table.getOrCreate(key));
        }
        table.clear();
        assertEquals(0, table.size());
        for (long h : handles) {
            assertFalse(table.isValid(h));
        }
        for (long key = 0; key < 100; key++) {
            assertEquals(NO_RECORD, table.find(key));
        }
        check(table);
        final long h = table.getOrCreate(5L);
        assertEquals(0, table.get(h, AVAILABLE));
        check(table);
    }

    /** a fixed store of 3 pages of 4 (11 records) is full before the index (32 slots, 30 keys) */
    @Test
    public void fixedCapacityFullStore() {
        final LongRecordTable table = LongRecordTable.builder().fields(1).expectedSize(10).pageSize(4)
                .fixedCapacity().build();
        for (long key = 0; key < 11; key++) {
            table.getOrCreate(key);
        }
        assertThrows(IllegalStateException.class, () -> table.getOrCreate(11L));
        assertEquals(11, table.size());
        assertFalse(table.containsKey(11L));
        assertEquals(table.find(3L), table.getOrCreate(3L));
        check(table);
        table.remove(3L);
        table.getOrCreate(11L);
        check(table);
    }

    /** the fixed index (16 slots, 14 keys) is full before the store (a page of 1024) - no record is left behind */
    @Test
    public void fixedCapacityFullIndex() {
        final LongRecordTable table = LongRecordTable.builder().fields(1).expectedSize(9).fixedCapacity().build();
        for (long key = 0; key < 14; key++) {
            table.getOrCreate(key);
        }
        assertThrows(IllegalStateException.class, () -> table.getOrCreate(14L));
        assertEquals(14, table.size());
        assertEquals(14, table.store().size());
        check(table);
    }

    @Test
    public void maxCapacityLimitsTheKeys() {
        final LongRecordTable table = LongRecordTable.builder().fields(1).maxCapacity(5).build();
        for (long key = 0; key < 5; key++) {
            table.getOrCreate(key);
        }
        assertThrows(IllegalStateException.class, () -> table.getOrCreate(5L));
        assertEquals(5, table.size());
        check(table);
    }

    @Test
    public void ensureCapacity() {
        final LongRecordTable table = table(1);
        table.ensureCapacity(10_000);
        assertTrue(table.store().capacity() >= 10_000);
        for (long key = 0; key < 10_000; key++) {
            table.getOrCreate(key);
        }
        assertThrows(IllegalArgumentException.class, () -> table.ensureCapacity(-1));
        check(table);
    }

    @Test
    public void builderRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class, () -> LongRecordTable.builder().build());
        assertThrows(IllegalArgumentException.class, () -> LongRecordTable.builder().fields(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongRecordTable.builder().fields(Integer.MAX_VALUE).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongRecordTable.builder().fields(1).expectedSize(-1).build());
        assertThrows(IllegalArgumentException.class, () -> LongRecordTable.builder().fields(1).pageSize(3).build());
        assertEquals(3, LongRecordTable.builder().fields(3).build().fields());
    }

    /**
     * Random operations against a model - keys to their fields, the handles kept by the caller, the handles of the
     * removed keys - with the integrity check along the way. Keys from a small range, so that they come back.
     */
    @Test
    public void randomOperationsMatchTheModel() {
        for (int seed = 0; seed < 20; seed++) {
            runAgainstModel(seed);
        }
    }

    private static void runAgainstModel(long seed) {
        final Random rand = new Random(seed);
        final int fields = 1 + rand.nextInt(3);
        final LongRecordTable table = LongRecordTable.builder().fields(fields).pageSize(1 << rand.nextInt(5))
                .expectedSize(rand.nextInt(50)).build();
        final Map<Long, long[]> model = new HashMap<>();
        final Map<Long, Long> handles = new HashMap<>();
        final List<Long> stale = new ArrayList<>();
        final String context = "seed=" + seed;
        final int keyRange = 50 + rand.nextInt(500);

        for (int op = 0; op < 20_000; op++) {
            final long key = rand.nextInt(keyRange) - keyRange / 2;
            final int choice = rand.nextInt(100);
            if (choice < 30) {
                final long h = table.getOrCreate(key);
                if (model.containsKey(key)) {
                    assertEquals(context, (long) handles.get(key), h);
                } else {
                    model.put(key, new long[fields]);
                    handles.put(key, h);
                }
            } else if (choice < 45) {
                final boolean present = model.remove(key) != null;
                assertEquals(context, present, table.remove(key));
                if (present) {
                    stale.add(handles.remove(key));
                }
            } else if (choice < 70 && model.containsKey(key)) {
                final long h = handles.get(key);
                final int f = rand.nextInt(fields);
                final long delta = rand.nextInt(2001) - 1000;
                model.get(key)[f] += delta;
                assertEquals(context, model.get(key)[f], table.addAndGet(h, f, delta));
            } else if (choice < 90) {
                final long h = table.find(key);
                if (model.containsKey(key)) {
                    assertEquals(context, (long) handles.get(key), h);
                    assertEquals(context, key, table.keyOf(h));
                    for (int f = 0; f < fields; f++) {
                        assertEquals(context, model.get(key)[f], table.get(h, f));
                    }
                } else {
                    assertEquals(context, NO_RECORD, h);
                }
            } else if (choice < 99 && !stale.isEmpty()) {
                final long h = stale.get(rand.nextInt(stale.size()));
                assertFalse(context, table.isValid(h));
                assertThrows(context, IllegalArgumentException.class, () -> table.get(h, 0));
            } else if (rand.nextInt(10) == 0) {
                table.clear();
                stale.addAll(handles.values());
                model.clear();
                handles.clear();
            }
            assertEquals(context, model.size(), table.size());
            if (op % 500 == 0) {
                check(table);
            }
        }
        check(table);
        final Map<Long, Long> visited = new HashMap<>();
        table.forEach((key, handle) -> assertEquals(context, null, visited.put(key, handle)));
        assertEquals(context, handles, visited);
    }
}
