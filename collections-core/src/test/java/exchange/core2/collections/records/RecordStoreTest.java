package exchange.core2.collections.records;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static exchange.core2.collections.records.RecordStore.NO_RECORD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordStoreTest {

    private static RecordStore store(int fields) {
        return RecordStore.builder().fields(fields).build();
    }

    @Test
    public void newRecordHasZeroFields() {
        final RecordStore store = store(3);
        final long h = store.allocate();
        for (int f = 0; f < 3; f++) {
            assertEquals(0L, store.get(h, f));
        }
        assertEquals(1, store.size());
        assertTrue(store.isValid(h));
        store.integrityCheck();
    }

    @Test
    public void fieldsAreIndependent() {
        final RecordStore store = store(3);
        final long a = store.allocate();
        final long b = store.allocate();
        store.set(a, 0, 10);
        store.set(a, 1, 11);
        store.set(a, 2, 12);
        store.set(b, 0, 20);
        assertEquals(10, store.get(a, 0));
        assertEquals(11, store.get(a, 1));
        assertEquals(12, store.get(a, 2));
        assertEquals(20, store.get(b, 0));
        assertEquals(0, store.get(b, 1));
        assertEquals(15, store.addAndGet(a, 0, 5));
        assertEquals(-1, store.addAndGet(b, 2, -1));
        assertEquals(15, store.get(a, 0));
        assertEquals(-1, store.get(b, 2));
        store.integrityCheck();
    }

    @Test
    public void noRecordIsNeverValid() {
        // no page yet, a page allocated at the build, records allocated
        final RecordStore empty = store(2);
        final RecordStore sized = RecordStore.builder().fields(2).expectedSize(10).build();
        final RecordStore used = store(2);
        used.allocate();
        used.allocate();
        for (RecordStore store : List.of(empty, sized, used)) {
            assertFalse(store.isValid(NO_RECORD));
            assertThrows(IllegalArgumentException.class, () -> store.get(NO_RECORD, 0));
            assertThrows(IllegalArgumentException.class, () -> store.set(NO_RECORD, 0, 1));
            assertThrows(IllegalArgumentException.class, () -> store.addAndGet(NO_RECORD, 0, 1));
            assertThrows(IllegalArgumentException.class, () -> store.addAndGetExact(NO_RECORD, 0, 1));
            assertThrows(IllegalArgumentException.class, () -> store.free(NO_RECORD));
            store.integrityCheck();
        }
        for (int i = 0; i < 10_000; i++) {
            assertNotEquals(NO_RECORD, used.allocate());
        }
    }

    @Test
    public void freedHandleIsStale() {
        final RecordStore store = store(2);
        final long h = store.allocate();
        store.set(h, 0, 42);
        store.set(h, 1, 43);
        store.free(h);
        assertEquals(0, store.size());
        assertFalse(store.isValid(h));
        assertThrows(IllegalArgumentException.class, () -> store.get(h, 0));
        assertThrows(IllegalArgumentException.class, () -> store.set(h, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> store.addAndGet(h, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> store.addAndGetExact(h, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> store.free(h));
        store.integrityCheck();

        // the slot is reused under a new generation: zeroed fields, the old handle still stale
        final long h2 = store.allocate();
        assertEquals(RecordStore.index(h), RecordStore.index(h2));
        assertNotEquals(h, h2);
        assertEquals(0, store.get(h2, 0));
        assertEquals(0, store.get(h2, 1));
        assertFalse(store.isValid(h));
        assertThrows(IllegalArgumentException.class, () -> store.set(h, 0, 1));
        assertEquals(0, store.get(h2, 0));
        store.integrityCheck();
    }

    @Test
    public void handleOfAnotherSlotOrForgedIsRejected() {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).build();
        final long h = store.allocate();
        // the right slot under a generation it never had, a slot never used, a page never allocated
        assertFalse(store.isValid(h + (2L << 32)));
        assertThrows(IllegalArgumentException.class, () -> store.get(h + (2L << 32), 0));
        assertFalse(store.isValid(h + 1));
        assertThrows(IllegalArgumentException.class, () -> store.get(h + 1, 0));
        // a slot far beyond the pages: the directory has no place for it
        final long beyond = (1L << 32) | 0x7FFF_FFF0L;
        assertFalse(store.isValid(beyond));
        assertThrows(IndexOutOfBoundsException.class, () -> store.set(beyond, 0, 1));
        assertFalse(store.isValid(-1L));
        assertThrows(IndexOutOfBoundsException.class, () -> store.get(-1L, 0));
        store.integrityCheck();
    }

    @Test
    public void fieldOutOfRangeDoesNotReachTheNeighbour() {
        final RecordStore store = store(2);
        final long a = store.allocate();
        final long b = store.allocate();
        assertThrows(IndexOutOfBoundsException.class, () -> store.get(a, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> store.get(a, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> store.set(a, 2, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> store.addAndGet(a, 3, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> store.addAndGetExact(a, -1, 7));
        assertEquals(0, store.get(b, 0));
        assertTrue(store.isValid(b));
        store.integrityCheck();
    }

    @Test
    public void addAndGetExactKeepsTheValueOnOverflow() {
        final RecordStore store = store(1);
        final long h = store.allocate();
        store.set(h, 0, Long.MAX_VALUE - 1);
        assertEquals(Long.MAX_VALUE, store.addAndGetExact(h, 0, 1));
        assertThrows(ArithmeticException.class, () -> store.addAndGetExact(h, 0, 1));
        assertEquals(Long.MAX_VALUE, store.get(h, 0));
        store.set(h, 0, Long.MIN_VALUE + 1);
        assertThrows(ArithmeticException.class, () -> store.addAndGetExact(h, 0, -2));
        assertEquals(Long.MIN_VALUE + 1, store.get(h, 0));
        // the plain one wraps around
        store.set(h, 0, Long.MAX_VALUE);
        assertEquals(Long.MIN_VALUE, store.addAndGet(h, 0, 1));
    }

    @Test
    public void lastFreedIsFirstReused() {
        final RecordStore store = store(1);
        final long a = store.allocate();
        final long b = store.allocate();
        final long c = store.allocate();
        store.free(a);
        store.free(c);
        assertEquals(RecordStore.index(c), RecordStore.index(store.allocate()));
        assertEquals(RecordStore.index(a), RecordStore.index(store.allocate()));
        assertEquals(RecordStore.index(c) + 1, RecordStore.index(store.allocate()));
        assertTrue(store.isValid(b));
        store.integrityCheck();
    }

    @Test
    public void sameOperationsSameHandles() {
        final long[] first = randomRun(new Random(7), 3);
        final long[] second = randomRun(new Random(7), 3);
        assertArrayEquals(first, second);
    }

    private static long[] randomRun(Random rand, int fields) {
        final RecordStore store = RecordStore.builder().fields(fields).pageSize(8).build();
        final List<Long> live = new ArrayList<>();
        final long[] handles = new long[5000];
        for (int i = 0; i < handles.length; i++) {
            if (!live.isEmpty() && rand.nextInt(3) == 0) {
                store.free(live.remove(rand.nextInt(live.size())));
            }
            handles[i] = store.allocate();
            live.add(handles[i]);
        }
        return handles;
    }

    @Test
    public void growsPageByPage() {
        final RecordStore store = RecordStore.builder().fields(2).pageSize(4).build();
        assertEquals(0, store.capacity());
        final long[] handles = new long[100];
        for (int i = 0; i < handles.length; i++) {
            handles[i] = store.allocate();
            store.set(handles[i], 0, i);
            store.set(handles[i], 1, -i);
            // slot 0 is in the first page
            assertEquals(((i + 2 + 3) / 4) * 4 - 1, store.capacity());
        }
        for (int i = 0; i < handles.length; i++) {
            assertEquals(i, store.get(handles[i], 0));
            assertEquals(-i, store.get(handles[i], 1));
            assertTrue(RecordStore.index(handles[i]) >= 1 && RecordStore.index(handles[i]) <= store.capacity());
        }
        store.integrityCheck();
    }

    @Test
    public void expectedSizeAllocatesThePagesUpFront() {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).expectedSize(7).build();
        assertEquals(7, store.capacity());
        for (int i = 0; i < 7; i++) {
            store.allocate();
        }
        assertEquals(7, store.capacity());
        store.allocate();
        assertEquals(11, store.capacity());
        store.integrityCheck();
    }

    @Test
    public void fixedCapacityRefusesBeyondItsPages() {
        // 10 records and slot 0: three pages of 4
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).expectedSize(10).fixedCapacity()
                .build();
        assertEquals(11, store.capacity());
        assertEquals(11, store.maxCapacity());
        final List<Long> handles = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            handles.add(store.allocate());
        }
        assertThrows(IllegalStateException.class, store::allocate);
        assertEquals(11, store.size());
        assertEquals(11, store.capacity());
        store.integrityCheck();
        store.free(handles.get(3));
        final long h = store.allocate();
        assertEquals(RecordStore.index(handles.get(3)), RecordStore.index(h));
        assertThrows(IllegalStateException.class, store::allocate);
        store.integrityCheck();
    }

    @Test
    public void fixedCapacityOfZero() {
        final RecordStore store = RecordStore.builder().fields(1).fixedCapacity().build();
        assertEquals(0, store.maxCapacity());
        assertThrows(IllegalStateException.class, store::allocate);
        assertThrows(IllegalArgumentException.class, () -> store.get(NO_RECORD, 0));
        store.integrityCheck();
    }

    @Test
    public void maxCapacityLimitsTheRecords() {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(2).maxCapacity(5).build();
        for (int i = 0; i < 5; i++) {
            store.allocate();
        }
        assertThrows(IllegalStateException.class, store::allocate);
        assertEquals(5, store.size());
        store.integrityCheck();
    }

    @Test
    public void forEachVisitsLiveRecordsInSlotOrder() {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).build();
        final List<Long> handles = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            handles.add(store.allocate());
        }
        store.free(handles.get(0));
        store.free(handles.get(7));
        store.free(handles.get(19));
        final List<Long> expected = new ArrayList<>(handles);
        expected.remove(19);
        expected.remove(7);
        expected.remove(0);
        final List<Long> visited = new ArrayList<>();
        store.forEach(visited::add);
        assertEquals(expected, visited);
    }

    @Test
    public void forEachMayFreeTheVisitedRecord() {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).build();
        for (int i = 0; i < 20; i++) {
            store.set(store.allocate(), 0, i);
        }
        final List<Long> visited = new ArrayList<>();
        store.forEach(h -> {
            visited.add(h);
            if (store.get(h, 0) % 2 == 0) {
                store.free(h);
            }
        });
        assertEquals(20, visited.size());
        assertEquals(10, store.size());
        store.forEach(h -> assertEquals(1, store.get(h, 0) % 2));
        store.integrityCheck();
    }

    @Test
    public void clearMakesEveryHandleStale() {
        final RecordStore store = RecordStore.builder().fields(2).pageSize(4).build();
        final RecordStore fresh = RecordStore.builder().fields(2).pageSize(4).build();
        final List<Long> handles = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            final long h = store.allocate();
            store.set(h, 1, 100 + i);
            handles.add(h);
        }
        store.free(handles.get(4));
        final int capacity = store.capacity();
        store.clear();
        assertEquals(0, store.size());
        assertEquals(capacity, store.capacity());
        store.integrityCheck();
        for (long h : handles) {
            assertFalse(store.isValid(h));
            assertThrows(IllegalArgumentException.class, () -> store.get(h, 0));
        }
        // the slots in the order of a new store, under new generations, zeroed
        for (int i = 0; i < 20; i++) {
            final long h = store.allocate();
            assertEquals(RecordStore.index(fresh.allocate()), RecordStore.index(h));
            assertEquals(0, store.get(h, 1));
            if (i < handles.size()) {
                assertNotEquals(handles.get(i).longValue(), h);
            }
        }
        store.integrityCheck();
    }

    @Test
    public void ensureCapacityAddsPages() {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).build();
        store.ensureCapacity(0);
        assertEquals(0, store.capacity());
        store.ensureCapacity(9);
        assertEquals(11, store.capacity());
        store.ensureCapacity(5);
        assertEquals(11, store.capacity());
        for (int i = 0; i < 11; i++) {
            store.allocate();
        }
        assertEquals(11, store.capacity());
        assertThrows(IllegalArgumentException.class, () -> store.ensureCapacity(-1));
        assertThrows(IllegalArgumentException.class, () -> store.ensureCapacity(RecordStore.MAX_CAPACITY + 1));
        store.integrityCheck();
    }

    @Test
    public void slotIsRetiredAfterItsLastGeneration() throws Exception {
        final RecordStore store = RecordStore.builder().fields(1).pageSize(4).build();
        final long first = store.allocate();
        // jump the slot to the generation before the last one: freed, it is reused once more
        final long beforeLast = withGeneration(store, first, 0xFFFF_FFFBL);
        store.set(beforeLast, 0, 5);
        assertFalse(store.isValid(first));
        store.free(beforeLast);
        final long last = store.allocate();
        assertEquals(RecordStore.index(first), RecordStore.index(last));
        assertEquals(0xFFFF_FFFDL, last >>> 32);
        assertEquals(0, store.get(last, 0));
        store.integrityCheck();

        store.free(last);
        assertFalse(store.isValid(last));
        assertThrows(IllegalArgumentException.class, () -> store.get(last, 0));
        store.integrityCheck();
        // never allocated again: neither now nor after a clear
        final long next = store.allocate();
        assertNotEquals(RecordStore.index(first), RecordStore.index(next));
        store.clear();
        store.integrityCheck();
        for (int i = 0; i < 10; i++) {
            assertNotEquals(RecordStore.index(first), RecordStore.index(store.allocate()));
        }
        store.integrityCheck();
    }

    /**
     * Rewrites the header of a live record to another generation - instead of 2^31 allocations and frees.
     */
    private static long withGeneration(RecordStore store, long handle, long generation) throws Exception {
        final Field pagesField = RecordStore.class.getDeclaredField("pages");
        pagesField.setAccessible(true);
        final long[][] pages = (long[][]) pagesField.get(store);
        final Field shiftField = RecordStore.class.getDeclaredField("pageShift");
        shiftField.setAccessible(true);
        final int shift = shiftField.getInt(store);
        final int index = RecordStore.index(handle);
        final int stride = store.fields() + 1;
        final long newHandle = generation << 32 | index;
        pages[index >>> shift][(index & ((1 << shift) - 1)) * stride] = newHandle;
        return newHandle;
    }

    @Test
    public void builderRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class, () -> RecordStore.builder().build());
        assertThrows(IllegalArgumentException.class, () -> RecordStore.builder().fields(0).build());
        assertThrows(IllegalArgumentException.class, () -> RecordStore.builder().fields(1).pageSize(0).build());
        assertThrows(IllegalArgumentException.class, () -> RecordStore.builder().fields(1).pageSize(3).build());
        assertThrows(IllegalArgumentException.class,
                () -> RecordStore.builder().fields(1).pageSize(RecordStore.MAX_PAGE_SIZE * 2).build());
        assertThrows(IllegalArgumentException.class,
                () -> RecordStore.builder().fields(Integer.MAX_VALUE).pageSize(1).build());
        assertThrows(IllegalArgumentException.class,
                () -> RecordStore.builder().fields(4096).pageSize(RecordStore.MAX_PAGE_SIZE).build());
        assertThrows(IllegalArgumentException.class, () -> RecordStore.builder().fields(1).maxCapacity(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> RecordStore.builder().fields(1).maxCapacity(RecordStore.MAX_CAPACITY + 1).build());
        assertThrows(IllegalArgumentException.class, () -> RecordStore.builder().fields(1).expectedSize(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> RecordStore.builder().fields(1).maxCapacity(10).expectedSize(11).build());
        // the largest page of one record each
        assertEquals(1, RecordStore.builder().fields(1).pageSize(1).build().fields());
    }

    @Test
    public void pageOfOneRecord() {
        final RecordStore store = RecordStore.builder().fields(2).pageSize(1).build();
        final List<Long> handles = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final long h = store.allocate();
            store.set(h, 1, i);
            handles.add(h);
        }
        assertEquals(50, store.capacity());
        for (int i = 0; i < 50; i++) {
            assertEquals(i, store.get(handles.get(i), 1));
        }
        store.integrityCheck();
    }

    /**
     * Random operations against a model - live handles with their fields, and handles freed before - with the
     * integrity check along the way.
     */
    @Test
    public void randomOperationsMatchTheModel() {
        for (int seed = 0; seed < 40; seed++) {
            runAgainstModel(seed);
        }
    }

    private static void runAgainstModel(long seed) {
        final Random rand = new Random(seed);
        final int fields = 1 + rand.nextInt(4);
        final int pageSize = 1 << rand.nextInt(6);
        final boolean limited = rand.nextBoolean();
        final RecordStore.Builder builder = RecordStore.builder().fields(fields).pageSize(pageSize)
                .expectedSize(rand.nextInt(20));
        final int maxCapacity = 1 + rand.nextInt(200);
        if (limited) {
            builder.maxCapacity(Math.max(maxCapacity, 20));
        }
        final RecordStore store = builder.build();
        final Map<Long, long[]> live = new HashMap<>();
        final List<Long> liveList = new ArrayList<>();
        final List<Long> stale = new ArrayList<>();
        final Set<Long> staleSet = new HashSet<>();
        final String context = "seed=" + seed + " fields=" + fields + " pageSize=" + pageSize;

        for (int op = 0; op < 20_000; op++) {
            final int choice = rand.nextInt(100);
            if (choice < 30 || liveList.isEmpty()) {
                if (limited && store.size() == store.maxCapacity()) {
                    assertThrows(context, IllegalStateException.class, store::allocate);
                } else {
                    final long h = store.allocate();
                    assertFalse(context, live.containsKey(h));
                    assertFalse(context, staleSet.contains(h));
                    live.put(h, new long[fields]);
                    liveList.add(h);
                }
            } else if (choice < 50) {
                final long h = liveList.remove(rand.nextInt(liveList.size()));
                live.remove(h);
                store.free(h);
                stale.add(h);
                staleSet.add(h);
            } else if (choice < 75) {
                final long h = liveList.get(rand.nextInt(liveList.size()));
                final int f = rand.nextInt(fields);
                final long value = rand.nextLong();
                store.set(h, f, value);
                live.get(h)[f] = value;
            } else if (choice < 85) {
                final long h = liveList.get(rand.nextInt(liveList.size()));
                final int f = rand.nextInt(fields);
                final long delta = rand.nextInt(2001) - 1000;
                live.get(h)[f] += delta;
                assertEquals(context, live.get(h)[f], store.addAndGet(h, f, delta));
            } else if (choice < 97) {
                final long h = liveList.get(rand.nextInt(liveList.size()));
                final int f = rand.nextInt(fields);
                assertEquals(context, live.get(h)[f], store.get(h, f));
            } else if (choice < 99 && !stale.isEmpty()) {
                final long h = stale.get(rand.nextInt(stale.size()));
                assertFalse(context, store.isValid(h));
                assertThrows(context, IllegalArgumentException.class, () -> store.get(h, 0));
                assertThrows(context, IllegalArgumentException.class, () -> store.set(h, 0, 1));
            } else if (rand.nextInt(20) == 0) {
                store.clear();
                stale.addAll(liveList);
                staleSet.addAll(liveList);
                live.clear();
                liveList.clear();
            }
            assertEquals(context, live.size(), store.size());
            if (op % 1000 == 0) {
                store.integrityCheck();
            }
        }
        store.integrityCheck();
        for (Map.Entry<Long, long[]> e : live.entrySet()) {
            for (int f = 0; f < fields; f++) {
                assertEquals(context, e.getValue()[f], store.get(e.getKey(), f));
            }
        }
        final List<Long> visited = new ArrayList<>();
        store.forEach(visited::add);
        assertEquals(context, live.size(), visited.size());
        assertTrue(context, live.keySet().containsAll(visited));
    }
}
