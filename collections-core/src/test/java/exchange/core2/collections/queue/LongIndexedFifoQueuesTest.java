package exchange.core2.collections.queue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.junit.Assert.*;

public class LongIndexedFifoQueuesTest {

    @Test
    public void queueAndEntryHandlesCannotBeInterchanged() {
        final var book = LongIndexedFifoQueues.fixedCapacity(1, 1);
        final long level = book.getOrCreateQueue(10);
        final long order = book.appendTo(level, 20, 30);
        assertNotEquals(level, order);
        assertThrows(IllegalArgumentException.class, () -> book.first(order));
        assertThrows(IllegalArgumentException.class, () -> book.queueKey(order));
        assertThrows(IllegalArgumentException.class, () -> book.removeFirstByHandle(order));
        assertThrows(IllegalArgumentException.class, () -> book.releaseEmptyQueue(order));
        assertThrows(IllegalArgumentException.class, () -> book.unretainQueue(order));
        assertThrows(IllegalArgumentException.class, () -> book.appendTo(order, 21, 0));
        assertThrows(IllegalArgumentException.class, () -> book.moveToLast(order, order));
        assertThrows(IllegalArgumentException.class, () -> book.moveToLast(level, level));
        assertThrows(IllegalArgumentException.class, () -> book.next(level));
        assertThrows(IllegalArgumentException.class, () -> book.idOf(level));
        assertThrows(IllegalArgumentException.class, () -> book.queueOf(level));
        assertThrows(IllegalArgumentException.class, () -> book.removeEntry(level));
        assertThrows(IllegalArgumentException.class, () -> book.getField(level, 0));
        assertThrows(IllegalArgumentException.class, () -> book.setField(level, 0, 99));
        assertEquals(level, book.queueOf(order));
        assertEquals(order, book.first(level));
        assertEquals(30, book.removeEntry(order));
        assertTrue(book.releaseEmptyQueue(level));
    }

    @Test
    public void repricedLevelsCanReturnToAutomaticRecycling() {
        final var book = LongIndexedFifoQueues.fixedCapacity(2, 1);
        book.append(0, 1, 100);
        final long order = book.findEntry(1);
        for (int price = 1; price <= 100; price++) {
            final long previous = book.queueOf(order);
            final long level = book.getOrCreateQueue(price);
            book.moveToLast(order, level);
            book.unretainQueue(level);
            book.unretainQueue(level);
            assertThrows(IllegalArgumentException.class, () -> book.first(previous));
            assertEquals(1, book.queueCount());
            assertEquals(order, book.first(level));
        }
        book.removeEntry(order);
        assertEquals(0, book.queueCount());
        final long empty = book.getOrCreateQueue(101);
        book.unretainQueue(empty);
        assertEquals(0, book.findQueue(101));
        assertThrows(IllegalArgumentException.class, () -> book.first(empty));
    }

    @Test
    public void checkedCancelAndWalkAcrossRetainedLevelReuse() {
        final var b = LongIndexedFifoQueues.fixedCapacity(1, 3, 3);
        final long q = b.getOrCreateQueue(0);
        final long a = b.appendTo(q, 0, Long.MIN_VALUE);
        final long mid = b.appendTo(q, 2, 10);
        final long c = b.appendTo(q, 3, 80);
        assertEquals(0, b.appendTo(q, 3, 99)); // full, but duplicate has precedence
        b.setField(mid, 1, 4);
        b.setField(mid, 2, 123);
        final long found = b.findEntry(2);
        assertEquals(mid, found);
        assertEquals(123, b.getField(found, 2));
        assertEquals(10, b.valueOf(2));
        assertFalse(b.releaseEmptyQueue(q));
        assertEquals(10, b.removeEntry(found));
        assertEquals(c, b.next(a));
        assertEquals(Long.MIN_VALUE, b.remove(0, -1));
        assertEquals(-1, b.remove(0, -1));
        assertEquals(80, b.removeFirstByHandle(q));
        assertEquals(0, b.first(q));
        assertEquals(q, b.findQueue(0));
        assertThrows(NoSuchElementException.class, () -> b.firstValue(0));
        assertThrows(NoSuchElementException.class, () -> b.removeFirstByHandle(q));
        assertThrows(NoSuchElementException.class, () -> b.valueOf(2));
        assertThrows(IllegalStateException.class, () -> b.append(4, 99, 1));
        assertEquals(0, b.findEntry(99));
        assertTrue(b.releaseEmptyQueue(q));
        final long newQ = b.getOrCreateQueue(7);
        assertNotEquals(q, newQ);
        final long reused = b.appendTo(newQ, 2, 5);
        assertEquals(0, b.getField(reused, 1));
        assertEquals(0, b.getField(reused, 2));
        assertThrows(IllegalArgumentException.class, () -> b.next(mid));
        assertThrows(IllegalArgumentException.class, () -> b.moveToLast(reused, q));
        assertThrows(IllegalArgumentException.class, () -> b.appendTo(q, 98, 1));
        assertEquals(0, b.findEntry(98));
        assertEquals(reused, b.first(newQ));
        assertThrows(IndexOutOfBoundsException.class, () -> b.setField(reused, -1, 8));
        assertThrows(IndexOutOfBoundsException.class, () -> b.setField(reused, 3, 8));
        assertEquals(7, b.queueKey(b.queueOf(reused)));
        b.clear();
        assertThrows(IllegalArgumentException.class, () -> b.first(newQ));
        assertThrows(IllegalArgumentException.class, () -> b.removeEntry(reused));
    }

    @Test
    public void movingLastOrderReleasesOnlyAnAutomaticSource() {
        final var b = new LongIndexedFifoQueues(1, 1);
        b.append(11, 101, 1001);
        final long auto = b.findQueue(11), e = b.findEntry(101);
        final long keep = b.getOrCreateQueue(12);
        b.moveToLast(e, keep);
        assertEquals(0, b.findQueue(11));
        assertThrows(IllegalArgumentException.class, () -> b.first(auto));
        assertEquals(e, b.first(keep));
        b.append(13, 103, 1003);
        final long retained = b.getOrCreateQueue(13); // retain an existing automatic level
        b.moveToLast(b.findEntry(103), keep);
        assertEquals(0, b.first(retained));
        assertEquals(e, b.first(keep));
        b.moveToLast(e, keep);
        assertEquals(103, b.firstId(12));
        assertEquals(1001, b.valueOf(101));
        assertEquals(e, b.next(b.first(keep)));
        assertEquals(0, b.next(e));
        assertTrue(b.releaseEmptyQueue(retained));
        assertEquals(1, b.queueCount());
    }

    @Test
    public void handlesAndInlineFieldsSurviveMixedMovesAndCancellations() {
        final var b = new LongIndexedFifoQueues(0, 0, 3);
        final var rng = new Random(8837);
        final long[] queues = new long[8], handles = new long[96];
        final int[] owner = new int[96];
        final List<List<Integer>> order = new ArrayList<>();
        for (int j = 0; j < queues.length; j++) {
            queues[j] = b.getOrCreateQueue(j);
            order.add(new ArrayList<>());
        }
        for (int i = 0; i < 30_000; i++) {
            int id = rng.nextInt(handles.length), j = rng.nextInt(queues.length);
            long h = handles[id];
            if (h == 0) {
                handles[id] = b.appendTo(queues[j], id, ~id);
                b.setField(handles[id], 1, 2L * id);
                b.setField(handles[id], 2, -3L * id);
                order.get(j).add(id);
                owner[id] = j;
            } else if (rng.nextInt(4) == 0) {
                assertEquals(~id, b.remove(id, 1234));
                order.get(owner[id]).remove(Integer.valueOf(id));
                handles[id] = 0;
            } else {
                b.moveToLast(h, queues[j]);
                order.get(owner[id]).remove(Integer.valueOf(id));
                order.get(j).add(id);
                owner[id] = j;
                assertEquals(h, b.findEntry(id));
            }
            if (i % 71 != 0) continue;
            for (int k = 0; k < queues.length; k++) {
                long e = b.first(queues[k]);
                for (int expected : order.get(k)) {
                    assertEquals(handles[expected], e);
                    assertEquals(expected, b.idOf(e));
                    assertEquals(queues[k], b.queueOf(e));
                    assertEquals(~expected, b.getField(e, 0));
                    assertEquals(2L * expected, b.getField(e, 1));
                    assertEquals(-3L * expected, b.getField(e, 2));
                    e = b.next(e);
                }
                assertEquals(0, e);
                assertEquals(order.get(k).size(), b.size(k));
            }
        }
    }

    @Test
    public void cancelByIdPreservesPriorityAcrossGrowthAndSlotReuse() {
        final LongIndexedFifoQueues book = new LongIndexedFifoQueues(0, 0);
        book.append(0, 0, Long.MAX_VALUE);
        book.append(0, Long.MIN_VALUE, 1);
        book.append(0, Long.MAX_VALUE, Long.MIN_VALUE);

        // Force index resizes and more than one record page in both stores while level zero stays live.
        for (int i = 1; i <= 2048; i++) {
            assertTrue(book.append(i, i, ~i));
        }
        for (int i = 2048; i > 0; i--) {
            assertEquals(~i, book.removeFirst(i));
        }
        assertFalse(book.append(1, 0, 42)); // globally unique; neither creates a level nor changes the value
        assertEquals(1, book.queueCount());
        assertTrue(book.remove(Long.MIN_VALUE));
        assertTrue(book.append(0, Long.MIN_VALUE, 0)); // same ID, new tail
        assertEquals(0, book.firstId(0));
        assertEquals(Long.MAX_VALUE, book.removeFirst(0));
        assertEquals(Long.MAX_VALUE, book.firstId(0));
        assertEquals(Long.MIN_VALUE, book.removeFirst(0));
        assertEquals(0, book.removeFirst(0));
        assertEquals(0, book.queueCount());
        assertThrows(NoSuchElementException.class, () -> book.firstId(0));
        assertThrows(NoSuchElementException.class, () -> book.firstValue(0));
        assertThrows(NoSuchElementException.class, () -> book.removeFirst(0));
    }

    @Test
    public void refusingEitherCapacityLimitLeavesTheBookUsable() {
        final LongIndexedFifoQueues book = LongIndexedFifoQueues.fixedCapacity(1, 2);
        assertTrue(book.append(1, 0, 10));
        assertThrows(IllegalStateException.class, () -> book.append(2, 1, 20)); // only the levels are full
        assertFalse(book.containsId(1));
        assertEquals(1, book.size());
        assertTrue(book.append(1, 1, 30));
        assertFalse(book.append(2, 0, 99)); // duplicate takes precedence over both limits
        assertThrows(IllegalStateException.class, () -> book.append(1, 2, 40));
        assertEquals(2, book.size(1));
        assertEquals(0, book.size(2));
        assertEquals(10, book.removeFirst(1));
        assertEquals(30, book.removeFirst(1));
        assertTrue(book.append(2, 2, 40));
        book.clear();
        assertTrue(book.append(3, 2, 50));
        assertEquals(50, book.firstValue(3));
        assertThrows(IllegalStateException.class, () -> LongIndexedFifoQueues.fixedCapacity(0, 2).append(0, 0, 0));
        assertThrows(IllegalStateException.class, () -> LongIndexedFifoQueues.fixedCapacity(2, 0).append(0, 0, 0));
    }

    @Test
    public void walkDetectsRemovalAndReuseOfTheCurrentRecord() {
        final LongIndexedFifoQueues book = new LongIndexedFifoQueues(1, 1);
        book.append(0, 0, 17);
        assertThrows(ConcurrentModificationException.class, () -> book.forEach(0, (id, value) -> {
            book.remove(id);
            book.append(1, id, value);
        }));
        assertEquals(17, book.removeFirst(1));
    }

    @Test
    public void mixedOperationsMatchIndependentLinkedHashMaps() {
        final Random rng = new Random(8320021);
        final LongIndexedFifoQueues book = new LongIndexedFifoQueues(1, 1);
        final Map<Long, LinkedHashMap<Long, Long>> levels = new HashMap<>();
        final Map<Long, Long> owner = new HashMap<>();
        for (int i = 0; i < 40_000; i++) {
            final long level = rng.nextInt(17) - 8;
            final long id = rng.nextInt(256) - 128;
            final LinkedHashMap<Long, Long> q = levels.get(level);
            switch (rng.nextInt(10)) {
                case 0, 1, 2, 3, 4 -> {
                    final long value = rng.nextLong();
                    final boolean fresh = owner.putIfAbsent(id, level) == null;
                    if (fresh) levels.computeIfAbsent(level, k -> new LinkedHashMap<>()).put(id, value);
                    assertEquals(fresh, book.append(level, id, value));
                }
                case 5, 6, 7 -> {
                    final Long price = owner.remove(id);
                    assertEquals(price != null, book.remove(id));
                    if (price != null) {
                        final Map<Long, Long> queue = levels.get(price);
                        queue.remove(id);
                        if (queue.isEmpty()) levels.remove(price);
                    }
                }
                default -> {
                    if (q != null) {
                        final Map.Entry<Long, Long> head = q.entrySet().iterator().next();
                        assertEquals(head.getKey().longValue(), book.firstId(level));
                        assertEquals(head.getValue().longValue(), book.firstValue(level));
                        assertEquals(head.getValue().longValue(), book.removeFirst(level));
                        owner.remove(head.getKey());
                        q.remove(head.getKey());
                        if (q.isEmpty()) levels.remove(level);
                    }
                }
            }
            assertEquals(owner.size(), book.size());
            assertEquals(levels.size(), book.queueCount());
            assertEquals(owner.containsKey(id), book.containsId(id));
            if (i % 101 == 0) {
                for (long price = -8; price <= 8; price++) {
                    final Map<Long, Long> queue = levels.getOrDefault(price, new LinkedHashMap<>());
                    final List<Map.Entry<Long, Long>> actual = new ArrayList<>();
                    book.forEach(price, (k, v) -> actual.add(Map.entry(k, v)));
                    assertEquals(queue.size(), book.size(price));
                    assertEquals(new ArrayList<>(queue.entrySet()), actual);
                }
            }
        }
    }
}
