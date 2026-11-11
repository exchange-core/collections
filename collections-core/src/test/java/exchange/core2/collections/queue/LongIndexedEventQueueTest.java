package exchange.core2.collections.queue;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.junit.Assert.*;

public class LongIndexedEventQueueTest {

    @Test
    public void sequenceExhaustionIsAtomicAndRecoversOnDrainOrClear() throws ReflectiveOperationException {
        final var q = new LongIndexedEventQueue(8);
        q.schedule(11, 30, 2);
        // Reach the boundary without a test hook in production or 2^63 insertions.
        final var counter = LongIndexedEventQueue.class.getDeclaredField("sequence");
        counter.setAccessible(true);
        counter.setLong(q, Long.MAX_VALUE - 1);
        assertTrue(q.schedule(22, 30, 2)); // the last sequence is usable
        assertThrows(IllegalStateException.class, () -> q.schedule(33, 1, 0));
        assertThrows(IllegalStateException.class, () -> q.reschedule(11, 90, 3));
        assertFalse(q.schedule(22, 0, 0)); // duplicate / absent still follow their ordinary contracts
        assertFalse(q.reschedule(33, 0, 0));
        assertFalse(q.containsId(33));
        assertEquals(2, q.size());
        assertEquals(30, q.firstTime());
        assertEquals(2, q.firstPriority());
        assertEquals(11, q.removeFirst()); // the refused reschedule did not change tie order
        assertEquals(22, q.removeFirst());
        assertTrue(q.schedule(33, 0, 0)); // draining reset the counter

        counter.setLong(q, Long.MAX_VALUE - 1);
        assertTrue(q.reschedule(33, 1, 1));
        assertThrows(IllegalStateException.class, () -> q.schedule(44, 0, 0));
        q.clear();
        assertTrue(q.schedule(44, 0, 0));
        assertEquals(44, q.removeFirst());
    }

    @Test
    public void signedTimeThenSignedPriorityThenArrival() {
        final var q = new LongIndexedEventQueue(0);
        q.schedule(0, Long.MAX_VALUE, Integer.MIN_VALUE);
        q.schedule(Long.MAX_VALUE, Long.MIN_VALUE, Integer.MAX_VALUE);
        q.schedule(41, Long.MIN_VALUE, Integer.MIN_VALUE);
        q.schedule(Long.MIN_VALUE, Long.MIN_VALUE, Integer.MIN_VALUE);
        assertFalse(q.schedule(Long.MIN_VALUE, 100, 1));
        assertEquals(41, q.firstId());
        assertEquals(Long.MIN_VALUE, q.firstTime());
        assertEquals(Integer.MIN_VALUE, q.firstPriority());
        assertTrue(q.reschedule(41, Long.MIN_VALUE, Integer.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, q.removeFirst());
        assertEquals(41, q.removeFirst());
        assertEquals(Long.MAX_VALUE, q.removeFirst());
        assertEquals(0, q.removeFirst());
        assertTrue(q.isEmpty());
        assertThrows(NoSuchElementException.class, q::firstId);
        assertThrows(NoSuchElementException.class, q::firstTime);
        assertThrows(NoSuchElementException.class, q::firstPriority);
        assertThrows(NoSuchElementException.class, q::removeFirst);
    }

    @Test
    public void exactCapacityDuplicatesAndReuse() {
        final var q = LongIndexedEventQueue.fixedCapacity(2);
        q.schedule(17, 1, 0);
        q.schedule(53, 1, 0);
        assertFalse(q.schedule(53, 0, 1));
        assertThrows(IllegalStateException.class, () -> q.schedule(25, 1, 0));
        assertFalse(q.containsId(25));
        assertEquals(17, q.firstId());
        assertTrue(q.reschedule(53, -2, 1));
        assertEquals(53, q.firstId());
        assertTrue(q.cancel(53));
        assertFalse(q.cancel(53));
        assertFalse(q.reschedule(53, 9, 0));
        assertTrue(q.schedule(53, 1, 0));
        assertEquals(17, q.removeFirst());
        q.clear();
        assertEquals(0, q.size());
        assertFalse(q.containsId(53));
        assertTrue(q.schedule(53, 2, 0));
        assertEquals(53, q.removeFirst());
        assertThrows(IllegalStateException.class, () -> LongIndexedEventQueue.fixedCapacity(0).schedule(0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new LongIndexedEventQueue(-1));
    }

    @Test
    public void burstSurvivesGrowthCancellationAndReties() {
        final var q = new LongIndexedEventQueue(1);
        // More than one record page; all events share a time and priority. Retie in reverse ID order.
        for (int i = 0; i < 4096; i++) q.schedule(i, 50, 2);
        for (int i = 0; i < 4096; i += 2) assertTrue(q.cancel(i));
        for (int i = 4093; i >= 1; i -= 4) assertTrue(q.reschedule(i, 50, 2));
        for (int i = 3; i < 4096; i += 4) assertEquals(i, q.removeFirst());
        for (int i = 4093; i >= 1; i -= 4) assertEquals(i, q.removeFirst());
        assertTrue(q.isEmpty());
    }

    private record Deadline(long time, int priority) {}

    // Iterate in arrival order and keep the first minimum. No heap, backpointers or sequence counters.
    private static long first(LinkedHashMap<Long, Deadline> model) {
        Map.Entry<Long, Deadline> best = null;
        for (var next : model.entrySet()) {
            final Deadline key = next.getValue();
            if (best == null || key.time < best.getValue().time ||
                    (key.time == best.getValue().time && key.priority < best.getValue().priority)) {
                best = next;
            }
        }
        return best.getKey();
    }

    @Test
    public void mixedOperationsMatchIndependentModel() {
        final var q = new LongIndexedEventQueue(3);
        final var model = new LinkedHashMap<Long, Deadline>();
        final var random = new Random(9127723);
        for (int i = 0; i < 60_000; i++) {
            final long id = random.nextInt(256) - 128;
            final long time = random.nextBoolean() ? random.nextInt(8) : random.nextLong();
            final int priority = random.nextInt(5) - 2;
            final Deadline deadline = new Deadline(time, priority);
            switch (random.nextInt(10)) {
                case 0, 1, 2, 3 -> assertEquals(model.putIfAbsent(id, deadline) == null, q.schedule(id, time, priority));
                case 4, 5, 6 -> {
                    final boolean found = model.remove(id) != null;
                    if (found) model.put(id, deadline);
                    assertEquals(found, q.reschedule(id, time, priority));
                }
                case 7, 8 -> assertEquals(model.remove(id) != null, q.cancel(id));
                default -> {
                    if (!model.isEmpty()) {
                        final long head = first(model);
                        assertEquals(head, q.removeFirst());
                        model.remove(head);
                    }
                }
            }
            assertEquals(model.size(), q.size());
            assertEquals(model.containsKey(id), q.containsId(id));
            if (!model.isEmpty()) {
                final long head = first(model);
                assertEquals(head, q.firstId());
                assertEquals(model.get(head).time, q.firstTime());
                assertEquals(model.get(head).priority, q.firstPriority());
            }
        }
        while (!model.isEmpty()) {
            final long head = first(model);
            assertEquals(head, q.removeFirst());
            model.remove(head);
        }
        assertTrue(q.isEmpty());
    }
}
