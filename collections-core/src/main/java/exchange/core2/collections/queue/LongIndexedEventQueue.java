package exchange.core2.collections.queue;

import exchange.core2.collections.hashtable.LongRecordTable;
import exchange.core2.collections.records.RecordStore;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Events ordered by signed time, then signed priority (smaller first), then insertion order.
 * IDs are unique; all long IDs and times are valid. Rescheduling counts as a fresh insertion for ties.
 * No clock is consulted. Single-threaded.
 *
 * <p>A four-way min-heap holds ordering keys alongside stable handles. Each indexed record stores its
 * heap position. Cancellation by ID removes the index entry once, and moving elements needs no ID lookups.
 * Peeking takes O(1); insertion, removal and rescheduling take O(log n) heap work plus the ID index
 * operation. Growing storage can resize on the calling thread; use {@link #fixedCapacity} to preallocate.
 */
public final class LongIndexedEventQueue {

    // One-based positions leave zero available to identify a newly allocated ID record.
    private static final int POSITION = 0;
    private final LongRecordTable events;
    private final long[] removedPosition = new long[1];
    private final boolean fixed;
    private long[] heap;
    private long[] times;
    private int[] priorities;
    private long[] sequences;
    // Reset when empty, so the counter never wraps past a sequence still in use.
    private long sequence;

    /** Preallocate for expectedSize live events and grow as needed. */
    public LongIndexedEventQueue(int expectedSize) {
        this(expectedSize, false);
    }

    /** Preallocate for exactly capacity live events. Zero is allowed. */
    public static LongIndexedEventQueue fixedCapacity(int capacity) {
        return new LongIndexedEventQueue(capacity, true);
    }

    private LongIndexedEventQueue(int expectedSize, boolean fixed) {
        final var builder = LongRecordTable.builder().fields(1).expectedSize(expectedSize);
        this.events = (fixed ? builder.fixedCapacity() : builder).build();
        this.heap = new long[expectedSize];
        this.times = new long[expectedSize];
        this.priorities = new int[expectedSize];
        this.sequences = new long[expectedSize];
        this.fixed = fixed;
    }

    /**
     * Returns false for a live ID, leaving its original time, priority and sequence unchanged, even if full.
     * @throws IllegalStateException if the new event does not fit or the insertion sequence is exhausted
     */
    public boolean schedule(long id, long time, int priority) {
        final int n = events.size();
        if (n == heap.length || sequence == Long.MAX_VALUE) {
            if (events.containsKey(id)) {
                return false;
            }
            checkSequence();
            if (fixed || n == RecordStore.MAX_CAPACITY) {
                throw new IllegalStateException("event queue is full: " + n);
            }
            grow((int) Math.min(RecordStore.MAX_CAPACITY, Math.max(8, 2L * n)));
        }
        final long event = events.getOrCreate(id);
        if (events.get(event, POSITION) != 0) {
            return false;
        }
        siftUp(n, event, time, priority, ++sequence);
        return true;
    }

    private void grow(int capacity) {
        final long[] newHeap = Arrays.copyOf(heap, capacity);
        final long[] newTimes = Arrays.copyOf(times, capacity);
        final int[] newPriorities = Arrays.copyOf(priorities, capacity);
        sequences = Arrays.copyOf(sequences, capacity);
        heap = newHeap;
        times = newTimes;
        priorities = newPriorities;
    }

    /** Returns false if absent. Even an unchanged time and priority move the event behind its peers. */
    public boolean reschedule(long id, long time, int priority) {
        final long event = events.find(id);
        if (event == LongRecordTable.NO_RECORD) {
            return false;
        }
        checkSequence();
        repair((int) events.get(event, POSITION) - 1, event, time, priority, ++sequence);
        return true;
    }

    private void checkSequence() {
        if (sequence == Long.MAX_VALUE) {
            throw new IllegalStateException("event sequence exhausted; drain or clear the queue");
        }
    }

    /** Removes the event immediately, freeing its capacity. Returns false if absent. */
    public boolean cancel(long id) {
        if (!events.removeAndCopyFields(id, removedPosition)) {
            return false;
        }
        fillHole((int) removedPosition[0] - 1);
        return true;
    }

    /** Returns the next event's ID; throws NoSuchElementException if empty. */
    public long firstId() {
        requireHead();
        return events.keyOf(heap[0]);
    }

    /** Returns the next event's time; throws NoSuchElementException if empty. */
    public long firstTime() {
        requireHead();
        return times[0];
    }

    /** Returns the next event's priority; throws NoSuchElementException if empty. */
    public int firstPriority() {
        requireHead();
        return priorities[0];
    }

    /** Removes the next event and returns its ID; throws NoSuchElementException if empty. */
    public long removeFirst() {
        final long id = firstId();
        events.remove(id);
        fillHole(0);
        return id;
    }

    private void requireHead() {
        if (isEmpty()) {
            throw new NoSuchElementException("empty event queue");
        }
    }

    // The record is already removed. The heap's old last element is at the new size.
    private void fillHole(int pos) {
        final int last = events.size();
        if (pos != last) {
            repair(pos, heap[last], times[last], priorities[last], sequences[last]);
        }
        if (last == 0) {
            sequence = 0;
        }
    }

    private boolean before(long time, int priority, long seq, int other) {
        if (time != times[other]) {
            return time < times[other];
        }
        if (priority != priorities[other]) {
            return priority < priorities[other];
        }
        return seq < sequences[other];
    }

    private void place(int pos, long event, long time, int priority, long seq) {
        heap[pos] = event;
        times[pos] = time;
        priorities[pos] = priority;
        sequences[pos] = seq;
        events.set(event, POSITION, pos + 1);
    }

    private void move(int from, int to) {
        place(to, heap[from], times[from], priorities[from], sequences[from]);
    }

    private void siftUp(int pos, long event, long time, int priority, long seq) {
        while (pos > 0) {
            final int parent = (pos - 1) >>> 2;
            if (!before(time, priority, seq, parent)) {
                break;
            }
            move(parent, pos);
            pos = parent;
        }
        place(pos, event, time, priority, seq);
    }

    private void repair(int pos, long event, long time, int priority, long seq) {
        if (pos > 0 && before(time, priority, seq, (pos - 1) >>> 2)) {
            siftUp(pos, event, time, priority, seq);
            return;
        }
        final int n = events.size();
        final int parents = (n + 2) >>> 2;
        while (pos < parents) {
            int child = 4 * pos + 1;
            final int end = Math.min(child + 4, n);
            for (int i = child + 1; i < end; i++) {
                if (before(times[i], priorities[i], sequences[i], child)) {
                    child = i;
                }
            }
            if (before(time, priority, seq, child)) {
                break;
            }
            move(child, pos);
            pos = child;
        }
        place(pos, event, time, priority, seq);
    }

    public boolean containsId(long id) {
        return events.containsKey(id);
    }

    public int size() {
        return events.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /** Drops all events, retains capacity and restarts the insertion sequence. */
    public void clear() {
        events.clear();
        sequence = 0;
    }
}
