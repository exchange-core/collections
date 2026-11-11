package exchange.core2.collections.queue;

import exchange.core2.collections.hashtable.LongLongConsumer;
import exchange.core2.collections.hashtable.LongRecordTable;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Objects;

import static exchange.core2.collections.hashtable.LongRecordTable.NO_RECORD;

/**
 * FIFO queues identified by long keys, with one index of entry IDs across all queues. For example, a side of an
 * order book can use price as the queue key, order ID as the entry ID and a handle to the order as the value.
 * Removing an ID needs neither the queue key nor a walk from its head. All long keys, IDs and values are supported.
 * Queues created by append are recycled when empty. An explicit {@link #getOrCreateQueue} retains a queue
 * until {@link #unretainQueue}, {@link #releaseEmptyQueue} or clear. Handles belong to this instance.
 * Passing an entry handle in place of a queue handle or vice versa throws {@link IllegalArgumentException}.
 * Zero is never a handle.
 *
 * <p>Two {@link LongRecordTable}s hold all the queues and their entries. Their stable handles link an entry to
 * its queue and its neighbours, independent of index resizes. Insertion and removal do a constant number of
 * hash operations and link updates; a growing index can rehash on the calling thread. Pre-size for the peak
 * number of allocated queues and live entries, or use {@link #fixedCapacity(int, int)}. Not thread-safe.
 */
public final class LongIndexedFifoQueues {

    public static final long NO_HANDLE = NO_RECORD;
    // RecordStore slots are at most 2^30. Tag only public queue handles, preserving the full generation.
    private static final long QUEUE_TAG = 1L << 31;
    private static final int HEAD = 0, TAIL = 1, COUNT = 2, RETAINED = 3;
    private static final int OWNER = 0, PREV = 1, NEXT = 2, VALUE = 3;

    private final LongRecordTable queues;
    private final LongRecordTable entries;
    private final int maxQueues, maxEntries;
    // The linked-list metadata and field zero; cancellation never copies the remaining application fields.
    private final long[] removed = new long[VALUE + 1];
    private long modCount;

    /** Pre-size both indexes and record stores; they grow when necessary. */
    public LongIndexedFifoQueues(int expectedQueues, int expectedEntries) {
        this(expectedQueues, expectedEntries, 1, false);
    }

    /** Fields per entry, including its original value (field zero). All additional fields start at zero. */
    public LongIndexedFifoQueues(int expectedQueues, int expectedEntries, int entryFields) {
        this(expectedQueues, expectedEntries, entryFields, false);
    }

    /** Preallocate for these exact limits (zero allowed). Successful operations never grow either table. */
    public static LongIndexedFifoQueues fixedCapacity(int maxQueues, int maxEntries) {
        return fixedCapacity(maxQueues, maxEntries, 1);
    }

    /** Preallocates both limits, with entryFields application fields per entry including the value at index zero. */
    public static LongIndexedFifoQueues fixedCapacity(int maxQueues, int maxEntries, int entryFields) {
        return new LongIndexedFifoQueues(maxQueues, maxEntries, entryFields, true);
    }

    private LongIndexedFifoQueues(int expectedQueues, int expectedEntries, int entryFields, boolean fixed) {
        if (entryFields < 1 || entryFields >= Integer.MAX_VALUE - VALUE) {
            throw new IllegalArgumentException("invalid entry field count: " + entryFields);
        }
        this.maxQueues = fixed ? expectedQueues : Integer.MAX_VALUE;
        this.maxEntries = fixed ? expectedEntries : Integer.MAX_VALUE;
        this.queues = table(4, expectedQueues, fixed);
        this.entries = table(VALUE + entryFields, expectedEntries, fixed);
    }

    private static LongRecordTable table(int fields, int expected, boolean fixed) {
        final LongRecordTable.Builder builder = LongRecordTable.builder().fields(fields).expectedSize(expected);
        return (fixed ? builder.fixedCapacity() : builder).build();
    }

    private static long getOrCreate(LongRecordTable table, long key, int limit) {
        if (table.size() == limit) {
            final long existing = table.find(key);
            if (existing == NO_RECORD) {
                throw new IllegalStateException("capacity exhausted: " + limit);
            }
            return existing;
        }
        return table.getOrCreate(key);
    }

    /**
     * Appends at the tail. Returns false if the ID already exists in any queue, preserving its value and priority.
     * @throws IllegalStateException if a new entry or queue does not fit; the queues are unchanged
     */
    public boolean append(long queueKey, long id, long value) {
        final long entry = getOrCreate(entries, id, maxEntries);
        // A fresh record has zeroed fields; a linked entry always has a nonzero queue handle.
        if (entries.get(entry, OWNER) != NO_RECORD) {
            return false;
        }
        final long queue;
        try {
            queue = getOrCreate(queues, queueKey, maxQueues);
        } catch (IllegalStateException full) {
            entries.remove(id);
            throw full;
        }
        entries.set(entry, VALUE, value);
        linkLast(queue, entry, queues.get(queue, TAIL));
        modCount++;
        return true;
    }

    /** Creates or retains a level for reuse, including when empty. Retention is idempotent, not reference counted. */
    public long getOrCreateQueue(long queueKey) {
        final long q = getOrCreate(queues, queueKey, maxQueues);
        if (queues.get(q, RETAINED) == 0) {
            queues.set(q, RETAINED, 1);
            modCount++;
        }
        return q | QUEUE_TAG;
    }

    /** A borrowed handle (zero if absent); does not retain an automatically managed queue. */
    public long findQueue(long queueKey) {
        final long queue = queues.find(queueKey);
        return queue == NO_RECORD ? NO_HANDLE : queue | QUEUE_TAG;
    }

    private static long queueRecord(long handle) {
        if ((handle & QUEUE_TAG) == 0) {
            throw new IllegalArgumentException("expected a queue handle");
        }
        return handle ^ QUEUE_TAG;
    }

    private static long entryRecord(long handle) {
        if ((handle & QUEUE_TAG) != 0) {
            throw new IllegalArgumentException("expected an entry handle");
        }
        return handle;
    }

    /** Restores automatic recycling. An empty level is released immediately, a live one when its last entry leaves. */
    public void unretainQueue(long queueHandle) {
        final long queue = queueRecord(queueHandle);
        if (queues.get(queue, COUNT) == 0) {
            queues.remove(queues.keyOf(queue));
        } else if (queues.get(queue, RETAINED) != 0) {
            queues.set(queue, RETAINED, 0);
        } else {
            return;
        }
        modCount++;
    }

    /** Releases an empty level; returns false for a nonempty level. A released handle becomes stale. */
    public boolean releaseEmptyQueue(long queueHandle) {
        final long queue = queueRecord(queueHandle);
        if (queues.get(queue, COUNT) != 0) {
            return false;
        }
        queues.remove(queues.keyOf(queue));
        modCount++;
        return true;
    }

    /** Appends via a live queue handle, returning the new entry handle or zero for a duplicate ID. */
    public long appendTo(long queueHandle, long id, long value) {
        final long queue = queueRecord(queueHandle);
        final long tail = queues.get(queue, TAIL); // validate before allocating
        final long entry = getOrCreate(entries, id, maxEntries);
        if (entries.get(entry, OWNER) != NO_RECORD) {
            return NO_HANDLE;
        }
        entries.set(entry, VALUE, value);
        linkLast(queue, entry, tail);
        modCount++;
        return entry;
    }

    private void linkLast(long queue, long entry, long tail) {
        entries.set(entry, OWNER, queue);
        entries.set(entry, PREV, tail);
        entries.set(entry, NEXT, NO_RECORD);
        if (tail == NO_RECORD) {
            queues.set(queue, HEAD, entry);
        } else {
            entries.set(tail, NEXT, entry);
        }
        queues.set(queue, TAIL, entry);
        queues.addAndGet(queue, COUNT, 1);
    }

    /** Cancels by entry ID alone. Returns false if absent. */
    public boolean remove(long id) {
        if (!entries.removeAndCopyFields(id, removed)) {
            return false;
        }
        detach(removed[OWNER], removed[PREV], removed[NEXT], true);
        modCount++;
        return true;
    }

    /** Cancels by ID in one ID lookup. Returns its value or the supplied default if absent. */
    public long remove(long id, long ifAbsent) {
        return remove(id) ? removed[VALUE] : ifAbsent;
    }

    /** Looks up the value by ID; throws NoSuchElementException if absent. */
    public long valueOf(long id) {
        final long entry = findEntry(id);
        if (entry == NO_HANDLE) {
            throw new NoSuchElementException("missing entry: " + id);
        }
        return entries.get(entry, VALUE);
    }

    /** Entry handle, or NO_HANDLE if the ID is absent. Repricing does not invalidate this handle. */
    public long findEntry(long id) {
        return entries.find(id);
    }

    /** ID of a live entry, without an index lookup. */
    public long idOf(long entry) {
        return entries.keyOf(entryRecord(entry));
    }

    /** Current queue handle of a live entry. */
    public long queueOf(long entry) {
        return entries.get(entryRecord(entry), OWNER) | QUEUE_TAG;
    }

    /** Key of a live queue, e.g. its price tick. */
    public long queueKey(long queue) {
        return queues.keyOf(queueRecord(queue));
    }

    /** First entry of a live queue, or zero if empty. */
    public long first(long queue) {
        return queues.get(queueRecord(queue), HEAD);
    }

    /** Next entry, or zero at the tail. Save it before removing or moving the current entry. */
    public long next(long entry) {
        return entries.get(entryRecord(entry), NEXT);
    }

    /** Number of application fields; field zero is the value used by the convenience methods. */
    public int entryFields() {
        return entries.fields() - VALUE;
    }

    /** Reads an application field of a live entry. Valid indices run from zero through entryFields() - 1. */
    public long getField(long entry, int field) {
        return entries.get(entryRecord(entry), VALUE + Objects.checkIndex(field, entryFields()));
    }

    /** Writes an application field of a live entry. Updating field zero changes the value returned on removal. */
    public void setField(long entry, int field, long value) {
        entries.set(entryRecord(entry), VALUE + Objects.checkIndex(field, entryFields()), value);
    }

    /**
     * Relinks the entry at the destination tail, preserving its ID, handle and fields. A handle from
     * {@link #findQueue} preserves the level's retention policy. If {@link #getOrCreateQueue} created a
     * destination for this move, call {@link #unretainQueue} after the move to recycle it when empty.
     */
    public void moveToLast(long entry, long destinationHandle) {
        final long destination = queueRecord(destinationHandle);
        final long source = entries.get(entryRecord(entry), OWNER);
        final long tail = queues.get(destination, TAIL);
        if (tail == entry) {
            return;
        }
        detach(source, entries.get(entry, PREV), entries.get(entry, NEXT), source != destination);
        linkLast(destination, entry, tail);
        modCount++;
    }

    /** Returns the oldest ID; throws NoSuchElementException if the queue is empty. */
    public long firstId(long queueKey) {
        return entries.keyOf(requireHead(requireQueue(queueKey)));
    }

    /** Returns the oldest value; throws NoSuchElementException if the queue is empty. */
    public long firstValue(long queueKey) {
        return entries.get(requireHead(requireQueue(queueKey)), VALUE);
    }

    /** Removes the oldest entry and returns its value; throws NoSuchElementException if the queue is empty. */
    public long removeFirst(long queueKey) {
        final long queue = requireQueue(queueKey);
        return removeKnown(queue, requireHead(queue));
    }

    /** Removes the head without looking up the queue key; throws NoSuchElementException if empty. */
    public long removeFirstByHandle(long queueHandle) {
        final long queue = queueRecord(queueHandle);
        return removeKnown(queue, requireHead(queue));
    }

    /** Removes a known live entry (e.g. after checking its uid) and returns its field zero. */
    public long removeEntry(long entry) {
        return removeKnown(entries.get(entryRecord(entry), OWNER), entry);
    }

    private long removeKnown(long queue, long entry) {
        final long value = entries.get(entry, VALUE);
        detach(queue, entries.get(entry, PREV), entries.get(entry, NEXT), true);
        entries.remove(entries.keyOf(entry));
        modCount++;
        return value;
    }

    private long requireHead(long queue) {
        final long entry = queues.get(queue, HEAD);
        if (entry == NO_HANDLE) {
            throw new NoSuchElementException("empty queue");
        }
        return entry;
    }

    private long requireQueue(long queueKey) {
        final long queue = queues.find(queueKey);
        if (queue == NO_RECORD) {
            throw new NoSuchElementException("empty queue: " + queueKey);
        }
        return queue;
    }

    private void detach(long queue, long prev, long next, boolean recycleEmpty) {
        if (prev == NO_RECORD) {
            queues.set(queue, HEAD, next);
        } else {
            entries.set(prev, NEXT, next);
        }
        if (next == NO_RECORD) {
            queues.set(queue, TAIL, prev);
        } else {
            entries.set(next, PREV, prev);
        }
        if (queues.addAndGet(queue, COUNT, -1) == 0 && recycleEmpty && queues.get(queue, RETAINED) == 0) {
            queues.remove(queues.keyOf(queue));
        }
    }

    /** Visits (ID, value) from oldest to newest. The callback must not change the queues. */
    public void forEach(long queueKey, LongLongConsumer consumer) {
        Objects.requireNonNull(consumer);
        final long queue = queues.find(queueKey);
        if (queue != NO_RECORD) {
            final long expected = modCount;
            for (long e = queues.get(queue, HEAD); e != NO_RECORD; e = entries.get(e, NEXT)) {
                consumer.accept(entries.keyOf(e), entries.get(e, VALUE));
                if (modCount != expected) {
                    throw new ConcurrentModificationException();
                }
            }
        }
    }

    public boolean containsId(long id) {
        return entries.containsKey(id);
    }

    /** Live entries across all queues. */
    public int size() {
        return entries.size();
    }

    /** Live entries in a queue, or zero for an unknown key. */
    public int size(long queueKey) {
        final long queue = queues.find(queueKey);
        return queue == NO_RECORD ? 0 : (int) queues.get(queue, COUNT);
    }

    /** Includes retained empty queues. */
    public int queueCount() {
        return queues.size();
    }

    /** Removes every queue and entry, retaining the allocated capacity. */
    public void clear() {
        entries.clear();
        queues.clear();
        modCount++;
    }
}
