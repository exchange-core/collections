package exchange.core2.collections.queue;

import exchange.core2.collections.hashtable.LongLongConsumer;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Fixed reorder window for one feed session. The window starts at the first undelivered sequence
 * and holds at most capacity consecutive sequence numbers. All signed long sequences and values are
 * supported. After delivering Long.MAX_VALUE, reset starts a new session. Single-threaded.
 *
 * <p>Recovery: while the next sequence is missing, {@code [nextExpected(), firstBuffered())} is the gap to request
 * again; after applying a snapshot up to sequence S, {@code skipTo(S + 1)} drops what the snapshot covers and keeps
 * the messages buffered after it.
 */
public final class LongSequenceBuffer {

    public enum OfferResult { ACCEPTED, DUPLICATE, TOO_OLD, TOO_FAR_AHEAD }

    private final long[] values;
    private final boolean[] present;
    private int head;
    private int size;
    private long expected;
    private boolean exhausted;
    private boolean draining;

    public LongSequenceBuffer(int capacity, long firstSequence) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        values = new long[capacity];
        present = new boolean[capacity];
        expected = firstSequence;
    }

    /** Only ACCEPTED changes the buffer. A duplicate preserves the first value offered. */
    public OfferResult offer(long sequence, long value) {
        checkNotDraining();
        if (exhausted || sequence < expected) {
            return OfferResult.TOO_OLD;
        }
        final long distance = sequence - expected;
        if (Long.compareUnsigned(distance, values.length) >= 0) {
            return OfferResult.TOO_FAR_AHEAD;
        }
        final long offset = head + distance;
        final int slot = (int) (offset < values.length ? offset : offset - values.length);
        if (present[slot]) {
            return OfferResult.DUPLICATE;
        }
        values[slot] = value;
        present[slot] = true;
        size++;
        return OfferResult.ACCEPTED;
    }

    /**
     * Delivers up to limit (sequence, value) pairs, stopping at the first gap. A pair is consumed
     * after its callback returns normally. If it throws, that pair is available for retry.
     * The callback may read the buffer but must not mutate it.
     */
    public int drainContiguous(LongLongConsumer consumer, int limit) {
        Objects.requireNonNull(consumer);
        if (limit < 0) {
            throw new IllegalArgumentException("negative limit");
        }
        checkNotDraining();
        draining = true;
        try {
            int count = 0;
            while (count < limit && present[head]) {
                consumer.accept(expected, values[head]);
                present[head] = false;
                head = next(head);
                size--;
                count++;
                if (expected == Long.MAX_VALUE) {
                    exhausted = true;
                    break;
                }
                expected++;
            }
            return count;
        } finally {
            draining = false;
        }
    }

    /** First undelivered number. Throws after the final signed sequence has been delivered. */
    public long nextExpected() {
        if (exhausted) {
            throw new NoSuchElementException("session exhausted");
        }
        return expected;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return values.length;
    }

    /**
     * The lowest buffered sequence: nextExpected() itself when it has arrived, otherwise the end of the gap that
     * starts at nextExpected(). Takes O(gap length).
     *
     * @throws NoSuchElementException if nothing is buffered
     */
    public long firstBuffered() {
        if (size == 0) {
            throw new NoSuchElementException("nothing buffered");
        }
        int slot = head;
        long distance = 0;
        while (!present[slot]) {
            slot = next(slot);
            distance++;
        }
        return expected + distance;
    }

    /**
     * Moves the window to start at sequence: drops the buffered entries below it, keeps those at or after it.
     * Takes O(min(sequence - nextExpected(), capacity)).
     *
     * @return the number of entries dropped
     * @throws IllegalArgumentException if sequence is below nextExpected()
     * @throws IllegalStateException    after the final signed sequence has been delivered; reset starts a new session
     */
    public int skipTo(long sequence) {
        checkNotDraining();
        if (exhausted) {
            throw new IllegalStateException("session exhausted");
        }
        if (sequence < expected) {
            throw new IllegalArgumentException("cannot skip back from " + expected + " to " + sequence);
        }
        final long distance = sequence - expected;
        int dropped = 0;
        if (Long.compareUnsigned(distance, values.length) >= 0) {
            dropped = size;
            Arrays.fill(present, false);
            head = 0;
        } else {
            for (long i = 0; i < distance; i++) {
                if (present[head]) {
                    present[head] = false;
                    dropped++;
                }
                head = next(head);
            }
        }
        size -= dropped;
        expected = sequence;
        return dropped;
    }

    private int next(int index) {
        return index + 1 == values.length ? 0 : index + 1;
    }

    /** Discards buffered entries, retaining capacity for the session starting at nextSequence. */
    public void reset(long nextSequence) {
        checkNotDraining();
        Arrays.fill(present, false);
        expected = nextSequence;
        head = 0;
        size = 0;
        exhausted = false;
    }

    private void checkNotDraining() {
        if (draining) {
            throw new IllegalStateException("cannot mutate the buffer from its drain callback");
        }
    }
}
