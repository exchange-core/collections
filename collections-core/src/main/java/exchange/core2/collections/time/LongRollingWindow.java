package exchange.core2.collections.time;

/**
 * Count and exact sum of signed deltas in (now - duration, now]. The caller supplies nondecreasing
 * signed timestamps and chooses the units. Memory is fixed at construction: 16 bytes per observation of
 * capacity. Single-threaded.
 *
 * <p>For a limit of N events per window, a capacity of N makes {@link #tryAdd} the check: it refuses the event that
 * would be the (N + 1)-th. One window per account keeps every event; for many accounts a counter per time bucket
 * takes less memory at the price of a coarser boundary.
 */
public final class LongRollingWindow {
    private final long duration;
    private final long[] times;
    private final long[] deltas;
    private int head;
    private int tail;
    private int size;
    private long now = Long.MIN_VALUE;
    // A signed 128-bit accumulator fits the sum of any int-sized array of longs, even during expiry.
    private long sumLow;
    private long sumHigh;

    public LongRollingWindow(long duration, int capacity) {
        if (duration <= 0 || capacity <= 0) {
            throw new IllegalArgumentException("duration and capacity must be positive");
        }
        this.duration = duration;
        times = new long[capacity];
        deltas = new long[capacity];
    }

    /** Advances time and appends a delta. If all slots are still live, throws without changing state. */
    public void add(long time, long delta) {
        if (!tryAdd(time, delta)) {
            throw new IllegalStateException("rolling window is full");
        }
    }

    /**
     * Advances time and appends a delta.
     *
     * @return false, changing nothing, if all capacity observations are still live at time
     */
    public boolean tryAdd(long time, long delta) {
        checkTime(time);
        if (size == times.length && !expired(time, times[head])) {
            return false;
        }
        expireThrough(time);
        times[tail] = time;
        deltas[tail] = delta;
        tail = next(tail);
        size++;
        final long low = sumLow + delta;
        sumHigh += (delta >> 63) + (Long.compareUnsigned(low, sumLow) < 0 ? 1 : 0);
        sumLow = low;
        return true;
    }

    /** Expires old observations even when no new event arrives. */
    public void advanceTo(long time) {
        checkTime(time);
        expireThrough(time);
    }

    private void checkTime(long time) {
        if (time < now) {
            throw new IllegalArgumentException("time moved backwards");
        }
    }

    private boolean expired(long time, long timestamp) {
        return Long.compareUnsigned(time - timestamp, duration) >= 0;
    }

    private void expireThrough(long time) {
        now = time;
        while (size > 0 && expired(time, times[head])) {
            final long delta = deltas[head];
            sumHigh -= (delta >> 63) + (Long.compareUnsigned(sumLow, delta) < 0 ? 1 : 0);
            sumLow -= delta;
            head = next(head);
            size--;
        }
    }

    private int next(int index) {
        return index + 1 == times.length ? 0 : index + 1;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return times.length;
    }

    public long duration() {
        return duration;
    }

    /** Drops every observation and forgets the current time, keeping the capacity. */
    public void clear() {
        head = 0;
        tail = 0;
        size = 0;
        now = Long.MIN_VALUE;
        sumLow = 0;
        sumHigh = 0;
    }

    /** Throws ArithmeticException if the current mathematical sum is outside the long range. */
    public long sum() {
        if (sumHigh != (sumLow >> 63)) {
            throw new ArithmeticException("window sum exceeds signed long range");
        }
        return sumLow;
    }
}
