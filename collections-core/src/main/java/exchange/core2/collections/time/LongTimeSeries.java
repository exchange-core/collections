package exchange.core2.collections.time;

import java.util.Arrays;
import java.util.Objects;

/**
 * Append-only pairs of signed timestamps and values. Equal timestamps retain insertion order.
 * Indices stay valid as the series grows. Single-threaded.
 *
 * <p>As-of lookups: {@link #floorIndex} counts an observation at the same timestamp as known, {@link #lowerIndex}
 * does not - the choice for a backtest where an event and a decision at one timestamp are simultaneous.
 * {@link #merge} replays several series in one deterministic order.
 */
public final class LongTimeSeries {
    private final boolean fixed;
    private long[] times;
    private long[] values;
    private int size;

    /** Preallocates expectedSize pairs. A growing append may copy both arrays. */
    public LongTimeSeries(int expectedSize) {
        this(expectedSize, false);
    }

    public static LongTimeSeries fixedCapacity(int capacity) {
        return new LongTimeSeries(capacity, true);
    }

    private LongTimeSeries(int capacity, boolean fixed) {
        if (capacity < 0) {
            throw new IllegalArgumentException("negative capacity");
        }
        this.fixed = fixed;
        times = new long[capacity];
        values = new long[capacity];
    }

    public void append(long time, long value) {
        if (size > 0 && time < times[size - 1]) {
            throw new IllegalArgumentException("time moved backwards");
        }
        if (size == times.length) {
            if (fixed || size == Integer.MAX_VALUE) {
                throw new IllegalStateException("time series is full");
            }
            final int capacity = (int) Math.min(Integer.MAX_VALUE, Math.max(8L, 2L * size));
            final long[] newTimes = Arrays.copyOf(times, capacity);
            values = Arrays.copyOf(values, capacity);
            times = newTimes;
        }
        times[size] = time;
        values[size++] = value;
    }

    /** Index of the last observation at or before time, or -1 before the first observation. */
    public int floorIndex(long time) {
        int lo = 0;
        int hi = size;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (times[mid] <= time) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo - 1;
    }

    /**
     * Index of the last observation strictly before time, or -1 if there is none. {@code lowerIndex(time) + 1} is
     * the first observation at or after time.
     */
    public int lowerIndex(long time) {
        int lo = 0;
        int hi = size;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (times[mid] < time) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo - 1;
    }

    public long timeAt(int index) {
        return times[Objects.checkIndex(index, size)];
    }

    public long valueAt(int index) {
        return values[Objects.checkIndex(index, size)];
    }

    public int size() {
        return size;
    }

    /** Replays [fromIndex, current size), in insertion order. Later appends are outside this cursor's range. */
    public Cursor cursor(int fromIndex) {
        Objects.checkFromIndexSize(fromIndex, 0, size);
        return new Cursor(fromIndex, size);
    }

    /**
     * Replays the observations at or after fromTime of several series in one time order. Equal timestamps go in the
     * order of the series in the arguments, then in insertion order, so a replay is the same on every run. Each
     * series is covered up to its size at this call. A step takes O(number of series).
     */
    public static MergeCursor merge(long fromTime, LongTimeSeries... series) {
        final LongTimeSeries[] copy = series.clone();
        final int[] next = new int[copy.length];
        final int[] limits = new int[copy.length];
        for (int s = 0; s < copy.length; s++) {
            next[s] = copy[s].lowerIndex(fromTime) + 1;
            limits[s] = copy[s].size;
        }
        return new MergeCursor(copy, next, limits);
    }

    public static final class MergeCursor {
        private final LongTimeSeries[] series;
        private final int[] next;
        private final int[] limits;
        private int source = -1;
        private int index = -1;

        private MergeCursor(LongTimeSeries[] series, int[] next, int[] limits) {
            this.series = series;
            this.next = next;
            this.limits = limits;
        }

        public boolean advance() {
            int best = -1;
            long bestTime = 0;
            for (int s = 0; s < series.length; s++) {
                if (next[s] < limits[s]) {
                    final long t = series[s].times[next[s]];
                    if (best < 0 || t < bestTime) {
                        best = s;
                        bestTime = t;
                    }
                }
            }
            if (best < 0) {
                source = -1;
                index = -1;
                return false;
            }
            source = best;
            index = next[best]++;
            return true;
        }

        /** The position of the current observation's series in the arguments of merge. */
        public int source() {
            checkCurrent();
            return source;
        }

        /** The index of the current observation in its series. */
        public int index() {
            checkCurrent();
            return index;
        }

        public long time() {
            checkCurrent();
            return series[source].times[index];
        }

        public long value() {
            checkCurrent();
            return series[source].values[index];
        }

        private void checkCurrent() {
            if (source < 0) {
                throw new IllegalStateException("cursor is not on an observation");
            }
        }
    }

    public final class Cursor {
        private int next;
        private final int limit;
        private int current = -1;

        private Cursor(int fromIndex, int limit) {
            this.next = fromIndex;
            this.limit = limit;
        }

        public boolean advance() {
            if (next == limit) {
                current = -1;
                return false;
            }
            current = next++;
            return true;
        }

        public long time() {
            return times[currentIndex()];
        }

        public long value() {
            return values[currentIndex()];
        }

        private int currentIndex() {
            if (current < 0) {
                throw new IllegalStateException("cursor is not on an observation");
            }
            return current;
        }
    }
}
