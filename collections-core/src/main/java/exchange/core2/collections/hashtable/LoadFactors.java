package exchange.core2.collections.hashtable;

/**
 * When a table grows: at {@link #loadFactor} of its capacity, or at {@link #smallTableLoadFactor} while its capacity
 * is below {@link #smallTableCapacity} - a small table is sparse for little memory, and its probes are shorter.
 */
final class LoadFactors {

    /** least load factor: 4 slots a key - a probe is ~1.2 slots there, sparser buys nothing */
    static final double MIN = 0.25;
    /** largest load factor: a miss probes ~50 slots at 0.9 */
    static final double MAX = 0.9;

    final double smallTableLoadFactor;
    final int smallTableCapacity;
    final double loadFactor;

    /**
     * @param smallTableLoadFactor NaN for the default: {@code min(defaultSmall, loadFactor)}
     */
    LoadFactors(double smallTableLoadFactor, double defaultSmall, int smallTableCapacity, double loadFactor) {
        this.loadFactor = check(loadFactor, "load factor");
        if (Double.isNaN(smallTableLoadFactor)) {
            this.smallTableLoadFactor = Math.min(defaultSmall, loadFactor);
        } else {
            this.smallTableLoadFactor = check(smallTableLoadFactor, "small table load factor");
            if (smallTableLoadFactor > loadFactor) {
                throw new IllegalArgumentException("small table load factor " + smallTableLoadFactor
                        + " is above the load factor " + loadFactor);
            }
        }
        this.smallTableCapacity = checkSmallTableCapacity(smallTableCapacity);
    }

    static double check(double loadFactor, String what) {
        if (!(loadFactor >= MIN && loadFactor <= MAX)) {
            throw new IllegalArgumentException(what + " must be from " + MIN + " to " + MAX + ": " + loadFactor);
        }
        return loadFactor;
    }

    static int checkSmallTableCapacity(int capacity) {
        if (capacity < 2 || capacity > HashingUtils.MAX_CAPACITY || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("small table capacity must be a power of two from 2 to "
                    + HashingUtils.MAX_CAPACITY + ": " + capacity);
        }
        return capacity;
    }

    /**
     * @return the load a table of the capacity grows at
     */
    double at(int capacity) {
        return capacity < smallTableCapacity ? smallTableLoadFactor : loadFactor;
    }

    /**
     * @return the size a table of the capacity grows at
     */
    long threshold(int capacity) {
        return (long) (capacity * at(capacity));
    }

    /**
     * @return the least capacity, a power of two, that takes expectedSize entries without growing
     * @throws IllegalArgumentException if expectedSize is negative or needs more than maxCapacity
     */
    int capacityFor(int expectedSize, int maxCapacity) {
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expected size must not be negative: " + expectedSize);
        }
        for (long capacity = 2; capacity <= maxCapacity; capacity <<= 1) {
            if (threshold((int) capacity) > expectedSize) {
                return (int) capacity;
            }
        }
        throw new IllegalArgumentException("expected size " + expectedSize + " exceeds the maximum of "
                + (threshold(maxCapacity) - 1) + " entries (capacity " + maxCapacity + ")");
    }

    /**
     * @return maxLoad, if it is at least the load the table grows at near its maximum capacity and below 1
     */
    double checkMaxLoad(double maxLoad, int maxCapacity) {
        final double growth = at(maxCapacity);
        if (!(maxLoad >= growth && maxLoad < 1.0)) {
            throw new IllegalArgumentException("max load must be from the load factor " + growth
                    + " to below 1: " + maxLoad);
        }
        return maxLoad;
    }
}
