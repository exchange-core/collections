package exchange.core2.collections.price;

import java.util.NoSuchElementException;

/**
 * Aggregated volume on one side of a book, over a fixed dense range of integral price ticks.
 * Both each level and the total must fit a nonnegative long. Quantities use caller-defined lots.
 * Point reads take O(1). Updates, range sums and searches by cumulative volume take O(log tickCount).
 * At most 2^30 ticks, 2^29 with costs. Single-threaded.
 *
 * <p>A ladder made by {@link #withCosts} also answers what taking a quantity costs ({@link #costFromLow},
 * {@link #costFromHigh}). It keeps a second Fenwick tree of {@code (tick - lowestTick) × volume} next to the first,
 * in the same array, so an update touches the same cache lines; that sum must fit a long as well.
 */
public final class LongPriceLadder {
    private final long lowestTick;
    private final long highestTick;
    private final long[] volumes;
    /**
     * Fenwick nodes 1..tickCount: the volume of node i at {@code i << shift}, its weighted volume at
     * {@code (i << shift) + 1} when costs are kept.
     */
    private final long[] tree;
    private final int shift;
    private long total;
    /** sum of (tick - lowestTick) * volume, when costs are kept */
    private long totalWeighted;

    public LongPriceLadder(long lowestTick, int tickCount) {
        this(lowestTick, tickCount, false);
    }

    /** A ladder that also keeps the costs of taking a quantity from either end. */
    public static LongPriceLadder withCosts(long lowestTick, int tickCount) {
        return new LongPriceLadder(lowestTick, tickCount, true);
    }

    private LongPriceLadder(long lowestTick, int tickCount, boolean costs) {
        // up to 2^30 ticks the descent's index + step stays below 2^31
        final int limit = costs ? 1 << 29 : 1 << 30;
        if (tickCount < 1 || tickCount > limit) {
            throw new IllegalArgumentException("invalid tick count: " + tickCount);
        }
        this.lowestTick = lowestTick;
        this.highestTick = Math.addExact(lowestTick, tickCount - 1L);
        this.volumes = new long[tickCount];
        this.shift = costs ? 1 : 0;
        this.tree = new long[(tickCount + 1) << shift];
    }

    public long lowestTick() {
        return lowestTick;
    }

    public long highestTick() {
        return highestTick;
    }

    public boolean keepsCosts() {
        return shift != 0;
    }

    public long volumeAt(long tick) {
        return volumes[index(tick)];
    }

    public long totalVolume() {
        return total;
    }

    /** Replaces the level. Zero removes all liquidity at that tick. */
    public void setVolume(long tick, long volume) {
        final int index = index(tick);
        update(index, volume);
    }

    /** Adds a signed delta. Negative volume or overflow leaves the entire ladder unchanged. */
    public void addVolume(long tick, long delta) {
        final int index = index(tick);
        update(index, Math.addExact(volumes[index], delta));
    }

    private void update(int index, long volume) {
        if (volume < 0) {
            throw new IllegalArgumentException("negative level volume");
        }
        final long delta = volume - volumes[index];
        if (delta == 0) {
            return;
        }
        final long newTotal = Math.addExact(total, delta);
        if (shift == 0) {
            total = newTotal;
            volumes[index] = volume;
            for (int i = index + 1; i > 0 && i <= volumes.length; i += i & -i) {
                tree[i] += delta;
            }
            return;
        }
        // index * delta is the difference of two products of a nonnegative weighted sum: it fits whenever the
        // new weighted total does, and then so does every node of the tree
        final long weightedDelta = Math.multiplyExact((long) index, delta);
        final long newWeighted = Math.addExact(totalWeighted, weightedDelta);
        total = newTotal;
        totalWeighted = newWeighted;
        volumes[index] = volume;
        for (int i = index + 1; i > 0 && i <= volumes.length; i += i & -i) {
            tree[i << 1] += delta;
            tree[(i << 1) + 1] += weightedDelta;
        }
    }

    /** Sum over both endpoints inclusive. Both ticks must lie in the configured range. */
    public long volumeBetween(long fromTick, long toTick) {
        if (fromTick > toTick) {
            throw new IllegalArgumentException("reversed tick range");
        }
        return prefix(index(toTick) + 1) - prefix(index(fromTick));
    }

    /** Volume at the tick and below it: what a buy limited at that price can take from the asks. */
    public long volumeAtOrBelow(long tick) {
        return prefix(index(tick) + 1);
    }

    /** Volume at the tick and above it: what a sell limited at that price can take from the bids. */
    public long volumeAtOrAbove(long tick) {
        return total - prefix(index(tick));
    }

    private long prefix(int count) {
        long sum = 0;
        for (int i = count; i > 0; i -= i & -i) {
            sum += tree[i << shift];
        }
        return sum;
    }

    /** The lowest tick with volume: the best ask. */
    public long lowestNonEmptyTick() {
        checkNotEmpty();
        return select(1);
    }

    /** The highest tick with volume: the best bid. */
    public long highestNonEmptyTick() {
        checkNotEmpty();
        return select(total);
    }

    /** Lowest tick at which the requested quantity is reached when walking asks from low to high. */
    public long priceFromLow(long quantity) {
        checkQuantity(quantity);
        return select(quantity);
    }

    /** Highest tick at which the requested quantity is reached when walking bids from high to low. */
    public long priceFromHigh(long quantity) {
        checkQuantity(quantity);
        return select(total - quantity + 1);
    }

    /**
     * What taking the quantity from the lowest tick up costs: the sum of tick × lots over the lots taken, the last
     * level possibly in part. Divided by the quantity, it is the average price of a buy that sweeps the asks.
     *
     * @throws ArithmeticException if the cost does not fit a long
     */
    public long costFromLow(long quantity) {
        checkCosts();
        checkQuantity(quantity);
        return absoluteCost(quantity, relativeCostFromLow(quantity));
    }

    /**
     * What taking the quantity from the highest tick down costs: the sum of tick × lots over the lots taken, the
     * last level possibly in part. Divided by the quantity, it is the average price of a sell that sweeps the bids.
     *
     * @throws ArithmeticException if the cost does not fit a long
     */
    public long costFromHigh(long quantity) {
        checkCosts();
        checkQuantity(quantity);
        // the highest lots are all of them but the lowest (total - quantity)
        return absoluteCost(quantity, totalWeighted - relativeCostFromLow(total - quantity));
    }

    private long absoluteCost(long quantity, long relative) {
        return Math.addExact(Math.multiplyExact(lowestTick, quantity), relative);
    }

    /** Sum of (tick - lowestTick) over the lowest quantity lots; zero for zero. */
    private long relativeCostFromLow(long quantity) {
        int index = 0;
        long remaining = quantity;
        long weightedBelow = 0;
        for (int step = Integer.highestOneBit(volumes.length); step != 0; step >>>= 1) {
            final int next = index + step;
            if (next <= volumes.length && tree[next << 1] < remaining) {
                remaining -= tree[next << 1];
                weightedBelow += tree[(next << 1) + 1];
                index = next;
            }
        }
        // levels below index are taken whole, the remaining lots at index
        return weightedBelow + index * remaining;
    }

    private void checkCosts() {
        if (shift == 0) {
            throw new IllegalStateException("this ladder does not keep costs; create it with withCosts");
        }
    }

    private void checkNotEmpty() {
        if (total == 0) {
            throw new NoSuchElementException("empty ladder");
        }
    }

    private void checkQuantity(long quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        if (quantity > total) {
            throw new NoSuchElementException("insufficient depth");
        }
    }

    // Skip complete Fenwick subtrees whose volume is strictly below the remaining quantity.
    private long select(long quantity) {
        int index = 0;
        for (int step = Integer.highestOneBit(volumes.length); step != 0; step >>>= 1) {
            final int next = index + step;
            if (next <= volumes.length && tree[next << shift] < quantity) {
                quantity -= tree[next << shift];
                index = next;
            }
        }
        return lowestTick + index;
    }

    private int index(long tick) {
        if (tick < lowestTick || tick > highestTick) {
            throw new IndexOutOfBoundsException("tick outside ladder: " + tick);
        }
        return (int) (tick - lowestTick);
    }
}
