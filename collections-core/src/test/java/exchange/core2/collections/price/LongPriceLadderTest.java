package exchange.core2.collections.price;

import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Random;

import static org.junit.Assert.*;

public class LongPriceLadderTest {
    @Test
    public void bothSidesMatchAnInclusiveScanAcrossEmptyLevels() {
        final long first = -25;
        final long[] levels = new long[51];
        final var ladder = LongPriceLadder.withCosts(first, levels.length);
        final var random = new Random(214);
        for (int i = 0; i < 8_000; i++) {
            final int level = random.nextInt(levels.length);
            final long volume = random.nextInt(10);
            if (i % 2 == 0) {
                ladder.setVolume(first + level, volume);
            } else {
                ladder.addVolume(first + level, volume - levels[level]);
            }
            levels[level] = volume;
            final int lo = random.nextInt(levels.length), hi = random.nextInt(lo, levels.length);
            long range = 0, total = 0;
            for (int j = 0; j < levels.length; j++) {
                total += levels[j];
                if (lo <= j && j <= hi) {
                    range += levels[j];
                }
            }
            assertEquals(range, ladder.volumeBetween(first + lo, first + hi));
            assertEquals(total, ladder.totalVolume());
            if (total > 0) {
                final long quantity = random.nextLong(total) + 1;
                int buy = 0, sell = levels.length - 1;
                long want = quantity;
                while (want > levels[buy]) {
                    want -= levels[buy++];
                }
                want = quantity;
                while (want > levels[sell]) {
                    want -= levels[sell--];
                }
                assertEquals(first + buy, ladder.priceFromLow(quantity));
                assertEquals(first + sell, ladder.priceFromHigh(quantity));
                assertEquals(scanCost(first, levels, quantity, true), ladder.costFromLow(quantity));
                assertEquals(scanCost(first, levels, quantity, false), ladder.costFromHigh(quantity));
                assertEquals(ladder.priceFromLow(1), ladder.lowestNonEmptyTick());
                assertEquals(ladder.priceFromHigh(1), ladder.highestNonEmptyTick());
            } else {
                assertThrows(NoSuchElementException.class, ladder::lowestNonEmptyTick);
                assertThrows(NoSuchElementException.class, ladder::highestNonEmptyTick);
            }
            final int at = random.nextInt(levels.length);
            assertEquals(ladder.volumeBetween(first, first + at), ladder.volumeAtOrBelow(first + at));
            assertEquals(ladder.volumeBetween(first + at, first + levels.length - 1), ladder.volumeAtOrAbove(first + at));
        }
    }

    /** Sum of tick * lots over the quantity lots nearest to one end. */
    private static long scanCost(long first, long[] levels, long quantity, boolean fromLow) {
        long cost = 0;
        for (int k = 0; k < levels.length && quantity > 0; k++) {
            final int j = fromLow ? k : levels.length - 1 - k;
            final long take = Math.min(quantity, levels[j]);
            cost += (first + j) * take;
            quantity -= take;
        }
        return cost;
    }

    @Test
    public void costsOfWideLevelsAndTheirLimits() {
        final var ladder = LongPriceLadder.withCosts(1_000_000, 1 << 20);
        final long top = ladder.highestTick();
        ladder.setVolume(1_000_000, 3);
        ladder.setVolume(top, 5);
        assertEquals(1_000_000L * 3 + top * 2, ladder.costFromLow(5));
        assertEquals(top * 5 + 1_000_000L, ladder.costFromHigh(6));
        assertEquals(1_000_000, ladder.lowestNonEmptyTick());
        assertEquals(top, ladder.highestNonEmptyTick());
        assertEquals(3, ladder.volumeAtOrBelow(top - 1));
        assertEquals(5, ladder.volumeAtOrAbove(1_000_001));

        // the weighted sum (tick - lowestTick) * volume must fit: the update that breaks it changes nothing
        assertThrows(ArithmeticException.class, () -> ladder.setVolume(top, Long.MAX_VALUE / 2));
        assertEquals(8, ladder.totalVolume());
        assertEquals(top * 5 + 1_000_000L, ladder.costFromHigh(6));
        // a cost below or above the long range
        final var low = LongPriceLadder.withCosts(Long.MIN_VALUE, 2);
        low.setVolume(Long.MIN_VALUE, 2);
        assertEquals(Long.MIN_VALUE, low.costFromLow(1));
        assertThrows(ArithmeticException.class, () -> low.costFromLow(2));

        final var plain = new LongPriceLadder(0, 4);
        plain.setVolume(1, 1);
        assertFalse(plain.keepsCosts());
        assertThrows(IllegalStateException.class, () -> plain.costFromLow(1));
        assertThrows(IllegalArgumentException.class, () -> LongPriceLadder.withCosts(0, (1 << 29) + 1));
        assertThrows(IllegalArgumentException.class, () -> new LongPriceLadder(0, (1 << 30) + 1));
    }

    @Test
    public void rejectedUpdatesLeaveAllAggregatesUnchanged() {
        final var ladder = new LongPriceLadder(Long.MIN_VALUE, 3);
        ladder.setVolume(Long.MIN_VALUE, Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> ladder.addVolume(Long.MIN_VALUE + 1, 1));
        assertThrows(ArithmeticException.class, () -> ladder.addVolume(Long.MIN_VALUE, 1));
        assertThrows(IllegalArgumentException.class, () -> ladder.addVolume(Long.MIN_VALUE + 1, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> ladder.setVolume(Long.MAX_VALUE, 2));
        assertEquals(Long.MAX_VALUE, ladder.volumeBetween(Long.MIN_VALUE, Long.MIN_VALUE + 2));
        assertEquals(Long.MIN_VALUE, ladder.priceFromHigh(1));
        assertEquals(Long.MIN_VALUE, ladder.priceFromLow(Long.MAX_VALUE));
        ladder.setVolume(Long.MIN_VALUE, 0);
        assertThrows(NoSuchElementException.class, () -> ladder.priceFromHigh(1));
        assertThrows(IllegalArgumentException.class, () -> ladder.priceFromLow(0));
        assertThrows(ArithmeticException.class, () -> new LongPriceLadder(Long.MAX_VALUE, 2));
    }
}
