package exchange.core2.collections.time;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class LongTimeSeriesTest {
    @Test
    public void asOfReturnsTheLastTieAndCursorKeepsItsOriginalPrefixAcrossGrowth() {
        final var s = new LongTimeSeries(0);
        assertEquals(-1, s.floorIndex(Long.MAX_VALUE));
        s.append(-2, 10);
        s.append(0, 20);
        s.append(0, 21);
        final var replay = s.cursor(1);
        assertThrows(IllegalStateException.class, replay::time);
        for (int i = 1; i < 4096; i++) {
            s.append(i * 2L, i);
        }
        s.append(Long.MAX_VALUE, Long.MIN_VALUE);
        assertEquals(-1, s.floorIndex(Long.MIN_VALUE));
        assertEquals(0, s.floorIndex(-1));
        assertEquals(2, s.floorIndex(0));
        assertEquals(0, s.lowerIndex(0));
        assertEquals(-1, s.lowerIndex(-2));
        assertEquals(2, s.lowerIndex(1));
        assertEquals(4097, s.lowerIndex(Long.MAX_VALUE));
        assertEquals(4097, s.floorIndex(8191));
        assertEquals(Long.MIN_VALUE, s.valueAt(s.floorIndex(Long.MAX_VALUE)));
        assertTrue(replay.advance());
        assertEquals(20, replay.value());
        assertTrue(replay.advance());
        assertEquals(0, replay.time());
        assertEquals(21, replay.value());
        assertFalse(replay.advance());
        assertThrows(IllegalStateException.class, replay::value);
    }

    @Test
    public void mergeOrdersByTimeThenSeriesThenInsertion() {
        final var random = new Random(77);
        final LongTimeSeries[] series = new LongTimeSeries[4];
        final List<long[]> all = new ArrayList<>(); // time, source, index, value
        for (int src = 0; src < series.length; src++) {
            series[src] = new LongTimeSeries(4);
            long time = -50;
            for (int i = 0, n = random.nextInt(300); i < n; i++) {
                time += random.nextInt(3); // many ties, within and across series
                final long value = random.nextLong();
                series[src].append(time, value);
                all.add(new long[]{time, src, i, value});
            }
        }
        final long from = -20;
        final var expected = all.stream().filter(e -> e[0] >= from)
                .sorted(Comparator.<long[]>comparingLong(e -> e[0]).thenComparingLong(e -> e[1])
                        .thenComparingLong(e -> e[2]))
                .toList();
        final var merged = LongTimeSeries.merge(from, series);
        series[0].append(Long.MAX_VALUE, 0); // after the call: not covered
        assertThrows(IllegalStateException.class, merged::time);
        for (long[] e : expected) {
            assertTrue(merged.advance());
            assertEquals(e[0], merged.time());
            assertEquals(e[1], merged.source());
            assertEquals(e[2], merged.index());
            assertEquals(e[3], merged.value());
        }
        assertFalse(merged.advance());
        assertThrows(IllegalStateException.class, merged::source);
        assertFalse(LongTimeSeries.merge(0).advance());
    }

    @Test
    public void fullAndOutOfOrderAppendsPreserveHistory() {
        final var s = LongTimeSeries.fixedCapacity(1);
        s.append(0, 5);
        assertThrows(IllegalArgumentException.class, () -> s.append(-1, 1));
        assertThrows(IllegalStateException.class, () -> s.append(0, 6));
        assertEquals(1, s.size());
        assertEquals(5, s.valueAt(0));
        assertFalse(s.cursor(1).advance());
        assertThrows(IndexOutOfBoundsException.class, () -> s.cursor(2));
        assertThrows(IndexOutOfBoundsException.class, () -> s.timeAt(-1));
    }
}
