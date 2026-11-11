package exchange.core2.collections.time;

import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Random;

import static org.junit.Assert.*;

public class LongRollingWindowTest {
    @Test
    public void exactCutoffCapacityAndSignedTimeExtremes() {
        final var w = new LongRollingWindow(10, 2);
        w.add(Long.MIN_VALUE, -1);
        w.add(Long.MIN_VALUE, 4);
        assertThrows(IllegalStateException.class, () -> w.add(Long.MIN_VALUE + 9, 9));
        w.advanceTo(Long.MIN_VALUE + 2); // failed append did not advance time
        assertEquals(3, w.sum());
        w.add(Long.MIN_VALUE + 10, Long.MIN_VALUE);
        assertEquals(1, w.size());
        assertThrows(IllegalArgumentException.class, () -> w.advanceTo(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, w.sum());
        w.advanceTo(Long.MAX_VALUE);
        assertEquals(0, w.size());
        assertEquals(0, w.sum());
    }

    @Test
    public void tryAddIsTheRateLimitAndClearForgetsTime() {
        // at most 3 orders in any 1000 ns
        final var w = new LongRollingWindow(1000, 3);
        assertTrue(w.tryAdd(0, 1));
        assertTrue(w.tryAdd(400, 1));
        assertTrue(w.tryAdd(600, 1));
        assertFalse(w.tryAdd(999, 1));
        w.advanceTo(800); // a refusal changes nothing, time included
        assertEquals(3, w.size());
        assertTrue(w.tryAdd(1000, 1)); // the order at 0 is out of (0, 1000]
        assertEquals(3, w.size());
        assertEquals(3, w.capacity());
        assertEquals(1000, w.duration());
        w.clear();
        assertEquals(0, w.size());
        assertEquals(0, w.sum());
        assertTrue(w.tryAdd(-5, 7)); // a fresh window accepts any time
        assertEquals(7, w.sum());
    }

    @Test
    public void sumRecoversFromOverflowAsOldDeltasExpire() {
        final var w = new LongRollingWindow(7, 32);
        final var live = new ArrayDeque<long[]>();
        final var random = new Random(192);
        long time = -100;
        for (int i = 0; i < 10_000; i++) {
            time += random.nextInt(4);
            while (!live.isEmpty() && live.peek()[0] <= time - 7) {
                live.remove();
            }
            final long delta = switch (random.nextInt(3)) {
                case 0 -> Long.MIN_VALUE;
                case 1 -> Long.MAX_VALUE;
                default -> random.nextLong();
            };
            w.add(time, delta);
            live.add(new long[]{time, delta});
            BigInteger sum = BigInteger.ZERO;
            for (long[] event : live) {
                sum = sum.add(BigInteger.valueOf(event[1]));
            }
            assertEquals(live.size(), w.size());
            if (sum.bitLength() > 63) {
                assertThrows(ArithmeticException.class, w::sum);
            } else {
                assertEquals(sum.longValueExact(), w.sum());
            }
        }
    }
}
