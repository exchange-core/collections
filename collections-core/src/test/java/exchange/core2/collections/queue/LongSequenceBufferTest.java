package exchange.core2.collections.queue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import static exchange.core2.collections.queue.LongSequenceBuffer.OfferResult.*;
import static org.junit.Assert.*;

public class LongSequenceBufferTest {
    @Test
    public void failedDeliveryKeepsItsPairAndReleasesTheDrainGuard() {
        final var b = new LongSequenceBuffer(3, 100);
        assertEquals(ACCEPTED, b.offer(102, 12));
        assertEquals(TOO_FAR_AHEAD, b.offer(103, 0));
        assertEquals(0, b.drainContiguous((s, v) -> fail(), 3));
        b.offer(100, Long.MIN_VALUE);
        b.offer(101, Long.MAX_VALUE);
        assertEquals(DUPLICATE, b.offer(101, 0));
        assertThrows(IllegalStateException.class, () -> b.drainContiguous((s, v) -> {
            if (s == 101) {
                b.reset(0);
            }
            assertEquals(Long.MIN_VALUE, v);
        }, 3));
        assertEquals(101, b.nextExpected());
        assertEquals(2, b.size());
        assertEquals(TOO_OLD, b.offer(100, 0));
        assertEquals(1, b.drainContiguous((s, v) -> assertEquals(Long.MAX_VALUE, v), 1));
        b.reset(Long.MIN_VALUE);
        assertEquals(0, b.size());
        assertEquals(TOO_FAR_AHEAD, b.offer(Long.MAX_VALUE, 0));
        b.reset(Long.MAX_VALUE);
        assertEquals(ACCEPTED, b.offer(Long.MAX_VALUE, 1));
        assertEquals(1, b.drainContiguous((s, v) -> assertEquals(Long.MAX_VALUE, s), 4));
        assertThrows(NoSuchElementException.class, b::nextExpected);
        assertEquals(TOO_OLD, b.offer(Long.MIN_VALUE, 0));
    }

    @Test
    public void randomOffersAndBoundedDrainsMatchSortedReferenceAcrossWraps() {
        final int capacity = 11;
        final var b = new LongSequenceBuffer(capacity, -20);
        final var pending = new TreeMap<Long, Long>();
        final var random = new Random(731);
        long expected = -20;
        for (int i = 0; i < 20_000; i++) {
            final long seq = expected - 2 + random.nextInt(capacity + 5);
            final long value = random.nextLong();
            final var result = b.offer(seq, value);
            if (seq < expected) {
                assertEquals(TOO_OLD, result);
            } else if (seq >= expected + capacity) {
                assertEquals(TOO_FAR_AHEAD, result);
            } else {
                assertEquals(pending.putIfAbsent(seq, value) == null ? ACCEPTED : DUPLICATE, result);
            }
            final int limit = random.nextInt(5);
            final List<Long> delivered = new ArrayList<>();
            int count = b.drainContiguous((s, v) -> {
                assertEquals(pending.remove(s).longValue(), v);
                delivered.add(s);
            }, limit);
            assertEquals(delivered.size(), count);
            assertTrue(count <= limit);
            for (long s : delivered) {
                assertEquals(expected++, s);
            }
            assertTrue(count == limit || !pending.containsKey(expected));
            assertEquals(expected, b.nextExpected());
            assertEquals(pending.size(), b.size());
            if (pending.isEmpty()) {
                assertThrows(NoSuchElementException.class, b::firstBuffered);
            } else {
                assertEquals(pending.firstKey().longValue(), b.firstBuffered());
            }
            if (random.nextInt(40) == 0) {
                // a recovery: the window jumps, inside or past its end
                final long target = expected + random.nextInt(capacity + 4);
                final var covered = pending.headMap(target);
                final int dropped = covered.size();
                covered.clear();
                assertEquals(dropped, b.skipTo(target));
                expected = target;
                assertEquals(expected, b.nextExpected());
                assertEquals(pending.size(), b.size());
            }
        }
    }

    @Test
    public void skipToKeepsLaterMessagesAndRejectsGoingBack() {
        final var b = new LongSequenceBuffer(4, 10);
        b.offer(11, 1);
        b.offer(13, 3);
        assertEquals(11, b.firstBuffered());
        assertEquals(1, b.skipTo(12));
        assertEquals(13, b.firstBuffered());
        assertEquals(ACCEPTED, b.offer(15, 5));
        assertEquals(0, b.skipTo(13));
        assertEquals(ACCEPTED, b.offer(14, 4));
        assertEquals(3, b.drainContiguous((s, v) -> assertEquals(s - 10, v), 10));
        assertEquals(16, b.nextExpected());
        b.offer(17, 7);
        assertEquals(1, b.skipTo(Long.MAX_VALUE));
        assertEquals(0, b.size());
        assertThrows(IllegalArgumentException.class, () -> b.skipTo(Long.MAX_VALUE - 1));
        assertEquals(ACCEPTED, b.offer(Long.MAX_VALUE, 7));
        assertEquals(1, b.drainContiguous((s, v) -> { }, 1));
        assertThrows(IllegalStateException.class, () -> b.skipTo(Long.MAX_VALUE));
    }
}
