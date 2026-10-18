package exchange.core2.collections.art;

import org.junit.Test;

import java.util.SplittableRandom;

import static org.junit.Assert.assertEquals;

/**
 * next / prev against a plain scan of the bits, for every starting slot. The masks shift by from % 64, so word
 * boundaries (63/64, 127/128, ...), the ends (0, 255) and the out-of-range starts (256, -1) are where it breaks.
 */
public class Bitmap256Test {

    @Test
    public void shouldMatchPlainScan() {
        final SplittableRandom random = new SplittableRandom(1);
        for (int trial = 0; trial < 2000; trial++) {
            final long[] bits = new long[4];
            switch (trial % 5) {
                case 0 -> { } // empty
                case 1 -> bits[random.nextInt(4)] = 1L << random.nextInt(64); // a single slot
                case 2 -> {   // word edges only
                    for (int w = 0; w < 4; w++) {
                        bits[w] = (random.nextBoolean() ? 1L : 0) | (random.nextBoolean() ? Long.MIN_VALUE : 0);
                    }
                }
                case 3 -> {   // sparse
                    for (int k = 0; k < 5; k++) {
                        final int slot = random.nextInt(256);
                        bits[slot >>> 6] |= 1L << slot;
                    }
                }
                default -> {
                    for (int w = 0; w < 4; w++) {
                        bits[w] = random.nextLong();
                    }
                }
            }
            for (int from = 0; from <= 256; from++) {
                assertEquals("next(" + from + ")", plainNext(bits, from),
                        Bitmap256.next(bits[0], bits[1], bits[2], bits[3], from));
            }
            for (int from = -1; from <= 255; from++) {
                assertEquals("prev(" + from + ")", plainPrev(bits, from),
                        Bitmap256.prev(bits[0], bits[1], bits[2], bits[3], from));
            }
        }
    }

    private static boolean isSet(final long[] bits, final int slot) {
        return (bits[slot >>> 6] & (1L << slot)) != 0;
    }

    private static int plainNext(final long[] bits, final int from) {
        for (int slot = from; slot < 256; slot++) {
            if (isSet(bits, slot)) {
                return slot;
            }
        }
        return -1;
    }

    private static int plainPrev(final long[] bits, final int from) {
        for (int slot = from; slot >= 0; slot--) {
            if (isSet(bits, slot)) {
                return slot;
            }
        }
        return -1;
    }
}
