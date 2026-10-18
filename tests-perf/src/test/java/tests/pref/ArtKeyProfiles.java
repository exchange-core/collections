package tests.pref;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * Key sets for the ordered-map benchmarks, named {@code <distribution>-<size>}, e.g. {@code book-20k}
 * (size: a number with an optional k or m suffix):
 * <ul>
 *   <li>{@code book} - price levels of an order book, in ticks: asks above the mid, bids below, the distance from
 *   the mid hyperbolic - P(distance &ge; d) = c / (c + d). Every tick next to the mid holds a level, further out
 *   the levels thin out, about as many per doubling of the distance - deep levels at round prices, far from the
 *   market. The scale c grows with the size, so the dense core around the mid holds about half of the levels.
 *   Size = levels of both sides; the sides are symmetric for now.</li>
 *   <li>{@code ids} - live order ids: of a range of sequential ids 5% are still live, the rest are gone
 *   (cancelled, filled).</li>
 *   <li>{@code random} - uniform 64-bit keys.</li>
 * </ul>
 * A profile also samples keys from its distribution, present or not - where the next order arrives. The
 * generator is cheap (one division per key); precision is not the point.
 */
final class ArtKeyProfiles {

    /**
     * The mid price of a book, in ticks. Low bytes 0x8080: the mid is in the middle of a 256-tick leaf and of a
     * 64K-tick block, as a real price can be anywhere - a mid on a node boundary would put the best levels of
     * both sides at the edges of their nodes, where an ordered walk finds them at once.
     */
    static final long MID = 0x05F5_8080L;

    /** farthest level of a book from the mid, in ticks */
    private static final double MAX_DISTANCE = 1 << 22;

    /** id range of the ids distribution: one in 20 ids is live */
    private static final int IDS_SPARSENESS = 20;

    private static final long IDS_BASE = 1_000_000_000L;

    private ArtKeyProfiles() {
    }

    static Profile parse(final String name, final long seed) {
        final int dash = name.lastIndexOf('-');
        if (dash < 0) {
            throw new IllegalArgumentException("expected <distribution>-<size>: " + name);
        }
        final String distribution = name.substring(0, dash);
        final int size = parseSize(name.substring(dash + 1));
        return switch (distribution) {
            case "book", "ids", "random" -> new Profile(distribution, size, seed);
            default -> throw new IllegalArgumentException("unknown distribution: " + distribution);
        };
    }

    private static int parseSize(final String s) {
        final char suffix = Character.toLowerCase(s.charAt(s.length() - 1));
        final int multiplier = suffix == 'k' ? 1000 : suffix == 'm' ? 1_000_000 : 1;
        final String digits = multiplier == 1 ? s : s.substring(0, s.length() - 1);
        return Math.multiplyExact(Integer.parseInt(digits), multiplier);
    }

    static final class Profile {

        final String distribution;
        final int size;

        /** stored keys, ascending */
        final long[] keys;

        /** keys above it are asks, below it bids; Long.MIN_VALUE for a single-sided key set */
        final long mid;

        /** hyperbolic scale of a book, in ticks */
        private final double scale;

        private Profile(final String distribution, final int size, final long seed) {
            this.distribution = distribution;
            this.size = size;
            this.mid = distribution.equals("book") ? MID : Long.MIN_VALUE;
            this.scale = Math.max(1.0, size / 64.0);
            if (twoSided() && size > MAX_DISTANCE) {
                // 2 * MAX_DISTANCE ticks in all: filling them up would take ever more samples
                throw new IllegalArgumentException("a book holds at most " + (long) MAX_DISTANCE + " levels here");
            }

            final SplittableRandom random = new SplittableRandom(seed);
            final LongOpenHashSet set = new LongOpenHashSet(size * 2);
            while (set.size() < size) {
                set.add(sample(random));
            }
            this.keys = set.toLongArray();
            Arrays.sort(keys);
        }

        boolean twoSided() {
            return mid != Long.MIN_VALUE;
        }

        /** a key from the distribution, stored or not */
        long sample(final SplittableRandom random) {
            switch (distribution) {
                case "book": {
                    final long distance = hyperbolicDistance(random);
                    return random.nextBoolean() ? mid + 1 + distance : mid - 1 - distance;
                }
                case "ids":
                    return IDS_BASE + random.nextLong((long) size * IDS_SPARSENESS);
                default:
                    return random.nextLong();
            }
        }

        /** P(d &ge; x) = scale / (scale + x): inverse transform of a uniform (0, 1] */
        private long hyperbolicDistance(final SplittableRandom random) {
            while (true) {
                final double u = 1.0 - random.nextDouble();
                final double distance = scale * (1.0 / u - 1.0);
                if (distance < MAX_DISTANCE) {
                    return (long) distance;
                }
            }
        }

        /** @return position of the key in {@link #keys}, or -(insertion point) - 1 */
        int indexOf(final long key) {
            return Arrays.binarySearch(keys, key);
        }

        /** @return position of the stored key nearest to the given one */
        int nearest(final long key) {
            final int found = indexOf(key);
            if (found >= 0) {
                return found;
            }
            final int ceiling = -found - 1;
            if (ceiling == 0) {
                return 0;
            }
            if (ceiling == keys.length) {
                return keys.length - 1;
            }
            final int floor = ceiling - 1;
            // unsigned distances: the random keys span the whole range
            return Long.compareUnsigned(key - keys[floor], keys[ceiling] - key) <= 0 ? floor : ceiling;
        }

        @Override
        public String toString() {
            return distribution + "-" + size + " (" + keys.length + " keys, " + keys[0] + ".." + keys[keys.length - 1] + ")";
        }
    }
}
