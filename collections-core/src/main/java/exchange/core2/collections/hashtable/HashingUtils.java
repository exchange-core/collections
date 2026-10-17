package exchange.core2.collections.hashtable;


import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

final class HashingUtils {

    private HashingUtils() {
    }

    /**
     * Stored key of an empty slot. The arrays hold keys XOR-ed with the {@link #SEED}; the key stored as 0 - the key
     * equal to the seed - is kept in fields of the table.
     */
    static final long NOT_ALLOWED_KEY = 0L;

    /**
     * Hash seed of all tables in this JVM: {@code -Dexchange.hashtable.seed=random} or a number (decimal, or hex with
     * 0x), default 0. The tables store and hash key ^ SEED. Where keys come from outside, a seed unknown there keeps
     * them from being picked to pile up in one probe cluster; seed 0 keeps the slot order - and the iteration order -
     * the same from run to run. Being a static final, it costs nothing at 0 (the JIT folds the XOR away) and an XOR
     * with a constant otherwise.
     */
    static final long SEED = seedFromProperty();

    private static long seedFromProperty() {
        final String value = System.getProperty("exchange.hashtable.seed", "0").trim();
        if (value.equalsIgnoreCase("random")) {
            return randomSeed();
        }
        // hex: all 64 bits, as printed by Long.toHexString
        return value.startsWith("0x") || value.startsWith("0X")
                ? Long.parseUnsignedLong(value.substring(2), 16)
                : Long.parseLong(value);
    }

    /**
     * Largest table: 2^29 slots, a long[2^30]. Resizing stops there - the next array length would not fit an int.
     */
    static final int MAX_CAPACITY = 1 << 29;

    /**
     * Load a table at its maximum capacity is allowed to reach - it can not grow, new keys are refused after that.
     */
    static final float FULL_LOAD = 0.9f;

    /**
     * @return maxCapacity, if it is a power of two from 2 to {@link #MAX_CAPACITY}
     */
    static int checkMaxCapacity(int maxCapacity) {
        if (maxCapacity < 2 || maxCapacity > MAX_CAPACITY || Integer.bitCount(maxCapacity) != 1) {
            throw new IllegalArgumentException("max capacity must be a power of two from 2 to " + MAX_CAPACITY
                    + ": " + maxCapacity);
        }
        return maxCapacity;
    }

    /**
     * Number of slots (a power of two) for a table to hold expectedSize entries without a resize. The tables resize
     * when the size reaches (int) (capacity * loadFactor), so that has to be above expectedSize.
     *
     * @param maxCapacity the largest capacity the table may have - IllegalArgumentException if more is needed
     */
    static int capacityFor(final int expectedSize, final float loadFactor, final int maxCapacity) {
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expected size must not be negative: " + expectedSize);
        }
        // rounding of the division could only matter where the result is an integer - and there it is exact
        final double minCapacity = Math.ceil((expectedSize + 1.0) / loadFactor);
        if (minCapacity > maxCapacity) {
            throw new IllegalArgumentException("expected size " + expectedSize + " exceeds the maximum of "
                    + ((int) (maxCapacity * loadFactor) - 1) + " entries (capacity " + maxCapacity + ")");
        }
        return nextPositivePowerOfTwo((int) minCapacity);
    }

    /**
     * Key mixing function: David Stafford's "Mix04" variant of the MurmurHash3 64-bit finalizer,
     * returning the high 32 bits - identical to {@code java.util.SplittableRandom#mix32}.
     * <p>
     * Both multipliers are odd, so each multiplication is invertible modulo 2^64 and the whole
     * function is a bijection on 64 bits - it redistributes bits without ever creating collisions.
     * Do not substitute hand-picked constants: these were found by a search over avalanche
     * statistics, and a worse mixer fails silently as key clustering, which in a linear-probing
     * table shows up only as latency tail.
     * <p>
     * Note the int extraction. Mix04 ends with a shift of 32, so folding the result the way agrona
     * does ({@code (int) x ^ (int) (x >>> 32)}) would cancel that shift out and hand back the
     * untouched low half. Taking the high 32 bits, as the JDK does, is the correct reduction here.
     *
     * @see <a href="http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html">Better Bit Mixing</a>
     */
    static int hash(final long value) {
        long z = value;
        z = (z ^ (z >>> 33)) * 0x62a9d9ed799705f5L;
        return (int) (((z ^ (z >>> 28)) * 0xcb24d0a5c88c35b3L) >>> 32);
    }

    static int nextPositivePowerOfTwo(final int value) {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * A hash seed nobody can predict, different on every call: the SplitMix64 sequence (as in SplittableRandom) from a
     * SecureRandom start.
     */
    static long randomSeed() {
        long z = RandomSeeds.NEXT.addAndGet(0x9e3779b97f4a7c15L);
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /** SecureRandom only when a random seed is asked for - not at the first use of a table */
    private static final class RandomSeeds {
        static final AtomicLong NEXT = new AtomicLong(new SecureRandom().nextLong());
    }


    /**
     * Finds free offset where key cell either empty or the same as provided
     * @param key - key provided
     * @param data - data array
     * @param mask - mask for data array
     * @return offset in the array
     */
    static int findFreeOffset(long key, long[] data, int mask) {
        final int hash = HashingUtils.hash(key);
        int pos = (hash & mask) << 1;
//        log.debug("collision on key:{} hash:{}(0x{})->pos:{}(0x{})", key, hash, String.format("%x", hash), pos, String.format("%x", pos));
        return findFreeOffset(key, pos, data);
    }

    /**
     * The probe loop terminates only if the array has at least one gap. If the array is full (or
     * its gap invariant was broken by a concurrent migration) it would otherwise spin forever,
     * which is indistinguishable from a deadlock. Bound it and fail loudly instead.
     */
    static int findFreeOffset(long key, int pos, long[] data) {
        final int startPos = pos;
        int i = 0;

        long existingKey = data[pos];
        while (existingKey != NOT_ALLOWED_KEY && existingKey != key) {
            // try next element
            pos += 2;
            if (pos == data.length) {
                pos = 0;
            }

            if (++i >= (data.length >> 1)) {
                throw new IllegalStateException("findFreeOffset: no gap found for key=" + key
                        + " startPos=" + startPos + " lastPos=" + pos + " capacity=" + (data.length >> 1));
            }

            existingKey = data[pos];

//            log.debug("try next pos={}", pos);
        }

        return pos;
    }
}
