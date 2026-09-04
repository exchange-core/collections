package exchange.core2.collections.hashtable;


public class HashingUtils {

    public static final long NOT_ALLOWED_KEY = 0L;

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
    public static int hash(final long value) {
        long z = value;
        z = (z ^ (z >>> 33)) * 0x62a9d9ed799705f5L;
        return (int) (((z ^ (z >>> 28)) * 0xcb24d0a5c88c35b3L) >>> 32);
    }

    public static int nextPositivePowerOfTwo(final int value) {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
    }

    public static long nextPositivePowerOfTwo(final long value) {
        return 1L << (Long.SIZE - Long.numberOfLeadingZeros(value - 1));
    }


    /**
     * Finds free offset where key cell either empty or the same as provided
     * @param key - key provided
     * @param data - data array
     * @param mask - mask for data array
     * @return offset in the array
     */
    public static int findFreeOffset(long key, long[] data, int mask) {
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
    public static int findFreeOffset(long key, int pos, long[] data) {
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
