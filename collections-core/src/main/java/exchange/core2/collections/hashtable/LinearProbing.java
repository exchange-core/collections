package exchange.core2.collections.hashtable;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

/**
 * Operations on a linear-probing array of interleaved key/value longs, shared by the hashtables.
 */
final class LinearProbing {

    private LinearProbing() {
    }

    /**
     * Removes the entry at offset by backward-shift deletion (Knuth's Algorithm R): one pass over the rest of the
     * cluster, moving back every entry the gap would cut off from its home. O(cluster), each entry hashed once - the
     * previous version searched the cluster backwards from its end for every shifted entry, O(cluster^2).
     */
    static void deleteAt(long[] data, int offset, long missingValue) {
        final int idxMask = data.length - 1;
        final int slotMask = idxMask >> 1;
        int gap = offset;
        int pos = offset;
        for (int i = 0; ; i++) {
            pos = (pos + 2) & idxMask;
            final long key = data[pos];
            if (key == NOT_ALLOWED_KEY) {
                break;
            }
            if (canFillGap(gap, (HashingUtils.hash(key) & slotMask) << 1, pos, idxMask)) {
                data[gap] = key;
                data[gap + 1] = data[pos + 1];
                gap = pos;
            }
            // the cluster ends at a gap - without one (full or corrupted array) this would never stop
            if (i > slotMask) {
                throw new IllegalStateException("deleteAt: no gap found, offset=" + offset
                        + " capacity=" + (slotMask + 1));
            }
        }
        data[gap] = NOT_ALLOWED_KEY;
        data[gap + 1] = missingValue;
    }

    /**
     * A table array with all slots free: key 0, and the missing value in the value cell - a lookup of a missing key
     * reads it from the gap its probe stops at, without comparing the key. With missingValue 0 this is just the
     * zeroed array the JVM allocates; otherwise one more pass over it.
     */
    static long[] newArray(int capacity, long missingValue) {
        final long[] data = new long[capacity * 2];
        if (missingValue != 0L) {
            for (int i = 1; i < data.length; i += 2) {
                data[i] = missingValue;
            }
        }
        return data;
    }

    /**
     * Frees all slots of the array, see {@link #newArray}.
     */
    static void clear(long[] data, long missingValue) {
        for (int i = 0; i < data.length; i += 2) {
            data[i] = NOT_ALLOWED_KEY;
            data[i + 1] = missingValue;
        }
    }

    /**
     * An entry at pos with the given home may move back into the gap if the gap lies on its probe path,
     * i.e. within [home, pos) on the ring. Positions in any unit, mask = ring size - 1.
     */
    static boolean canFillGap(int gap, int home, int pos, int mask) {
        return ((pos - home) & mask) >= ((pos - gap) & mask);
    }

    /*
     * Iteration: the array holds the keys XOR-ed with the seed, and the entry stored as key 0 (the key equal to the
     * seed) is kept by the table in fields - it comes first, then the occupied slots in the array order.
     */

    static void forEach(boolean hasZeroKey, long zeroKeyValue, long[] data, long seed, LongLongConsumer consumer) {
        if (hasZeroKey) {
            consumer.accept(seed, zeroKeyValue);
        }
        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key != NOT_ALLOWED_KEY) {
                consumer.accept(key ^ seed, data[i + 1]);
            }
        }
    }

    /**
     * Keys in the order of {@link #forEach}; the stream reads the array lazily.
     */
    static LongStream keysStream(boolean hasZeroKey, long[] data, long seed) {
        final LongStream slots = IntStream.range(0, data.length >> 1)
                .filter(i -> data[i << 1] != NOT_ALLOWED_KEY)
                .mapToLong(i -> data[i << 1] ^ seed);
        return hasZeroKey ? LongStream.concat(LongStream.of(seed), slots) : slots;
    }

    /**
     * Values in the order of {@link #forEach}; the stream reads the array lazily.
     */
    static LongStream valuesStream(boolean hasZeroKey, long zeroKeyValue, long[] data) {
        final LongStream slots = IntStream.range(0, data.length >> 1)
                .filter(i -> data[i << 1] != NOT_ALLOWED_KEY)
                .mapToLong(i -> data[(i << 1) + 1]);
        return hasZeroKey ? LongStream.concat(LongStream.of(zeroKeyValue), slots) : slots;
    }

    /**
     * All entries as {@code {key=value, ...}} in the order of {@link #forEach} - the format of {@code java.util.Map}.
     */
    static String entriesToString(boolean hasZeroKey, long zeroKeyValue, long[] data, long seed) {
        final StringBuilder sb = new StringBuilder("{");
        if (hasZeroKey) {
            sb.append(seed).append('=').append(zeroKeyValue);
        }
        for (int i = 0; i < data.length; i += 2) {
            if (data[i] != NOT_ALLOWED_KEY) {
                if (sb.length() > 1) {
                    sb.append(", ");
                }
                sb.append(data[i] ^ seed).append('=').append(data[i + 1]);
            }
        }
        return sb.append('}').toString();
    }
}
