package exchange.core2.collections.hashtable;

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
    static void deleteAt(long[] data, int offset) {
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
        data[gap + 1] = 0;
    }

    /**
     * An entry at pos with the given home may move back into the gap if the gap lies on its probe path,
     * i.e. within [home, pos) on the ring. Positions in any unit, mask = ring size - 1.
     */
    static boolean canFillGap(int gap, int home, int pos, int mask) {
        return ((pos - home) & mask) >= ((pos - gap) & mask);
    }

    /**
     * All entries as {@code {key=value, ...}} in the array order - the format of {@code java.util.Map}.
     */
    static String entriesToString(long[] data) {
        final StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < data.length; i += 2) {
            if (data[i] != NOT_ALLOWED_KEY) {
                if (sb.length() > 1) {
                    sb.append(", ");
                }
                sb.append(data[i]).append('=').append(data[i + 1]);
            }
        }
        return sb.append('}').toString();
    }
}
