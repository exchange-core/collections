package exchange.core2.collections.hashtable;


class HashtableResizer {


    private final long[] data;
    private final long missingValue;

    HashtableResizer(long[] data, long missingValue) {
        this.data = data;
        this.missingValue = missingValue;
    }

    long[] resizeSync() {
        return rehash(data, data.length, missingValue);
    }

    /**
     * Copies all entries of src into a new array of the given capacity (in slots, a power of two).
     */
    static long[] rehash(long[] src, int capacity, long missingValue) {
        final long[] data2 = LinearProbing.newArray(capacity, missingValue);
        final int newMask = capacity - 1;

        for (int i = 0; i < src.length; i += 2) {
            final long key = src[i];

            if (key == HashingUtils.NOT_ALLOWED_KEY) {
                continue;
            }

            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);

            data2[offset] = key;
            data2[offset + 1] = src[i + 1];
        }

        return data2;
    }
}
