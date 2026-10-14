package exchange.core2.collections.hashtable;


public class HashtableResizer {


    private final long[] data;

    public HashtableResizer(long[] data) {
        this.data = data;
    }

    public long[] resizeSync() {
        // log.info("(S) Allocating array: long[{}] ... -----------------", data.length * 2);
        final long[] data2 = new long[data.length * 2];
        final int newMask = data.length - 1;

        // log.info("(S) Copying data (up to {} elements) ...", data.length / 2);
        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];

            if (key == HashingUtils.NOT_ALLOWED_KEY) {
                continue;
            }

            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);

            data2[offset] = key;
            data2[offset + 1] = data[i + 1];
        }

        // log.info("(S) Copying completed ----------------------");
        return data2;
    }
}
