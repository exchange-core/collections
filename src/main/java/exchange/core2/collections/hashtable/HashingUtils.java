package exchange.core2.collections.hashtable;

import org.agrona.collections.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashingUtils {

    public static final long NOT_ALLOWED_KEY = 0L;

    private static final Logger log = LoggerFactory.getLogger(HashingUtils.class);

    public static int threshold = 512; // TODO not threadsafe!!

    /**
     * Finds free offset where key cell either empty or the same as provided
     * @param key - key provided
     * @param data - data array
     * @param mask - mask for data array
     * @return offset in the array
     */
    public static int findFreeOffset(long key, long[] data, int mask) {
        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;
//        log.debug("collision on key:{} hash:{}(0x{})->pos:{}(0x{})", key, hash, String.format("%x", hash), pos, String.format("%x", pos));
        return findFreeOffset(key, pos, data);
    }

    public static int findFreeOffset(long key, int pos, long[] data) {
        long i = 0;

        long existingKey = data[pos];
        while (existingKey != NOT_ALLOWED_KEY && existingKey != key) {
            // try next element
            pos += 2;
            if (pos == data.length) {
                pos = 0;
            }
            existingKey = data[pos];

//            log.debug("try next pos={}", pos);
            i++;
        }

        if (i > threshold) {
            log.warn("findFreeOffset took {} iterations", i);
            threshold *= 2;
        }

        return pos;
    }
}
