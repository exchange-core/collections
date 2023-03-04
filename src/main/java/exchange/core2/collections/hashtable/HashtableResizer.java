package exchange.core2.collections.hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class HashtableResizer {

    private static final Logger log = LoggerFactory.getLogger(HashtableResizer.class);

    private final long[] data;

    private final Queue<Long> invalidationQueue;

    public HashtableResizer(long[] data, Queue<Long> invalidationQueue) {
        this.data = data;
        this.invalidationQueue = invalidationQueue;
    }

    public long[] resizeSync() {
        return resize();
    }

    public CompletableFuture<long[]> resizeAsync() {
        return CompletableFuture.supplyAsync(() -> {
            final long[] data2 = resize();
            replicate(data, data2, invalidationQueue);
            return data2;
        });
    }


    private long[] resize() {

        log.info("(A) Allocating array: long[{}] ... -----------------", data.length * 2);
        final long[] data2 = new long[data.length * 2];
        final int newMask = data.length - 1;

        log.info("(A) Copying data (up to {} elements) ...", data.length / 2);
        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];

            if (key == HashingUtils.NOT_ALLOWED_KEY) {
                continue;
            }

            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);

            data2[offset] = key;
            data2[offset + 1] = data[i + 1];
        }

        log.info("(A) Copying completed ----------------------");

        return data2;

    }

    public static void replicate(long[] data, long[] data2, Queue<Long> invalidationQueue) {

        int prevMask = (data.length >> 1) - 1;
        final int newMask = data.length - 1;
        log.debug("(B) Background replication started: prevMask={} newMask={} invalidationQueue.size()={} --------------- ", prevMask, newMask, invalidationQueue.size());
        log.debug("(B) sizes data={} data2={} ", data.length, data2.length);

        long ranges = 0;
        long copied = 0;

        Long e;
        while ((e = invalidationQueue.poll()) != null) {
            final int from = (int) (e >> 32);
            final int toExcl = ((int) (long) e + 1) & prevMask;
            //log.debug("Invalidating: [{}, {}) e={}", from, toExcl, e);
            ranges++;

//            if(ranges % 100000 == 0) {
//                log.debug("Processed {} ranges ... ", ranges);
//            }


            int i = from;
            do {

                // log.debug("(B) ***** Invalidating data2 pos {} & {}", i , (i ) + data.length/2);

                data2[i << 1] = HashingUtils.NOT_ALLOWED_KEY;
                data2[(i << 1) + data.length] = HashingUtils.NOT_ALLOWED_KEY;

                i = (i + 1) & prevMask;

            } while (i != toExcl);

            // log.debug("Copying: {}-{}", from, to);

            i = from;
            do {

                final long key = data[i << 1];

                if (key != HashingUtils.NOT_ALLOWED_KEY) {
                    final int offset = HashingUtils.findFreeOffset(key, data2, newMask);

                    data2[offset] = key;
                    data2[offset + 1] = data[(i << 1) + 1];
                    copied++;

                }
                i = (i + 1) & prevMask;
            } while (i != toExcl);
        }

        if (data.length >= 256) {
            log.debug("(B) CHECK data[122 * 2]={} data", data[122 << 1]);
        }


        log.debug("(B) Background replication completed, processed {} ranges, copied {} records ----------------", ranges, copied);

    }


}
