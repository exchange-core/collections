package exchange.core2.collections.hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class HashtableResizer {

    private static final Logger log = LoggerFactory.getLogger(HashtableResizer.class);

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


    public long[] resizeParallelSync() {

        log.info("(S) Allocating array: long[{}] ... -----------------", data.length * 2);
        final long[] data2 = new long[data.length * 2];
        final int newMask = data.length - 1;


        final int threads = 4; // TODO use ForkJoinPool (optional)
        final int step = data.length / threads;

        final int[] gaps = new int[threads];
        for (int i = 0; i < threads; i++) {
            final int gap = findNextGapPos(data, (i * step) & 0xFFFFFFFE);
            // TODO gap can overlap!!
            log.debug("{}/{} gap={}", i, threads, gap);
            gaps[i] = gap;
        }

        final CompletableFuture<?>[] futures = new CompletableFuture[threads];
        for (int i = 0; i < threads; i++) {
            final int fromIncl = gaps[i == 0 ? threads - 1 : i - 1];
            final int toExl = gaps[i];
            futures[i] = CompletableFuture.runAsync(() -> migrateSegment(fromIncl, toExl, data2, newMask));
        }

        log.info("(S) Waiting workers");
        CompletableFuture.allOf(futures).join();

        log.info("(S) Copying completed ----------------------");
        return data2;
    }

    private void migrateSegment(int fromIncl, int toExl, long[] data2, int newMask) {
        if (fromIncl == toExl) {
            throw new IllegalArgumentException("Not allowed fromIncl == toExl");
        }

        log.info("(S) Copying data {}..{} ...", fromIncl, toExl);
        for (int i = fromIncl; ; i += 2) {
            if (i == data.length) {
                i = 0;
            }

            if (i == toExl) {
                break;
            }

            final long key = data[i];

            if (key == HashingUtils.NOT_ALLOWED_KEY) {
                continue;
            }

            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);

            data2[offset] = key;
            data2[offset + 1] = data[i + 1];
        }
        log.info("(S) Copied data {}..{}!", fromIncl, toExl);
    }


    private int findNextGapPos(final long[] prevData, final int initialPos) {

        int pos = initialPos;

        long existingKey = prevData[pos];
        while (existingKey != NOT_ALLOWED_KEY) {
            pos += 2;
            if (pos == prevData.length) {
                pos = 0;
            }
            if (pos == initialPos) {
                throw new IllegalStateException("No gap found, can not perform migration");
            }
            existingKey = prevData[pos];
        }

        return pos;
    }

}
