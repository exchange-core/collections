package exchange.core2.collections.hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class HashtableAsyncResizer {

    private static final Logger log = LoggerFactory.getLogger(HashtableAsyncResizer.class);

    private final long[] prevData;

    public HashtableAsyncResizer(long[] prevData) {
        this.prevData = prevData;
    }

    public long[] resizeAsync() {
        return initResize();
    }


    private long[] initResize() {

        log.info("(S) Allocating array: long[{}] ... -----------------", prevData.length * 2);

        final int g0 = findNextGapPos(0);
        final int g0next = g0 + 2; // TODO migrate 32 elements?
        final int gp = findNextGapPos(g0next == prevData.length ? 0 : g0next);
        log.info("found g0={} gp={}", g0, gp);


        // TODO allocate in new thread and don't block until allocatied
        final long[] data2 = new long[prevData.length * 2];
        final int newMask = prevData.length - 1;


        log.info("(S) Initial migration g0..gp segments ...");
        for (int pos = g0next; pos != gp; pos += 2) {
            if (pos == prevData.length) {
                pos = 0;
            }
            final long key = prevData[pos];
            if (key == NOT_ALLOWED_KEY) {
                continue;
            }
            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);
            data2[offset] = key;
            data2[offset + 1] = prevData[pos + 1];
        }


        int pos = gp;
        log.info("(S) Next segment after {}...", pos);
        do {
            pos += 2;
            if (pos == prevData.length) {
                pos = 0;
            }
            final long key = prevData[pos];
            if (key == NOT_ALLOWED_KEY) {
                // TODO report every 1024 keys
                continue;
            }
            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);
            data2[offset] = key;
            data2[offset + 1] = prevData[pos + 1];
        } while (pos != g0);

        // log.info("(S) Segment ended {}...", pos);


        log.info("(S) Copying completed ----------------------");

        return data2;

    }

    private int findNextGapPos(final int initialPos) {

        int pos = initialPos;

        long existingKey = prevData[pos];
        while (existingKey != NOT_ALLOWED_KEY) {
            pos += 2;
            if (pos == prevData.length) {
                pos = 0;
            }
            if (pos == initialPos) {
                throw new IllegalStateException("No gap found, can not perform async migration");
            }
            existingKey = prevData[pos];
        }

        return pos;
    }

}
