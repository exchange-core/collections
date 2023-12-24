package exchange.core2.collections.hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class HashtableAsync2Resizer {

    private static final Logger log = LoggerFactory.getLogger(HashtableAsync2Resizer.class);

    private final long[] srcData;
    private final long[] dstData;
    private final int newMask;

    private int startingPosition;
    private volatile int processedPosition = -1;
    private volatile int allowedPosition;

    private CompletableFuture<Void> copyingProcess;

    public HashtableAsync2Resizer(long[] srcData, long[] dstData, int initialOffset, int allowedPosition) {
        this.srcData = srcData;
        this.dstData = dstData;
        this.newMask = srcData.length - 1;
        this.startingPosition = initialOffset;
        this.allowedPosition = allowedPosition;
    }

    public void resizeAsync() {
        log.info("(A) ----------- starting async migration capacity: {}->{} -----------------", srcData.length / 2, srcData.length);
        final String threadName = Thread.currentThread().getName();
        copyingProcess = CompletableFuture.runAsync(() -> doMigration(threadName + "-M"));
    }

    public boolean isFinished() {
        return copyingProcess.isDone();
    }

    public long[] waitCompletion() {
        copyingProcess.join();
        return dstData;
    }



    /**
     * Check if position is surely non migrated
     * Range considered as exclusive - always preserving gaps
     *
     * @param pos             - requested position
     * @param allowedPosition - allowed position (from hashtable's prospective, not migrator's)
     * @return true if it is safe to access old data array, false if maybe in migrated (may need further checks on recent progress)
     */
    public boolean isInOldData(int pos, int allowedPosition) {
        if (startingPosition < allowedPosition) {
            return pos < startingPosition || pos > allowedPosition;
        } else {
            return pos < startingPosition && pos > allowedPosition;
        }
    }

    /**
     * (new data also includes 0=A=P)
     */
    public boolean notInNewData(int pos, int lasKnownProgress) { // TODO looks the same ^^^^

        if(startingPosition == lasKnownProgress){
            return false;
        }

        if (startingPosition <= lasKnownProgress) {
            return pos < startingPosition || pos > lasKnownProgress;
        } else {
            return pos < startingPosition && pos > lasKnownProgress;
        }
    }


    public long[] getNewDataArray() {
        return dstData;
    }

    public int getNewMask() {
        return newMask;
    }


    public void setAllowedPosition(int allowedPosition) {
        this.allowedPosition = allowedPosition; // TODO Lazyset
    }

    // only gaps
    public int getProcessedPosition() {
        return processedPosition;
    }

    public int getStartingPosition() {
        return startingPosition;
    }

    public void setStartingPosition(int startingPosition) {
        this.startingPosition = startingPosition;
    }

    private void doMigration(String threadName) {
        Thread.currentThread().setName(threadName); // TODO remove

        log.info("(A) Allocated new array, startingPosition={}, copying initial...", startingPosition);

        int allowedLocal = allowedPosition;

        if (allowedLocal == -1) {
            log.error("Unexpected -1 here");
            throw new IllegalStateException();
        }

        copyInterval(startingPosition, allowedLocal);
        int processedTill = allowedLocal;
        processedPosition = allowedLocal;

        while (true) {

            allowedLocal = allowedPosition;

            if (allowedLocal == -1) {
                // signalled to finish
                log.info("(A) Copying completed ----------------------");
                return;
            }

            if (processedTill != allowedLocal) {
                copyInterval(processedTill, allowedLocal);
                processedTill = allowedLocal;
                processedPosition = allowedLocal;
                log.debug("processedPosition = {} , allowedPosition={}", processedPosition, allowedPosition);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private void copyInterval(final int from, int to) {
        log.debug("(A) Copying segment {}..{} ...", from, to);
        int pos = from;
        do {
            pos += 2;
            if (pos == srcData.length) {
                pos = 0;
            }
            final long key = srcData[pos];
            if (key != NOT_ALLOWED_KEY) {
                final int offset = HashingUtils.findFreeOffset(key, dstData, newMask);
                dstData[offset] = key;
                dstData[offset + 1] = srcData[pos + 1];
            }
        } while (pos != to);
        log.debug("(A) Copied segment {}..{}", from, to);
    }


    public static int findNextGapPos(final long[] data, final int initialPos) {

        int pos = initialPos;

        long existingKey = data[pos];
        while (existingKey != NOT_ALLOWED_KEY) {
            pos += 2;
            if (pos == data.length) {
                pos = 0;
            }
            if (pos == initialPos) {
                throw new IllegalStateException("No gap found, can not perform async migration");
            }
            existingKey = data[pos];
        }

        return pos;
    }

}
