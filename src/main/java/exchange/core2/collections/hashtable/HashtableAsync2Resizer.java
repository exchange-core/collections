package exchange.core2.collections.hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class HashtableAsync2Resizer {

    private static final Logger log = LoggerFactory.getLogger(HashtableAsync2Resizer.class);

    /**
     * allowedPosition value meaning "stop now" - published by the hashtable in switchToNewArray().
     */
    public static final int FINISH_SIGNAL = -1;

    private final long[] srcData;
    private final long[] dstData;
    private final int newMask;

    /**
     * Written by the application (backwards cluster extension), read by the migrator - it is the
     * termination condition of {@link #copy()}, so a non-published write means the migrator would
     * never stop.
     */
    private volatile int startingPosition;

    private volatile int toProcessPosition;
    private volatile int allowedPosition;


    public HashtableAsync2Resizer(long[] srcData, long[] dstData, int startingPosition, int allowedPosition) {
        this.srcData = srcData;
        this.dstData = dstData;
        this.newMask = srcData.length - 1;
        this.startingPosition = startingPosition;
        // startingPosition is a gap and is never copied - start right after it (wrapping around)
        this.toProcessPosition = (startingPosition + 2) & (srcData.length - 1);
        this.allowedPosition = allowedPosition;
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
     * Check follows after confirmed that not is not in old data yet.
     * This method would return true if data is not migrated yet, but will be soon.
     * If it returns false - migrated data can be accessed safely (unless it requires put-extension).
     *
     * (new data also includes 0=A=P)
     */
    public boolean notInNewData(int pos, int lasKnownProgress) { // TODO looks the same ^^^^

        if (startingPosition == lasKnownProgress) {
            return true;
            //return false;
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
        return toProcessPosition;
    }

    public int getStartingPosition() {
        return startingPosition;
    }

    public void setStartingPosition(int startingPosition) {
        this.startingPosition = startingPosition;
    }

    public void copy() {
  //      log.info("(A) ----------- starting async migration capacity: {}->{} -----------------", srcData.length / 2, srcData.length);

        //   log.info("(A) Allocated new array, startingPosition={}, copying initial...", startingPosition);

        int allowedLocal;
        int processedLocal = toProcessPosition;

      //  log.info("allowedPosition={} toProcessPosition={}", allowedPosition, toProcessPosition);

        while (true) {

            allowedLocal = allowedPosition;

            if (allowedLocal == FINISH_SIGNAL) {
                // switchToNewArray() gave up on us - the table no longer reads dstData through
                // this resizer, so stop immediately instead of copying into a detached array
                log.debug("(A) Finish signalled at {} (startingPosition={})", processedLocal, startingPosition);
                return;
            }

            if (processedLocal != allowedLocal) {
                copyInterval(processedLocal, allowedLocal);
                processedLocal = allowedLocal;
                toProcessPosition = allowedLocal;
//                log.debug("processedPosition = {} , allowedPosition={}", processedPosition, allowedPosition);
                if (processedLocal == startingPosition) {
             //       log.debug("(A) Completed async processing at {}", startingPosition);
                    return;
                }

            } else {
                Thread.onSpinWait();
            }
        }
    }

    public void copyInterval(final int from, int to) {
       // log.debug("(A) Copying range {}..{} ...", from, to);

        // 'to' is only ever reached by even wrapping increments - anything else (notably the
        // FINISH_SIGNAL) would make the loop below run forever
        if (to < 0 || to >= srcData.length || (to & 1) != 0) {
            throw new IllegalStateException("copyInterval: unreachable 'to'=" + to
                    + " (from=" + from + " srcData.length=" + srcData.length + ")");
        }

        int pos = from;
        do {
            final long key = srcData[pos];
            if (key != NOT_ALLOWED_KEY) {
                final int offset = HashingUtils.findFreeOffset(key, dstData, newMask);
                // TODO // TODO non epmty gp piblem - should not put anything into FROM ??

                dstData[offset] = key;
                dstData[offset + 1] = srcData[pos + 1];
            }

            pos += 2;
            if (pos == srcData.length) {
                pos = 0;
            }
        } while (pos != to);
        // log.debug("(A) Copied range {}..{}", from, to);
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
