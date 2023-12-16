package exchange.core2.collections.hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

public class HashtableAsyncResizer {

    private static final Logger log = LoggerFactory.getLogger(HashtableAsyncResizer.class);

    private final long[] prevData;

    private long[] data2;
    private int newMask;
    private volatile int g0 = -1;
    private volatile int gp = -1;

    private volatile long pauseRequest = 0;
    private volatile long pauseResponse = 0;

    private CompletableFuture<long[]> arrayFeature;

    private final CompletableFuture<Void> copyingAllowed = new CompletableFuture<>();
    private CompletableFuture<Void> copyingProcess;

    public HashtableAsyncResizer(long[] prevData) {
        this.prevData = prevData;
    }

    public void resizeAsync() {
        initResize();
    }

    public boolean isFinished() {
        return copyingProcess.isDone();
    }

    public long[] waitCompletion() {
        copyingAllowed.complete(null);
        copyingProcess.join();
        return data2;
    }

    public boolean isArrayAllocated() {
        return arrayFeature.isDone();
    }

    public boolean isWaitingPermissionToCopy() {
        return arrayFeature.isDone() && !copyingAllowed.isDone();
    }

    public void allowCopying() {
        copyingAllowed.complete(null);
    }

    public boolean isInMigratedSegment(int pos) {
        final int from = g0;
        final int to = gp;
        if (from == -1 || to == -1) { // TODO remove double volatile read
            return false;
        }
        //   log.debug("check pos:{} in migrated segment {}..{} ", pos, g0, gp);

        if (from == to) {
            // assume finished
            return true;
        }

        if (from <= to) {
            // |--from++++++to--------------|
            return pos > from && pos < to;
        } else {
            // |++++++++++++to--------from++|
            return pos > from || pos < to;
        }
    }

    public static boolean isInMigratedSegment(int pos, int g0, int gp) {
        if (g0 == -1 || gp == -1) {
            return false;
        }

        if (g0 == gp) {
            // assume finished
            return true;
        }

        if (g0 <= gp) {
            return pos > g0 && pos < gp;
        } else {
            return pos > g0 || pos < gp;
        }
    }

    public long[] getNewDataArray() {
        return data2;
    }

    public int getNewMask() {
        return newMask;
    }

    public int getG0() {
        return g0;
    }

    public void setG0(int g0) {
        this.g0 = g0;
    }

    public int getGp() {
        return gp;
    }


    public boolean pause() {
        long ticket = pauseRequest;
        if ((ticket & 1) == 1) {
            throw new IllegalStateException("Already paused");
        }
        ticket++;
        pauseRequest = ticket;
        //     log.debug("pauseRequest = {}, waiting..", ticket);
        while (pauseResponse != ticket) {
            if (copyingProcess.isDone()) {
                return true;
            }
            Thread.yield();
        }
        //   log.debug("request granted: pauseResponse={}  copyingProcess.isDone()={}", pauseResponse, copyingProcess.isDone());
        return false;
    }

    public long resume() {
        long ticket = pauseRequest;
        if ((ticket & 1) == 0) {
            throw new IllegalStateException("Not paused");
        }
        ticket++;

        //     log.debug("pauseRelease:  pauseRequest={}", ticket);
        pauseRequest = ticket;
        return ticket;
    }

    private void initResize() {
        log.info("(A) ----------- starting async migration capacity: {}->{} -----------------", prevData.length / 2, prevData.length);
        final String threadName = Thread.currentThread().getName();
        arrayFeature = CompletableFuture.supplyAsync(() -> new long[prevData.length * 2]);
        copyingProcess = CompletableFuture.runAsync(() -> doMigration(threadName + "-M"));
    }

    private void doMigration(String threadName) {

        Thread.currentThread().setName(threadName); // TODO remove

        log.info("(A) Allocating array: long[{}] ...", prevData.length * 2);

        copyingAllowed.join();
        log.info("(A) waiting arrayFeature...");
        data2 = arrayFeature.join();
        newMask = prevData.length - 1;

        g0 = findNextGapPos(0);
        log.info("(A) Allocated new array, first gap g0={}, copying...", g0);


        int endPos = g0;
        int pos = endPos;

        int counter = 0;
        log.info("(A) Next segment after {}...", pos);
        do {

            pos += 2;
            if (pos == prevData.length) {
                pos = 0;
            }
            final long key = prevData[pos];
            if (key == NOT_ALLOWED_KEY) {
                if (counter >= 127) {
//                    log.debug("Report gp={}", pos);
                    gp = pos; // TODO lazy set
                    counter = 0;

                    // check if pause requested
                    long ticket = pauseRequest;
                    if ((ticket & 1) == 1) {
                        //log.debug("(A) Detected pause requested: {} (gp=pos={})", ticket, pos);
                        pauseResponse = ticket;
                        // indicate pause and wait for release from application
                        while (pauseRequest == ticket) {
                            Thread.yield();
                        }

                        //log.debug("(A) Detected pause released: {}", ticket);
                        // indicate pause release (previous pause, not just any)
                        pauseResponse = ticket + 1;

                        // can be changed in rare scenario
                        final int newEndPos = g0;
                        if (newEndPos != endPos) {
                            log.debug("(A) RARE: updated endPos {}->{} (cur pos={})", endPos, newEndPos, pos);
                            endPos = newEndPos;
                        }
                    }
                }
                continue;
            }
            final int offset = HashingUtils.findFreeOffset(key, data2, newMask);

            data2[offset] = key;
            data2[offset + 1] = prevData[pos + 1];
            counter++;
        } while (pos != endPos);

        gp = pos;
        log.info("(A) Copying completed gp={} pauseResponse={} ----------------------", gp, pauseResponse);

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
