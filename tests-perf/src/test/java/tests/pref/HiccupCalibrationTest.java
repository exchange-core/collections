package tests.pref;

import tests.common.LatencyTools;
import exchange.core2.collections.hashtable.HashingUtils;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Hashing;
import org.agrona.collections.MutableInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;

public class HiccupCalibrationTest {

    private static final Logger log = LoggerFactory.getLogger(HiccupCalibrationTest.class);

    @Test
    public void hiccupCalibrationTest() {

        Thread thread = new Thread(() -> {
//            int size = 1024 * 1024 * 1;
            try (AffinityLock ignore = AffinityLock.acquireCore()) {
                int size = 1024;
                while (true) {
                    try {
                        Thread.sleep(3000);
                        long t = System.currentTimeMillis();
                        log.debug("allocating size: {} ...", size);
                        long[] array = new long[size];
                        log.debug("allocated: {} in {}ms", array.length, System.currentTimeMillis() - t);
                        //size = (size * 2) > 0 ? size * 2 : size;
                        size = (size * 2) < 300_000_000 ? size * 2 : size;
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        });

        int n = 100_000_000;

        try (AffinityLock ignore = AffinityLock.acquireCore()) {

            Random rand = new Random(1L);
            final long[] keys = new long[n];
            for (int i = 0; i < n; i++) keys[i] = rand.nextLong();

            thread.start();

            int tps = 1_000_000;

            final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

            final long picosPerCmd = (1024L * 1_000_000_000L) / tps;
            final long startTimeNs = System.nanoTime();
            long nextPublishTimeNs = startTimeNs + 1_000_000_000L;

            long planneTimeOffsetPs = 0L;
            long lastKnownTimeOffsetPs = 0L;

            long accum = 0L;

            for (int i = 0; i < keys.length; i++) {
                final long key = keys[i];
                planneTimeOffsetPs += picosPerCmd;
                while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                    lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                    // spin until its time to send next command
                    // Thread.onSpinWait(); // 1us-26  max34
                    // LockSupport.parkNanos(2000L); // 1us-25 max29
                    // Thread.yield();   // 1us-28  max32
                }

                final int hash = Hashing.hash(key);
                accum += hash;

                final long nanoTime = System.nanoTime();
                final long putNs = nanoTime - startTimeNs - (planneTimeOffsetPs >> 10);
                histogramPut.recordValue(putNs);

                if (nanoTime > nextPublishTimeNs) {
                    nextPublishTimeNs = nanoTime + 1_000_000_000L;
                    planneTimeOffsetPs += 4_000_000_000L;
                    log.info("{} ({})", LatencyTools.createLatencyReportFast(histogramPut), accum % 2);
                    histogramPut.reset();
                }


            }


        }
    }


}
