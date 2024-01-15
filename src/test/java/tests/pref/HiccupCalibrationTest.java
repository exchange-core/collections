package tests.pref;

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


    /*
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=8.1us, W=11.8us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=6.5us, W=10.2us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=7.6us, W=11.4us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=9.0us, W=12.8us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=7.8us, W=11.6us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.8us, W=8.7us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.0us, W=7.8us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.6us, W=8.5us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=5.6us, W=9.5us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=3.7us, W=7.6us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.7us, W=8.5us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=5.1us, W=8.8us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.1us, W=8.0us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.0us, W=7.8us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=3.7us, W=7.8us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=8.9us, W=27.9us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.1us, W=7.9us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=6.1us, W=9.9us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=3.9us, W=7.6us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=4.7us, W=8.6us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=3.6us, W=7.4us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=3.7us, W=7.6us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=28.9us, W=33us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=9.8us, W=13.6us} (-1)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=6.2us, W=10.0us} (0)
{50.0%=0.03us, 90.0%=0.03us, 95.0%=0.03us, 99.0%=0.03us, 99.9%=0.03us, 99.99%=6.7us, W=10.5us} (0)
     */
    @Test
    public void hiccupCalibrationTest() {

        Thread thread = new Thread(() -> {
//            int size = 1024 * 1024 * 1;
            try (AffinityLock ignore = AffinityLock.acquireCore()) {
                int size = 1024;
                while (true) {
                    try {
                        Thread.sleep(1000);
                        long t = System.currentTimeMillis();
                        log.debug("allocating size: {} ...", size);
                        long[] array = new long[size];
//                    log.debug("allocated: {} in {}ms", array.length, System.currentTimeMillis() - t);
                       //size = (size * 2) > 0 ? size * 2 : size;
                        size = (size * 2) < 300_000_000 ? size * 2 : size;
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        });

        Thread thread2 = new Thread(() -> {

            final Executor CORE_LOCK_EXECUTOR = task -> {
                Thread thread3 = new Thread(() -> {
                    try (AffinityLock affinityLock = AffinityLock.acquireCore()) {
                        Thread.currentThread().setName("AFC" + affinityLock.cpuId());
                        task.run();
                    }
                });
                thread3.start();
            };

//            int size = 1024 * 1024 * 1;
            try (AffinityLock ignore = AffinityLock.acquireCore()) {
                MutableInteger size = new MutableInteger(1024);
                while (true) {
                    try {
                        Thread.sleep(1000);

                        CORE_LOCK_EXECUTOR.execute(()->{

                            long t = System.currentTimeMillis();
                            log.debug("allocating size: {} ...", size.value);
                            long[] array = new long[size.value];
//                    log.debug("allocated: {} in {}ms", array.length, System.currentTimeMillis() - t);
                            //size = (size * 2) > 0 ? size * 2 : size;
                            size.set( (size.value * 2) < 300_000_000 ? size.value * 2 : size.value);

                        });

                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        });



        int n = 100_000_000;
        int resetNums = 10;

        try (AffinityLock ignore = AffinityLock.acquireCore()) {

            Random rand = new Random(1L);
            final long[] keys = new long[n];
            for (int i = 0; i < n; i++) keys[i] = rand.nextLong();

            thread.start();

            int tps = 1000_000;

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
                    //Thread.onSpinWait(); // 1us-26  max34
                    // LockSupport.parkNanos(2000L); // 1us-25 max29
                    //Thread.yield();   // 1us-28  max32
                }

                final int hash = Hashing.hash(key);
                accum += hash;

                final long nanoTime = System.nanoTime();
                final long putNs = nanoTime - startTimeNs - (lastKnownTimeOffsetPs >> 10);
                histogramPut.recordValue(putNs);

                if (nanoTime > nextPublishTimeNs) {
                    nextPublishTimeNs = nanoTime + 1_000_000_000L;
                    log.info("{} ({})", LatencyTools.createLatencyReportFast(histogramPut), accum % 2);
                    if (resetNums-- > 0) {
                        histogramPut.reset();
                    }
                }


            }


        }
    }


}
