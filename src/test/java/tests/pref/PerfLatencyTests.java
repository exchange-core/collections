package tests.pref;

import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLLHashtable;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PerfLatencyTests {
    private static final Logger log = LoggerFactory.getLogger(PerfLatencyTests.class);

    /*
11000000: {50.0%=6.7ms, 90.0%=12.6ms, 95.0%=13.4ms, 99.0%=14.0ms, 99.9%=14.1ms, 99.99%=14.1ms, W=325ms}
21900000: {50.0%=8.3ms, 90.0%=17.7ms, 95.0%=18.7ms, 99.0%=19.5ms, 99.9%=19.7ms, 99.99%=19.7ms, W=784ms}
43700000: {50.0%=5.9ms, 90.0%=13.7ms, 95.0%=14.7ms, 99.0%=15.5ms, 99.9%=15.7ms, 99.99%=15.7ms, W=1.31s}
87300000: {50.0%=1.7ms, 90.0%=9.7ms, 95.0%=10.7ms, 99.0%=11.5ms, 99.9%=11.7ms, 99.99%=11.8ms, W=2.51s}
174500000: {50.0%=0.2us, 90.0%=1.57ms, 95.0%=2.65ms, 99.0%=3.7ms, 99.9%=3.8ms, 99.99%=3.9ms, W=5.0s}

2 threads:
11000000: {50.0%=7.7ms, 90.0%=14.0ms, 95.0%=14.9ms, 99.0%=15.5ms, 99.9%=15.6ms, 99.99%=15.6ms, W=299ms}
21900000: {50.0%=7.2ms, 90.0%=14.4ms, 95.0%=15.4ms, 99.0%=16.2ms, 99.9%=16.4ms, 99.99%=16.4ms, W=581ms}
43700000: {50.0%=6.8ms, 90.0%=14.9ms, 95.0%=16.0ms, 99.0%=16.8ms, 99.9%=17.0ms, 99.99%=17.0ms, W=1.11s}
87300000: {50.0%=1.78ms, 90.0%=10.2ms, 95.0%=11.3ms, 99.0%=12.1ms, 99.9%=12.3ms, 99.99%=12.3ms, W=2.18s}

3 threads:
11000000: {50.0%=7.8ms, 90.0%=14.5ms, 95.0%=15.3ms, 99.0%=16.0ms, 99.9%=16.1ms, 99.99%=16.2ms, W=299ms}
21900000: {50.0%=7.5ms, 90.0%=14.4ms, 95.0%=15.2ms, 99.0%=15.9ms, 99.9%=16.0ms, 99.99%=16.0ms, W=606ms}
43700000: {50.0%=7.2ms, 90.0%=17.2ms, 95.0%=18.4ms, 99.0%=19.3ms, 99.9%=19.5ms, 99.99%=19.5ms, W=1.08s}
87300000: {50.0%=1.8ms, 90.0%=10.2ms, 95.0%=11.3ms, 99.0%=12.1ms, 99.9%=12.3ms, 99.99%=12.3ms, W=2.07s}





21:51:22.626 [main] INFO  e.c.c.hashtable.HashtableResizer - (S) Allocating array: long[67108864] ... -----------------
21:51:22.833 [main] DEBUG e.c.c.hashtable.HashtableResizer - 0/6 gap=18
21:51:22.833 [main] DEBUG e.c.c.hashtable.HashtableResizer - 1/6 gap=5592404
21:51:22.833 [main] DEBUG e.c.c.hashtable.HashtableResizer - 2/6 gap=11184814
21:51:22.834 [main] DEBUG e.c.c.hashtable.HashtableResizer - 3/6 gap=16777216
21:51:22.834 [main] DEBUG e.c.c.hashtable.HashtableResizer - 4/6 gap=22369620
21:51:22.834 [main] DEBUG e.c.c.hashtable.HashtableResizer - 5/6 gap=27962030
21:51:22.834 [main] INFO  e.c.c.hashtable.HashtableResizer - (S) Waiting workers
21:51:22.834 [ForkJoinPool.commonPool-worker-1] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying data 27962030..18 ...
21:51:22.834 [ForkJoinPool.commonPool-worker-6] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying data 18..5592404 ...
21:51:22.834 [ForkJoinPool.commonPool-worker-5] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying data 5592404..11184814 ...
21:51:22.834 [ForkJoinPool.commonPool-worker-4] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying data 11184814..16777216 ...
21:51:22.834 [ForkJoinPool.commonPool-worker-3] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying data 22369620..27962030 ...
21:51:22.834 [ForkJoinPool.commonPool-worker-2] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying data 16777216..22369620 ...
21:51:22.906 [ForkJoinPool.commonPool-worker-3] INFO  e.c.c.hashtable.HashtableResizer - (S) Copied data 22369620..27962030!
21:51:22.908 [ForkJoinPool.commonPool-worker-6] INFO  e.c.c.hashtable.HashtableResizer - (S) Copied data 18..5592404!
21:51:22.908 [ForkJoinPool.commonPool-worker-5] INFO  e.c.c.hashtable.HashtableResizer - (S) Copied data 5592404..11184814!
21:51:22.908 [ForkJoinPool.commonPool-worker-2] INFO  e.c.c.hashtable.HashtableResizer - (S) Copied data 16777216..22369620!
21:51:22.909 [ForkJoinPool.commonPool-worker-4] INFO  e.c.c.hashtable.HashtableResizer - (S) Copied data 11184814..16777216!
21:51:22.909 [ForkJoinPool.commonPool-worker-1] INFO  e.c.c.hashtable.HashtableResizer - (S) Copied data 27962030..18!
21:51:22.909 [main] INFO  e.c.c.hashtable.HashtableResizer - (S) Copying completed ----------------------


     */
    @Test
    public void benchmarkBasic() {
        benchmarkAbstract(
            (long[] kv) -> {
                final ILongLongHashtable hashtable = new LongLongHashtable(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (ILongLongHashtable hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    @Test
    public void benchmarkLL() {
        benchmarkAbstract(
            (long[] kv) -> {
                final ILongLongHashtable hashtable = new LongLongLLHashtable(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (ILongLongHashtable hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }


    /*
11000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.9us, 99.9%=1.8us, 99.99%=16.4us, W=465ms}
21900000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=1.7us, 99.99%=3.8us, W=787ms}
43700000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=1.2us, 99.9%=2.1us, 99.99%=13.6us, W=1.57s}
87300000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.4us, W=3.1s}
175600000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.1us, 99.99%=5.0us, W=7.5us}
     */
    @Test
    public void benchmarkAgrona() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Long2LongHashMap hashtable = new Long2LongHashMap(5_000_000, Hashing.DEFAULT_LOAD_FACTOR, 0L);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Long2LongHashMap hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    /*
25200000: {50.0%=0.6us, 90.0%=12.2ms, 95.0%=13.6ms, 99.0%=14.8ms, 99.9%=15.1ms, 99.99%=15.1ms, W=1.46s}
27900000: {50.0%=6.5ms, 90.0%=19.3ms, 95.0%=603ms, 99.0%=605ms, 99.9%=605ms, 99.99%=605ms, W=605ms}
50400000: {50.0%=6.6ms, 90.0%=19.9ms, 95.0%=21.7ms, 99.0%=22.9ms, 99.9%=23.2ms, 99.99%=23.2ms, W=3.5s}
64300000: {50.0%=10.1ms, 90.0%=922ms, 95.0%=924ms, 99.0%=925ms, 99.9%=926ms, 99.99%=926ms, W=926ms}
100700000: {50.0%=1.0us, 90.0%=9.2ms, 95.0%=10.9ms, 99.0%=12.2ms, 99.9%=12.5ms, 99.99%=12.5ms, W=8.1s}
168900000: {50.0%=18.2ms, 90.0%=39ms, 95.0%=41ms, 99.0%=1.35s, 99.9%=1.35s, 99.99%=1.35s, W=1.35s}
     */
    @Test
    public void benchmarkStdHashMap() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Map<Long, Long> hashtable = new HashMap<>(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Map<Long, Long> hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    /*
12600000: {50.0%=0.4us, 90.0%=5.8ms, 95.0%=9.3ms, 99.0%=11.9ms, 99.9%=12.5ms, 99.99%=12.6ms, W=1.29s}
25200000: {50.0%=0.6us, 90.0%=10.9ms, 95.0%=14.1ms, 99.0%=16.6ms, 99.9%=17.0ms, 99.99%=17.1ms, W=3.6s}
50400000: {50.0%=8.4ms, 90.0%=23.9ms, 95.0%=25.7ms, 99.0%=27.1ms, 99.9%=27.5ms, 99.99%=27.5ms, W=5.1s}
100700000: {50.0%=0.7us, 90.0%=11.9ms, 95.0%=14.0ms, 99.0%=15.5ms, 99.9%=15.8ms, 99.99%=15.8ms, W=12.9s}
     */
    @Test
    public void benchmarkStdCHM() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Map<Long, Long> hashtable = new ConcurrentHashMap<>(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Map<Long, Long> hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }


    @Test
    public void benchmarkChronicleMap() {
        benchmarkAbstract(
            (long[] kv) -> {

                final ChronicleMapBuilder<Long, Long> longsMapBuilder =
                    ChronicleMapBuilder.of(Long.class, Long.class)
                        .name("long-long-benchmark-map")
                        .entries(100_000_000);
                final ChronicleMap<Long, Long> longsMap =
                    longsMapBuilder.create();

                for (long l : kv) longsMap.put(l, l);
                return longsMap;
            },
            this::benchmark,
            (Map<Long, Long> hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }


    private <T> void benchmarkAbstract(Function<long[], T> factory,
                                       BiFunction<T, long[], SingleResult> singleTest,
                                       BiConsumer<T, long[]> extraLoader) {
        int n = 4_000_000;
        long seed = 2918723469278364978L;


        log.debug("Pre-filling {} random k/v pairs...", n);
        Random rand = new Random(seed);
        final long[] prefillKeys = new long[n];
        for (int i = 0; i < n; i++) prefillKeys[i] = rand.nextLong();
//
//
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            hashtable.put(key, value);
//        }

        int n2 = 100_000;

        final T hashtable = factory.apply(prefillKeys);
        log.debug("Benchmarking...");

        final long[] keys = new long[n2];

        // TODO make continuous test (non-stop)

        for (int j = 0; j < 1780; j++) {
            for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();
            final SingleResult benchmark = singleTest.apply(hashtable, keys);
            log.info("{}: {}", benchmark.size, LatencyTools.createLatencyReportFast(benchmark.avgGet));
            extraLoader.accept(hashtable, keys);
        }


//
//        log.info("done");
    }

    /*

21:30:50.390 87100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.2us, 99.0%=2.1us, 99.9%=7.1us, 99.99%=16.3us, W=32us}
21:30:50.509 87200000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=1.8us, 99.9%=5.0us, 99.99%=14.3us, W=33us}
21:30:50.567 - (A) ----------- starting async migration capacity: 134217728->268435456 -----------------
21:30:50.567 table - Resize preparation: 134us
21:30:50.596 r - (A) Allocating array: long[536870912] ...
21:30:50.677 87300000: {50.0%=6.0ms, 90.0%=34ms, 95.0%=37ms, 99.0%=41ms, 99.9%=42ms, 99.99%=42ms, W=42ms}
21:30:50.956 87400000: {50.0%=7.7ms, 90.0%=24.0ms, 95.0%=28.3ms, 99.0%=34ms, 99.9%=35ms, 99.99%=35ms, W=35ms}
21:30:51.235 87500000: {50.0%=7.6ms, 90.0%=29.2ms, 95.0%=36ms, 99.0%=41ms, 99.9%=43ms, 99.99%=43ms, W=43ms}
21:30:51.544 87600000: {50.0%=8.6ms, 90.0%=29.6ms, 95.0%=34ms, 99.0%=39ms, 99.9%=41ms, 99.99%=41ms, W=41ms}
21:30:51.818 87700000: {50.0%=7.1ms, 90.0%=22.1ms, 95.0%=27.1ms, 99.0%=33ms, 99.9%=34ms, 99.99%=34ms, W=34ms}
21:30:52.107 87800000: {50.0%=8.0ms, 90.0%=29.5ms, 95.0%=33ms, 99.0%=37ms, 99.9%=39ms, 99.99%=39ms, W=39ms}
21:30:52.344 r - (A) Allocated new array, first gap g0=0, copying...
21:30:52.344 r - (A) Next segment after 0...
21:30:52.412 87900000: {50.0%=8.9ms, 90.0%=44ms, 95.0%=56ms, 99.0%=67ms, 99.9%=69ms, 99.99%=69ms, W=69ms}
21:30:52.855 88000000: {50.0%=24.5ms, 90.0%=60ms, 95.0%=70ms, 99.0%=78ms, 99.9%=81ms, 99.99%=81ms, W=81ms}
21:30:53.206 88100000: {50.0%=19.0ms, 90.0%=43ms, 95.0%=46ms, 99.0%=49ms, 99.9%=50ms, 99.99%=50ms, W=50ms}
21:30:53.457 88200000: {50.0%=2.37ms, 90.0%=8.0ms, 95.0%=9.7ms, 99.0%=11.3ms, 99.9%=11.8ms, 99.99%=11.9ms, W=11.9ms}
21:30:53.668 88300000: {50.0%=2.3us, 90.0%=547us, 95.0%=1.1ms, 99.0%=2.05ms, 99.9%=2.51ms, 99.99%=2.59ms, W=2.61ms}
21:30:53.841 88400000: {50.0%=0.6us, 90.0%=20.4us, 95.0%=46us, 99.0%=106us, 99.9%=148us, 99.99%=176us, W=184us}
21:30:53.986 88500000: {50.0%=0.2us, 90.0%=2.1us, 95.0%=4.2us, 99.0%=198us, 99.9%=548us, 99.99%=603us, W=610us}
21:30:54.075 r - (A) Copying completed gp=0 pauseResponse=3563620 ----------------------
21:30:54.110 88600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.4us, 99.9%=13.8us, 99.99%=29.3us, W=34us}
21:30:54.220 88700000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.4us, 99.9%=7.6us, 99.99%=24.9us, W=34us}

     */

    private SingleResult benchmark(ILongLongHashtable hashtable, long[] keys) {

        int tps = 1_000_000;


        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / tps;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        //int nanoTimeRequestsCounter = 0;

        for (int i = 0; i < keys.length; i++) {

            final long key = keys[i];

            planneTimeOffsetPs += picosPerCmd;

            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {

                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;

                // nanoTimeRequestsCounter++;

                // spin until its time to send next command
                Thread.onSpinWait(); // 1us-26  max34
                // LockSupport.parkNanos(1L); // 1us-25 max29
                // Thread.yield();   // 1us-28  max32
            }

            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (lastKnownTimeOffsetPs >> 10);

            histogramPut.recordValue(putNs);
        }


//        for (long key : keys) {
//            long t = System.nanoTime();
//            hashtable.put(key, key);
//            long putNs = (System.nanoTime() - t);
//            histogramPut.recordValue(putNs);
//
//            if (putNs > 1_000_000) {
//                log.debug("{}: took too long {}ms, key={} ", hashtable.size(), putNs / 1000000, key);
//            }
//        }


//        t = System.nanoTime();
//        long acc = 0;
//        for (long key : keys) acc += hashtable.get(key);
//        long getNs = (System.nanoTime() - t) / keys.length;
//
//        t = System.nanoTime();
//        for (long key : keys) hashtable.remove(key);
//        long removNs = (System.nanoTime() - t) / keys.length;

//        log.info("validating remove...");
//        Random rand = new Random(keys[0]);
//        for (int i = 0; i < keys.length; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertThat(hashtable.remove(key), Is.is(value));
//        }
//
//        log.info("confirm empty...");
//        rand = new Random(keys[0]);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertFalse(hashtable.containsKey(key));
//        }


        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmark(Long2LongHashMap hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        for (long key : keys) {
            long t = System.nanoTime();
            hashtable.put(key, key);
            long putNs = (System.nanoTime() - t);
            histogramPut.recordValue(putNs);
        }

//        t = System.nanoTime();
//        long acc = 0;
//        for (long key : keys) acc += hashtable.get(key);
//        long getNs = (System.nanoTime() - t) / keys.length;
//
//        t = System.nanoTime();
//        for (long key : keys) hashtable.remove(key);
//        long removNs = (System.nanoTime() - t) / keys.length;
//        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }


    private SingleResult benchmark(Map<Long, Long> hashtable, long[] keys) {

        int tps = 1_000_000;


        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / tps;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                // spin until its time to send next command
                Thread.onSpinWait(); // 1us-26  max34
                // LockSupport.parkNanos(1L); // 1us-25 max29
                // Thread.yield();   // 1us-28  max32
            }
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (lastKnownTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }


    record SingleResult(long size, Histogram avgPut, Histogram avgGet, Histogram avgRemove) {

    }


}
