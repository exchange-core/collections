package tests.pref;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
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
01:20:56.341 [main] INFO  tests.pref.PerfLatencyTests - 10900000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=3.3us, 99.9%=9.0us, 99.99%=25.9us, W=34us}
01:20:56.361 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[67108864] ...
01:20:56.457 [main] INFO  tests.pref.PerfLatencyTests - 11000000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=3.3us, 99.9%=75us, 99.99%=100us, W=281us}
01:20:56.569 [main] INFO  tests.pref.PerfLatencyTests - 11100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.3us, 99.0%=21.7us, 99.9%=93us, 99.99%=467us, W=469us}
01:20:56.598 [main] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 16777216->33554432 -----------------
01:20:56.598 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Allocated new array, startingPosition=28, copying initial...
01:20:56.687 [main] INFO  tests.pref.PerfLatencyTests - 11200000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=4.9us, 99.9%=44us, 99.99%=66us, W=281us}
01:20:56.749 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
01:20:56.809 [main] INFO  tests.pref.PerfLatencyTests - 11300000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.3us, 99.0%=3.6us, 99.9%=8.9us, 99.99%=29.5us, W=35us}

01:21:08.764 [main] INFO  tests.pref.PerfLatencyTests - 21800000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.3us, 99.0%=3.2us, 99.9%=15.9us, 99.99%=29.6us, W=38us}
01:21:08.789 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[134217728] ...
01:21:08.879 [main] INFO  tests.pref.PerfLatencyTests - 21900000: {50.0%=0.2us, 90.0%=1.1us, 95.0%=2.1us, 99.0%=7.2us, 99.9%=27.6us, 99.99%=34us, W=196us}
01:21:09.009 [main] INFO  tests.pref.PerfLatencyTests - 22000000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=3.0us, 99.9%=58us, 99.99%=96us, W=99us}
01:21:09.123 [main] INFO  tests.pref.PerfLatencyTests - 22100000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=4.6us, 99.9%=21.9us, 99.99%=59us, W=63us}
01:21:09.242 [main] INFO  tests.pref.PerfLatencyTests - 22200000: {50.0%=0.2us, 90.0%=0.6us, 95.0%=1.0us, 99.0%=1.9us, 99.9%=8.4us, 99.99%=23.1us, W=31us}
01:21:09.323 [main] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 33554432->67108864 -----------------
01:21:09.324 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Allocated new array, startingPosition=0, copying initial...
01:21:09.357 [main] INFO  tests.pref.PerfLatencyTests - 22300000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=8.0us, 99.9%=139us, 99.99%=173us, W=342us}
01:21:09.483 [main] INFO  tests.pref.PerfLatencyTests - 22400000: {50.0%=0.5us, 90.0%=4.6us, 95.0%=13.7us, 99.0%=57us, 99.9%=92us, 99.99%=108us, W=199us}
01:21:09.592 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT 359045713944176631: RARE Extending backwards section g0=0
01:21:09.593 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - Found new g0=67108860, migrating from old g0=0
01:21:09.593 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - DONE RARE: offsetFinal=67108864 prevVal=0
01:21:09.611 [main] INFO  tests.pref.PerfLatencyTests - 22500000: {50.0%=0.4us, 90.0%=3.7us, 95.0%=181us, 99.0%=701us, 99.9%=1.06ms, 99.99%=1.11ms, W=1.29ms}
01:21:09.733 [main] INFO  tests.pref.PerfLatencyTests - 22600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.3us, 99.9%=11.4us, 99.99%=24.8us, W=32us}
01:21:09.738 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
01:21:09.845 [main] INFO  tests.pref.PerfLatencyTests - 22700000: {50.0%=0.2us, 90.0%=1.3us, 95.0%=2.9us, 99.0%=16.6us, 99.9%=58us, 99.99%=75us, W=126us}
01:21:09.964 [main] INFO  tests.pref.PerfLatencyTests - 22800000: {50.0%=0.2us, 90.0%=1.3us, 95.0%=2.6us, 99.0%=21.6us, 99.9%=146us, 99.99%=499us, W=501us}
01:21:10.075 [main] INFO  tests.pref.PerfLatencyTests - 22900000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.4us, 99.9%=14.2us, 99.99%=26.4us, W=32us}

01:21:33.721 [main] INFO  tests.pref.PerfLatencyTests - 43500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=1.7us, 99.9%=5.3us, 99.99%=14.3us, W=20.6us}
01:21:33.836 [main] INFO  tests.pref.PerfLatencyTests - 43600000: {50.0%=0.2us, 90.0%=1.1us, 95.0%=4.4us, 99.0%=155us, 99.9%=261us, 99.99%=281us, W=1.13ms}
01:21:33.875 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[268435456] ...
01:21:33.955 [main] INFO  tests.pref.PerfLatencyTests - 43700000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=45us, 99.9%=167us, 99.99%=735us, W=737us}
01:21:34.071 [main] INFO  tests.pref.PerfLatencyTests - 43800000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=2.2us, 99.0%=39us, 99.9%=221us, 99.99%=241us, W=1.12ms}
01:21:34.195 [main] INFO  tests.pref.PerfLatencyTests - 43900000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.5us, 99.9%=12.7us, 99.99%=32us, W=35us}
01:21:34.310 [main] INFO  tests.pref.PerfLatencyTests - 44000000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.2us, 99.9%=20.0us, 99.99%=37us, W=44us}
01:21:34.430 [main] INFO  tests.pref.PerfLatencyTests - 44100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.9us, 99.9%=7.3us, 99.99%=24.0us, W=31us}
01:21:34.548 [main] INFO  tests.pref.PerfLatencyTests - 44200000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=4.6us, 99.9%=18.9us, 99.99%=39us, W=43us}
01:21:34.672 [main] INFO  tests.pref.PerfLatencyTests - 44300000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=2.1us, 99.9%=7.0us, 99.99%=14.7us, W=48us}
01:21:34.788 [main] INFO  tests.pref.PerfLatencyTests - 44400000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.7us, 99.9%=23.0us, 99.99%=38us, W=41us}
01:21:34.912 [main] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 67108864->134217728 -----------------
01:21:34.912 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Allocated new array, startingPosition=0, copying initial...
01:21:34.912 [main] INFO  tests.pref.PerfLatencyTests - 44500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=3.1us, 99.9%=17.2us, 99.99%=28.3us, W=163us}
01:21:35.029 [main] INFO  tests.pref.PerfLatencyTests - 44600000: {50.0%=0.2us, 90.0%=1.1us, 95.0%=1.9us, 99.0%=5.5us, 99.9%=20.1us, 99.99%=28.6us, W=37us}
01:21:35.155 [main] INFO  tests.pref.PerfLatencyTests - 44700000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.1us, 99.0%=2.4us, 99.9%=26.2us, 99.99%=32us, W=34us}
01:21:35.271 [main] INFO  tests.pref.PerfLatencyTests - 44800000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.7us, 99.0%=4.9us, 99.9%=17.6us, 99.99%=26.9us, W=41us}
01:21:35.385 [main] INFO  tests.pref.PerfLatencyTests - 44900000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=3.3us, 99.9%=27.5us, 99.99%=63us, W=65us}
01:21:35.499 [main] INFO  tests.pref.PerfLatencyTests - 45000000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.8us, 99.9%=10.8us, 99.99%=24.4us, W=33us}
01:21:35.520 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
01:21:35.617 [main] INFO  tests.pref.PerfLatencyTests - 45100000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=1.9us, 99.9%=6.3us, 99.99%=16.3us, W=32us}

01:22:24.278 [main] INFO  tests.pref.PerfLatencyTests - 87200000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=3.0us, 99.9%=8.8us, 99.99%=23.2us, W=34us}
01:22:24.344 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[536870912] ...
01:22:24.403 [main] INFO  tests.pref.PerfLatencyTests - 87300000: {50.0%=0.3us, 90.0%=1.1us, 95.0%=1.8us, 99.0%=7.1us, 99.9%=63us, 99.99%=245us, W=248us}
01:22:24.664 [main] INFO  tests.pref.PerfLatencyTests - 87400000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.6us, 99.9%=10.8us, 99.99%=26.2us, W=31us}
01:22:24.782 [main] INFO  tests.pref.PerfLatencyTests - 87500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=2.3us, 99.9%=8.7us, 99.99%=17.5us, W=27.2us}
01:22:24.899 [main] INFO  tests.pref.PerfLatencyTests - 87600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.0us, 99.9%=13.9us, 99.99%=27.3us, W=34us}
01:22:25.021 [main] INFO  tests.pref.PerfLatencyTests - 87700000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=3.5us, 99.9%=15.7us, 99.99%=28.6us, W=31us}
01:22:25.138 [main] INFO  tests.pref.PerfLatencyTests - 87800000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.0us, 99.9%=16.6us, 99.99%=33us, W=60us}
01:22:25.256 [main] INFO  tests.pref.PerfLatencyTests - 87900000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.6us, 99.9%=19.4us, 99.99%=33us, W=39us}
01:22:25.373 [main] INFO  tests.pref.PerfLatencyTests - 88000000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=4.9us, 99.9%=26.3us, 99.99%=51us, W=56us}
01:22:25.494 [main] INFO  tests.pref.PerfLatencyTests - 88100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.6us, 99.9%=25.3us, 99.99%=59us, W=61us}
01:22:25.611 [main] INFO  tests.pref.PerfLatencyTests - 88200000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=3.5us, 99.9%=10.7us, 99.99%=23.8us, W=35us}
01:22:25.738 [main] INFO  tests.pref.PerfLatencyTests - 88300000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.1us, 99.0%=2.6us, 99.9%=9.1us, 99.99%=23.1us, W=33us}
01:22:25.855 [main] INFO  tests.pref.PerfLatencyTests - 88400000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=3.3us, 99.9%=20.0us, 99.99%=32us, W=34us}
01:22:25.975 [main] INFO  tests.pref.PerfLatencyTests - 88500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=2.3us, 99.9%=9.4us, 99.99%=23.8us, W=38us}
01:22:26.092 [main] INFO  tests.pref.PerfLatencyTests - 88600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=5.0us, 99.9%=26.0us, 99.99%=41us, W=49us}
01:22:26.213 [main] INFO  tests.pref.PerfLatencyTests - 88700000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.7us, 99.9%=8.5us, 99.99%=18.9us, W=27.9us}
01:22:26.331 [main] INFO  tests.pref.PerfLatencyTests - 88800000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=5.1us, 99.9%=32us, 99.99%=51us, W=59us}
01:22:26.450 [main] INFO  tests.pref.PerfLatencyTests - 88900000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.5us, 99.9%=8.7us, 99.99%=19.6us, W=33us}
01:22:26.457 [main] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 134217728->268435456 -----------------
01:22:26.457 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Allocated new array, startingPosition=0, copying initial...
01:22:26.567 [main] INFO  tests.pref.PerfLatencyTests - 89000000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=4.7us, 99.9%=21.7us, 99.99%=43us, W=72us}
01:22:26.694 [main] INFO  tests.pref.PerfLatencyTests - 89100000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.9us, 99.9%=12.3us, 99.99%=25.3us, W=60us}
01:22:26.810 [main] INFO  tests.pref.PerfLatencyTests - 89200000: {50.0%=0.2us, 90.0%=1.1us, 95.0%=2.0us, 99.0%=31us, 99.9%=578us, 99.99%=606us, W=608us}
01:22:26.934 [main] INFO  tests.pref.PerfLatencyTests - 89300000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.5us, 99.9%=8.8us, 99.99%=26.8us, W=33us}
01:22:27.050 [main] INFO  tests.pref.PerfLatencyTests - 89400000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=4.1us, 99.9%=13.3us, 99.99%=24.1us, W=33us}
01:22:27.169 [main] INFO  tests.pref.PerfLatencyTests - 89500000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.7us, 99.9%=7.9us, 99.99%=18.4us, W=31us}
01:22:27.284 [main] INFO  tests.pref.PerfLatencyTests - 89600000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.7us, 99.0%=5.4us, 99.9%=23.0us, 99.99%=33us, W=43us}
01:22:27.402 [main] INFO  tests.pref.PerfLatencyTests - 89700000: {50.0%=0.2us, 90.0%=1.2us, 95.0%=2.3us, 99.0%=6.2us, 99.9%=25.8us, 99.99%=36us, W=37us}
01:22:27.520 [main] INFO  tests.pref.PerfLatencyTests - 89800000: {50.0%=0.2us, 90.0%=1.1us, 95.0%=1.8us, 99.0%=5.4us, 99.9%=20.3us, 99.99%=27.8us, W=39us}
01:22:27.639 [main] INFO  tests.pref.PerfLatencyTests - 89900000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.9us, 99.9%=11.3us, 99.99%=26.9us, W=34us}
01:22:27.724 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
01:22:27.756 [main] INFO  tests.pref.PerfLatencyTests - 90000000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=4.1us, 99.9%=14.9us, 99.99%=27.9us, W=32us}

01:24:05.563 [main] INFO  tests.pref.PerfLatencyTests - 174300000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=1.8us, 99.9%=4.2us, 99.99%=12.1us, W=16.2us}
01:24:05.680 [main] INFO  tests.pref.PerfLatencyTests - 174400000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.2us, 99.9%=30us, 99.99%=58us, W=63us}
01:24:05.781 [main] INFO  e.c.c.hashtable.LongLongLL2Hashtable - (A) Allocating array: long[1073741824] ...
01:24:05.798 [main] INFO  tests.pref.PerfLatencyTests - 174500000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.2us, 99.0%=2.0us, 99.9%=18.2us, 99.99%=40us, W=190us}
01:24:06.171 [main] INFO  tests.pref.PerfLatencyTests - 174600000: {50.0%=0.2us, 90.0%=1.17ms, 95.0%=2.19ms, 99.0%=9.6ms, 99.9%=9.9ms, 99.99%=9.9ms, W=9.9ms}
01:24:06.288 [main] INFO  tests.pref.PerfLatencyTests - 174700000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.1us, 99.0%=3.2us, 99.9%=14.7us, 99.99%=31us, W=36us}
01:24:06.406 [main] INFO  tests.pref.PerfLatencyTests - 174800000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=5.4us, 99.9%=31us, 99.99%=60us, W=64us}
01:24:06.532 [main] INFO  tests.pref.PerfLatencyTests - 174900000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.5us, 99.9%=10.5us, 99.99%=25.8us, W=36us}
01:24:06.649 [main] INFO  tests.pref.PerfLatencyTests - 175000000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=4.9us, 99.9%=18.7us, 99.99%=29.2us, W=40us}
01:24:06.767 [main] INFO  tests.pref.PerfLatencyTests - 175100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.8us, 99.9%=9.8us, 99.99%=27.2us, W=33us}
01:24:06.885 [main] INFO  tests.pref.PerfLatencyTests - 175200000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=4.2us, 99.9%=24.1us, 99.99%=35us, W=37us}
01:24:07.005 [main] INFO  tests.pref.PerfLatencyTests - 175300000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.7us, 99.9%=8.7us, 99.99%=21.6us, W=35us}
01:24:07.123 [main] INFO  tests.pref.PerfLatencyTests - 175400000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.1us, 99.9%=15.6us, 99.99%=27.5us, W=37us}
01:24:07.241 [main] INFO  tests.pref.PerfLatencyTests - 175500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.7us, 99.9%=14.5us, 99.99%=23.7us, W=35us}
01:24:07.358 [main] INFO  tests.pref.PerfLatencyTests - 175600000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=5.0us, 99.9%=22.2us, 99.99%=63us, W=66us}
01:24:07.476 [main] INFO  tests.pref.PerfLatencyTests - 175700000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.1us, 99.0%=3.0us, 99.9%=14.8us, 99.99%=39us, W=67us}
01:24:07.593 [main] INFO  tests.pref.PerfLatencyTests - 175800000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=8.1us, 99.9%=51us, 99.99%=77us, W=80us}
01:24:07.712 [main] INFO  tests.pref.PerfLatencyTests - 175900000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.4us, 99.9%=18.9us, 99.99%=35us, W=41us}
01:24:07.829 [main] INFO  tests.pref.PerfLatencyTests - 176000000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=6.0us, 99.9%=34us, 99.99%=74us, W=78us}
01:24:07.950 [main] INFO  tests.pref.PerfLatencyTests - 176100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=2.0us, 99.9%=12.4us, 99.99%=21.8us, W=34us}
01:24:08.068 [main] INFO  tests.pref.PerfLatencyTests - 176200000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.5us, 99.9%=17.1us, 99.99%=27.8us, W=31us}
01:24:08.196 [main] INFO  tests.pref.PerfLatencyTests - 176300000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=4.0us, 99.9%=18.7us, 99.99%=32us, W=38us}
01:24:08.312 [main] INFO  tests.pref.PerfLatencyTests - 176400000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=6.1us, 99.9%=27.5us, 99.99%=42us, W=48us}
01:24:08.430 [main] INFO  tests.pref.PerfLatencyTests - 176500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.8us, 99.9%=7.8us, 99.99%=18.7us, W=34us}
01:24:08.548 [main] INFO  tests.pref.PerfLatencyTests - 176600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.5us, 99.0%=5.0us, 99.9%=26.3us, 99.99%=38us, W=56us}
01:24:08.668 [main] INFO  tests.pref.PerfLatencyTests - 176700000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.8us, 99.9%=8.4us, 99.99%=26.5us, W=33us}
01:24:08.786 [main] INFO  tests.pref.PerfLatencyTests - 176800000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.2us, 99.9%=11.8us, 99.99%=26.1us, W=35us}
01:24:08.906 [main] INFO  tests.pref.PerfLatencyTests - 176900000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.3us, 99.9%=8.6us, 99.99%=21.8us, W=33us}
01:24:09.023 [main] INFO  tests.pref.PerfLatencyTests - 177000000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=4.1us, 99.9%=18.3us, 99.99%=28.4us, W=34us}
01:24:09.143 [main] INFO  tests.pref.PerfLatencyTests - 177100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=2.2us, 99.9%=12.2us, 99.99%=28.4us, W=35us}
01:24:09.261 [main] INFO  tests.pref.PerfLatencyTests - 177200000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=5.1us, 99.9%=21.8us, 99.99%=31us, W=34us}
01:24:09.381 [main] INFO  tests.pref.PerfLatencyTests - 177300000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.0us, 99.0%=1.9us, 99.9%=12.1us, 99.99%=26.3us, W=40us}
01:24:09.498 [main] INFO  tests.pref.PerfLatencyTests - 177400000: {50.0%=0.2us, 90.0%=2.0us, 95.0%=4.6us, 99.0%=37us, 99.9%=114us, 99.99%=489us, W=492us}
01:24:09.621 [main] INFO  tests.pref.PerfLatencyTests - 177500000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=2.8us, 99.9%=14.4us, 99.99%=44us, W=48us}
01:24:09.739 [main] INFO  tests.pref.PerfLatencyTests - 177600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=5.4us, 99.9%=28.0us, 99.99%=38us, W=43us}
01:24:09.756 [main] INFO  e.c.c.h.HashtableAsync2Resizer - (A) ----------- starting async migration capacity: 268435456->536870912 -----------------
01:24:09.756 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Allocated new array, startingPosition=24, copying initial...
01:24:09.863 [main] INFO  tests.pref.PerfLatencyTests - 177700000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.5us, 99.9%=20.6us, 99.99%=32us, W=37us}
01:24:09.982 [main] INFO  tests.pref.PerfLatencyTests - 177800000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=5.5us, 99.9%=24.8us, 99.99%=37us, W=42us}
01:24:10.112 [main] INFO  tests.pref.PerfLatencyTests - 177900000: {50.0%=0.3us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=4.1us, 99.9%=11.0us, 99.99%=26.2us, W=31us}
01:24:10.231 [main] INFO  tests.pref.PerfLatencyTests - 178000000: {50.0%=0.3us, 90.0%=1.4us, 95.0%=2.6us, 99.0%=8.2us, 99.9%=34us, 99.99%=51us, W=58us}
01:24:10.351 [main] INFO  tests.pref.PerfLatencyTests - 178100000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=5.1us, 99.9%=19.7us, 99.99%=32us, W=39us}
01:24:10.470 [main] INFO  tests.pref.PerfLatencyTests - 178200000: {50.0%=0.3us, 90.0%=1.3us, 95.0%=2.2us, 99.0%=7.3us, 99.9%=32us, 99.99%=59us, W=66us}
01:24:10.529 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT -3865192479369310576: RARE offset & mask == knownProgressCached (156231468)
01:24:10.530 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT -3865192479369310576: RARE finished resizer=exchange.core2.collections.hashtable.HashtableAsync2Resizer@409bf450
01:24:10.530 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT -3865192479369310576: RARE offset & mask == knownProgressCached (156231468)
01:24:10.530 [main] DEBUG e.c.c.hashtable.LongLongLL2Hashtable - PUT -3865192479369310576: RARE finished resizer=exchange.core2.collections.hashtable.HashtableAsync2Resizer@409bf450
01:24:10.593 [main] INFO  tests.pref.PerfLatencyTests - 178300000: {50.0%=0.3us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=10.7us, 99.9%=157us, 99.99%=194us, W=356us}
01:24:10.714 [main] INFO  tests.pref.PerfLatencyTests - 178400000: {50.0%=0.3us, 90.0%=1.2us, 95.0%=2.1us, 99.0%=7.8us, 99.9%=25.8us, 99.99%=42us, W=56us}
01:24:10.844 [main] INFO  tests.pref.PerfLatencyTests - 178500000: {50.0%=0.3us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=3.9us, 99.9%=11.2us, 99.99%=22.0us, W=28.7us}
01:24:10.962 [main] INFO  tests.pref.PerfLatencyTests - 178600000: {50.0%=0.3us, 90.0%=1.3us, 95.0%=2.3us, 99.0%=6.9us, 99.9%=25.1us, 99.99%=42us, W=60us}
01:24:11.082 [main] INFO  tests.pref.PerfLatencyTests - 178700000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.4us, 99.0%=4.0us, 99.9%=12.5us, 99.99%=27.8us, W=31us}
01:24:11.198 [main] INFO  tests.pref.PerfLatencyTests - 178800000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=4.5us, 99.9%=24.7us, 99.99%=45us, W=50us}
01:24:11.319 [main] INFO  tests.pref.PerfLatencyTests - 178900000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.2us, 99.9%=6.3us, 99.99%=16.1us, W=27.5us}
01:24:11.436 [main] INFO  tests.pref.PerfLatencyTests - 179000000: {50.0%=0.3us, 90.0%=1.7us, 95.0%=3.1us, 99.0%=7.2us, 99.9%=22.7us, 99.99%=32us, W=36us}
01:24:11.556 [main] INFO  tests.pref.PerfLatencyTests - 179100000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.2us, 99.9%=12.6us, 99.99%=23.8us, W=64us}
01:24:11.671 [main] INFO  tests.pref.PerfLatencyTests - 179200000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.3us, 99.9%=11.2us, 99.99%=25.7us, W=33us}
01:24:11.790 [main] INFO  tests.pref.PerfLatencyTests - 179300000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.6us, 99.0%=4.5us, 99.9%=14.7us, 99.99%=25.4us, W=30us}
01:24:11.908 [main] INFO  tests.pref.PerfLatencyTests - 179400000: {50.0%=0.3us, 90.0%=1.2us, 95.0%=2.2us, 99.0%=5.9us, 99.9%=19.2us, 99.99%=26.6us, W=31us}
01:24:12.026 [main] INFO  tests.pref.PerfLatencyTests - 179500000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.4us, 99.0%=3.8us, 99.9%=10.8us, 99.99%=25.5us, W=30us}
01:24:12.141 [main] INFO  tests.pref.PerfLatencyTests - 179600000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=4.3us, 99.9%=15.2us, 99.99%=27.4us, W=31us}
01:24:12.261 [main] INFO  tests.pref.PerfLatencyTests - 179700000: {50.0%=0.2us, 90.0%=1.0us, 95.0%=1.5us, 99.0%=4.2us, 99.9%=15.6us, 99.99%=27.4us, W=29.6us}
01:24:12.328 [main-M] INFO  e.c.c.h.HashtableAsync2Resizer - (A) Copying completed ----------------------
01:24:12.376 [main] INFO  tests.pref.PerfLatencyTests - 179800000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.7us, 99.9%=10.7us, 99.99%=26.3us, W=31us}

     */

    @Test
    public void benchmarkLL2() {
        benchmarkAbstract(
            (long[] kv) -> {
                final ILongLongHashtable hashtable = new LongLongLL2Hashtable(5000000);
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
    11000000: {50.0%=7.7ms, 90.0%=14.2ms, 95.0%=15.0ms, 99.0%=15.6ms, 99.9%=15.8ms, 99.99%=15.8ms, W=417ms}
    21900000: {50.0%=6.8ms, 90.0%=13.0ms, 95.0%=13.8ms, 99.0%=14.5ms, 99.9%=14.6ms, 99.99%=14.6ms, W=873ms}
    43700000: {50.0%=5.9ms, 90.0%=13.6ms, 95.0%=14.5ms, 99.0%=15.3ms, 99.9%=15.5ms, 99.99%=15.5ms, W=1.65s}
    87300000: {50.0%=1.73ms, 90.0%=10.1ms, 95.0%=11.2ms, 99.0%=12.0ms, 99.9%=12.2ms, 99.99%=12.2ms, W=3.2s}
    174500000: {50.0%=0.2us, 90.0%=1.49ms, 95.0%=2.54ms, 99.0%=3.4ms, 99.9%=3.6ms, 99.99%=3.6ms, W=6.3s}

     */
    @Test
    public void benchmarkAgrona() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Long2LongHashMap hashtable = new Long2LongHashMap(5_000_000, Hashing.DEFAULT_LOAD_FACTOR, 0L);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmarkAgrona,
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
    @Test
    public void benchmarkAdaptiveRadixTree() {
        benchmarkAbstract(
            (long[] kv) -> {
                final LongAdaptiveRadixTreeMap<Long> map = new LongAdaptiveRadixTreeMap<>();
                for (long l : kv) map.put(l, l);
                return map;
            },
            this::benchmark,
            (LongAdaptiveRadixTreeMap<Long> hashtable, long[] kv) -> {
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

    private void benchmarkLLnew() {
        int n = 4_000_000;
        long seed = 2918723469278364978L;


        log.debug("Pre-filling {} random k/v pairs...", n);
        Random rand = new Random(seed);
        final long[] prefillKeys = new long[n];
        for (int i = 0; i < n; i++) prefillKeys[i] = rand.nextLong();


        int n2 = 100_000;

        final ILongLongHashtable hashtable = new LongLongLLHashtable();
        log.debug("Benchmarking...");

        final long[] keys = new long[n2];

        // TODO make continuous test (non-stop)

//        for (int j = 0; j < 1780; j++) {
//            for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();
//            final SingleResult benchmark = singleTest.apply(hashtable, keys);
//            log.info("{}: {}", benchmark.size, LatencyTools.createLatencyReportFast(benchmark.avgGet));
//            extraLoader.accept(hashtable, keys);
//        }


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

    private SingleResult benchmarkAgrona(Long2LongHashMap hashtable, long[] keys) {

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


    private SingleResult benchmark(LongAdaptiveRadixTreeMap<Long> hashtable, long[] keys) {

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

        return new SingleResult(hashtable.size(Integer.MAX_VALUE), histogramPut, histogramPut, histogramPut);
    }

    record SingleResult(long size, Histogram avgPut, Histogram avgGet, Histogram avgRemove) {

    }


}
