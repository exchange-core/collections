package tests.pref;

import tests.common.LatencyTools;
import androidx.collection.MutableLongLongMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.hash.HashLongLongMap;
import com.koloboke.collect.map.hash.HashLongLongMaps;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.jctools.maps.NonBlockingHashMapLong;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import javolution.util.FastMap;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC
 * -XX:+UseZGC
 */

public class PerfLatencyTests {
    private static final Logger log = LoggerFactory.getLogger(PerfLatencyTests.class);

    /**
     * Offered load, the same for every benchmark. Latency taken at different offered rates is not
     * comparable - whoever is asked for less has more slack and simply looks better.
     */
    private static final int TPS = 1_000_000;

    /** Every map starts empty - no capacity hint - so resizing is part of what is measured. */
    private static final int INITIAL_CAPACITY = 16;

    /** Same target load factor for every map that lets you pick one. */
    private static final float LOAD_FACTOR = 0.65f;

    /** Entries put into a throwaway instance at max rate before measuring, to warm up JIT. */
    private static final int WARMUP_ENTRIES = 20_000_000;


    final Executor CORE_LOCK_EXECUTOR = task -> {
        Thread thread = new Thread(() -> {
            try (AffinityLock affinityLock = AffinityLock.acquireCore()) {
                Thread.currentThread().setName("AFC" + affinityLock.cpuId());
                task.run();
            }
        });
        thread.start();
    };

    @Test
    public void benchmarkBasic() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final ILongLongHashtable hashtable = new LongLongHashtable();
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkExchangeHashtable
        );
    }

    /**
     * -Xms31g -Xmx31g -XX:+AlwaysPreTouch
     */
    @Test
    public void benchmarkLL2() {

        benchmarkAbstract(
                (long[] kv) -> {
                    final ILongLongHashtable hashtable = new LongLongLL2Hashtable(CORE_LOCK_EXECUTOR);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkExchangeHashtable
        );
    }

    @Test
    public void benchmarkAgrona() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Long2LongHashMap hashtable = new Long2LongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, 0L);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkAgrona
        );
    }


    @Test
    public void benchmarkStdHashMap() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Map<Long, Long> hashtable = new HashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkStd
        );
    }


    @Test
    public void benchmarkStdCHM() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Map<Long, Long> hashtable = new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, 1);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkStd
        );
    }


    @Test
    public void benchmarkKoloboke() {
        benchmarkAbstract(
                (long[] kv) -> {
                    // primitive API - going through the boxed Map<Long,Long> view charged
                    // Koloboke for autoboxing that is not inherent to it
                    final HashLongLongMap hashtable = HashLongLongMaps.getDefaultFactory()
                            .withHashConfig(HashConfig.fromLoads(0.2, LOAD_FACTOR, LOAD_FACTOR))
                            .newMutableMap(INITIAL_CAPACITY);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkKoloboke
        );
    }

    @Test
    public void benchmarkJavoluton() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Map<Long, Long> hashtable = new FastMap<>();
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkStd
        );
    }


    @Test
    public void benchmarkHppc() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final LongLongHashMap hashtable = new LongLongHashMap(INITIAL_CAPACITY, LOAD_FACTOR);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkHppc
        );
    }

    @Test
    public void benchmarkFastUtil() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Long2LongOpenHashMap hashtable = new Long2LongOpenHashMap(INITIAL_CAPACITY, LOAD_FACTOR);
                    hashtable.defaultReturnValue(0L);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkFastUtil
        );
    }

    @Test
    public void benchmarkAndroidX() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final MutableLongLongMap hashtable = new MutableLongLongMap(INITIAL_CAPACITY); // no load factor knob
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmarkAndroidX
        );
    }

    /**
     * Cliff Click's lock-free map (JCTools port of high-scale-lib).
     * <p>
     * CAVEAT: it is a long -> Object map, there is no primitive long -> long variant. Storing a
     * long value therefore allocates a Long per entry, so its numbers include boxing and the GC
     * pressure that comes with it. That is the real cost of using it for this use case, but it is
     * not an apples-to-apples comparison against the primitive maps.
     */
    @Test
    public void benchmarkNonBlockingHashMapLong() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final NonBlockingHashMapLong<Long> hashtable = new NonBlockingHashMapLong<>(INITIAL_CAPACITY); // no load factor knob
                    // explicit boxing: put(long, TypeV) and put(Long, TypeV) are both applicable
                    // once the value needs boxing, so the call would be ambiguous otherwise
                    for (long l : kv) hashtable.put(l, Long.valueOf(l));
                    return hashtable;
                },
                this::benchmarkNonBlockingHashMapLong
        );
    }

    @Test
    public void benchmarkChronicleMap() {

        // Chronicle (still in 2026.1) reaches for jdk.internal.ref.Cleaner, which no longer exists
        // in recent JDKs - no --add-exports can bring back a deleted class.
        Assume.assumeTrue("ChronicleMap needs a JDK that still ships jdk.internal.ref.Cleaner",
                Runtime.version().feature() < 24);

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
                this::benchmarkStd
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
                this::benchmarkStd
        );
    }


    private <T> void benchmarkAbstract(Function<long[], T> factory,
                                       BiFunction<T, long[], SingleResult> singleTest) {

        final int n2 = 1_000_000;
        final long seed = 1918723469278364978L;
        final KeyGenerator keys0 = new KeyGenerator(seed);
        log.info("Key profile: {}, offered rate: {} tps, load factor: {}", KEY_PROFILE, TPS, LOAD_FACTOR);

        try (AffinityLock ignore = AffinityLock.acquireCore()) {

            // ---- warm up on a THROWAWAY instance, unthrottled ----
            // The factory fills whatever keys it is handed, so handing it WARMUP_ENTRIES keys is a
            // plain max-rate load. Without this, the first measured window also pays for JIT
            // compilation of put() and of the whole resize path.
            log.debug("Warming up on a separate instance: {} entries at max rate...", WARMUP_ENTRIES);
            final long[] warmupKeys = new long[WARMUP_ENTRIES];
            for (int i = 0; i < WARMUP_ENTRIES; i++) warmupKeys[i] = keys0.next();
            final long warmupStartNs = System.nanoTime();
            final T warmup = factory.apply(warmupKeys);
            // one throttled pass too, so the measurement loop itself gets compiled
            singleTest.apply(warmup, Arrays.copyOf(warmupKeys, n2));
            log.debug("Warmup done in {}ms (instance discarded)", (System.nanoTime() - warmupStartNs) / 1_000_000);

            // ---- measure on a fresh EMPTY instance ----
            final T hashtable = factory.apply(EMPTY_KEYS);
            log.debug("Benchmarking...");

            final long[] keys = new long[n2];

            for (int j = 0; j < 1780; j++) {
                for (int i = 0; i < n2; i++) keys[i] = keys0.next();
                final SingleResult benchmark = singleTest.apply(hashtable, keys);
                log.info("{}: {}", (long) n2 * (j + 1), LatencyTools.createLatencyReportFast(benchmark.avgGet));
            }
        }
    }

    private static final long[] EMPTY_KEYS = new long[0];

    /**
     * Key distribution. Uniformly random keys are the easy case for a hash table and an unrealistic
     * one for an exchange, where ids are typically sequential - and they are the hard case for a
     * radix tree, so comparing on random keys alone systematically flatters hash tables.
     * <p>
     * Select with -Dbenchmark.keys=SEQUENTIAL (default RANDOM).
     */
    public enum KeyProfile {
        /** uniformly random positive longs */
        RANDOM,
        /** 1, 2, 3, ... - plain order ids */
        SEQUENTIAL,
        /** 1*S, 2*S, 3*S ... - ids carrying a tag in the low bits, hostile to low-bit indexing */
        SEQUENTIAL_SPARSE,
        /** several sequential ranges interleaved - several instruments traded at once */
        CLUSTERED
    }

    private static final KeyProfile KEY_PROFILE =
            KeyProfile.valueOf(System.getProperty("benchmark.keys", "RANDOM").toUpperCase());

    private static final long SPARSE_STRIDE = 256L;
    private static final int CLUSTERS = 16;

    /**
     * Stateful so SEQUENTIAL can keep counting. One instance is shared by the warmup and the
     * measured run, so the measured keys never collide with the warmed-up ones.
     */
    private static final class KeyGenerator {

        private final Random rand;
        private final long[] clusterBase = new long[CLUSTERS];
        private final long[] clusterNext = new long[CLUSTERS];
        private long counter = 0;

        private KeyGenerator(long seed) {
            this.rand = new Random(seed);
            for (int c = 0; c < CLUSTERS; c++) {
                // far apart so clusters do not merge into one range
                clusterBase[c] = (rand.nextLong() & Long.MAX_VALUE) | 1L;
            }
        }

        // never returns 0: it is the "missing" marker for LL2 and agrona
        private long next() {
            switch (KEY_PROFILE) {
                case SEQUENTIAL:
                    return ++counter;
                case SEQUENTIAL_SPARSE:
                    return ++counter * SPARSE_STRIDE;
                case CLUSTERED: {
                    final int c = rand.nextInt(CLUSTERS);
                    return clusterBase[c] + (clusterNext[c]++);
                }
                case RANDOM:
                default: {
                    long k = rand.nextLong() & Long.MAX_VALUE;
                    return k == 0 ? 1 : k;
                }
            }
        }
    }

    private SingleResult benchmarkExchangeHashtable(ILongLongHashtable hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
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

            // Response time, not service time: measure from the time this command was SCHEDULED
            // to run, not from the time we last looked at the clock.
            //
            // Once we fall behind (a migration stalls a put for tens of ms) the while loop above
            // stops executing entirely - every command is already overdue - so
            // lastKnownTimeOffsetPs keeps a stale value and subtracting it would report the delay
            // of one single command and then near-zero for the whole backlog. Subtracting the
            // planned offset instead charges every delayed command with how late it actually is,
            // which is the point of the fixed-rate schedule (Gil Tene's coordinated omission).
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);

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


        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
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
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }


    private SingleResult benchmarkKoloboke(HashLongLongMap hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                Thread.onSpinWait();
            }
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmarkHppc(LongLongHashMap hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                Thread.onSpinWait();
            }
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmarkFastUtil(Long2LongOpenHashMap hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                Thread.onSpinWait();
            }
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmarkAndroidX(MutableLongLongMap hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                Thread.onSpinWait();
            }
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.getSize(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmarkNonBlockingHashMapLong(NonBlockingHashMapLong<Long> hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                Thread.onSpinWait();
            }
            hashtable.put(key, Long.valueOf(key));
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmarkStd(Map<Long, Long> hashtable, long[] keys) {



        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
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
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }


    private SingleResult benchmarkFair(LongAdaptiveRadixTreeMap<Long> hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
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
            final long putNs = System.nanoTime() - startTimeNs - (planneTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
            if (putNs > 300_000) {
                log.debug("key:{} {}us", key, putNs / 1000);
            }
        }

        return new SingleResult(0, histogramPut, histogramPut, histogramPut);
    }


    private SingleResult benchmarkStd(LongAdaptiveRadixTreeMap<Long> hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            long startTimeNs = System.nanoTime();
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs;
            histogramPut.recordValue(putNs);
            if (putNs > 300_000) {
                log.debug("{} key:{} {}us", hashtable.size(Integer.MAX_VALUE), key, putNs / 1000);
            }
        }

        return new SingleResult(0, histogramPut, histogramPut, histogramPut);
    }


    @Test
    public void nonStopPutBenchmarkStdHashtable() {

        final BlockingQueue<long[]> randBuffer = new LinkedBlockingQueue<>(2);
        final int bufSize = 1000_000;

        Runnable randomGenerator = () -> {
            try (AffinityLock affinityLock = AffinityLock.acquireCore()) {
                log.debug("Core for random generator: {}", affinityLock);
                Random rand = new Random();
                do {
//                    log.debug("Allocating array...");
                    final long[] keys = new long[bufSize]; // TODO take from pool
//                    log.debug("Generating {} randoms", n);
                    for (int i = 0; i < bufSize; i++) keys[i] = rand.nextLong();
//                    log.debug("Generated, inserting..");
                    randBuffer.put(keys);
                } while (true);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        };

        for (int i = 0; i < 2; i++) {
            Thread randomSupplier = new Thread(randomGenerator);
            randomSupplier.start();
        }

        //try {
        try (AffinityLock ignore = AffinityLock.acquireCore()) {
            final int prefills = 5;
            final ILongLongHashtable map = new LongLongLL2Hashtable(1000000, CORE_LOCK_EXECUTOR);
            //ILongLongHashtable map = new LongLongHashtable();
            //Long2LongHashMap map = new Long2LongHashMap(0L);
            //final LongAdaptiveRadixTreeMap<Long> map = new LongAdaptiveRadixTreeMap<>();

            log.debug("Prefilling {} entries..", bufSize * prefills);
            for (int i = 0; i < prefills; i++) {
                final long[] buf = randBuffer.take();
                for (long key : buf) {
                    map.put(key, key);
                }
                //map = new LongLongLL2Hashtable(CORE_LOCK_EXECUTOR);
            }
            log.debug("Prefilling done");


            final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

            final long picosPerCmd = (1024L * 1_000_000_000L) / TPS;
            final long startTimeNs = System.nanoTime();
            long nextPublishTimeNs = startTimeNs + 1_000_000_000L;

            long planneTimeOffsetPs = 0L;
            long lastKnownTimeOffsetPs = 0L;

            int pos = 0;
            long[] buf = randBuffer.take();

            for (int i = 0; i < 500_000_000; i++) {

                if (pos == buf.length) {
                    buf = randBuffer.take();
                    pos = 0;
                }

                final long key = buf[pos++];
                planneTimeOffsetPs += picosPerCmd;
                while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                    lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                    // spin until its time to send next command
                    Thread.onSpinWait(); // 1us-26  max34
                    // LockSupport.parkNanos(2000L); // 1us-25 max29
                    //Thread.yield();   // 1us-28  max32
                }

                map.put(key, key);

                final long nanoTime = System.nanoTime();
                final long putNs = nanoTime - startTimeNs - (planneTimeOffsetPs >> 10);
                histogramPut.recordValue(putNs);

                if (nanoTime > nextPublishTimeNs) {
                    nextPublishTimeNs = nanoTime + 1_000_000_000L;
                    log.info("{} {}", i, LatencyTools.createLatencyReportFast(histogramPut));
                    histogramPut.reset();
                }

            }

        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }


    record SingleResult(long size, Histogram avgPut, Histogram avgGet, Histogram avgRemove) {

    }


}
