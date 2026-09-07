package tests.pref;

import com.koloboke.collect.map.hash.HashLongLongMaps;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
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

public class PerfThroughputTests {
    private static final Logger log = LoggerFactory.getLogger(PerfThroughputTests.class);

    @Test
    public void benchmarkBasic() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final LongLongHashtable hashtable = new LongLongHashtable();
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmark,
                (LongLongHashtable hashtable, long[] kv) -> {
                    for (long l : kv) hashtable.put(l, l);
                }
        );
    }

    @Test
    public void benchmarkLL2() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final ILongLongHashtable hashtable = new LongLongLL2Hashtable();
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
    public void benchmarkAgrona() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Long2LongHashMap hashtable = new Long2LongHashMap(0L);
                    for (long l : kv) hashtable.put(l, l);
                    return hashtable;
                },
                this::benchmark,
                (Long2LongHashMap hashtable, long[] kv) -> {
                    for (long l : kv) hashtable.put(l, l);
                }
        );
    }

    @Test
    public void benchmarkStdHashMap() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Map<Long, Long> hashtable = new HashMap<>();
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
    public void benchmarkStdCHM() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Map<Long, Long> hashtable = new ConcurrentHashMap<>();
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
    public void benchmarkKoloboke() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final Map<Long, Long> hashtable = HashLongLongMaps.newMutableMap();
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
    public void benchmarkArt() {
        benchmarkAbstract(
                (long[] kv) -> {
                    final LongAdaptiveRadixTreeMap<Long> map = new LongAdaptiveRadixTreeMap<>();
                    for (long l : kv) map.put(l, l);
                    return map;
                },
                this::benchmark,
                (LongAdaptiveRadixTreeMap<Long> map, long[] kv) -> {
                    for (long l : kv) map.put(l, l);
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

        int n2 = 1_000_000;
        log.debug("Allocating {} random k/v pairs...", n2);

        final T hashtable = factory.apply(prefillKeys);
        log.debug("Benchmarking...");

        final long[] keys = new long[n2];

        for (int j = 0; j < 190; j++) {
            for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();
            final SingleResult benchmark = singleTest.apply(hashtable, keys);
            log.info("{}", benchmark);
            extraLoader.accept(hashtable, keys);
        }


//
//        log.info("done");
    }

    private SingleResult benchmark(ILongLongHashtable hashtable, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) hashtable.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += hashtable.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) hashtable.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;

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


        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);
    }

    private SingleResult benchmark(Long2LongHashMap hashtable, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) hashtable.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += hashtable.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) hashtable.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;
        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);
    }

    private SingleResult benchmark(Map<Long, Long> hashtable, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) hashtable.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += hashtable.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) hashtable.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;
        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);
    }

    private SingleResult benchmark(LongAdaptiveRadixTreeMap<Long> map, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) map.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += map.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) map.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;
        return new SingleResult(map.size(Integer.MAX_VALUE), putNs, getNs, removNs, acc);
    }


    record SingleResult(long size, long avgPut, long avgGet, long avgRemove, long acc) {

    }


}
