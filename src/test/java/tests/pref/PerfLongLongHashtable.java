package tests.pref;

import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class PerfLongLongHashtable {

    private static final Logger log = LoggerFactory.getLogger(PerfLongLongHashtable.class);

    ILongLongHashtable hashtable;

    @Before
    public void before() {
        hashtable = new LongLongHashtable();
    }


    @Test
    public void should_upsize() {


        hashtable = new LongLongHashtable();
        Map<Long, Long> refMap = new HashMap<>();
        Random rand = new Random(1L);
        for (int i = 0; i < 100_000_000; i++) {
            final long key = rand.nextLong();
            final long value = rand.nextLong();
            hashtable.put(key, value);
            refMap.put(key, value);

            //refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

        }

        log.info("validating get...");
        refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));
        log.info("validating remove...");
        refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(v)));
        log.info("confirm empty...");
        refMap.forEach((k, v) -> assertFalse(hashtable.containsKey(k)));
        log.info("done");
    }


    /**
     * 15:26:26.635 [main] INFO exchange.core2.collections.hashtable.LongLongHashtableTest - Benchmarking put 1000000 elements...
     * 15:26:26.713 [main] INFO exchange.core2.collections.hashtable.LongLongHashtableTest - PUT speed: 78ns
     * 15:26:26.776 [main] INFO exchange.core2.collections.hashtable.LongLongHashtableTest - GET speed: 63ns (acc=4832899208090569750)
     * 15:26:26.901 [main] INFO exchange.core2.collections.hashtable.LongLongHashtableTest - REMOVE speed: 125ns
     */
    @Test
    public void should_upsize_perf() {
        int n = 42_000_000;
        long seed = 2918723469278364978L;

        hashtable = new LongLongHashtable();
        Random rand = new Random(seed);
        for (int i = 0; i < n; i++) {
            final long key = rand.nextLong();
            final long value = rand.nextLong();
            hashtable.put(key, value);
        }

        int n2 = 1_000_000;
        long[] keys = new long[n2];
        for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();

        log.info("Benchmarking put {} elements...", n2);

        long t = System.currentTimeMillis();
        for (int i = 0; i < n2; i++) {
            long key = keys[i];
            hashtable.put(key, key);
        }
        long timePerElementNs = (long) (1_000_000.0 * (System.currentTimeMillis() - t) / n2);
        log.info("PUT speed: {}ns", timePerElementNs);


        t = System.currentTimeMillis();
        long acc = 0;
        for (int i = 0; i < n2; i++) {
            long key = keys[i];
            acc += hashtable.get(key);
        }
        timePerElementNs = (long) (1_000_000.0 * (System.currentTimeMillis() - t) / n2);
        log.info("GET speed: {}ns (acc={})", timePerElementNs, acc);


        t = System.currentTimeMillis();
        for (int i = 0; i < n2; i++) {
            long key = keys[i];
            hashtable.remove(key);
        }
        timePerElementNs = (long) (1_000_000.0 * (System.currentTimeMillis() - t) / n2);
        log.info("REMOVE speed: {}ns", timePerElementNs);


//        log.info("validating remove...");
//        rand = new Random(seed);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertThat(hashtable.remove(key), is(value));
//        }
//
//        log.info("confirm empty...");
//        rand = new Random(seed);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertFalse(hashtable.containsKey(key));
//        }
//
//        log.info("done");
    }


    @Test
    public void should_upsize_perf_agrona() {
        int n = 42_000_000;
        long seed = 2918723469278364978L;

        Long2LongHashMap hashtable = new Long2LongHashMap(0L);
        Random rand = new Random(seed);
        for (int i = 0; i < n; i++) {
            final long key = rand.nextLong();
            final long value = rand.nextLong();
            hashtable.put(key, value);
        }

        int n2 = 1_000_000;
        long[] keys = new long[n2];
        for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();

        log.info("Benchmarking put {} elements...", n2);

        long t = System.currentTimeMillis();
        for (int i = 0; i < n2; i++) {
            long key = keys[i];
            hashtable.put(key, key);
        }
        long timePerElementNs = (long) (1_000_000.0 * (System.currentTimeMillis() - t) / n2);
        log.info("PUT speed: {}ns", timePerElementNs);


        t = System.currentTimeMillis();
        long acc = 0;
        for (int i = 0; i < n2; i++) {
            long key = keys[i];
            acc += hashtable.get(key);
        }
        timePerElementNs = (long) (1_000_000.0 * (System.currentTimeMillis() - t) / n2);
        log.info("GET speed: {}ns (acc={})", timePerElementNs, acc);


        t = System.currentTimeMillis();
        for (int i = 0; i < n2; i++) {
            long key = keys[i];
            hashtable.remove(key);
        }
        timePerElementNs = (long) (1_000_000.0 * (System.currentTimeMillis() - t) / n2);
        log.info("REMOVE speed: {}ns", timePerElementNs);


//        log.info("validating remove...");
//        rand = new Random(seed);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertThat(hashtable.remove(key), is(value));
//        }
//
//        log.info("confirm empty...");
//        rand = new Random(seed);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertFalse(hashtable.containsKey(key));
//        }
//
//        log.info("done");
    }

    @Test
    public void should_upsize_throughput() {

        final RandomDataSetsProvider randomDataSetsProvider = RandomDataSetsProvider.create();

        long t = System.currentTimeMillis();
        final int mask = (1 << 23) - 1;
        log.debug("mask=" + mask);

        hashtable = new LongLongHashtable();

        int j = 0;
        long[] dataset = randomDataSetsProvider.next();

        for (int i = 0; i < 1_000_000_000; i++) {

            if (j > dataset.length - 2) {
                dataset = randomDataSetsProvider.next();
                j = 0;
            }

            final long key = dataset[j++];
            final long value = dataset[j++];

            hashtable.put(key, value);

            if ((i & mask) == 0) {
                final long t2 = System.currentTimeMillis();
                log.debug("i={} t={}ms", i, t2 - t);
                t = t2;
            }
        }

    }


    @Test
    public void should_upsize_async() throws InterruptedException {
        int n = 13_000_000;
        long seed = -923421549367843497L;

        hashtable = new LongLongHashtable();
        Random rand = new Random(seed);
        for (int i = 0; i < n; i++) {
            final long key = rand.nextLong();
            final long value = rand.nextLong();
            hashtable.put(key, value);
        }

        log.info("Put completed");

        Thread.sleep(100);

        log.info("validating get...");
        rand = new Random(seed);
        for (int i = 0; i < n; i++) {
            final long key = rand.nextLong();
            final long value = rand.nextLong();

            final long found = hashtable.get(key);
            if (found != value) {
                log.error("key={} found={} expected={} i={}", key, found, value, i);
            }

            assertThat(hashtable.get(key), is(value));
//            assertThat(hashtable.remove(key), is(value));
        }


//        log.info("confirm empty...");
//        rand = new Random(seed);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertFalse(hashtable.containsKey(key));
//        }
//
//        log.info("done");
    }


}
