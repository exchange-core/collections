package exchange.core2.collections.hashtable;

import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongLongHashtableTest {


    private static final Logger log = LoggerFactory.getLogger(LongLongHashtableTest.class);

    ILongLongHashtable hashtable;

    @Before
    public void before() {
        hashtable = new LongLongHashtable();
    }

    @Test
    public void should_add_number() {

        hashtable = new LongLongHashtable();
        assertFalse(hashtable.containsKey(1L));
        hashtable.put(1L, 2529L);
        assertTrue(hashtable.containsKey(1L));

        assertThat(hashtable.get(1L), is(2529L));

    }


    @Test
    public void should_add_collision_numbers() {

        int initialCapacity = 16;

        hashtable = new LongLongHashtable();
        assertFalse(hashtable.containsKey(1L));
        hashtable.put(1L, 2529L);
        assertTrue(hashtable.containsKey(1L));

        long key2 = findCollision(1L, initialCapacity);
        assertFalse(hashtable.containsKey(key2));

        hashtable.put(key2, 9384L);
        assertTrue(hashtable.containsKey(1L));
        assertTrue(hashtable.containsKey(key2));

        assertThat(hashtable.get(1L), is(2529L));
        assertThat(hashtable.get(key2), is(9384L));

    }


    @Test
    public void should_remove_simple() {

        hashtable = new LongLongHashtable();
        assertFalse(hashtable.containsKey(39L));
        hashtable.put(39L, 2529L);

        final long removed = hashtable.remove(39L);
        assertThat(removed, is(2529L));
        assertFalse(hashtable.containsKey(39L));
    }


    @Test
    public void should_not_remove_another_key() {
        int initialCapacity = 16;

        hashtable = new LongLongHashtable();
        assertFalse(hashtable.containsKey(39L));
        hashtable.put(39L, 2529L);
        long key2 = findCollision(39L, initialCapacity);

        final long removed = hashtable.remove(key2);
        assertThat(removed, is(0L));
        assertTrue(hashtable.containsKey(39L));
    }

    @Test
    public void should_remove_simple_collision_first() {
        int initialCapacity = 16;

        hashtable = new LongLongHashtable();

        long key1 = findKeyForPosition(5, 98712634L, initialCapacity);
        assertFalse(hashtable.containsKey(key1));
        hashtable.put(key1, 5429182349871232876L);
        long key2 = findCollision(key1, initialCapacity);
        hashtable.put(key2, -7928349273546258723L);

        final long removed = hashtable.remove(key1);
        assertThat(removed, is(5429182349871232876L));
        assertFalse(hashtable.containsKey(key1));
        assertThat(hashtable.get(key2), is(-7928349273546258723L));

    }

    @Test
    public void should_remove_simple_collision_second() {
        int initialCapacity = 16;

        hashtable = new LongLongHashtable();

        long key1 = findKeyForPosition(5, 98712634L, initialCapacity);
        assertFalse(hashtable.containsKey(key1));
        hashtable.put(key1, 5429182349871232876L);
        long key2 = findCollision(key1, initialCapacity);
        hashtable.put(key2, -7928349273546258723L);

        final long removed = hashtable.remove(key2);
        assertThat(removed, is(-7928349273546258723L));
        assertFalse(hashtable.containsKey(key2));
        assertThat(hashtable.get(key1), is(5429182349871232876L));
    }

    @Test
    public void should_remove_full_collision_series() {

        int initialCapacity = 16;

        for (int startPos = 0; startPos < initialCapacity; startPos++) {

            log.info("----------------------------- {} FORWARD ------------------ ", startPos);

            hashtable = new LongLongHashtable();

            final long[] keys = findKeysForPosition(startPos, 7434L, 4, initialCapacity);
            Arrays.stream(keys).forEach(key -> hashtable.put(key, -key));

            for (int i = 0; i < 4; i++) {
                final long removed = hashtable.remove(keys[i]);
                assertThat(removed, is(-keys[i]));
                assertFalse(hashtable.containsKey(i));
            }

            log.info("----------------------------- {} BACKWARDS------------------ ", startPos);

            hashtable = new LongLongHashtable();

            Arrays.stream(keys).forEach(key -> hashtable.put(key, -key));

            for (int i = 3; i >= 0; i--) {
                final long removed = hashtable.remove(keys[i]);
                assertThat(removed, is(-keys[i]));
                assertFalse(hashtable.containsKey(i));
            }

        }
    }

    // todo add more complex collision removal tests

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

    @Test
    public void should_upsize_throughput() {

        RandomDataSetsProvider randomDataSetsProvider = RandomDataSetsProvider.create();

        long t = System.currentTimeMillis();
        final int mask = (1 << 23) - 1;
        log.debug("mask=" + mask);

        hashtable = new LongLongHashtable();
        Random rand = new Random(1L);

        int j = 0;
        long[] dataset = randomDataSetsProvider.next();

        for (int i = 0; i < 1_000_000_000; i++) {

            if(j > dataset.length - 2){
                dataset = randomDataSetsProvider.next();
                j = 0;
            }

            final long key = dataset[j++];
            final long value = dataset[j++];

//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
            hashtable.put(key, value);

            //refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

            if ((i & mask) == 0) {
                final long t2 = System.currentTimeMillis();
                log.debug("i={} t={}ms", i, t2 - t);
                t = t2;
            }
        }

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
    public void should_correctly_compare_gap() {
        int size = 8;
        int mask = size - 1;

        for (int k = 0; k < size; k++) {
            for (int h = 0; h < size; h++) {
                if (k == h) {
                    continue;
                }
                for (int g = 0; g < size; g++) {
                    if (g == k) {
                        continue;
                    }

                    boolean c = canFillGapAndFinish(k, h, g);
                    boolean c2 = LongLongHashtable.canFillGapAndFinish(k, h, g, mask);
                    System.out.println("k:" + k + " h:" + h + " g:" + g + " - " + c + "/" + c2);

                    assertThat(c2, is(c));
                }

            }
        }


    }

    /*


     */

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


    public boolean canFillGapAndFinish(int k, int h, int g) {

        if (g == k) {
            throw new IllegalStateException("gap == k, should finish");
        }
        if (h == k) {
            throw new IllegalStateException("h == k, should skip to k-1 !");
        }

        if (h < k) {
            return g >= h && g < k;
        } else {
            return g >= h || g < k;
        }
    }


    private long findCollision(long key, int capacity) {

        final int keyPosition = Hashing.hash(key) & (capacity - 1);
        do {
            key++;

        } while ((Hashing.hash(key) & (capacity - 1)) != keyPosition);

        return key;
    }


    private long findKeyForPosition(int keyPosition, long afterKey, int capacity) {

        long key = afterKey;
        do {
            key++;

        } while ((Hashing.hash(key) & (capacity - 1)) != keyPosition);

        return key;
    }

    private long[] findKeysForPosition(int keyPosition, long afterKey, int keysNum, int capacity) {

        final long[] keys = new long[keysNum];
        for (int i = 0; i < keysNum; i++) {
            afterKey = findKeyForPosition(keyPosition, afterKey, capacity);
            keys[i] = afterKey;
        }

        return keys;
    }

}