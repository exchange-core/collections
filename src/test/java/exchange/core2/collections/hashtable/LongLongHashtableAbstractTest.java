package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.MutableInteger;
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

public abstract class LongLongHashtableAbstractTest {


    private static final Logger log = LoggerFactory.getLogger(LongLongHashtableAbstractTest.class);

    ILongLongHashtable hashtable;


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
        Random rand = new Random(14232313L);
        for (int iteration = 0; iteration < 10; iteration++) {

            hashtable = new LongLongHashtable();
            Map<Long, Long> refMap = new HashMap<>();

            final long size = 50000L;
            for (long i = 0; i < size; i++) {
                final long key = rand.nextLong();
                final long value = rand.nextLong();

                //log.info("=============== put {}={}", key, value);
                Long prevRef = refMap.put(key, value);
                long prev = hashtable.put(key, value);

                try {
                    assertThat(prev, is(prevRef == null ? 0L : prevRef));

                    //log.info("--------------- get {} expected {}", key, value);
                    assertThat(hashtable.get(key), is(value));
                    assertThat(hashtable.remove(key), is(value));
                    assertThat(hashtable.get(key), is(0L));
                    assertThat(hashtable.remove(key), is(0L));

                    assertThat(hashtable.size(), is(i));
                    hashtable.put(key, value);
//                    agronaMap.put(key, value);


                } catch (Throwable er) {
                    log.error("ERR: KEY={} VALUE={} i={} iter={}", key, value, i, iteration);
                    throw er;
                }
                //refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

                if (BitUtil.isPowerOfTwo(i)) {

                    //hashtable.printLayout();

                    assertThat(hashtable.size(), is(i + 1L));
                    log.info("Periodic validating get - size={} iteration={}...", hashtable.size(), iteration);
                    log.debug("refMap size = {}", refMap.size());
//                    log.debug("agronaMap size = {}", agronaMap.size());
                    log.debug("hashtable size = {}", hashtable.size());
                    MutableInteger errCnt = new MutableInteger(0);
                    refMap.forEach((k, v) -> {
                        try {
                            assertThat(hashtable.get(k), is(v));
                        } catch (Throwable er) {
                            log.info("PERIODIC: KEY={} VALUE={} {} {}", k, v, er.getClass(), er.getMessage());
                            if (errCnt.incrementAndGet() > 40) {
                                throw er;
                            }
                        }
                    });

                    if (errCnt.get() != 0) {
                        throw new IllegalStateException();
                    }
                }

            }

            assertThat(hashtable.size(), is(size));
            log.info("DONE iteration: {}", iteration);
            log.info("validating get...");
            refMap.forEach((k, v) -> {
                try {
                    assertThat(hashtable.get(k), is(v));
                } catch (Throwable er) {
                    log.info("PERIODIC: KEY={} VALUE={} {} {}", k, v, er.getClass(), er.getMessage());
                    throw er;
                }
            });
            log.info("validating remove...");
            refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(v)));
            assertThat(hashtable.size(), is(0L));
            log.info("validating remove 0...");
            refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(0L)));
            log.info("confirm empty...");
            refMap.forEach((k, v) -> assertFalse(hashtable.containsKey(k)));
            assertThat(hashtable.size(), is(0L));
            log.info("done");
        }
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