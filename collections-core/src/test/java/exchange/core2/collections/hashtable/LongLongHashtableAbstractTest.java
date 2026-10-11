package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Contract tests, run against every implementation through {@link #create(int)}.
 */
public abstract class LongLongHashtableAbstractTest {


    private static final Logger log = LoggerFactory.getLogger(LongLongHashtableAbstractTest.class);

    /**
     * Expected size giving a 16-slot table at load factor 0.65 (10 / 0.65 = 15.4, rounded up to a power of two),
     * which the collision tests below rely on. Up to 10 entries it does not resize.
     */
    private static final int SMALL_SIZE = 10;
    private static final int SMALL_CAPACITY = 16;

    private final List<ILongLongHashtable> created = new ArrayList<>();

    ILongLongHashtable hashtable;

    /**
     * @param expectedSize number of entries the table is pre-sized for
     */
    protected abstract ILongLongHashtable create(int expectedSize);

    private ILongLongHashtable newHashtable(int expectedSize) {
        final ILongLongHashtable table = create(expectedSize);
        created.add(table);
        return table;
    }

    @After
    public void closeAll() throws Exception {
        for (ILongLongHashtable table : created) {
            if (table instanceof AutoCloseable closeable) {
                closeable.close();
            }
        }
    }


    @Test
    public void should_add_number() {

        hashtable = newHashtable(SMALL_SIZE);
        assertFalse(hashtable.containsKey(1L));
        hashtable.put(1L, 2529L);
        assertTrue(hashtable.containsKey(1L));

        assertThat(hashtable.get(1L), is(2529L));

    }


    @Test
    public void should_add_collision_numbers() {

        hashtable = newHashtable(SMALL_SIZE);
        assertFalse(hashtable.containsKey(1L));
        hashtable.put(1L, 2529L);
        assertTrue(hashtable.containsKey(1L));

        long key2 = findCollision(1L, SMALL_CAPACITY);
        assertFalse(hashtable.containsKey(key2));

        hashtable.put(key2, 9384L);
        assertTrue(hashtable.containsKey(1L));
        assertTrue(hashtable.containsKey(key2));

        assertThat(hashtable.get(1L), is(2529L));
        assertThat(hashtable.get(key2), is(9384L));

    }


    @Test
    public void should_remove_simple() {

        hashtable = newHashtable(SMALL_SIZE);
        assertFalse(hashtable.containsKey(39L));
        hashtable.put(39L, 2529L);

        final long removed = hashtable.remove(39L);
        assertThat(removed, is(2529L));
        assertFalse(hashtable.containsKey(39L));
    }


    @Test
    public void should_not_remove_another_key() {

        hashtable = newHashtable(SMALL_SIZE);
        assertFalse(hashtable.containsKey(39L));
        hashtable.put(39L, 2529L);
        long key2 = findCollision(39L, SMALL_CAPACITY);

        final long removed = hashtable.remove(key2);
        assertThat(removed, is(0L));
        assertTrue(hashtable.containsKey(39L));
    }

    @Test
    public void should_remove_simple_collision_first() {

        hashtable = newHashtable(SMALL_SIZE);

        long key1 = findKeyForPosition(5, 98712634L, SMALL_CAPACITY);
        assertFalse(hashtable.containsKey(key1));
        hashtable.put(key1, 5429182349871232876L);
        long key2 = findCollision(key1, SMALL_CAPACITY);
        hashtable.put(key2, -7928349273546258723L);

        final long removed = hashtable.remove(key1);
        assertThat(removed, is(5429182349871232876L));
        assertFalse(hashtable.containsKey(key1));
        assertThat(hashtable.get(key2), is(-7928349273546258723L));

    }

    @Test
    public void should_remove_simple_collision_second() {

        hashtable = newHashtable(SMALL_SIZE);

        long key1 = findKeyForPosition(5, 98712634L, SMALL_CAPACITY);
        assertFalse(hashtable.containsKey(key1));
        hashtable.put(key1, 5429182349871232876L);
        long key2 = findCollision(key1, SMALL_CAPACITY);
        hashtable.put(key2, -7928349273546258723L);

        final long removed = hashtable.remove(key2);
        assertThat(removed, is(-7928349273546258723L));
        assertFalse(hashtable.containsKey(key2));
        assertThat(hashtable.get(key1), is(5429182349871232876L));
    }




    @Test
    public void should_remove_full_collision_series() {

        for (int startPos = 0; startPos < SMALL_CAPACITY; startPos++) {

            log.info("----------------------------- {} FORWARD ------------------ ", startPos);

            hashtable = newHashtable(SMALL_SIZE);

            final long[] keys = findKeysForPosition(startPos, 7434L, 4, SMALL_CAPACITY);
            Arrays.stream(keys).forEach(key -> hashtable.put(key, -key));

            for (int i = 0; i < 4; i++) {
                final long removed = hashtable.remove(keys[i]);
                assertThat(removed, is(-keys[i]));
                assertFalse(hashtable.containsKey(keys[i]));
                for (int j = i + 1; j < 4; j++) {
                    assertThat(hashtable.get(keys[j]), is(-keys[j]));
                }
            }

            log.info("----------------------------- {} BACKWARDS------------------ ", startPos);

            hashtable = newHashtable(SMALL_SIZE);

            Arrays.stream(keys).forEach(key -> hashtable.put(key, -key));

            for (int i = 3; i >= 0; i--) {
                final long removed = hashtable.remove(keys[i]);
                assertThat(removed, is(-keys[i]));
                assertFalse(hashtable.containsKey(keys[i]));
                for (int j = 0; j < i; j++) {
                    assertThat(hashtable.get(keys[j]), is(-keys[j]));
                }
            }

        }
    }

    // todo add more complex collision removal tests

    @Test
    public void should_reject_key_zero() {

        hashtable = newHashtable(SMALL_SIZE);
        hashtable.put(5L, 50L);

        assertThrows(IllegalArgumentException.class, () -> hashtable.put(0L, 1L));
        assertFalse(hashtable.containsKey(0L));
        assertThat(hashtable.get(0L), is(0L));
        assertThat(hashtable.size(), is(1L));
    }

    /**
     * hash(0) = 0, and an empty slot holds key 0 - removeInternal used to take that gap for the key and decrement
     * size. A negative size never reaches the resize threshold, so the table then filled up completely.
     */
    @Test
    public void should_not_change_size_on_remove_of_key_zero() {

        hashtable = newHashtable(SMALL_SIZE);
        assertThat(hashtable.remove(0L), is(0L));
        assertThat(hashtable.size(), is(0L));

        hashtable.put(5L, 50L);
        for (int i = 0; i < 3; i++) {
            assertThat(hashtable.remove(0L), is(0L));
        }
        assertThat(hashtable.size(), is(1L));
        assertThat(hashtable.get(5L), is(50L));

        // a key 0 remove must not get in the way of the resize either
        final Random rand = new Random(3L);
        final long[] keys = new long[1000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rand.nextLong() | 1L;
            hashtable.put(keys[i], i);
            hashtable.remove(0L);
        }
        assertThat(hashtable.size(), is(keys.length + 1L));
        for (int i = 0; i < keys.length; i++) {
            assertThat(hashtable.get(keys[i]), is((long) i));
        }
    }

    @Test
    public void should_contain_key_with_value_zero() {

        hashtable = newHashtable(SMALL_SIZE);
        assertThat(hashtable.put(7L, 0L), is(0L));

        assertTrue(hashtable.containsKey(7L));
        assertThat(hashtable.get(7L), is(0L));
        assertThat(hashtable.size(), is(1L));

        // the same through a collision chain: key2 sits behind key 7
        final long key2 = findCollision(7L, SMALL_CAPACITY);
        hashtable.put(key2, 0L);
        assertTrue(hashtable.containsKey(key2));

        assertThat(hashtable.remove(7L), is(0L));
        assertFalse(hashtable.containsKey(7L));
        assertTrue(hashtable.containsKey(key2));
        assertThat(hashtable.size(), is(1L));
    }

    @Test
    public void should_validate_expected_size() {

        assertThrows(IllegalArgumentException.class, () -> create(-1));
        // used to overflow to a zero-length array (AIOOBE on the first put) or to a negative one
        assertThrows(IllegalArgumentException.class, () -> create(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> create(400_000_000));

        hashtable = newHashtable(0);
        for (long key = 1; key <= 100; key++) {
            hashtable.put(key, key * 10);
        }
        for (long key = 1; key <= 100; key++) {
            assertThat(hashtable.get(key), is(key * 10));
        }
        assertThat(hashtable.size(), is(100L));
    }

    @Test
    public void should_upsize() {
        Random rand = new Random(14232313L);
        for (int iteration = 0; iteration < 10; iteration++) {

            hashtable = newHashtable(16);
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


    /*
     * The helpers below must use the tables' own hash: keys found with any other function (these used agrona's)
     * land in unrelated slots, and the collision tests silently stop colliding anything.
     */

    private static int homePosition(long key, int capacity) {
        return HashingUtils.hash(key) & (capacity - 1);
    }

    private long findCollision(long key, int capacity) {

        final int keyPosition = homePosition(key, capacity);
        do {
            key++;

        } while (homePosition(key, capacity) != keyPosition);

        return key;
    }


    private long findKeyForPosition(int keyPosition, long afterKey, int capacity) {

        long key = afterKey;
        do {
            key++;

        } while (homePosition(key, capacity) != keyPosition);

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
