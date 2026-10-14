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
     * Expected size giving a 16-slot table, which the collision tests below rely on: it resizes at the 10th entry
     * (16 * 0.65 = 10.4), so it takes up to 9.
     */
    private static final int SMALL_SIZE = 9;
    private static final int SMALL_CAPACITY = 16;

    private final List<ILongLongHashtable> created = new ArrayList<>();

    ILongLongHashtable hashtable;

    /**
     * @param expectedSize number of entries the table is pre-sized for
     */
    protected abstract ILongLongHashtable create(int expectedSize);

    /**
     * @param maxCapacity slots the table stops growing at, instead of the real 2^29
     */
    protected abstract ILongLongHashtable createLimited(int expectedSize, int maxCapacity);

    private ILongLongHashtable newHashtable(int expectedSize) {
        final ILongLongHashtable table = create(expectedSize);
        created.add(table);
        return table;
    }

    /**
     * The tables' own invariant check: every key reachable from its home, size = occupied slots, a free slot left.
     * LL2 checks only between migrations - close() finishes one in flight, the table stays usable.
     */
    static void checkIntegrity(ILongLongHashtable table) {
        if (table instanceof LongLongHashtable t) {
            t.integrityCheck();
        } else if (table instanceof LongLongLL2Hashtable t) {
            t.close();
            t.integrityCheck();
        }
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
            checkIntegrity(hashtable);
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
                for (int g = 0; g < size; g++) {
                    if (g == k) {
                        continue;
                    }
                    assertThat("k:" + k + " h:" + h + " g:" + g,
                            LinearProbing.canFillGap(g, h, k, mask), is(canFillGapAndFinish(k, h, g)));
                }
            }
        }
    }


    /** reference: the gap g lies within [h, k) on the ring - empty for an entry at its home (h == k) */
    public boolean canFillGapAndFinish(int k, int h, int g) {

        if (g == k) {
            throw new IllegalStateException("gap == k, should finish");
        }

        if (h == k) {
            return false;
        } else if (h < k) {
            return g >= h && g < k;
        } else {
            return g >= h || g < k;
        }
    }

    /**
     * Removal fuzz on a 16-slot table: keys homed in the last and first slots form clusters that wrap around the
     * end of the array, removed in random order. After every removal all remaining keys must still be found.
     */
    @Test
    public void should_keep_keys_reachable_after_removals_in_wrapping_clusters() {

        final Random rand = new Random(21L);
        final int[] homes = {12, 13, 14, 15, 0, 1, 2};
        for (int round = 0; round < 2000; round++) {

            hashtable = newHashtable(SMALL_SIZE);
            final Map<Long, Long> ref = new HashMap<>();
            final int count = 2 + rand.nextInt(SMALL_SIZE - 1); // up to 9 entries - the 10th would resize
            long next = rand.nextInt(1_000_000) + 1;
            for (int i = 0; i < count; i++) {
                final long key = findKeyForPosition(homes[rand.nextInt(homes.length)], next, SMALL_CAPACITY);
                next = key;
                hashtable.put(key, -key);
                ref.put(key, -key);
            }

            final List<Long> order = new ArrayList<>(ref.keySet());
            java.util.Collections.shuffle(order, rand);
            for (long removed : order) {
                assertThat(hashtable.remove(removed), is(-removed));
                ref.remove(removed);
                assertFalse(hashtable.containsKey(removed));
                for (Map.Entry<Long, Long> e : ref.entrySet()) {
                    assertThat("round " + round + " key " + e.getKey(), hashtable.get(e.getKey()), is(e.getValue()));
                }
                assertThat(hashtable.size(), is((long) ref.size()));
                checkIntegrity(hashtable);
            }
        }
    }

    /**
     * At its maximum capacity a table can not grow: it takes new keys up to load 0.9 and refuses the next one,
     * unchanged - instead of filling up completely, with ever longer probes, and failing only at 100%.
     */
    @Test
    public void should_refuse_new_keys_at_full_load_of_the_maximum_capacity() {

        final int maxCapacity = 64;
        final int limit = (int) (maxCapacity * HashingUtils.FULL_LOAD); // 57
        hashtable = createLimited(SMALL_SIZE, maxCapacity);             // 16 -> 32 -> 64 slots, then stops
        created.add(hashtable);

        final Map<Long, Long> ref = new HashMap<>();
        for (long key = 1; key <= limit; key++) {
            assertThat(hashtable.put(key, key * 10), is(0L));
            ref.put(key, key * 10);
        }
        assertThat(hashtable.size(), is((long) limit));

        final long refused = limit + 1;
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> hashtable.put(refused, 1L));
        assertTrue(e.getMessage(), e.getMessage().contains("table is full"));
        assertFalse(hashtable.containsKey(refused));
        assertThat(hashtable.size(), is((long) limit));
        ref.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));
        checkIntegrity(hashtable);

        // updates still work, and a removal makes room for one new key
        assertThat(hashtable.put(1L, 11L), is(10L));
        assertThat(hashtable.remove(2L), is(20L));
        assertThat(hashtable.put(refused, 1L), is(0L));
        assertThat(hashtable.size(), is((long) limit));
        assertThrows(IllegalStateException.class, () -> hashtable.put(refused + 1, 1L));
        checkIntegrity(hashtable);
    }


    @Test
    public void should_put_if_absent() {

        hashtable = newHashtable(SMALL_SIZE);
        assertThat(hashtable.putIfAbsent(5L, 50L), is(0L));
        assertThat(hashtable.putIfAbsent(5L, 51L), is(50L));
        assertThat(hashtable.get(5L), is(50L));

        // behind a collision, and a present key with value 0 - present, so unchanged
        final long key2 = findCollision(5L, SMALL_CAPACITY);
        assertThat(hashtable.putIfAbsent(key2, 0L), is(0L));
        assertThat(hashtable.putIfAbsent(key2, 61L), is(0L));
        assertTrue(hashtable.containsKey(key2));
        assertThat(hashtable.get(key2), is(0L));
        assertThat(hashtable.size(), is(2L));

        assertThrows(IllegalArgumentException.class, () -> hashtable.putIfAbsent(0L, 1L));
        assertThat(hashtable.size(), is(2L));
    }

    @Test
    public void should_add_and_get() {

        hashtable = newHashtable(SMALL_SIZE);
        assertThat(hashtable.addAndGet(5L, 7L), is(7L));
        assertThat(hashtable.addAndGet(5L, -10L), is(-3L));
        assertThat(hashtable.get(5L), is(-3L));

        // reaching 0 keeps the entry, adding 0 to a missing key puts it
        assertThat(hashtable.addAndGet(5L, 3L), is(0L));
        assertTrue(hashtable.containsKey(5L));
        final long key2 = findCollision(5L, SMALL_CAPACITY);
        assertThat(hashtable.addAndGet(key2, 0L), is(0L));
        assertTrue(hashtable.containsKey(key2));
        assertThat(hashtable.addAndGet(key2, 4L), is(4L));
        assertThat(hashtable.size(), is(2L));

        // wraps around like +
        assertThat(hashtable.addAndGet(7L, Long.MAX_VALUE), is(Long.MAX_VALUE));
        assertThat(hashtable.addAndGet(7L, 1L), is(Long.MIN_VALUE));

        assertThrows(IllegalArgumentException.class, () -> hashtable.addAndGet(0L, 1L));
        assertThat(hashtable.size(), is(3L));
    }

    @Test
    public void should_compute_if_absent() {

        hashtable = newHashtable(SMALL_SIZE);
        final int[] calls = {0};
        assertThat(hashtable.computeIfAbsent(5L, k -> ++calls[0] * k), is(5L));
        assertThat(hashtable.computeIfAbsent(5L, k -> ++calls[0] * k), is(5L));
        assertThat(calls[0], is(1));

        final long key2 = findCollision(5L, SMALL_CAPACITY);
        hashtable.put(key2, 0L);
        assertThat(hashtable.computeIfAbsent(key2, k -> ++calls[0]), is(0L));
        assertThat(calls[0], is(1));

        assertThrows(IllegalArgumentException.class, () -> hashtable.computeIfAbsent(0L, k -> ++calls[0]));
        assertThat(calls[0], is(1));
        assertThat(hashtable.size(), is(2L));
    }

    /**
     * The function grows the table through resizes: the offset found before calling it is stale, the computed value
     * must still land under the key.
     */
    @Test
    public void should_compute_if_absent_when_the_function_modifies_the_table() {

        hashtable = newHashtable(SMALL_SIZE);
        final long key = 1_000_001L;
        assertThat(hashtable.computeIfAbsent(key, k -> {
            for (long i = 1; i <= 100; i++) {
                hashtable.put(i, i);
            }
            return 77L;
        }), is(77L));

        assertThat(hashtable.get(key), is(77L));
        assertThat(hashtable.size(), is(101L));
        for (long i = 1; i <= 100; i++) {
            assertThat(hashtable.get(i), is(i));
        }
        checkIntegrity(hashtable);
    }

    @Test
    public void should_remove_only_with_matching_value() {

        hashtable = newHashtable(SMALL_SIZE);
        hashtable.put(5L, 50L);
        final long key2 = findCollision(5L, SMALL_CAPACITY);
        hashtable.put(key2, 0L);

        assertFalse(hashtable.remove(5L, 51L));
        assertThat(hashtable.get(5L), is(50L));
        assertTrue(hashtable.remove(5L, 50L));
        assertFalse(hashtable.containsKey(5L));
        assertFalse(hashtable.remove(5L, 50L));

        // value 0: matches a key stored with 0, not a missing key
        assertFalse(hashtable.remove(6L, 0L));
        assertTrue(hashtable.containsKey(key2));
        assertTrue(hashtable.remove(key2, 0L));
        assertFalse(hashtable.containsKey(key2));

        assertFalse(hashtable.remove(0L, 0L));
        assertThat(hashtable.size(), is(0L));
    }

    @Test
    public void should_print_entries() {

        hashtable = newHashtable(SMALL_SIZE);
        assertThat(hashtable.toString(), is("{}"));
        hashtable.put(5L, 50L);
        assertThat(hashtable.toString(), is("{5=50}"));
        hashtable.put(-7L, 0L);
        final String s = hashtable.toString();
        assertTrue(s, s.equals("{5=50, -7=0}") || s.equals("{-7=0, 5=50}"));
    }

    /**
     * A table sized for n entries takes n without a resize, with the smallest capacity that does. Expected sizes
     * right at a threshold, (int) (capacity * 0.65), used to get that capacity - and to resize at the last entry.
     */
    @Test
    public void should_fit_the_expected_size_without_resize() {

        final List<Integer> sizes = new ArrayList<>();
        for (int n = 0; n <= 100; n++) {
            sizes.add(n);
        }
        for (int capacity = 128; capacity <= 1 << 16; capacity <<= 1) {
            final int threshold = (int) (capacity * 0.65f);
            sizes.add(threshold - 1);
            sizes.add(threshold);
            sizes.add(threshold + 1);
        }

        for (int n : sizes) {
            final ILongLongHashtable table = newHashtable(n);
            final int capacity = table.capacity();
            assertTrue("n=" + n + " capacity=" + capacity, (int) (capacity * 0.65f) > n);
            assertTrue("n=" + n + " capacity=" + capacity, (int) ((capacity >> 1) * 0.65f) <= n);
            for (long key = 1; key <= n; key++) {
                table.put(key, key);
            }
            assertThat("n=" + n, table.capacity(), is(capacity));
        }
    }

    @Test
    public void should_ensure_capacity() {

        hashtable = newHashtable(SMALL_SIZE);
        final Map<Long, Long> ref = new HashMap<>();
        for (long key = 1; key <= SMALL_SIZE; key++) {
            hashtable.put(key * 7919, key);
            ref.put(key * 7919, key);
        }
        assertThat(hashtable.capacity(), is(SMALL_CAPACITY));

        hashtable.ensureCapacity(5); // fits already
        assertThat(hashtable.capacity(), is(SMALL_CAPACITY));

        hashtable.ensureCapacity(10_000); // 10001 / 0.65 = 15386 slots needed - one rehash to 16384
        assertThat(hashtable.capacity(), is(16384));
        ref.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));
        checkIntegrity(hashtable);

        for (long key = 1; hashtable.size() < 10_000; key++) {
            hashtable.put(-key, key);
        }
        assertThat(hashtable.capacity(), is(16384));
        ref.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

        assertThrows(IllegalArgumentException.class, () -> hashtable.ensureCapacity(-1));
        assertThrows(IllegalArgumentException.class, () -> hashtable.ensureCapacity(400_000_000));
        assertThat(hashtable.capacity(), is(16384));
        checkIntegrity(hashtable);
    }

    /**
     * All operations against a reference map while the table grows through resizes - asynchronous ones in LL2, past
     * 8200 entries, so that the compound operations run during migrations too.
     */
    @Test
    public void should_match_reference_map_with_all_operations() {

        final Random rand = new Random(5L);
        hashtable = newHashtable(16);
        final Map<Long, Long> ref = new HashMap<>();
        final long[] keys = new long[40_000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rand.nextLong() | 1L;
        }

        for (int op = 0; op < 400_000; op++) {
            // a growing set of keys, so that the table keeps resizing
            final long key = keys[rand.nextInt(Math.min(keys.length, 100 + op / 8))];
            final long value = rand.nextInt(4) == 0 ? 0L : rand.nextLong();
            final int kind = rand.nextInt(9);
            switch (kind) {
                case 0 -> check(op, kind, hashtable.put(key, value), orZero(ref.put(key, value)));
                case 1 -> check(op, kind, hashtable.putIfAbsent(key, value), orZero(ref.putIfAbsent(key, value)));
                case 2 -> check(op, kind, hashtable.addAndGet(key, value), ref.merge(key, value, Long::sum));
                case 3 -> check(op, kind, hashtable.computeIfAbsent(key, k -> k ^ value),
                        ref.computeIfAbsent(key, k -> k ^ value));
                case 4 -> check(op, kind, hashtable.remove(key), orZero(ref.remove(key)));
                case 5 -> {
                    final long expected = rand.nextBoolean() ? orZero(ref.get(key)) : value;
                    final boolean removed = hashtable.remove(key, expected);
                    check(op, kind, removed ? 1 : 0, ref.remove(key, expected) ? 1 : 0);
                }
                case 6 -> check(op, kind, hashtable.containsKey(key) ? 1 : 0, ref.containsKey(key) ? 1 : 0);
                default -> check(op, kind, hashtable.get(key), orZero(ref.get(key)));
            }
            check(op, -1, hashtable.size(), ref.size());
        }

        log.info("size={} capacity={}", hashtable.size(), hashtable.capacity());
        ref.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));
        checkIntegrity(hashtable);
    }

    /** assertThat with a message built only on failure - the reference test runs millions of checks */
    private static void check(int op, int kind, long actual, long expected) {
        if (actual != expected) {
            throw new AssertionError("op " + op + " kind " + kind + ": expected " + expected + " but was " + actual);
        }
    }

    private static long orZero(Long value) {
        return value == null ? 0L : value;
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
