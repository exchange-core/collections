package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IntObjectHashtableTest {

    /** expected size of a 16-slot table at load factor 0.65 ({@link #at065}): it resizes at the 10th entry */
    private static final int SMALL_SIZE = 9;
    private static final int SMALL_CAPACITY = 16;

    /** a table of expectedSize growing at load 0.65 at any capacity - the tests of the layout do not follow the defaults */
    private static <V> IntObjectHashtable<V> at065(int expectedSize) {
        return IntObjectHashtable.builder()
                .expectedSize(expectedSize)
                .loadFactor(0.65)
                .smallTableLoadFactor(0.65)
                .build();
    }

    /** the key kept in a field: stored as 0, the free-slot marker */
    private static int seedKey(IntObjectHashtable<?> table) {
        return (int) table.seed();
    }

    @Test
    public void basic_operations() {
        final IntObjectHashtable<String> table = new IntObjectHashtable<>();
        assertNull(table.put(42, "a"));
        assertThat(table.get(42), is("a"));
        assertNull(table.get(43));
        assertTrue(table.containsKey(42));
        assertFalse(table.containsKey(43));
        assertThat(table.put(42, "b"), is("a"));
        assertThat(table.putIfAbsent(42, "c"), is("b"));
        assertNull(table.putIfAbsent(7, "seven"));
        assertThat(table.computeIfAbsent(8, k -> "v" + k), is("v8"));
        assertThat(table.computeIfAbsent(8, k -> "other"), is("v8"));
        assertNull(table.computeIfAbsent(9, k -> null));
        assertFalse(table.containsKey(9));
        assertThat(table.getOrDefault(9, "d"), is("d"));
        assertThat(table.getOrDefault(8, "d"), is("v8"));
        assertFalse(table.remove(42, "x"));
        assertFalse(table.remove(42, null));
        assertTrue(table.remove(42, new String("b")));
        assertThat(table.remove(7), is("seven"));
        assertNull(table.remove(7));
        assertThat(table.size(), is(1L));
        assertThat(table.toString(), is("{8=v8}"));
        table.clear();
        assertThat(table.size(), is(0L));
        assertNull(table.get(8));
        table.integrityCheck();
    }

    @Test
    public void null_values_are_refused() {
        final IntObjectHashtable<String> table = new IntObjectHashtable<>();
        assertThrows(NullPointerException.class, () -> table.put(1, null));
        assertThrows(NullPointerException.class, () -> table.putIfAbsent(1, null));
        assertThrows(NullPointerException.class, () -> table.put(seedKey(table), null));
        assertThat(table.size(), is(0L));
        table.integrityCheck();
    }

    /** key 0 and the key equal to (int) seed - one of them is the gap marker after the XOR, kept in a field */
    @Test
    public void any_key() {
        final IntObjectHashtable<String> table = new IntObjectHashtable<>();
        final int seedKey = seedKey(table);
        final int[] keys = {0, seedKey, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, ~seedKey, -seedKey};
        for (int key : keys) {
            table.put(key, "v" + key);
        }
        final long distinct = Arrays.stream(keys).distinct().count();
        assertThat(table.size(), is(distinct));
        for (int key : keys) {
            assertThat(table.get(key), is("v" + key));
            assertTrue(table.containsKey(key));
            assertThat(table.putIfAbsent(key, "x"), is("v" + key));
        }
        final Map<Integer, String> visited = new HashMap<>();
        table.forEach((k, v) -> assertNull(visited.put(k, v)));
        assertThat((long) visited.size(), is(distinct));
        for (int key : keys) {
            assertThat(visited.get(key), is("v" + key));
        }
        assertThat(table.keysStream().boxed().collect(Collectors.toList()), is(orderOf(table)));
        table.integrityCheck();
        for (int key : keys) {
            table.remove(key);
            assertFalse(table.containsKey(key));
        }
        assertThat(table.size(), is(0L));
        table.integrityCheck();
    }

    /** a negative key and the positive one of the same magnitude are different keys */
    @Test
    public void negative_keys_are_distinct() {
        final IntObjectHashtable<Integer> table = at065(SMALL_SIZE);
        for (int key = -4_000; key <= 4_000; key++) {
            table.put(key, key);
        }
        assertThat(table.size(), is(8_001L));
        for (int key = -4_000; key <= 4_000; key++) {
            assertThat(table.get(key), is(key));
        }
        table.integrityCheck();
    }

    private static List<Integer> orderOf(IntObjectHashtable<?> table) {
        final List<Integer> order = new ArrayList<>();
        table.forEach((k, v) -> order.add(k));
        return order;
    }

    /**
     * @return count keys whose (seeded) home is the given slot of a table of the capacity
     */
    private static int[] keysHomedAt(int slot, int capacity, int count, int from) {
        final int[] keys = new int[count];
        int found = 0;
        for (int key = from; found < count; key++) {
            final int k = key ^ HashingUtils.INT_SEED;
            if (k != 0 && (HashingUtils.hashInt(k) & (capacity - 1)) == slot) {
                keys[found++] = key;
            }
        }
        return keys;
    }

    /** a cluster across the end of the array: the removal of each key shifts back the right ones, wrapping */
    @Test
    public void removal_shifts_back_a_wrapping_cluster() {
        final int[] atEnd = keysHomedAt(SMALL_CAPACITY - 1, SMALL_CAPACITY, 4, 1);
        final int[] atStart = keysHomedAt(0, SMALL_CAPACITY, 3, 1);
        for (int removed = 0; removed < atEnd.length + atStart.length; removed++) {
            final IntObjectHashtable<Integer> table = at065(SMALL_SIZE);
            final List<Integer> all = new ArrayList<>();
            for (int key : atEnd) {
                all.add(key);
            }
            for (int key : atStart) {
                all.add(key);
            }
            for (int key : all) {
                table.put(key, key * 10);
            }
            assertThat(table.capacity(), is(SMALL_CAPACITY));
            final int gone = all.get(removed);
            assertThat(table.remove(gone), is(gone * 10));
            table.integrityCheck();
            for (int key : all) {
                if (key == gone) {
                    assertNull(table.get(key));
                } else {
                    assertThat(table.get(key), is(key * 10));
                }
            }
        }
    }

    @Test
    public void grows_and_keeps_the_entries() {
        final IntObjectHashtable<Integer> table = new IntObjectHashtable<>(1);
        for (int key = 0; key < 100_000; key++) {
            table.put(key * 31, key);
        }
        assertThat(table.size(), is(100_000L));
        assertTrue(table.capacity() > 100_000 / IntObjectHashtable.DEFAULT_LOAD_FACTOR);
        for (int key = 0; key < 100_000; key++) {
            assertThat(table.get(key * 31), is(key));
        }
        table.integrityCheck();
    }

    @Test
    public void ensure_capacity_rehashes_once() {
        final IntObjectHashtable<Integer> table = at065(SMALL_SIZE);
        for (int key = 0; key < SMALL_SIZE; key++) {
            table.put(key, key);
        }
        table.ensureCapacity(1000);
        final int capacity = table.capacity();
        assertThat(capacity, is(2048));
        for (int key = 0; key < 1000; key++) {
            table.put(key, key);
        }
        assertThat(table.capacity(), is(capacity));
        table.integrityCheck();
        assertThrows(IllegalArgumentException.class, () -> table.ensureCapacity(-1));
    }

    /** 16 slots, fixed, maxLoad 0.75: 12 keys in the array */
    private static IntObjectHashtable<Integer> fixed16() {
        return IntObjectHashtable.builder()
                .expectedSize(SMALL_SIZE)
                .loadFactor(0.65)
                .smallTableLoadFactor(0.65)
                .fixedCapacity()
                .maxLoad(0.75)
                .build();
    }

    /** 16 slots, maxLoad 0.75: 12 keys, the 13th refused and the table unchanged */
    @Test
    public void fixed_capacity_refuses_new_keys_at_max_load() {
        final IntObjectHashtable<Integer> table = fixed16();
        // from 1: not the key equal to the seed of 0, kept outside the array
        for (int key = 1; key <= 12; key++) {
            table.put(key, key);
        }
        final String before = table.toString();
        assertThrows(IllegalStateException.class, () -> table.put(100, 1));
        assertThrows(IllegalStateException.class, () -> table.putIfAbsent(101, 1));
        assertThrows(IllegalStateException.class, () -> table.computeIfAbsent(102, k -> 1));
        assertThat(table.toString(), is(before));
        assertThat(table.size(), is(12L));
        assertThat(table.capacity(), is(SMALL_CAPACITY));
        table.integrityCheck();
        // updates and removals go on working
        assertThat(table.put(3, 33), is(3));
        assertThat(table.remove(4), is(4));
        assertNull(table.put(100, 1));
        table.integrityCheck();
    }

    /**
     * The key equal to (int) seed is kept outside the array and does not count towards the max load: first or last,
     * a full table holds 12 keys of the array next to it, and after a removal takes a new one again - by any call.
     */
    @Test
    public void the_key_equal_to_the_seed_does_not_count_towards_the_max_load() {
        for (boolean first : new boolean[]{true, false}) {
            final String msg = first ? "seed key first" : "seed key last";
            final IntObjectHashtable<Integer> table = fixed16();
            final int seedKey = seedKey(table);
            if (first) {
                table.put(seedKey, -1);
            }
            for (int key = 1; key <= 12; key++) {
                table.put(key, key);
            }
            if (!first) {
                table.put(seedKey, -1);
            }
            assertThat(msg, table.size(), is(13L));
            assertThrows(msg, IllegalStateException.class, () -> table.put(100, 1));
            assertThrows(msg, IllegalStateException.class, () -> table.putIfAbsent(100, 1));
            assertThrows(msg, IllegalStateException.class, () -> table.computeIfAbsent(100, k -> 1));
            assertThat(msg, table.size(), is(13L));
            table.ensureCapacity(13); // what it holds
            assertThrows(msg, IllegalArgumentException.class, () -> table.ensureCapacity(14));
            table.integrityCheck();

            // a removal makes room for one key, whichever call inserts it
            assertThat(msg, table.remove(1), is(1));
            assertNull(msg, table.put(101, 101));
            assertThrows(msg, IllegalStateException.class, () -> table.put(200, 1));
            assertThat(msg, table.remove(2, 2), is(true));
            assertNull(msg, table.putIfAbsent(102, 102));
            assertThrows(msg, IllegalStateException.class, () -> table.putIfAbsent(200, 1));
            assertThat(msg, table.remove(3), is(3));
            assertThat(msg, table.computeIfAbsent(103, k -> k), is(103));
            assertThrows(msg, IllegalStateException.class, () -> table.computeIfAbsent(200, k -> k));
            assertThat(msg, table.size(), is(13L));

            // the key outside goes and comes back at a full array
            assertThat(msg, table.remove(seedKey), is(-1));
            assertThat(msg, table.size(), is(12L));
            assertThat(msg, table.computeIfAbsent(seedKey, k -> -2), is(-2));
            assertThat(msg, table.size(), is(13L));
            assertThat(msg, table.capacity(), is(SMALL_CAPACITY));
            table.integrityCheck();
        }
    }

    /** at the maximum capacity ensureCapacity takes what the table takes at max load, not only below its threshold */
    @Test
    public void ensure_capacity_at_the_maximum_capacity() {
        final IntObjectHashtable<Integer> table = fixed16();
        for (int key = 1; key <= 12; key++) {
            table.put(key, key);
        }
        table.ensureCapacity(10);
        table.ensureCapacity(12);
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> table.ensureCapacity(13));
        assertTrue(e.getMessage(), e.getMessage().contains("maximum of 12 entries"));
        assertThat(table.capacity(), is(SMALL_CAPACITY));

        // a growing table limited to 64 slots: rehashed to them, also for more than its growth threshold there
        final IntObjectHashtable<Integer> limited = IntObjectHashtable.builder().expectedSize(0).maxCapacity(64).build();
        limited.ensureCapacity(50); // 64 slots grow at 32, take 60 at max load 0.95
        assertThat(limited.capacity(), is(64));
        for (int key = 1; key <= 60; key++) {
            limited.put(key, key);
        }
        assertThat(limited.capacity(), is(64));
        assertThrows(IllegalStateException.class, () -> limited.put(100, 1));
        assertThrows(IllegalArgumentException.class, () -> limited.ensureCapacity(61));
        limited.integrityCheck();
        // the builder the same
        assertThat(IntObjectHashtable.builder().expectedSize(60).maxCapacity(64).build().capacity(), is(64));
        assertThrows(IllegalArgumentException.class,
                () -> IntObjectHashtable.builder().expectedSize(61).maxCapacity(64).build());
    }

    @Test
    public void max_capacity_stops_the_growth() {
        final IntObjectHashtable<Integer> table = IntObjectHashtable.builder().expectedSize(1).maxCapacity(32).build();
        for (int key = 1; key <= 30; key++) {
            table.put(key, key);
        }
        assertThat(table.capacity(), is(32));
        assertThrows(IllegalStateException.class, () -> table.put(100, 1));
        table.integrityCheck();
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().maxCapacity(100));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().maxLoad(1.0).build());
        // 128 slots take 121 keys at max load 0.95
        assertThrows(IllegalArgumentException.class,
                () -> IntObjectHashtable.builder().expectedSize(122).maxCapacity(128).build());
    }

    /** small table load factor below the small table capacity, the load factor from there on */
    @Test
    public void load_factors_set_where_the_table_grows() {
        final IntObjectHashtable<Integer> table = IntObjectHashtable.builder()
                .expectedSize(0)
                .loadFactor(0.75)
                .smallTableLoadFactor(0.5)
                .smallTableCapacity(64)
                .build();
        assertThat(table.capacity(), is(2));
        final List<String> growth = new ArrayList<>();
        int capacity = table.capacity();
        // from 1: not the key equal to the seed of 0, which is kept in a field and grows nothing
        for (int key = 1; key <= 150; key++) {
            table.put(key, key);
            if (table.capacity() != capacity) {
                capacity = table.capacity();
                growth.add(table.size() + "->" + capacity);
            }
        }
        // 2, 4, 8, 16, 32 slots grow at half load, 64, 128 at 3/4
        assertThat(growth, is(List.of("1->4", "2->8", "4->16", "8->32", "16->64", "48->128", "96->256")));
        table.integrityCheck();

        // pre-sizing follows the same thresholds: 47 keys fit 64 slots, 15 fit 32
        final IntObjectHashtable.Builder builder = IntObjectHashtable.builder()
                .loadFactor(0.75).smallTableLoadFactor(0.5).smallTableCapacity(64);
        assertThat(builder.expectedSize(47).build().capacity(), is(64));
        assertThat(builder.expectedSize(48).build().capacity(), is(128));
        assertThat(builder.expectedSize(15).build().capacity(), is(32));
        assertThat(builder.expectedSize(16).build().capacity(), is(64));
        final IntObjectHashtable<Integer> grown = builder.expectedSize(0).build();
        grown.ensureCapacity(16);
        assertThat(grown.capacity(), is(64));

        // capacity 2: every table at the load factor
        assertThat(IntObjectHashtable.builder().loadFactor(0.75).smallTableCapacity(2).expectedSize(11).build()
                .capacity(), is(16));
    }

    @Test
    public void small_table_load_factor_defaults_to_at_most_the_load_factor() {
        // unset: the default 0.5, or the load factor if lower - here 0.25, 60 keys need 256 slots
        assertThat(IntObjectHashtable.builder().loadFactor(0.25).expectedSize(60).build().capacity(), is(256));
        // the defaults: 0.5 below 2^16 slots - 10k keys take 32768
        assertThat(new IntObjectHashtable<>(10_000).capacity(), is(32768));
    }

    @Test
    public void load_factors_are_checked() {
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().loadFactor(0.2));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().loadFactor(0.95));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().loadFactor(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().smallTableLoadFactor(0.1));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().smallTableCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().smallTableCapacity(1));
        assertThrows(IllegalArgumentException.class, () -> IntObjectHashtable.builder().smallTableCapacity(100));
        assertThrows(IllegalArgumentException.class,
                () -> IntObjectHashtable.builder().smallTableCapacity(1 << 30));
        // small above the load factor
        assertThrows(IllegalArgumentException.class,
                () -> IntObjectHashtable.builder().loadFactor(0.5).smallTableLoadFactor(0.6).build());
        // maxLoad below the load the table grows at near its maximum capacity
        assertThrows(IllegalArgumentException.class,
                () -> IntObjectHashtable.builder().loadFactor(0.8).maxLoad(0.75).build());
        // ... a fixed small table grows (would grow) at the small table load factor
        assertThat(IntObjectHashtable.builder().expectedSize(SMALL_SIZE).fixedCapacity().loadFactor(0.8)
                .smallTableLoadFactor(0.65).maxLoad(0.75).build().capacity(), is(SMALL_CAPACITY));
    }

    /** the function may change the table: its result still ends up under the key */
    @Test
    public void compute_if_absent_with_a_function_that_changes_the_table() {
        final IntObjectHashtable<Integer> table = new IntObjectHashtable<>(SMALL_SIZE);
        final Integer value = table.computeIfAbsent(1, k -> {
            for (int key = 100; key < 120; key++) {
                table.put(key, key);
            }
            return -1;
        });
        assertThat(value, is(-1));
        assertThat(table.get(1), is(-1));
        assertThat(table.size(), is(21L));
        table.integrityCheck();

        // ... clears it, puts the same key: the result replaces that value; the key equal to the seed the same
        for (int key : new int[]{5, seedKey(table)}) {
            table.put(7, 7);
            assertThat(table.computeIfAbsent(key, k -> {
                table.clear();
                table.put(k, -5);
                return -6;
            }), is(-6));
            assertThat(table.get(key), is(-6));
            assertThat(table.size(), is(1L));
            assertFalse(table.containsKey(7));
            table.integrityCheck();
            table.clear();
        }
    }

    @Test
    public void streams_and_for_each_agree() {
        final IntObjectHashtable<Integer> table = new IntObjectHashtable<>();
        for (int key = -50; key < 50; key++) {
            table.put(key, key * 2);
        }
        table.put(seedKey(table), seedKey(table) * 2);
        final List<Integer> keys = table.keysStream().boxed().collect(Collectors.toList());
        final List<Integer> values = table.valuesStream().collect(Collectors.toList());
        assertThat(keys, is(orderOf(table)));
        assertThat((long) keys.size(), is(table.size()));
        for (int i = 0; i < keys.size(); i++) {
            assertThat(values.get(i), is(keys.get(i) * 2));
        }
    }

    @Test
    public void matches_a_reference_map() {
        final Random random = new Random(11);
        final IntObjectHashtable<Long> table = new IntObjectHashtable<>(1);
        final Map<Integer, Long> ref = new HashMap<>();
        for (int i = 0; i < 300_000; i++) {
            // few keys: long clusters, frequent removals in them
            final int key = random.nextInt(4_000) - 2_000 + (random.nextInt(8) == 0 ? HashingUtils.INT_SEED : 0);
            final Long value = random.nextLong();
            final int op = random.nextInt(6);
            final Object actual = switch (op) {
                case 0, 1 -> table.put(key, value);
                case 2 -> table.remove(key);
                case 3 -> table.putIfAbsent(key, value);
                case 4 -> table.containsKey(key);
                default -> table.get(key);
            };
            final Object expected = switch (op) {
                case 0, 1 -> ref.put(key, value);
                case 2 -> ref.remove(key);
                case 3 -> ref.putIfAbsent(key, value);
                case 4 -> ref.containsKey(key);
                default -> ref.get(key);
            };
            assertEquals("op " + i, expected, actual);
            if (i % 50_000 == 0) {
                table.integrityCheck();
            }
        }
        assertThat(table.size(), is((long) ref.size()));
        final Map<Integer, Long> visited = new HashMap<>();
        table.forEach(visited::put);
        assertThat(visited, is(ref));
        table.integrityCheck();
    }

    /** a removed value is not kept by the table: the slot it left holds null (checked by the integrity check too) */
    @Test
    public void removed_values_are_released() {
        final IntObjectHashtable<Object> table = new IntObjectHashtable<>();
        final Object value = new Object();
        table.put(5, value);
        assertSame(value, table.remove(5));
        assertThat(table.valuesStream().count(), is(0L));
        table.integrityCheck();
    }
}
