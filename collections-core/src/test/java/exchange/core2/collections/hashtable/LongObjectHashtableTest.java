package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.util.ArrayList;
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

public class LongObjectHashtableTest {

    /** expected size of a 16-slot table at load factor 0.65 ({@link #at065}): it resizes at the 10th entry */
    private static final int SMALL_SIZE = 9;
    private static final int SMALL_CAPACITY = 16;

    /** a table of expectedSize growing at load 0.65 at any capacity - the tests of the layout do not follow the defaults */
    private static <V> LongObjectHashtable<V> at065(int expectedSize) {
        return LongObjectHashtable.builder()
                .expectedSize(expectedSize)
                .loadFactor(0.65)
                .smallTableLoadFactor(0.65)
                .build();
    }

    @Test
    public void basic_operations() {
        final LongObjectHashtable<String> table = new LongObjectHashtable<>();
        assertNull(table.put(42L, "a"));
        assertThat(table.get(42L), is("a"));
        assertNull(table.get(43L));
        assertTrue(table.containsKey(42L));
        assertFalse(table.containsKey(43L));
        assertThat(table.put(42L, "b"), is("a"));
        assertThat(table.putIfAbsent(42L, "c"), is("b"));
        assertNull(table.putIfAbsent(7L, "seven"));
        assertThat(table.computeIfAbsent(8L, k -> "v" + k), is("v8"));
        assertThat(table.computeIfAbsent(8L, k -> "other"), is("v8"));
        assertNull(table.computeIfAbsent(9L, k -> null));
        assertFalse(table.containsKey(9L));
        assertThat(table.getOrDefault(9L, "d"), is("d"));
        assertThat(table.getOrDefault(8L, "d"), is("v8"));
        assertFalse(table.remove(42L, "x"));
        assertFalse(table.remove(42L, null));
        assertTrue(table.remove(42L, new String("b")));
        assertThat(table.remove(7L), is("seven"));
        assertNull(table.remove(7L));
        assertThat(table.size(), is(1L));
        assertThat(table.toString(), is("{8=v8}"));
        table.clear();
        assertThat(table.size(), is(0L));
        assertNull(table.get(8L));
        table.integrityCheck();
    }

    @Test
    public void null_values_are_refused() {
        final LongObjectHashtable<String> table = new LongObjectHashtable<>();
        assertThrows(NullPointerException.class, () -> table.put(1L, null));
        assertThrows(NullPointerException.class, () -> table.putIfAbsent(1L, null));
        assertThrows(NullPointerException.class, () -> table.put(table.seed(), null));
        assertThat(table.size(), is(0L));
        table.integrityCheck();
    }

    /** key 0 and the key equal to the seed - one of them is the gap marker after the XOR, kept in a field */
    @Test
    public void any_key() {
        final LongObjectHashtable<String> table = new LongObjectHashtable<>();
        final long[] keys = {0L, table.seed(), -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE, ~table.seed()};
        for (long key : keys) {
            table.put(key, "v" + key);
        }
        final long distinct = java.util.Arrays.stream(keys).distinct().count();
        assertThat(table.size(), is(distinct));
        for (long key : keys) {
            assertThat(table.get(key), is("v" + key));
            assertTrue(table.containsKey(key));
            assertThat(table.putIfAbsent(key, "x"), is("v" + key));
        }
        final Map<Long, String> visited = new HashMap<>();
        table.forEach((k, v) -> assertNull(visited.put(k, v)));
        assertThat((long) visited.size(), is(distinct));
        for (long key : keys) {
            assertThat(visited.get(key), is("v" + key));
        }
        assertThat(table.keysStream().boxed().collect(Collectors.toList()), is(new ArrayList<>(orderOf(table))));
        table.integrityCheck();
        for (long key : keys) {
            table.remove(key);
            assertFalse(table.containsKey(key));
        }
        assertThat(table.size(), is(0L));
        table.integrityCheck();
    }

    private static List<Long> orderOf(LongObjectHashtable<?> table) {
        final List<Long> order = new ArrayList<>();
        table.forEach((k, v) -> order.add(k));
        return order;
    }

    /**
     * @return count keys whose (seeded) home is the given slot of a table of the capacity
     */
    private static long[] keysHomedAt(int slot, int capacity, int count, long from) {
        final long[] keys = new long[count];
        int found = 0;
        for (long key = from; found < count; key++) {
            final long k = key ^ HashingUtils.SEED;
            if (k != 0L && (HashingUtils.hash(k) & (capacity - 1)) == slot) {
                keys[found++] = key;
            }
        }
        return keys;
    }

    /** a cluster across the end of the array: the removal of each key shifts back the right ones, wrapping */
    @Test
    public void removal_shifts_back_a_wrapping_cluster() {
        final long[] atEnd = keysHomedAt(SMALL_CAPACITY - 1, SMALL_CAPACITY, 4, 1);
        final long[] atStart = keysHomedAt(0, SMALL_CAPACITY, 3, 1);
        for (int removed = 0; removed < atEnd.length + atStart.length; removed++) {
            final LongObjectHashtable<Long> table = at065(SMALL_SIZE);
            final List<Long> all = new ArrayList<>();
            for (long key : atEnd) {
                all.add(key);
            }
            for (long key : atStart) {
                all.add(key);
            }
            for (long key : all) {
                table.put(key, key * 10);
            }
            assertThat(table.capacity(), is(SMALL_CAPACITY));
            final long gone = all.get(removed);
            assertThat(table.remove(gone), is(gone * 10));
            table.integrityCheck();
            for (long key : all) {
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
        final LongObjectHashtable<Long> table = new LongObjectHashtable<>(1);
        for (long key = 0; key < 100_000; key++) {
            table.put(key * 31, key);
        }
        assertThat(table.size(), is(100_000L));
        assertTrue(table.capacity() > 100_000 / LongObjectHashtable.DEFAULT_LOAD_FACTOR);
        for (long key = 0; key < 100_000; key++) {
            assertThat(table.get(key * 31), is(key));
        }
        table.integrityCheck();
    }

    @Test
    public void ensure_capacity_rehashes_once() {
        final LongObjectHashtable<Long> table = at065(SMALL_SIZE);
        for (long key = 0; key < SMALL_SIZE; key++) {
            table.put(key, key);
        }
        table.ensureCapacity(1000);
        final int capacity = table.capacity();
        assertThat(capacity, is(2048));
        for (long key = 0; key < 1000; key++) {
            table.put(key, key);
        }
        assertThat(table.capacity(), is(capacity));
        table.integrityCheck();
        assertThrows(IllegalArgumentException.class, () -> table.ensureCapacity(-1));
    }

    /** 16 slots, maxLoad 0.75: 12 keys, the 13th refused and the table unchanged */
    @Test
    public void fixed_capacity_refuses_new_keys_at_max_load() {
        final LongObjectHashtable<Long> table = LongObjectHashtable.builder()
                .expectedSize(SMALL_SIZE)
                .loadFactor(0.65)
                .smallTableLoadFactor(0.65)
                .fixedCapacity()
                .maxLoad(0.75)
                .build();
        for (long key = 0; key < 12; key++) {
            table.put(key, key);
        }
        final String before = table.toString();
        assertThrows(IllegalStateException.class, () -> table.put(100L, 1L));
        assertThrows(IllegalStateException.class, () -> table.putIfAbsent(101L, 1L));
        assertThrows(IllegalStateException.class, () -> table.computeIfAbsent(102L, k -> 1L));
        assertThat(table.toString(), is(before));
        assertThat(table.size(), is(12L));
        assertThat(table.capacity(), is(SMALL_CAPACITY));
        table.integrityCheck();
        // updates and removals go on working
        assertThat(table.put(3L, 33L), is(3L));
        assertThat(table.remove(4L), is(4L));
        assertNull(table.put(100L, 1L));
        table.integrityCheck();
    }

    @Test
    public void max_capacity_stops_the_growth() {
        final LongObjectHashtable<Long> table = LongObjectHashtable.builder().expectedSize(1).maxCapacity(32).build();
        for (long key = 0; key < 30; key++) {
            table.put(key, key);
        }
        assertThat(table.capacity(), is(32));
        assertThrows(IllegalStateException.class, () -> table.put(100L, 1L));
        table.integrityCheck();
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().maxCapacity(100));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().maxLoad(1.0).build());
        assertThrows(IllegalArgumentException.class,
                () -> LongObjectHashtable.builder().expectedSize(100).maxCapacity(128).build());
    }

    /** small table load factor below the small table capacity, the load factor from there on */
    @Test
    public void load_factors_set_where_the_table_grows() {
        final LongObjectHashtable<Long> table = LongObjectHashtable.builder()
                .expectedSize(0)
                .loadFactor(0.75)
                .smallTableLoadFactor(0.5)
                .smallTableCapacity(64)
                .build();
        assertThat(table.capacity(), is(2));
        final List<String> growth = new ArrayList<>();
        int capacity = table.capacity();
        // from 1: not the key equal to the seed of 0, which is kept in a field and grows nothing
        for (long key = 1; key <= 150; key++) {
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
        final LongObjectHashtable.Builder builder = LongObjectHashtable.builder()
                .loadFactor(0.75).smallTableLoadFactor(0.5).smallTableCapacity(64);
        assertThat(builder.expectedSize(47).build().capacity(), is(64));
        assertThat(builder.expectedSize(48).build().capacity(), is(128));
        assertThat(builder.expectedSize(15).build().capacity(), is(32));
        assertThat(builder.expectedSize(16).build().capacity(), is(64));
        final LongObjectHashtable<Long> grown = builder.expectedSize(0).build();
        grown.ensureCapacity(16);
        assertThat(grown.capacity(), is(64));

        // capacity 2: every table at the load factor
        assertThat(LongObjectHashtable.builder().loadFactor(0.75).smallTableCapacity(2).expectedSize(11).build()
                .capacity(), is(16));
    }

    @Test
    public void small_table_load_factor_defaults_to_at_most_the_load_factor() {
        // unset: the default 0.5, or the load factor if lower - here 0.25, 60 keys need 256 slots
        assertThat(LongObjectHashtable.builder().loadFactor(0.25).expectedSize(60).build().capacity(), is(256));
    }

    @Test
    public void load_factors_are_checked() {
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().loadFactor(0.2));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().loadFactor(0.95));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().loadFactor(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().smallTableLoadFactor(0.1));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().smallTableCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().smallTableCapacity(1));
        assertThrows(IllegalArgumentException.class, () -> LongObjectHashtable.builder().smallTableCapacity(100));
        assertThrows(IllegalArgumentException.class,
                () -> LongObjectHashtable.builder().smallTableCapacity(1 << 30));
        // small above the load factor
        assertThrows(IllegalArgumentException.class,
                () -> LongObjectHashtable.builder().loadFactor(0.5).smallTableLoadFactor(0.6).build());
        // maxLoad below the load the table grows at near its maximum capacity
        assertThrows(IllegalArgumentException.class,
                () -> LongObjectHashtable.builder().loadFactor(0.8).maxLoad(0.75).build());
        // ... a fixed small table grows (would grow) at the small table load factor
        assertThat(LongObjectHashtable.builder().expectedSize(SMALL_SIZE).fixedCapacity().loadFactor(0.8)
                .smallTableLoadFactor(0.65).maxLoad(0.75).build().capacity(), is(SMALL_CAPACITY));
    }

    /** the function may change the table: its result still ends up under the key */
    @Test
    public void compute_if_absent_with_a_function_that_changes_the_table() {
        final LongObjectHashtable<Long> table = new LongObjectHashtable<>(SMALL_SIZE);
        final Long value = table.computeIfAbsent(1L, k -> {
            for (long key = 100; key < 120; key++) {
                table.put(key, key);
            }
            return -1L;
        });
        assertThat(value, is(-1L));
        assertThat(table.get(1L), is(-1L));
        assertThat(table.size(), is(21L));
        table.integrityCheck();
    }

    @Test
    public void streams_and_for_each_agree() {
        final LongObjectHashtable<Long> table = new LongObjectHashtable<>();
        for (long key = -50; key < 50; key++) {
            table.put(key, key * 2);
        }
        final List<Long> keys = table.keysStream().boxed().collect(Collectors.toList());
        final List<Long> values = table.valuesStream().collect(Collectors.toList());
        assertThat(keys, is(orderOf(table)));
        for (int i = 0; i < keys.size(); i++) {
            assertThat(values.get(i), is(keys.get(i) * 2));
        }
    }

    @Test
    public void matches_a_reference_map() {
        final Random random = new Random(11);
        final LongObjectHashtable<Long> table = new LongObjectHashtable<>(1);
        final Map<Long, Long> ref = new HashMap<>();
        for (int i = 0; i < 300_000; i++) {
            // few keys: long clusters, frequent removals in them
            final long key = random.nextInt(4_000) - 2_000 + (random.nextInt(8) == 0 ? HashingUtils.SEED : 0);
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
        final Map<Long, Long> visited = new HashMap<>();
        table.forEach(visited::put);
        assertThat(visited, is(ref));
        table.integrityCheck();
    }

    /** a removed value is not kept by the table: the slot it left holds null (checked by the integrity check too) */
    @Test
    public void removed_values_are_released() {
        final LongObjectHashtable<Object> table = new LongObjectHashtable<>();
        final Object value = new Object();
        table.put(5L, value);
        assertSame(value, table.remove(5L));
        assertThat(table.valuesStream().count(), is(0L));
        table.integrityCheck();
    }
}
