package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class UuidObjectHashtableTest {

    /** expected size of a 16-slot table at load factor 0.65 ({@link #at065}): it resizes at the 10th entry */
    private static final int SMALL_SIZE = 9;
    private static final int SMALL_CAPACITY = 16;

    /** a table growing at load 0.65 at any capacity - the tests of the layout do not follow the defaults */
    private static UuidObjectHashtable.Builder at065(int expectedSize) {
        return UuidObjectHashtable.builder()
                .expectedSize(expectedSize)
                .loadFactor(0.65)
                .smallTableLoadFactor(0.65);
    }

    private static final UUID NIL = new UUID(0L, 0L);

    @Test
    public void basic_operations_both_key_forms() {
        final UuidObjectHashtable<String> table = new UuidObjectHashtable<>();
        final UUID a = UUID.randomUUID();
        final long hi = a.getMostSignificantBits();
        final long lo = a.getLeastSignificantBits();

        assertNull(table.put(a, "a"));
        assertThat(table.get(hi, lo), is("a"));
        assertThat(table.get(new UUID(hi, lo)), is("a"));
        assertTrue(table.containsKey(hi, lo));
        assertTrue(table.containsKey(a));
        assertThat(table.put(hi, lo, "b"), is("a"));
        assertThat(table.putIfAbsent(a, "c"), is("b"));
        assertThat(table.putIfAbsent(hi, lo, "c"), is("b"));
        assertThat(table.getOrDefault(a, "d"), is("b"));
        assertThat(table.getOrDefault(hi, lo + 1, "d"), is("d"));
        assertFalse(table.containsKey(hi, lo + 1));
        assertFalse(table.containsKey(new UUID(hi + 1, lo)));

        assertThat(table.computeIfAbsent(1L, 2L, (h, l) -> "v" + h + l), is("v12"));
        assertThat(table.computeIfAbsent(new UUID(1L, 2L), k -> "other"), is("v12"));
        final UUID c = new UUID(3L, 4L);
        assertThat(table.computeIfAbsent(c, UUID::toString), is(c.toString()));
        assertNull(table.computeIfAbsent(5L, 6L, (h, l) -> null));
        assertFalse(table.containsKey(5L, 6L));

        assertFalse(table.remove(a, "x"));
        assertFalse(table.remove(hi, lo, null));
        assertTrue(table.remove(hi, lo, new String("b")));
        assertNull(table.get(a));
        assertThat(table.remove(1L, 2L), is("v12"));
        assertThat(table.remove(c), is(c.toString()));
        assertNull(table.remove(c));
        assertThat(table.size(), is(0L));
        table.integrityCheck();
    }

    @Test
    public void null_values_are_refused() {
        final UuidObjectHashtable<String> table = new UuidObjectHashtable<>();
        assertThrows(NullPointerException.class, () -> table.put(1L, 2L, null));
        assertThrows(NullPointerException.class, () -> table.put(NIL, null));
        assertThrows(NullPointerException.class, () -> table.putIfAbsent(1L, 2L, null));
        assertThrows(NullPointerException.class, () -> table.get(null));
        assertThat(table.size(), is(0L));
    }

    /** the nil UUID is kept in a field; keys with one zero half are ordinary keys */
    @Test
    public void nil_and_half_zero_keys() {
        final UuidObjectHashtable<String> table = new UuidObjectHashtable<>();
        final long[][] keys = {{0, 0}, {0, 1}, {1, 0}, {0, -1}, {-1, 0}, {Long.MIN_VALUE, 0}, {-1, -1}};
        for (long[] key : keys) {
            table.put(key[0], key[1], key[0] + ":" + key[1]);
        }
        assertThat(table.size(), is((long) keys.length));
        for (long[] key : keys) {
            assertThat(table.get(key[0], key[1]), is(key[0] + ":" + key[1]));
            assertTrue(table.containsKey(new UUID(key[0], key[1])));
        }
        assertThat(table.get(NIL), is("0:0"));
        final List<UUID> visited = new ArrayList<>();
        table.forEach((h, l, v) -> {
            assertThat(v, is(h + ":" + l));
            visited.add(new UUID(h, l));
        });
        assertThat(visited.get(0), is(NIL));
        assertThat(table.keysStream().collect(Collectors.toList()), is(visited));
        table.integrityCheck();
        assertThat(table.remove(NIL), is("0:0"));
        assertFalse(table.containsKey(0L, 0L));
        assertThat(table.size(), is((long) keys.length - 1));
        table.integrityCheck();
    }

    /**
     * @return count random keys whose home is the given slot of a table of the capacity
     */
    private static List<UUID> keysHomedAt(int slot, int capacity, int count, Random random) {
        final List<UUID> keys = new ArrayList<>();
        while (keys.size() < count) {
            final long hi = random.nextLong();
            final long lo = random.nextLong();
            if ((HashingUtils.hash(hi, lo) & (capacity - 1)) == slot) {
                keys.add(new UUID(hi, lo));
            }
        }
        return keys;
    }

    /** a cluster across the end of the array: the removal of each key shifts back the right ones, wrapping */
    @Test
    public void removal_shifts_back_a_wrapping_cluster() {
        final Random random = new Random(5);
        final List<UUID> all = new ArrayList<>(keysHomedAt(SMALL_CAPACITY - 1, SMALL_CAPACITY, 4, random));
        all.addAll(keysHomedAt(0, SMALL_CAPACITY, 3, random));
        for (UUID gone : all) {
            final UuidObjectHashtable<UUID> table = at065(SMALL_SIZE).build();
            for (UUID key : all) {
                table.put(key, key);
            }
            assertThat(table.capacity(), is(SMALL_CAPACITY));
            assertSame(gone, table.remove(gone));
            table.integrityCheck();
            for (UUID key : all) {
                if (key == gone) {
                    assertNull(table.get(key));
                } else {
                    assertSame(key, table.get(key));
                }
            }
        }
    }

    /** UUIDs of one prefix and a counter (sequential ids, UUIDv7 of one millisecond) spread like random ones */
    @Test
    public void grows_and_keeps_the_entries() {
        final UuidObjectHashtable<Long> table = new UuidObjectHashtable<>(1);
        final long prefix = 0x0190_0000_0000_7000L;
        for (long i = 0; i < 100_000; i++) {
            table.put(prefix, i, i);
            table.put(i, 0L, -i);
        }
        assertThat(table.size(), is(200_000L));
        for (long i = 0; i < 100_000; i++) {
            assertThat(table.get(prefix, i), is(i));
            assertThat(table.get(i, 0L), is(-i));
        }
        table.integrityCheck();
    }

    /** 16 slots, maxLoad 0.75: 12 keys, the 13th refused and the table unchanged */
    @Test
    public void fixed_capacity_refuses_new_keys_at_max_load() {
        final UuidObjectHashtable<Long> table = at065(SMALL_SIZE)
                .fixedCapacity()
                .maxLoad(0.75)
                .build();
        for (long key = 1; key <= 12; key++) {
            table.put(key, key, key);
        }
        final String before = table.toString();
        assertThrows(IllegalStateException.class, () -> table.put(100L, 1L, 1L));
        assertThat(table.toString(), is(before));
        assertThat(table.size(), is(12L));
        assertThat(table.capacity(), is(SMALL_CAPACITY));
        table.integrityCheck();
        assertThat(table.remove(4L, 4L), is(4L));
        assertNull(table.put(100L, 1L, 1L));
        // the nil key lives in a field: not limited by the slots
        assertNull(table.put(NIL, 0L));
        table.integrityCheck();
    }

    @Test
    public void ensure_capacity_and_clear() {
        final UuidObjectHashtable<Long> table = at065(SMALL_SIZE).build();
        for (long key = 0; key < SMALL_SIZE; key++) {
            table.put(key, key, key);
        }
        table.ensureCapacity(1000);
        assertThat(table.capacity(), is(2048));
        for (long key = 0; key < SMALL_SIZE; key++) {
            assertThat(table.get(key, key), is(key));
        }
        table.integrityCheck();
        table.clear();
        assertThat(table.size(), is(0L));
        assertThat(table.capacity(), is(2048));
        assertNull(table.get(NIL));
        table.integrityCheck();
    }

    /** small table load factor below the small table capacity, the load factor from there on */
    @Test
    public void load_factors_set_where_the_table_grows() {
        final UuidObjectHashtable.Builder builder = UuidObjectHashtable.builder()
                .loadFactor(0.75).smallTableLoadFactor(0.5).smallTableCapacity(64);
        final UuidObjectHashtable<Long> table = builder.expectedSize(0).build();
        final List<String> growth = new ArrayList<>();
        int capacity = table.capacity();
        // not the nil key: kept in a field, it grows nothing
        for (long key = 1; key <= 150; key++) {
            table.put(key, -key, key);
            if (table.capacity() != capacity) {
                capacity = table.capacity();
                growth.add(table.size() + "->" + capacity);
            }
        }
        assertThat(growth, is(List.of("1->4", "2->8", "4->16", "8->32", "16->64", "48->128", "96->256")));
        table.integrityCheck();
        assertThat(builder.expectedSize(47).build().capacity(), is(64));
        assertThat(builder.expectedSize(16).build().capacity(), is(64));
        assertThat(builder.expectedSize(15).build().capacity(), is(32));
        final UuidObjectHashtable<Long> grown = builder.expectedSize(0).build();
        grown.ensureCapacity(48);
        assertThat(grown.capacity(), is(128));
        // the default small table load factor is at most the load factor
        assertThat(UuidObjectHashtable.builder().loadFactor(0.25).expectedSize(60).build().capacity(), is(256));
        assertThrows(IllegalArgumentException.class,
                () -> UuidObjectHashtable.builder().loadFactor(0.5).smallTableLoadFactor(0.6).build());
        assertThrows(IllegalArgumentException.class,
                () -> UuidObjectHashtable.builder().loadFactor(0.8).maxLoad(0.75).build());
        assertThrows(IllegalArgumentException.class, () -> UuidObjectHashtable.builder().maxLoad(1.0).build());
        assertThrows(IllegalArgumentException.class, () -> UuidObjectHashtable.builder().loadFactor(0.91));
        assertThrows(IllegalArgumentException.class, () -> UuidObjectHashtable.builder().smallTableCapacity(48));
    }

    @Test
    public void matches_a_reference_map() {
        final Random random = new Random(13);
        final UuidObjectHashtable<Long> table = new UuidObjectHashtable<>(1);
        final Map<UUID, Long> ref = new HashMap<>();
        for (int i = 0; i < 300_000; i++) {
            // few keys, some with a zero half, the nil one among them
            final long hi = random.nextInt(8) == 0 ? 0L : random.nextInt(60);
            final long lo = random.nextInt(8) == 0 ? 0L : random.nextInt(60);
            final UUID key = new UUID(hi, lo);
            final Long value = random.nextLong();
            final int op = random.nextInt(7);
            final Object actual = switch (op) {
                case 0 -> table.put(hi, lo, value);
                case 1 -> table.put(key, value);
                case 2 -> table.remove(hi, lo);
                case 3 -> table.putIfAbsent(key, value);
                case 4 -> table.containsKey(hi, lo);
                case 5 -> table.get(key);
                default -> table.get(hi, lo);
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
        final Map<UUID, Long> visited = new HashMap<>();
        table.forEach((h, l, v) -> visited.put(new UUID(h, l), v));
        assertThat(visited, is(ref));
        final List<UUID> keys = table.keysStream().collect(Collectors.toList());
        final List<Long> values = table.valuesStream().collect(Collectors.toList());
        for (int i = 0; i < keys.size(); i++) {
            assertThat(values.get(i), is(ref.get(keys.get(i))));
        }
        table.integrityCheck();
    }
}
