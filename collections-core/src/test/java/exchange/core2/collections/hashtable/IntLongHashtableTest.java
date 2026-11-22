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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The int API over {@link LongLongHashtable}: the contract itself is tested there - here the widening, the int
 * iteration, the builder passing its parameters on.
 */
public class IntLongHashtableTest {

    @Test
    public void basic_operations() {
        final IntLongHashtable table = new IntLongHashtable();
        assertThat(table.put(42, 1234L), is(0L));
        assertThat(table.get(42), is(1234L));
        assertThat(table.get(43), is(0L));
        assertTrue(table.containsKey(42));
        assertFalse(table.containsKey(43));
        assertThat(table.put(42, 5L), is(1234L));
        assertThat(table.putIfAbsent(42, 6L), is(5L));
        assertThat(table.addAndGet(42, 10L), is(15L));
        assertThat(table.addAndGet(7, 3L), is(3L));
        assertThat(table.computeIfAbsent(8, k -> k * 10L), is(80L));
        assertThat(table.computeIfAbsent(8, k -> -1L), is(80L));
        assertThat(table.getOrDefault(9, -2L), is(-2L));
        assertFalse(table.remove(42, 14L));
        assertTrue(table.remove(42, 15L));
        assertThat(table.remove(7), is(3L));
        assertThat(table.size(), is(1L));
        assertThat(table.toString(), is("{8=80}"));
        table.clear();
        assertThat(table.size(), is(0L));
    }

    @Test
    public void any_int_key() {
        final IntLongHashtable table = new IntLongHashtable();
        // the seed as an int: the key kept in fields if the seed fits an int (0 by default)
        final int[] keys = {0, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, (int) table.seed()};
        final Map<Integer, Long> ref = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            table.put(keys[i], i + 100L);
            ref.put(keys[i], i + 100L);
        }
        for (int key : keys) {
            assertThat(table.get(key), is(ref.get(key)));
        }
        final Map<Integer, Long> visited = new HashMap<>();
        table.forEach((k, v) -> assertEquals(null, visited.put(k, v)));
        assertThat(visited, is(ref));
        assertThat(table.size(), is((long) ref.size()));
        table.integrityCheck();
        final List<Integer> streamed = table.keysStream().boxed().collect(Collectors.toList());
        final List<Integer> forEachOrder = new ArrayList<>();
        table.forEach((k, v) -> forEachOrder.add(k));
        assertThat(streamed, is(forEachOrder));
        assertThat(table.valuesStream().sum(), is(visited.values().stream().mapToLong(Long::longValue).sum()));
    }

    @Test
    public void builder_parameters_reach_the_table() {
        final IntLongHashtable table = IntLongHashtable.builder()
                .expectedSize(9)
                .missingValue(-1L)
                .fixedCapacity()
                .maxLoad(0.75)
                .build();
        assertThat(table.missingValue(), is(-1L));
        assertThat(table.get(5), is(-1L));
        assertThat(table.capacity(), is(16));
        for (int k = 0; k < 12; k++) {
            table.put(k, k);
        }
        assertThrows(IllegalStateException.class, () -> table.put(100, 1L));
        assertThat(table.capacity(), is(16));
        table.integrityCheck();

        final IntLongHashtable limited = IntLongHashtable.builder().maxCapacity(32).build();
        for (int k = 0; k < 30; k++) {
            limited.put(k, k);
        }
        assertThat(limited.capacity(), is(32));
        assertThrows(IllegalStateException.class, () -> limited.put(100, 1L));
    }

    @Test
    public void matches_a_reference_map() {
        final IntLongHashtable table = new IntLongHashtable(4);
        final Map<Integer, Long> ref = new HashMap<>();
        final Random random = new Random(7);
        for (int i = 0; i < 200_000; i++) {
            final int key = random.nextInt(5_000) - 2_500;
            final long value = random.nextLong();
            final int op = random.nextInt(4);
            final long actual = switch (op) {
                case 0, 1 -> table.put(key, value);
                case 2 -> table.remove(key);
                default -> table.get(key);
            };
            final Long expected = switch (op) {
                case 0, 1 -> ref.put(key, value);
                case 2 -> ref.remove(key);
                default -> ref.get(key);
            };
            assertEquals("op " + i, expected == null ? 0L : expected, actual);
        }
        assertThat(table.size(), is((long) ref.size()));
        final Map<Integer, Long> visited = new HashMap<>();
        table.forEach(visited::put);
        assertThat(visited, is(ref));
        table.integrityCheck();
    }
}
