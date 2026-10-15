package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class LongLongHashtableTest extends LongLongHashtableAbstractTest{

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return new LongLongHashtable(expectedSize);
    }

    @Override
    protected ILongLongHashtable create(int expectedSize, long missingValue) {
        return new LongLongHashtable(expectedSize, missingValue);
    }

    @Override
    protected ILongLongHashtable createLimited(int expectedSize, int maxCapacity) {
        return new LongLongHashtable(expectedSize, 0L, maxCapacity);
    }

    /**
     * integrityCheck is the oracle of other tests here - it must catch what it claims to.
     */
    @Test
    public void integrityCheck_should_detect_broken_invariants() throws Exception {

        final Field dataField = LongLongHashtable.class.getDeclaredField("data");
        final Field sizeField = LongLongHashtable.class.getDeclaredField("size");
        dataField.setAccessible(true);
        sizeField.setAccessible(true);

        final LongLongHashtable table = new LongLongHashtable();
        for (long key = 1; key <= 15; key++) {
            table.put(key, key);
        }
        table.integrityCheck();

        // a key moved one slot away from where its probe stops: unreachable
        final long[] data = (long[]) dataField.get(table);
        final LongLongHashtable broken = new LongLongHashtable();
        broken.put(5L, 50L);
        final long[] b = (long[]) dataField.get(broken);
        final int at = HashingUtils.findFreeOffset(5L, b, (b.length >> 1) - 1);
        final int next = (at + 2) & (b.length - 1);
        b[next] = b[at];
        b[next + 1] = b[at + 1];
        b[at] = 0;
        b[at + 1] = 0;
        assertThrows(IllegalStateException.class, broken::integrityCheck);

        // a wrong size
        sizeField.setLong(table, 14L);
        assertThrows(IllegalStateException.class, table::integrityCheck);
        sizeField.setLong(table, 15L);
        table.integrityCheck();

        // a value left in a free slot - a missing key would read it
        int free = 0;
        while (data[free] != 0) {
            free += 2;
        }
        data[free + 1] = 7L;
        assertThrows(IllegalStateException.class, table::integrityCheck);
    }


    /**
     * forEach, keysStream and valuesStream (documented in the README) used to throw UnsupportedOperationException.
     */
    @Test
    public void should_iterate_all_entries() {

        final LongLongHashtable table = new LongLongHashtable();
        final Map<Long, Long> ref = fillWithRandomEntries(table);

        final Map<Long, Long> visited = new HashMap<>();
        table.forEach((k, v) -> assertEquals("duplicate key " + k, null, visited.put(k, v)));
        assertEquals(ref, visited);

        final long[] keys = table.keysStream().toArray();
        final long[] values = table.valuesStream().toArray();
        assertThat(keys.length, is(ref.size()));
        assertThat(values.length, is(ref.size()));
        for (int i = 0; i < keys.length; i++) {
            // both streams walk the table in the same order
            assertThat(values[i], is(ref.get(keys[i])));
        }
        assertEquals(ref.keySet(), table.keysStream().boxed().collect(Collectors.toSet()));
    }

    @Test
    public void should_iterate_empty_table() {

        final LongLongHashtable table = new LongLongHashtable();
        table.forEach((k, v) -> {
            throw new AssertionError("unexpected entry " + k);
        });
        assertThat(table.keysStream().count(), is(0L));
        assertThat(table.valuesStream().count(), is(0L));
    }

    @Test
    public void should_clear() {

        final LongLongHashtable table = new LongLongHashtable();
        final Map<Long, Long> ref = fillWithRandomEntries(table);

        table.clear();

        assertThat(table.size(), is(0L));
        assertThat(table.keysStream().count(), is(0L));
        for (long key : ref.keySet()) {
            assertFalse(table.containsKey(key));
            assertThat(table.get(key), is(0L));
        }

        // still usable, at the capacity it had grown to
        final Map<Long, Long> ref2 = fillWithRandomEntries(table);
        assertThat(table.size(), is((long) ref2.size()));
        ref2.forEach((k, v) -> assertThat(table.get(k), is(v)));
    }

    /** free slots hold the missing value, also after clear */
    @Test
    public void should_clear_with_missing_value() {

        final LongLongHashtable table = new LongLongHashtable(16, -1L);
        final Map<Long, Long> ref = fillWithRandomEntries(table);

        table.clear();

        assertThat(table.size(), is(0L));
        for (long key : ref.keySet()) {
            assertThat(table.get(key), is(-1L));
        }
        table.integrityCheck();
        final Map<Long, Long> ref2 = fillWithRandomEntries(table);
        ref2.forEach((k, v) -> assertThat(table.get(k), is(v)));
        table.integrityCheck();
    }

    /**
     * Grows the table through several resizes and removes a part of the keys, so that iteration sees both
     * collision chains and the gaps left by removal.
     */
    private static Map<Long, Long> fillWithRandomEntries(LongLongHashtable table) {
        final Random rand = new Random(7L);
        final Map<Long, Long> ref = new HashMap<>();
        table.put(0L, 77L); // kept outside the array
        ref.put(0L, 77L);
        for (int i = 0; i < 5000; i++) {
            final long key = rand.nextLong() | 1L;
            final long value = rand.nextInt(4) == 0 ? 0L : rand.nextLong(); // value 0 is a regular value
            table.put(key, value);
            ref.put(key, value);
            if (rand.nextInt(3) == 0) {
                table.remove(key);
                ref.remove(key);
            }
        }
        return ref;
    }

}
