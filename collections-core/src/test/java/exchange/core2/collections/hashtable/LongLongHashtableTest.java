package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LongLongHashtableTest extends LongLongHashtableAbstractTest{

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return new LongLongHashtable(expectedSize);
    }


    @Test
    public void should_migrate_mas() {
        int initialCapacity = 16;

        hashtable = new LongLongHashtable();

        hashtable.put(15417, 1);
        hashtable.put(28723, 2);
        hashtable.put(43344, 3);
        hashtable.put(88455, 4);
        hashtable.put(23543, 5);
        hashtable.put(93234, 6);
        hashtable.put(79555, 7);
        hashtable.put(53733, 8);
        hashtable.put(27863, 9);

        LongLongHashtable dst = new LongLongHashtable();

        ((LongLongHashtable)hashtable).extractMatching(dst, 0x8000_0000, 0x8000_0000);


        // TODO finalize test
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

    /**
     * Grows the table through several resizes and removes a part of the keys, so that iteration sees both
     * collision chains and the gaps left by removal.
     */
    private static Map<Long, Long> fillWithRandomEntries(LongLongHashtable table) {
        final Random rand = new Random(7L);
        final Map<Long, Long> ref = new HashMap<>();
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
