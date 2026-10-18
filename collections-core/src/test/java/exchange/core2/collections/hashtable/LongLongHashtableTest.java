package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LongLongHashtableTest extends LongLongHashtableAbstractTest{

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return new LongLongHashtable(expectedSize);
    }

    @Override
    protected ILongLongHashtable create(int expectedSize, long missingValue) {
        return LongLongHashtable.builder().expectedSize(expectedSize).missingValue(missingValue).build();
    }

    @Override
    protected ILongLongHashtable createLimited(int expectedSize, int maxCapacity) {
        return LongLongHashtable.builder().expectedSize(expectedSize).maxCapacity(maxCapacity).build();
    }

    @Override
    protected ILongLongHashtable createFixed(int expectedSize) {
        return LongLongHashtable.builder().expectedSize(expectedSize).fixedCapacity().build();
    }

    @Test
    public void builder_should_validate_the_max_capacity() {
        for (int invalid : new int[]{0, 1, 3, 100, -64, HashingUtils.MAX_CAPACITY * 2}) {
            assertThrows(String.valueOf(invalid), IllegalArgumentException.class,
                    () -> LongLongHashtable.builder().maxCapacity(invalid));
        }
        // smaller than the expected size needs
        assertThrows(IllegalArgumentException.class,
                () -> LongLongHashtable.builder().expectedSize(100).maxCapacity(128).build());
        assertThat(LongLongHashtable.builder().expectedSize(100).maxCapacity(256).build().capacity(), is(256));
    }

    @Test
    public void builder_should_validate_the_max_load() {
        for (double invalid : new double[]{0.5, 0.64, 1.0, 1.5, Double.NaN}) {
            assertThrows(String.valueOf(invalid), IllegalArgumentException.class,
                    () -> LongLongHashtable.builder().maxLoad(invalid));
        }
    }

    /** 16 slots: maxLoad 0.75 takes 12 keys; 0.999 would take 15.98 - two slots stay free, 14. */
    @Test
    public void should_take_new_keys_up_to_the_max_load() {
        checkMaxLoad(LongLongHashtable.builder().expectedSize(9).fixedCapacity().maxLoad(0.75).build(), 12);
        checkMaxLoad(LongLongHashtable.builder().expectedSize(9).fixedCapacity().maxLoad(0.999).build(), 14);
        // the smallest table: 2 slots, no entry in the array - only the one kept outside
        checkMaxLoad(LongLongHashtable.builder().expectedSize(0).fixedCapacity().build(), 0, 2);
    }

    static void checkMaxLoad(ILongLongHashtable table, int limit) {
        checkMaxLoad(table, limit, 16);
    }

    static void checkMaxLoad(ILongLongHashtable table, int limit, int capacity) {
        assertThat(table.capacity(), is(capacity));
        for (long key = 1; key <= limit; key++) {
            table.put(key, key);
        }
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> table.put(-1L, 1L));
        assertTrue(e.getMessage(), e.getMessage().contains("table is full"));
        assertThat(table.size(), is((long) limit));
        assertThat(table.capacity(), is(capacity));
        checkIntegrity(table);
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
        final int at = HashingUtils.findFreeOffset(5L ^ HashingUtils.SEED, b, (b.length >> 1) - 1);
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

    @Test
    public void getOrInsert_calls_the_function_only_for_a_missing_key() {
        final LongLongHashtable table = new LongLongHashtable();
        final int[] calls = {0};
        assertEquals(70L, table.getOrInsert(7L, k -> {
            calls[0]++;
            return k * 10;
        }));
        assertEquals(1, calls[0]);
        assertEquals(70L, table.getOrInsert(7L, k -> {
            calls[0]++;
            return -1;
        }));
        assertEquals(1, calls[0]);
        assertEquals(70L, table.get(7L));
        assertEquals(1, table.size());
        table.integrityCheck();
    }

    /** the key stored as 0 - kept in fields, not in the array */
    @Test
    public void getOrInsert_takes_the_key_equal_to_the_seed() {
        final LongLongHashtable table = new LongLongHashtable();
        final long key = table.seed();
        assertEquals(5L, table.getOrInsert(key, k -> 5L));
        assertEquals(5L, table.getOrInsert(key, k -> 6L));
        assertEquals(5L, table.get(key));
        assertEquals(1, table.size());
        table.integrityCheck();
    }

    @Test
    public void getOrInsert_grows_the_table_as_put_does() {
        final LongLongHashtable table = new LongLongHashtable(0);
        final LongLongHashtable byPut = new LongLongHashtable(0);
        final Random rand = new Random(3);
        for (int i = 0; i < 50_000; i++) {
            final long key = rand.nextLong();
            assertEquals(key + 1, table.getOrInsert(key, k -> k + 1));
            byPut.put(key, key + 1);
            assertEquals(byPut.capacity(), table.capacity());
        }
        assertEquals(50_000, table.size());
        table.forEach((key, value) -> assertEquals(key + 1, value));
        table.integrityCheck();
    }

    /** 16 slots, maxLoad 0.95: 14 keys (two slots stay free); then refused before the function runs */
    @Test
    public void getOrInsert_refuses_a_key_of_a_full_table_before_the_function_runs() {
        final LongLongHashtable table = LongLongHashtable.builder().expectedSize(9).fixedCapacity().build();
        for (int i = 1; i <= 14; i++) {
            table.getOrInsert(i, k -> k);
        }
        assertEquals(14, table.size());
        final int[] calls = {0};
        assertThrows(IllegalStateException.class, () -> table.getOrInsert(15L, k -> {
            calls[0]++;
            return k;
        }));
        assertEquals(0, calls[0]);
        assertEquals(14, table.size());
        assertFalse(table.containsKey(15L));
        // present keys still answer
        assertEquals(3L, table.getOrInsert(3L, k -> -1));
        table.integrityCheck();
    }

    @Test
    public void getOrInsert_changes_nothing_if_the_function_throws() {
        final LongLongHashtable table = new LongLongHashtable(0);
        for (int i = 1; i <= 9; i++) {
            table.put(i, i);
        }
        // the 10th key would resize: the room is made, the key is not inserted
        assertThrows(IllegalStateException.class, () -> table.getOrInsert(10L, k -> {
            throw new IllegalStateException("store is full");
        }));
        assertThrows(IllegalStateException.class, () -> table.getOrInsert(table.seed(), k -> {
            throw new IllegalStateException("store is full");
        }));
        assertEquals(9, table.size());
        assertFalse(table.containsKey(10L));
        assertFalse(table.containsKey(table.seed()));
        table.integrityCheck();
    }

}
