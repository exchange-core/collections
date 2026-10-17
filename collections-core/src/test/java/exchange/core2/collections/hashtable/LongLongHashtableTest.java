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

}
