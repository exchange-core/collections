package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.util.concurrent.Executor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

/**
 * The contract tests against LongLongLL2Hashtable. With the default executor and sync-resize bound, the tables
 * here resize synchronously up to 8200 entries and asynchronously past that (should_upsize goes to 50000).
 */
public class LongLongLL2HashtableTest extends LongLongHashtableAbstractTest {

    private static final Executor THREAD_PER_TASK = task -> {
        final Thread thread = new Thread(task, "test-migrator");
        thread.setDaemon(true);
        thread.start();
    };

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return new LongLongLL2Hashtable(expectedSize);
    }

    @Override
    protected ILongLongHashtable create(int expectedSize, long missingValue) {
        return new LongLongLL2Hashtable(expectedSize, missingValue);
    }

    @Override
    protected ILongLongHashtable createLimited(int expectedSize, int maxCapacity) {
        return new LongLongLL2Hashtable(expectedSize, THREAD_PER_TASK, LongLongLL2Hashtable.DEFAULT_SYNC_RESIZE_BELOW,
                0L, maxCapacity);
    }

    /**
     * The same limit when the table got there by asynchronous migrations: the last one ends inside a put, which then
     * inserts into the new array and checks the threshold on its own path.
     */
    @Test
    public void should_refuse_new_keys_at_full_load_after_async_migrations() {

        final int limit = (int) (64 * HashingUtils.FULL_LOAD);
        try (LongLongLL2Hashtable table = new LongLongLL2Hashtable(10, THREAD_PER_TASK, 0, 0L, 64)) {
            for (long key = 1; key <= limit; key++) {
                table.put(key, key);
            }
            assertThat(table.size(), is((long) limit));
            assertThrows(IllegalStateException.class, () -> table.put(limit + 1L, 1L));
            assertThat(table.size(), is((long) limit));
            for (long key = 1; key <= limit; key++) {
                assertThat(table.get(key), is(key));
            }
            checkIntegrity(table);
        }
    }
}
