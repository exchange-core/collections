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
        return LongLongLL2Hashtable.builder().expectedSize(expectedSize).missingValue(missingValue).build();
    }

    @Override
    protected ILongLongHashtable createLimited(int expectedSize, int maxCapacity) {
        return LongLongLL2Hashtable.builder().expectedSize(expectedSize).executor(THREAD_PER_TASK)
                .maxCapacity(maxCapacity).build();
    }

    /**
     * Builder defaults are those of the constructors; the seed is the one of the JVM.
     */
    @Test
    public void builder_should_set_the_parameters() {

        try (LongLongLL2Hashtable plain = new LongLongLL2Hashtable();
             LongLongLL2Hashtable built = LongLongLL2Hashtable.builder().expectedSize(1000).missingValue(-7L)
                     .executor(THREAD_PER_TASK).syncResizeBelow(0).build()) {

            assertThat(plain.missingValue(), is(0L));
            assertThat(plain.capacity(), is(64));
            assertThat(built.missingValue(), is(-7L));
            assertThat(built.capacity(), is(2048));
            assertThat(built.get(1L), is(-7L));
            assertThat(built.seed(), is(HashingUtils.SEED));
        }
    }

    /**
     * The same limit when the table got there by asynchronous migrations: the last one ends inside a put, which then
     * inserts into the new array and checks the threshold on its own path.
     */
    @Test
    public void should_refuse_new_keys_at_full_load_after_async_migrations() {

        final int limit = (int) (64 * HashingUtils.FULL_LOAD);
        try (LongLongLL2Hashtable table = LongLongLL2Hashtable.builder().expectedSize(10)
                .executor(THREAD_PER_TASK).syncResizeBelow(0).maxCapacity(64).build()) {
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
