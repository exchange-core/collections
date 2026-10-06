package exchange.core2.collections.hashtable;

import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;

public class LongLongLL2HashtableMigratorExecutorTest {

    /** ~16 async migrations through newMigratorExecutor(), checked against a reference map. */
    @Test
    public void should_keep_all_entries_with_migrator_executor() {
        checkAgainstReference(new LongLongLL2Hashtable(16, LongLongLL2Hashtable.newMigratorExecutor()));
    }

    /** Async resize from tiny tables on (sync only below 100 entries) - exercises the proportional segments. */
    @Test
    public void should_keep_all_entries_with_async_resize_of_small_tables() {
        checkAgainstReference(new LongLongLL2Hashtable(16, LongLongLL2Hashtable.newMigratorExecutor(), 100));
    }

    private static void checkAgainstReference(LongLongLL2Hashtable hashtable) {
        final Random rand = new Random(1L);
        final Long2LongHashMap ref = new Long2LongHashMap(0L);
        final long[] keys = new long[1_000_000];
        try (LongLongLL2Hashtable table = hashtable) {
            for (int i = 0; i < keys.length; i++) {
                final long key = (rand.nextLong() & Long.MAX_VALUE) | 1L; // non-zero
                final long value = rand.nextLong();
                keys[i] = key;
                assertEquals("put " + key, ref.put(key, value), table.put(key, value));
            }
            for (long key : keys) {
                assertEquals("get " + key, ref.get(key), table.get(key));
            }
        }
    }
}
