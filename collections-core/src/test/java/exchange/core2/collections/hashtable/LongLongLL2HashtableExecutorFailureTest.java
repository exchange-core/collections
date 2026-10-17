package exchange.core2.collections.hashtable;

import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Executors that can not run the migration tasks as intended: the table does the work itself, no operation fails.
 * The tables start at 2048 slots and resize asynchronously from the first resize on; each test goes through
 * several resizes, checked against a reference map.
 */
public class LongLongLL2HashtableExecutorFailureTest {

    /** The allocation rejected: a synchronous resize - the put that crossed the threshold keeps its key. */
    @Test(timeout = 60_000)
    public void rejected_allocation_resizes_synchronously() {
        final AtomicInteger rejected = new AtomicInteger();
        final Executor rejecting = task -> {
            rejected.incrementAndGet();
            throw new RejectedExecutionException("test");
        };
        try (LongLongLL2Hashtable table = table(rejecting, LongLongLL2Hashtable.DEFAULT_HANG_TIMEOUT)) {
            checkAgainstReference(table, () -> {
            });
            assertTrue(rejected + " tasks", rejected.get() > 2);
        }
    }

    /** The allocation runs, the copying is rejected: the table copies itself, a segment where an operation waits. */
    @Test(timeout = 60_000)
    public void rejected_copying_is_done_by_the_table() {
        final AtomicInteger tasks = new AtomicInteger();
        final AtomicInteger rejected = new AtomicInteger();
        final Executor allocationOnly = task -> {
            if (tasks.getAndIncrement() % 2 == 1) {
                rejected.incrementAndGet();
                throw new RejectedExecutionException("test");
            }
            daemon(task);
        };
        try (LongLongLL2Hashtable table = table(allocationOnly, LongLongLL2Hashtable.DEFAULT_HANG_TIMEOUT)) {
            checkAgainstReference(table, () -> {
            });
            assertTrue(rejected + " tasks", rejected.get() > 2);
        }
    }

    /**
     * Runnable::run (or CallerRunsPolicy): the copying task, run on the table's thread, would wait there for an
     * authorization only that thread can give - it hung forever. Now it leaves the copying to the table.
     */
    @Test(timeout = 60_000)
    public void inline_executor_does_not_hang() {
        try (LongLongLL2Hashtable table = table(Runnable::run, LongLongLL2Hashtable.DEFAULT_HANG_TIMEOUT)) {
            checkAgainstReference(table, () -> {
            });
        }
    }

    /**
     * The application's own event loop as the executor: the tasks run later, between the operations, on the table's
     * thread. No wait for the hang timeout (30s, beyond the test timeout).
     */
    @Test(timeout = 20_000)
    public void executor_running_the_tasks_later_on_the_table_thread() {
        final Queue<Runnable> eventLoop = new ArrayDeque<>();
        final AtomicInteger tasks = new AtomicInteger();
        final Executor executor = task -> {
            tasks.incrementAndGet();
            eventLoop.add(task);
        };
        try (LongLongLL2Hashtable table = table(executor, Duration.ofSeconds(30))) {
            checkAgainstReference(table, () -> {
                Runnable task;
                while ((task = eventLoop.poll()) != null) {
                    task.run();
                }
            });
            assertTrue(tasks + " tasks", tasks.get() > 4);
        }
    }

    /**
     * An executor that never runs the tasks (a queue behind busy threads): the table allocates the array at the block
     * threshold, and copies, itself - no wait for the hang timeout (30s, beyond the test timeout).
     */
    @Test(timeout = 20_000)
    public void tasks_never_run_are_done_by_the_table() {
        try (LongLongLL2Hashtable table = table(task -> {
        }, Duration.ofSeconds(30))) {
            checkAgainstReference(table, () -> {
            });
        }
    }

    /** After close() the table resizes synchronously: no more tasks for the executor. */
    @Test(timeout = 60_000)
    public void closed_table_resizes_synchronously() {
        final AtomicInteger tasks = new AtomicInteger();
        final Executor counting = task -> {
            tasks.incrementAndGet();
            daemon(task);
        };
        final LongLongLL2Hashtable table = table(counting, LongLongLL2Hashtable.DEFAULT_HANG_TIMEOUT);
        for (long key = 1; key <= 1500; key++) {
            table.put(key, key); // the resize at 1331 entries
        }
        assertTrue(tasks + " tasks", tasks.get() > 0);
        table.close();
        final int before = tasks.get();
        for (long key = 1501; key <= 20_000; key++) {
            table.put(key, key);
        }
        assertThat(tasks.get(), is(before));
        assertThat(table.capacity(), is(32768));
        for (long key = 1; key <= 20_000; key++) {
            assertThat(table.get(key), is(key));
        }
        table.integrityCheck();
    }

    @Test
    public void builder_should_validate_the_executor_and_sync_resize_bound() {
        assertThrows(NullPointerException.class, () -> LongLongLL2Hashtable.builder().executor(null));
        assertThrows(IllegalArgumentException.class, () -> LongLongLL2Hashtable.builder().syncResizeBelow(-1));
    }

    private static LongLongLL2Hashtable table(Executor executor, Duration hangTimeout) {
        return LongLongLL2Hashtable.builder().expectedSize(1000).executor(executor).syncResizeBelow(0)
                .hangTimeout(hangTimeout).build();
    }

    /**
     * Puts, removals and lookups of a pool of keys - the table grows from 2048 to 32768 slots - then all keys,
     * the size and the integrity, after the migration in flight is finished.
     *
     * @param betweenOperations runs after each operation
     */
    private static void checkAgainstReference(LongLongLL2Hashtable table, Runnable betweenOperations) {
        final Random rand = new Random(3L);
        final Long2LongHashMap ref = new Long2LongHashMap(0L);
        final long[] keys = new long[30_000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rand.nextLong() | 1L;
        }
        for (int op = 0; op < 200_000; op++) {
            final long key = keys[rand.nextInt(Math.min(keys.length, 1000 + op / 4))];
            final int kind = rand.nextInt(10);
            if (kind < 6) {
                final long value = rand.nextLong();
                assertEquals("put", ref.put(key, value), table.put(key, value));
            } else if (kind < 8) {
                assertEquals("remove", ref.remove(key), table.remove(key));
            } else {
                assertEquals("get", ref.get(key), table.get(key));
            }
            betweenOperations.run();
        }
        assertTrue("capacity " + table.capacity(), table.capacity() >= 16384);
        table.finishMigration();
        assertEquals(ref.size(), table.size());
        for (long key : keys) {
            assertEquals(ref.get(key), table.get(key));
        }
        table.integrityCheck();
    }

    private static void daemon(Runnable task) {
        final Thread thread = new Thread(task, "test-migrator");
        thread.setDaemon(true);
        thread.start();
    }
}
