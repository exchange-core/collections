package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LongSnapshotRecordTableTest {
    @Test
    public void successiveVersionsKeepTheirOwnKeysAndValuesAcrossGrowthAndReuse() {
        final var t = LongSnapshotRecordTable.builder().fields(2).build();
        final long[] row = {5, 9};
        t.put(Long.MIN_VALUE, row);
        row[0] = 12;
        final var one = t.snapshot(100);
        assertTrue(t.remove(Long.MIN_VALUE));
        final var empty = t.snapshot(101);
        t.set(Long.MIN_VALUE, 1, Long.MAX_VALUE);
        assertEquals(0, t.get(Long.MIN_VALUE, 0));
        final var two = t.snapshot(102);
        for (int i = 0; i < 2048; i++) {
            t.set(i, 0, i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> t.set(-1, 2, 0));
        assertThrows(IllegalArgumentException.class, () -> t.put(-1, new long[0]));
        assertFalse(t.containsKey(-1));
        assertEquals(2049, t.size());
        assertFalse(empty.containsKey(Long.MIN_VALUE));
        assertThrows(NoSuchElementException.class, () -> empty.get(Long.MIN_VALUE, 1));
        assertEquals(5, one.get(Long.MIN_VALUE, 0));
        assertEquals(9, one.get(Long.MIN_VALUE, 1));
        assertEquals(100, one.sequence());
        assertEquals(Long.MAX_VALUE, two.get(Long.MIN_VALUE, 1));
        assertEquals(1, two.size());
    }

    @Test
    public void historicalScansMatchIndependentMaps() {
        final var t = LongSnapshotRecordTable.builder().fields(1).expectedSize(700).shardCapacity(16).build();
        final var model = new HashMap<Long, Long>();
        final List<Map<Long, Long>> history = new ArrayList<>();
        final List<LongSnapshotRecordTable.Snapshot> views = new ArrayList<>();
        final var random = new Random(235);
        for (int i = 0; i < 8_000; i++) {
            final long key = random.nextInt(1000);
            if (random.nextBoolean()) {
                final long value = random.nextLong();
                t.set(key, 0, value);
                model.put(key, value);
            } else {
                assertEquals(model.remove(key) != null, t.remove(key));
            }
            if (i % 200 == 0) {
                history.add(Map.copyOf(model));
                views.add(t.snapshot(i));
            }
        }
        for (int i = 0; i < views.size(); i++) {
            final var actual = new HashMap<Long, Long>();
            views.get(i).forEach(new long[1], (key, row) -> {
                assertNull(actual.put(key, row[0]));
                row[0] = 0;
            });
            assertEquals(history.get(i), actual);
        }
    }

    @Test
    public void publishedSnapshotCanBeSerializedWhileWriterReplacesRecords() throws Exception {
        final var t = LongSnapshotRecordTable.builder().fields(2).expectedSize(1000).build();
        for (long k = 0; k < 1000; k++) {
            t.put(k, new long[]{k, ~k});
        }
        final var snapshot = t.snapshot(99);
        final var reading = new CountDownLatch(1);
        final var updated = new CountDownLatch(1);
        final var executor = Executors.newSingleThreadExecutor();
        try {
            final var future = executor.submit(() -> {
                final long[] visited = {0};
                snapshot.forEach(new long[2], (key, row) -> {
                    if (visited[0]++ == 0) {
                        reading.countDown();
                        try {
                            assertTrue(updated.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                    assertEquals(key, row[0]);
                    assertEquals(~key, row[1]);
                    assertEquals(key, snapshot.get(key, 0));
                });
                return visited[0];
            });
            assertTrue(reading.await(10, TimeUnit.SECONDS));
            for (int k = 0; k < 1000; k++) {
                t.remove(k);
                t.set(k, 1, 0);
            }
            updated.countDown();
            assertEquals(1000L, future.get(10, TimeUnit.SECONDS).longValue());
        } finally {
            updated.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void openSnapshotsStayExactWhileClosedOnesRecycleShards() {
        final var t = LongSnapshotRecordTable.builder().fields(2).shardCapacity(16).build();
        final var model = new HashMap<Long, Long>();
        final var openViews = new ArrayDeque<Object[]>(); // {snapshot, its model}
        final var random = new Random(4711);
        for (int i = 0; i < 60_000; i++) {
            final long key = random.nextInt(3000) - 1500L;
            if (random.nextInt(4) != 0) {
                final long value = random.nextLong();
                if (random.nextBoolean()) {
                    t.set(key, 1, value);
                } else {
                    t.put(key, new long[]{0, value});
                }
                assertEquals(0, t.get(key, 0));
                model.put(key, value);
            } else {
                assertEquals(model.remove(key) != null, t.remove(key));
            }
            assertEquals(model.size(), t.size());
            if (i % 97 == 0) {
                openViews.addLast(new Object[]{t.snapshot(i), Map.copyOf(model)});
            }
            if (openViews.size() > 1 + random.nextInt(6)) {
                // close one of the open ones, not always the oldest
                final var view = random.nextBoolean() ? openViews.pollFirst() : openViews.pollLast();
                check((LongSnapshotRecordTable.Snapshot) view[0], (Map<Long, Long>) view[1], random);
                ((LongSnapshotRecordTable.Snapshot) view[0]).close();
            }
        }
        for (Object[] view : openViews) {
            check((LongSnapshotRecordTable.Snapshot) view[0], (Map<Long, Long>) view[1], random);
        }
        assertTrue("closed snapshots release shards for reuse", t.reusedShards() > 0);
        assertEquals(16, t.largestShardCapacity());
        openViews.forEach(view -> ((LongSnapshotRecordTable.Snapshot) view[0]).close());
        assertEquals(0, t.pinnedShards());
    }

    private static void check(LongSnapshotRecordTable.Snapshot snapshot, Map<Long, Long> expected, Random random) {
        assertEquals(expected.size(), snapshot.size());
        final var actual = new HashMap<Long, Long>();
        snapshot.forEach(new long[2], (key, row) -> {
            assertEquals(0, row[0]);
            assertNull(actual.put(key, row[1]));
        });
        assertEquals(expected, actual);
        for (int k = 0; k < 20; k++) {
            final long key = random.nextInt(3000) - 1500L;
            final Long value = expected.get(key);
            assertEquals(value != null, snapshot.containsKey(key));
            if (value != null) {
                assertEquals(value.longValue(), snapshot.get(key, 1));
            }
        }
    }

    @Test
    public void directoryGrowsSoShardsKeepTheirCapacity() {
        final var t = LongSnapshotRecordTable.builder().fields(1).shardCapacity(64).build();
        final var random = new SplittableRandom(5);
        final long[] keys = new long[200_000];
        LongSnapshotRecordTable.Snapshot held = null;
        for (int i = 0; i < keys.length; i++) {
            keys[i] = random.nextLong();
            t.set(keys[i], 0, i);
            if (i % 10_000 == 0) {
                if (held != null) {
                    held.close();
                }
                held = t.snapshot(i);
            }
        }
        assertEquals(keys.length, t.size());
        assertEquals(64, t.largestShardCapacity());
        // a split size of 40 keys: about 4 directory entries per 40 keys for random keys
        assertTrue("directory " + t.directoryLength(), t.directoryLength() <= 16 * keys.length / 40);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(i, t.get(keys[i], 0));
        }
        assertEquals(190_000, held.get(keys[190_000], 0));
        assertFalse(held.containsKey(keys[190_001]));
    }

    @Test
    public void keysWithOneHashGrowTheirShardInsteadOfTheDirectory() {
        final var t = LongSnapshotRecordTable.builder().fields(1).shardCapacity(16).build();
        final long[] keys = sameHashKeys(600);
        for (int i = 0; i < keys.length; i++) {
            t.set(keys[i], 0, i);
            if (i == 300) {
                t.snapshot(i);
            }
        }
        final var snapshot = t.snapshot(600);
        for (int i = 0; i < keys.length; i++) {
            t.set(keys[i], 0, -i);
        }
        // split size 9: the directory stops at 32 entries per 9 keys
        assertTrue("directory " + t.directoryLength(), t.directoryLength() <= 32 * keys.length / 9);
        assertTrue(t.largestShardCapacity() > 16);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(-i, t.get(keys[i], 0));
            assertEquals(i, snapshot.get(keys[i], 0));
        }
    }

    /** Keys whose 32-bit directory hash is the same: Mix04 run backwards from outputs with one high half. */
    private static long[] sameHashKeys(int count) {
        final long c1 = 0x62a9d9ed799705f5L;
        final long c2 = 0xcb24d0a5c88c35b3L;
        final long[] keys = new long[count];
        for (int i = 0; i < count; i++) {
            final long w = (0x1234_5678L << 32) | (i * 0x9e37_79b9L & 0xffff_ffffL);
            final long y = w * inverse(c2);
            final long z = y ^ (y >>> 28) ^ (y >>> 56);
            final long u = z * inverse(c1);
            final long v = u ^ (u >>> 33);
            keys[i] = v ^ HashingUtils.SEED;
            assertEquals(0x1234_5678, HashingUtils.hash(keys[i] ^ HashingUtils.SEED));
        }
        return keys;
    }

    private static long inverse(long odd) {
        long x = odd;
        for (int i = 0; i < 5; i++) {
            x *= 2 - odd * x;
        }
        return x;
    }

    @Test
    public void closedSnapshotRejectsReads() {
        final var t = LongSnapshotRecordTable.builder().fields(1).build();
        t.set(7, 0, 70);
        final var snapshot = t.snapshot(1);
        assertFalse(snapshot.isClosed());
        snapshot.close();
        snapshot.close();
        assertTrue(snapshot.isClosed());
        assertThrows(IllegalStateException.class, () -> snapshot.get(7, 0));
        assertThrows(IllegalStateException.class, () -> snapshot.containsKey(7));
        assertThrows(IllegalStateException.class, () -> snapshot.forEach(new long[1], (k, row) -> { }));
        assertEquals(1, snapshot.size());
        assertEquals(1, snapshot.sequence());
        assertThrows(IllegalArgumentException.class, () -> LongSnapshotRecordTable.builder().shardCapacity(48));
        assertThrows(IllegalArgumentException.class, () -> LongSnapshotRecordTable.builder().fields(0));
    }

    @Test
    public void droppedSnapshotsCountAsClosedOnceCollected() {
        final var t = LongSnapshotRecordTable.builder().fields(1).shardCapacity(16).build();
        final var random = new SplittableRandom(3);
        for (int i = 0; i < 10_000; i++) {
            t.set(random.nextLong(), 0, i);
        }
        for (int round = 0; round < 20; round++) {
            t.snapshot(round); // never closed, never kept
            for (int i = 0; i < 500; i++) {
                t.set(random.nextLong(), 0, i);
            }
        }
        assertTrue(t.pinnedShards() > 0);
        for (int attempt = 0; attempt < 50 && t.pinnedShards() > 0; attempt++) {
            System.gc();
        }
        assertEquals(0, t.pinnedShards());
        // the released shards serve the copies after the next snapshot
        final long reused = t.reusedShards();
        t.snapshot(99).close();
        for (int i = 0; i < 500; i++) {
            t.set(random.nextLong(), 0, i);
        }
        assertTrue(t.reusedShards() > reused);
    }

    @Test
    public void writesBetweenClosedSnapshotsDoNotAllocate() {
        final var t = LongSnapshotRecordTable.builder().fields(4).expectedSize(100_000).build();
        final var random = new SplittableRandom(9);
        final long[] keys = new long[100_000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = random.nextLong();
            t.set(keys[i], 0, i);
        }
        final var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long thread = Thread.currentThread().threadId();
        long before = 0;
        LongSnapshotRecordTable.Snapshot held = t.snapshot(0);
        final int writes = 400_000;
        for (int round = 0; round < 2; round++) { // the first fills the pool
            before = threads.getThreadAllocatedBytes(thread);
            for (int i = 1; i <= writes; i++) {
                t.set(keys[random.nextInt(keys.length)], 1, i);
                if (i % 1000 == 0) {
                    held.close();
                    held = t.snapshot(i);
                }
            }
        }
        final double perWrite = (double) (threads.getThreadAllocatedBytes(thread) - before) / writes;
        assertTrue("bytes per write " + perWrite, perWrite < 8);
    }
}
