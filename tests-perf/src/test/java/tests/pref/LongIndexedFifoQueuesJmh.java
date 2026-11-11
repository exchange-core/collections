package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.queue.LongIndexedFifoQueues;
import org.openjdk.jmh.annotations.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Steady population: cancel a random live order by ID and replace it with a new ID at the same price.
 * The JDK baseline also has a global ID index, pointing directly to the order's FIFO. Run with -prof gc
 * to see allocation as well as time per cancel/append pair.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class LongIndexedFifoQueuesJmh {

    @Param({"indexedFifo", "linkedHashMap"})
    public String impl;

    @Param({"64", "4096"})
    public int levels;

    private LongIndexedFifoQueues fifo;
    private HashMap<Long, LinkedHashMap<Long, Long>> queues, owners;
    private long[] liveIds;
    private long nextId;
    private SplittableRandom random;
    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        liveIds = new long[levels * 16];
        nextId = 1_000_000;
        random = new SplittableRandom(82);
        if (impl.equals("indexedFifo")) {
            fifo = LongIndexedFifoQueues.fixedCapacity(levels, liveIds.length);
        } else {
            queues = HashMap.newHashMap(levels);
            owners = HashMap.newHashMap(liveIds.length);
            for (long price = 0; price < levels; price++) {
                queues.put(price, LinkedHashMap.newLinkedHashMap(16));
            }
        }
        for (int slot = 0; slot < liveIds.length; slot++) {
            final long price = slot / 16;
            final long id = liveIds[slot] = nextId++;
            append(price, id);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (affinity != null) {
            affinity.close();
        }
    }

    private void append(long price, long id) {
        if (fifo != null) {
            fifo.append(price, id, ~id);
        } else {
            final var q = queues.get(price);
            final Long key = id;
            q.put(key, ~id);
            owners.put(key, q);
        }
    }

    @Benchmark
    public boolean cancelAndAppend() {
        final int slot = random.nextInt(liveIds.length);
        final long old = liveIds[slot];
        final boolean removed = fifo != null ? fifo.remove(old) : owners.remove(old).remove(old) != null;
        final long id = liveIds[slot] = nextId++;
        append(slot / 16, id);
        return removed;
    }
}
