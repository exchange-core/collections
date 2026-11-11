package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.queue.LongIndexedEventQueue;
import org.openjdk.jmh.annotations.*;

import java.util.HashMap;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/** Actual cancellation in both implementations: no tombstones or linear searches in the JDK baseline. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class LongIndexedEventQueueJmh {

    private static final int BURST = 64;

    private record Event(long id, long time, int priority, long sequence) implements Comparable<Event> {
        @Override
        public int compareTo(Event other) {
            int c = Long.compare(time, other.time);
            if (c == 0) c = Integer.compare(priority, other.priority);
            return c != 0 ? c : Long.compare(sequence, other.sequence);
        }
    }

    @State(Scope.Thread)
    public static class Workload {
        @Param({"indexedHeap", "treeAndHashMap"})
        public String impl;
        @Param({"1024", "65536", "1000000"})
        public int size;

        LongIndexedEventQueue heap;
        TreeSet<Event> tree;
        HashMap<Long, Event> index;
        SplittableRandom random;
        long[] ids;
        long nextId, sequence;
        private CpuAffinity affinity;

        @Setup(Level.Trial)
        public void setup() {
            if (System.getProperty("affinity.reserved") != null) {
                affinity = CpuAffinity.acquireCore();
                System.out.println("benchmark thread: " + affinity);
            }
            if (impl.equals("indexedHeap")) {
                heap = LongIndexedEventQueue.fixedCapacity(size + BURST);
            } else {
                tree = new TreeSet<>();
                index = HashMap.newHashMap(size + BURST);
            }
            random = new SplittableRandom(97281);
            ids = new long[size];
            nextId = 1_000_000;
            for (int i = 0; i < size; i++) {
                ids[i] = nextId++;
                schedule(ids[i], random.nextInt(size), random.nextInt(4));
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (affinity != null) {
                affinity.close();
            }
        }

        void schedule(long id, long time, int priority) {
            if (heap != null) {
                heap.schedule(id, time, priority);
            } else {
                final var event = new Event(id, time, priority, ++sequence);
                tree.add(event);
                index.put(id, event);
            }
        }

        boolean cancel(long id) {
            return heap != null ? heap.cancel(id) : tree.remove(index.remove(id));
        }

        long take() {
            if (heap != null) return heap.removeFirst();
            final long id = tree.pollFirst().id;
            index.remove(id);
            return id;
        }
    }

    @Benchmark
    public boolean cancelAndSchedule(Workload state) {
        final int pos = state.random.nextInt(state.size);
        final boolean removed = state.cancel(state.ids[pos]);
        final long id = state.ids[pos] = state.nextId++;
        state.schedule(id, state.random.nextInt(state.size), state.random.nextInt(4));
        return removed;
    }

    @Benchmark
    public boolean reschedule(Workload state) {
        final long id = state.ids[state.random.nextInt(state.size)];
        final long time = state.random.nextInt(state.size);
        final int priority = state.random.nextInt(4);
        if (state.heap != null) return state.heap.reschedule(id, time, priority);
        final boolean removed = state.cancel(id);
        state.schedule(id, time, priority);
        return removed;
    }

    @State(Scope.Thread)
    public static class Burst extends Workload {
        @Setup(Level.Invocation)
        public void ready() {
            for (int i = 1; i <= BURST; i++) schedule(-i, -1, 0);
        }
    }

    /** Time per removal: 64 tied events ahead of a live backlog. Setup inserts them outside timed code. */
    @Benchmark
    @OperationsPerInvocation(BURST)
    public long drainDueBurst(Burst state) {
        long sum = 0;
        for (int i = 0; i < BURST; i++) sum += state.take();
        return sum;
    }
}
