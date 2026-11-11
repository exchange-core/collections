package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.queue.LongIndexedFifoQueues;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openjdk.jmh.annotations.*;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/** Level/order mechanics only. Both layouts have an ID index, reusable storage and cached level references. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms2g", "-Xmx3g"})
public class FifoOrderBookJmh {
    static final int LEVELS = 4096, BATCH = 16;
    static final class Order {
        long id, size, filled, uid;
        Order prev, next;
        Bucket owner;
    }
    static final class Bucket { Order head, tail; int count; }

    @State(Scope.Thread)
    public static class Book {
        @Param({"1000000", "10000000"}) public int orders;
        @Param({"records", "links"}) public String layout;
        LongIndexedFifoQueues fifo;
        long[] handles, liveIds;
        Bucket[] levels;
        LongObjectHashMap<Order> byId;
        LongObjectHashMap<Bucket> byPrice;
        Order free;
        final Bucket spareLevel = new Bucket();
        SplittableRandom random;
        long nextId;
        private CpuAffinity affinity;

        @Setup(Level.Trial)
        public void setup() {
            if (System.getProperty("affinity.reserved") != null) {
                affinity = CpuAffinity.acquireCore();
                System.out.println("benchmark thread: " + affinity);
            }
            liveIds = new long[orders];
            random = new SplittableRandom(83);
            nextId = orders + 1L;
            if (layout.equals("records")) {
                fifo = LongIndexedFifoQueues.fixedCapacity(LEVELS + 2, orders + 1, 3);
                handles = new long[LEVELS + 1];
                for (int i = 0; i < handles.length; i++) handles[i] = fifo.getOrCreateQueue(i);
            } else {
                byId = new LongObjectHashMap<>(orders + 1);
                byPrice = new LongObjectHashMap<>(LEVELS + 2);
                levels = new Bucket[LEVELS + 1];
                for (int i = 0; i < levels.length; i++) byPrice.put(i, levels[i] = new Bucket());
            }
            // Neighbours at the same price were allocated 4096 orders apart.
            for (int i = 0; i < orders; i++) submit(i & (LEVELS - 1), liveIds[i] = i + 1L);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (affinity != null) {
                affinity.close();
            }
        }

        void submit(int price, long id) {
            if (fifo != null) {
                long e = fifo.appendTo(handles[price], id, 100);
                fifo.setField(e, 1, id & 7);
                fifo.setField(e, 2, ~id);
            } else add(levels[price], id, 100, id & 7, ~id);
        }

        void add(Bucket q, long id, long size, long filled, long uid) {
            Order o = free;
            if (o == null) o = new Order(); else free = o.next;
            o.id = id; o.size = size; o.filled = filled; o.uid = uid;
            o.owner = q; o.prev = q.tail; o.next = null;
            if (q.tail == null) q.head = o; else q.tail.next = o;
            q.tail = o; q.count++;
            byId.put(id, o);
        }
        void unlink(Order o) {
            Bucket q = o.owner;
            if (o.prev == null) q.head = o.next; else o.prev.next = o.next;
            if (o.next == null) q.tail = o.prev; else o.next.prev = o.prev;
            q.count--;
            o.owner = null; o.prev = null; o.next = free; free = o;
        }
    }

    @Benchmark public long cancelAndAppend(Book b) {
        int i = b.random.nextInt(b.orders);
        long old = b.liveIds[i], result;
        if (b.fifo != null) result = b.fifo.remove(old, -1);
        else {
            Order o = b.byId.remove(old);
            result = o.size;
            b.unlink(o);
        }
        b.submit(i & (LEVELS - 1), b.liveIds[i] = b.nextId++);
        return result;
    }

    @State(Scope.Thread)
    public static class Match extends Book {
        int price;
        @Setup(Level.Invocation) public void chooseLevel() { price = random.nextInt(LEVELS); }
        @TearDown(Level.Invocation) public void replenish() {
            for (int i = 0; i < BATCH; i++) submit(price, nextId++);
        }
    }

    /** Time per fully executed order, with refill outside the timed region. */
    @Benchmark @OperationsPerInvocation(BATCH)
    public long executeHeads(Match b) {
        long volume = 0;
        if (b.fifo != null) {
            long e = b.fifo.first(b.handles[b.price]);
            for (int i = 0; i < BATCH; i++) {
                long next = b.fifo.next(e);
                long filled = b.fifo.getField(e, 1);
                volume += b.fifo.removeEntry(e) - filled;
                e = next;
            }
        } else {
            Order o = b.levels[b.price].head;
            for (int i = 0; i < BATCH; i++) {
                Order next = o.next;
                volume += o.size - o.filled;
                b.byId.remove(o.id);
                b.unlink(o);
                o = next;
            }
        }
        return volume;
    }

    /** Single hot price repeatedly goes from empty to one order to empty. */
    @Benchmark public long churnByKey(Book b) {
        if (b.fifo != null) {
            b.fifo.append(-1, b.nextId++, 100);
            return b.fifo.removeFirst(-1);
        }
        b.byPrice.put(-1, b.spareLevel);
        b.add(b.spareLevel, b.nextId++, 100, 0, 0);
        Order head = b.byPrice.get(-1).head;
        b.byId.remove(head.id);
        b.unlink(head);
        b.byPrice.remove(-1);
        return head.size;
    }

    @Benchmark public long churnRetained(Book b) {
        if (b.fifo != null) {
            long q = b.handles[LEVELS];
            b.fifo.appendTo(q, b.nextId++, 100);
            return b.fifo.removeFirstByHandle(q);
        }
        Bucket q = b.levels[LEVELS];
        b.add(q, b.nextId++, 100, 0, 0);
        Order head = q.head;
        b.byId.remove(head.id);
        b.unlink(head);
        return head.size;
    }
}
