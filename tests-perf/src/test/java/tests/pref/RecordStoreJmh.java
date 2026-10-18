package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.records.RecordStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * Balances (available, reserved) as {@link RecordStore} records against the ways to keep them without it:
 * <ul>
 *     <li>{@code RecordStore} - a handle per record, the generation checked on every access;</li>
 *     <li>{@code objectRefs} - an object per record, the caller holds the reference (no id, no check);</li>
 *     <li>{@code objectList} - an object per record in an {@code ArrayList}, the caller holds a {@code long} id
 *     (generation + index, as a handle - what a {@code long} hashtable or a field of another record can hold) and
 *     checks the generation kept in the object; freed objects go to a pool;</li>
 *     <li>{@code flatArray} - one {@code long[]}, the caller holds the index: no check, no pages, no allocator - the
 *     floor.</li>
 * </ul>
 * The records are reached in a random order, through a shuffled array of all handles / references read in sequence
 * (the prefetcher hides that read), so a record that is not in the cache costs one miss for every layout but
 * {@code objectList} (the list's array of references, then the object). The accesses are independent, so their misses
 * overlap - except in {@code chase}, which follows a cycle through the records, each access waiting for the one
 * before: the latency of one access. From L2-resident (10k) to DRAM (10M):
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main RecordStoreJmh -rf json -rff out.json
 * </pre>
 * The objects are allocated in their order, one after another - the best case for {@code scan}; churn scatters them.
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class RecordStoreJmh {

    @Param({"RecordStore", "objectRefs", "objectList", "flatArray"})
    public String impl;

    /** records: 10000 (240 KB as RecordStore), 1000000 (24 MB), 10000000 (240 MB) */
    @Param({"10000", "1000000", "10000000"})
    public int size;

    private static final int RECORD_STORE = 0;
    private static final int OBJECT_REFS = 1;
    private static final int OBJECT_LIST = 2;
    private static final int FLAT_ARRAY = 3;

    private static final int AVAILABLE = 0;
    private static final int RESERVED = 1;

    /** an object per record: the fields, and the generation for objectList */
    static final class Balance {
        long available;
        long reserved;
        int generation;
        /** objectRefs: the next object of chase */
        Balance next;
    }

    private int kind;

    private RecordStore store;
    /** objectRefs: the objects in their order */
    private Balance[] objects;
    private List<Balance> list;
    private final Balance[] pool = new Balance[16];
    private int poolSize;
    private final int[] freeIndexes = new int[16];
    private int freeCount;
    private long[] flat;

    /** every record once, in a random order: handles, ids or indexes - refs for objectRefs */
    private long[] order;
    private Balance[] refs;
    private int i;

    /** chase: the record reached, its RESERVED field (next for objectRefs) holds the next one */
    private long current;
    private Balance currentRef;

    private long scanSum;
    private final LongConsumer addAvailable = h -> scanSum += store.get(h, AVAILABLE);

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        final SplittableRandom rand = new SplittableRandom(42L);
        order = new long[size];
        switch (impl) {
            case "RecordStore" -> {
                kind = RECORD_STORE;
                store = RecordStore.builder().fields(2).expectedSize(size).build();
                for (int k = 0; k < size; k++) {
                    order[k] = store.allocate();
                    store.set(order[k], AVAILABLE, 1_000_000_000L);
                }
            }
            case "objectRefs" -> {
                kind = OBJECT_REFS;
                objects = new Balance[size];
                for (int k = 0; k < size; k++) {
                    objects[k] = new Balance();
                    objects[k].available = 1_000_000_000L;
                }
                refs = objects.clone();
                shuffle(refs, rand);
                order = null;
            }
            case "objectList" -> {
                kind = OBJECT_LIST;
                list = new ArrayList<>(size);
                for (int k = 0; k < size; k++) {
                    final Balance b = new Balance();
                    b.available = 1_000_000_000L;
                    b.generation = 1;
                    list.add(b);
                    order[k] = 1L << 32 | k;
                }
            }
            case "flatArray" -> {
                kind = FLAT_ARRAY;
                flat = new long[size * 2];
                for (int k = 0; k < size; k++) {
                    flat[k * 2] = 1_000_000_000L;
                    order[k] = k;
                }
            }
            default -> throw new IllegalArgumentException(impl);
        }
        linkCycle(rand);
        if (order != null) {
            shuffle(order, rand);
        }
    }

    /**
     * One random cycle through all records (Sattolo's shuffle): each record holds the next one in its RESERVED field
     * (objectRefs: in next), so that chase reaches the records one after another - every access waits for the one
     * before, no misses overlap.
     */
    private void linkCycle(SplittableRandom rand) {
        final int[] cycle = new int[size];
        for (int k = 0; k < size; k++) {
            cycle[k] = k;
        }
        for (int k = size - 1; k > 0; k--) {
            final int j = rand.nextInt(k);
            final int t = cycle[k];
            cycle[k] = cycle[j];
            cycle[j] = t;
        }
        for (int k = 0; k < size; k++) {
            final int from = cycle[k];
            final int to = cycle[k + 1 == size ? 0 : k + 1];
            switch (kind) {
                case RECORD_STORE -> store.set(order[from], RESERVED, order[to]);
                case OBJECT_REFS -> objects[from].next = objects[to];
                case OBJECT_LIST -> list.get(from).reserved = order[to];
                default -> flat[from * 2 + 1] = order[to];
            }
        }
        current = order == null ? 0 : order[cycle[0]];
        currentRef = objects == null ? null : objects[cycle[0]];
    }

    private static void shuffle(long[] a, SplittableRandom rand) {
        for (int k = a.length - 1; k > 0; k--) {
            final int j = rand.nextInt(k + 1);
            final long t = a[k];
            a[k] = a[j];
            a[j] = t;
        }
    }

    private static void shuffle(Object[] a, SplittableRandom rand) {
        for (int k = a.length - 1; k > 0; k--) {
            final int j = rand.nextInt(k + 1);
            final Object t = a[k];
            a[k] = a[j];
            a[j] = t;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (affinity != null) {
            affinity.close();
        }
    }

    private int next() {
        final int k = i;
        i = k + 1 == size ? 0 : k + 1;
        return k;
    }

    private Balance checkedObject(long id) {
        final Balance b = list.get((int) id);
        if (b == null || b.generation != (int) (id >>> 32)) {
            throw new IllegalArgumentException("stale id " + id);
        }
        return b;
    }

    /** both fields of one record */
    @Benchmark
    public long read() {
        final int k = next();
        switch (kind) {
            case RECORD_STORE: {
                final long h = order[k];
                return store.get(h, AVAILABLE) + store.get(h, RESERVED);
            }
            case OBJECT_REFS: {
                final Balance b = refs[k];
                return b.available + b.reserved;
            }
            case OBJECT_LIST: {
                final Balance b = checkedObject(order[k]);
                return b.available + b.reserved;
            }
            default: {
                final int index = (int) order[k] * 2;
                return flat[index] + flat[index + 1];
            }
        }
    }

    /** the next record of the cycle: the latency of an access, not overlapped with the one before */
    @Benchmark
    public long chase() {
        switch (kind) {
            case RECORD_STORE:
                return current = store.get(current, RESERVED);
            case OBJECT_REFS:
                return (currentRef = currentRef.next).available;
            case OBJECT_LIST:
                return current = checkedObject(current).reserved;
            default:
                return current = flat[(int) current * 2 + 1];
        }
    }

    /** a reservation: available down, reserved up */
    @Benchmark
    public long reserve() {
        final int k = next();
        switch (kind) {
            case RECORD_STORE: {
                final long h = order[k];
                store.addAndGet(h, AVAILABLE, -1);
                return store.addAndGet(h, RESERVED, 1);
            }
            case OBJECT_REFS: {
                final Balance b = refs[k];
                b.available--;
                return ++b.reserved;
            }
            case OBJECT_LIST: {
                final Balance b = checkedObject(order[k]);
                b.available--;
                return ++b.reserved;
            }
            default: {
                final int index = (int) order[k] * 2;
                flat[index]--;
                return ++flat[index + 1];
            }
        }
    }

    /**
     * A record ends and a new one takes its place - the size stays. For objectRefs and objectList the object goes
     * to a pool and comes back from it, as with ObjectsPool; flatArray has no allocator (only the read of the order).
     */
    @Benchmark
    public long freeAllocate() {
        final int k = next();
        switch (kind) {
            case RECORD_STORE: {
                store.free(order[k]);
                return order[k] = store.allocate();
            }
            case OBJECT_REFS: {
                final Balance b = refs[k];
                b.available = 0;
                b.reserved = 0;
                pool[poolSize++] = b;
                final Balance n = pool[--poolSize];
                pool[poolSize] = null;
                refs[k] = n;
                return n.available;
            }
            case OBJECT_LIST: {
                final long id = order[k];
                final int index = (int) id;
                final Balance b = checkedObject(id);
                list.set(index, null);
                b.available = 0;
                b.reserved = 0;
                b.generation++;
                pool[poolSize++] = b;
                freeIndexes[freeCount++] = index;

                final Balance n = pool[--poolSize];
                pool[poolSize] = null;
                final int newIndex = freeIndexes[--freeCount];
                list.set(newIndex, n);
                return order[k] = (long) n.generation << 32 | newIndex;
            }
            default:
                return order[k];
        }
    }

    /** the sum of one field over all records, in their order in the store / array / list */
    @Benchmark
    @Measurement(iterations = 5, time = 2)
    public long scan() {
        switch (kind) {
            case RECORD_STORE: {
                scanSum = 0;
                store.forEach(addAvailable);
                return scanSum;
            }
            case OBJECT_REFS: {
                long sum = 0;
                for (Balance b : objects) {
                    sum += b.available;
                }
                return sum;
            }
            case OBJECT_LIST: {
                long sum = 0;
                for (int k = 0; k < list.size(); k++) {
                    sum += list.get(k).available;
                }
                return sum;
            }
            default: {
                long sum = 0;
                for (int k = 0; k < flat.length; k += 2) {
                    sum += flat[k];
                }
                return sum;
            }
        }
    }
}
