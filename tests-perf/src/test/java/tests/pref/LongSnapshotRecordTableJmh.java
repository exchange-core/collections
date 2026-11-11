package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.hashtable.LongSnapshotRecordTable;
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

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Writes to a snapshot table while a reader holds the latest snapshot: every {@code snapshotEvery} writes the writer
 * takes a new snapshot and closes the previous one. The cost of a write includes its share of the snapshots and
 * of the shard copies they cause; {@code -prof gc} shows the bytes allocated per write. {@code snapshotEvery = 0}
 * never takes one. {@code write} reports the mean, {@code writeSampled} the percentiles of single writes (the first
 * write to a shard after a snapshot copies it; the write that takes a snapshot copies the directory). Random keys in
 * a random order, four fields per record.
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main LongSnapshotRecordTableJmh -prof gc -rf json -rff out.json
 * </pre>
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class LongSnapshotRecordTableJmh {

    private static final int FIELDS = 4;

    @Param({"10000", "1000000"})
    public int size;

    @Param({"0", "1000", "100000"})
    public int snapshotEvery;

    @Param({"64", "256"})
    public int shardCapacity;

    private LongSnapshotRecordTable table;
    private LongSnapshotRecordTable.Snapshot held;
    private long[] order;
    private int mask;
    private int next;
    private int untilSnapshot;
    private long sequence;
    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp() {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        final var random = new SplittableRandom(42);
        table = LongSnapshotRecordTable.builder().fields(FIELDS).expectedSize(size).shardCapacity(shardCapacity)
                .build();
        final long[] keys = new long[size];
        for (int i = 0; i < size; i++) {
            keys[i] = random.nextLong();
            table.set(keys[i], 0, i);
        }
        // writes in a random order: a power-of-two ring of the keys, each key several times
        order = new long[Integer.highestOneBit(size) * 4];
        for (int i = 0; i < order.length; i++) {
            order[i] = keys[random.nextInt(size)];
        }
        mask = order.length - 1;
        untilSnapshot = snapshotEvery;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (affinity != null) {
            affinity.close();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public long writeSampled() {
        return write();
    }

    @Benchmark
    public long write() {
        final long key = order[next];
        next = (next + 1) & mask;
        table.set(key, 1, sequence);
        if (snapshotEvery != 0 && --untilSnapshot == 0) {
            untilSnapshot = snapshotEvery;
            if (held != null) {
                held.close();
            }
            held = table.snapshot(sequence);
        }
        return ++sequence;
    }
}
