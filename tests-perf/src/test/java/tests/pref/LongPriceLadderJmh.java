package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.price.LongPriceLadder;
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
 * One side of a book: 1,024 levels with volume in a range of {@code ticks} (the whole range of 1,024 ticks, or a band
 * in the middle of 1,048,576). {@link LongPriceLadder} without and with costs against a plain array of levels that a
 * query walks from the best tick:
 * <ul>
 *     <li>{@code update*} - a level gets a new random volume;</li>
 *     <li>{@code price*} - the tick a buy of a random quantity (up to a quarter of the volume) reaches;</li>
 *     <li>{@code cost*} - what that buy costs, sum of tick × lots.</li>
 * </ul>
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main LongPriceLadderJmh -rf json -rff out.json
 * </pre>
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class LongPriceLadderJmh {

    static final int LEVELS = 1024;
    static final int OPS = 1 << 16;

    @State(Scope.Thread)
    public abstract static class Book {
        @Param({"1024", "1048576"})
        public int ticks;

        int first;
        int[] updateTicks;
        long[] updateVolumes;
        long[] quantities;
        int next;
        private CpuAffinity affinity;

        abstract void set(int tick, long volume);

        abstract long total();

        @Setup(Level.Trial)
        public void setUp() {
            if (System.getProperty("affinity.reserved") != null) {
                affinity = CpuAffinity.acquireCore();
                System.out.println("benchmark thread: " + affinity);
            }
            final var random = new SplittableRandom(7);
            first = (ticks - LEVELS) / 2;
            for (int i = 0; i < LEVELS; i++) {
                set(first + i, 1 + random.nextInt(1000));
            }
            updateTicks = new int[OPS];
            updateVolumes = new long[OPS];
            quantities = new long[OPS];
            for (int i = 0; i < OPS; i++) {
                updateTicks[i] = first + random.nextInt(LEVELS);
                updateVolumes[i] = 1 + random.nextInt(1000);
                quantities[i] = 1 + random.nextLong(total() / 4);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (affinity != null) {
                affinity.close();
            }
        }

        final int step() {
            final int i = next;
            next = (i + 1) & (OPS - 1);
            return i;
        }
    }

    public static class Plain extends Book {
        LongPriceLadder ladder;

        @Override
        void set(int tick, long volume) {
            if (ladder == null) {
                ladder = new LongPriceLadder(0, ticks);
            }
            ladder.setVolume(tick, volume);
        }

        @Override
        long total() {
            return ladder.totalVolume();
        }
    }

    public static class WithCosts extends Book {
        LongPriceLadder ladder;

        @Override
        void set(int tick, long volume) {
            if (ladder == null) {
                ladder = LongPriceLadder.withCosts(0, ticks);
            }
            ladder.setVolume(tick, volume);
        }

        @Override
        long total() {
            return ladder.totalVolume();
        }
    }

    /** The levels in an array; the best ask is the lowest tick with volume, all ticks of the band have some. */
    public static class Levels extends Book {
        long[] volumes;
        long total;

        @Override
        void set(int tick, long volume) {
            if (volumes == null) {
                volumes = new long[ticks];
            }
            total += volume - volumes[tick];
            volumes[tick] = volume;
        }

        @Override
        long total() {
            return total;
        }
    }

    @Benchmark
    public void updatePlain(Plain book) {
        final int i = book.step();
        book.ladder.setVolume(book.updateTicks[i], book.updateVolumes[i]);
    }

    @Benchmark
    public void updateWithCosts(WithCosts book) {
        final int i = book.step();
        book.ladder.setVolume(book.updateTicks[i], book.updateVolumes[i]);
    }

    @Benchmark
    public void updateArray(Levels book) {
        final int i = book.step();
        book.set(book.updateTicks[i], book.updateVolumes[i]);
    }

    @Benchmark
    public long pricePlain(Plain book) {
        return book.ladder.priceFromLow(book.quantities[book.step()]);
    }

    @Benchmark
    public long priceArray(Levels book) {
        long want = book.quantities[book.step()];
        final long[] volumes = book.volumes;
        int tick = book.first;
        while (want > volumes[tick]) {
            want -= volumes[tick++];
        }
        return tick;
    }

    @Benchmark
    public long costWithCosts(WithCosts book) {
        return book.ladder.costFromLow(book.quantities[book.step()]);
    }

    @Benchmark
    public long costArray(Levels book) {
        long want = book.quantities[book.step()];
        final long[] volumes = book.volumes;
        long cost = 0;
        int tick = book.first;
        while (want > volumes[tick]) {
            want -= volumes[tick];
            cost += (long) tick * volumes[tick];
            tick++;
        }
        return cost + (long) tick * want;
    }
}
