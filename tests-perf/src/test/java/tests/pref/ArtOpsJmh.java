package tests.pref;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.art.LongObjConsumer;
import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Average time of the {@link LongAdaptiveRadixTreeMap} operations against {@code TreeMap} and fastutil's
 * {@code Long2ObjectRBTreeMap} (primitive keys - no boxing, a fairer tree to compare with), on the key sets of
 * {@link ArtKeyProfiles}. On a {@code book} profile the asks and the bids are two maps, as in an order book; the
 * keys looked up are sampled from the distribution - most near the mid. For A/B runs of collections-core builds:
 * <pre>
 * java -cp test-classes:lib/* org.openjdk.jmh.Main ArtOpsJmh -rf json -rff out.json
 * java -cp test-classes:lib/* org.openjdk.jmh.Main ArtOpsJmh -p impl=ART -p keys=book-1k,book-20k -f 1
 * </pre>
 * Any {@code <distribution>-<size>} works as {@code keys}; {@code Long2ObjectAVLTreeMap} as {@code impl} too.
 * With -Daffinity.reserved (isolated CPUs) the benchmark thread pins itself to one of them.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ArtOpsJmh {

    @Param({"ART", "TreeMap", "Long2ObjectRBTreeMap"})
    public String impl;

    /** 1000 / 20000 price levels, 50000 live order ids out of 1M, 1M random keys (DRAM) */
    @Param({"book-1k", "book-20k", "ids-50k", "random-1m"})
    public String keys;

    private static final int RING = 1 << 16;
    private static final int RING_MASK = RING - 1;

    private static final int TOP = 10;

    private ArtKeyProfiles.Profile profile;
    private long mid;
    private SortedIndex asks;
    /** the same map as asks for a single-sided key set */
    private SortedIndex bids;

    /** stored keys to look up, and their values (to put them back) */
    private final long[] present = new long[RING];
    private final Object[] presentValues = new Object[RING];
    private final long[] absent = new long[RING];
    private int i;

    private byte[] flowOps;
    private long[] flowKeys;
    private Object[] flowValues;
    private int flowPos;

    private CpuAffinity affinity;

    @Setup(Level.Trial)
    public void setUp(final BenchmarkParams params) {
        if (System.getProperty("affinity.reserved") != null) {
            affinity = CpuAffinity.acquireCore();
            System.out.println("benchmark thread: " + affinity);
        }
        profile = ArtKeyProfiles.parse(keys, 42L);
        mid = profile.mid;
        asks = newIndex(impl);
        bids = profile.twoSided() ? newIndex(impl) : asks;

        // the value of a key is its boxed self: lookups can be checked, every map holds the same objects
        final Object[] values = new Object[profile.keys.length];
        for (int k = 0; k < values.length; k++) {
            final long key = profile.keys[k];
            values[k] = key;
            side(key).put(key, values[k]);
        }

        final SplittableRandom random = new SplittableRandom(43L);
        for (int k = 0; k < RING; k++) {
            final int idx = profile.nearest(profile.sample(random));
            present[k] = profile.keys[idx];
            presentValues[k] = values[idx];
            long key;
            do {
                key = profile.sample(random);
            } while (profile.indexOf(key) >= 0);
            absent[k] = key;
        }
        selfCheck();

        if (params.getBenchmark().endsWith(".bookFlow")) {
            buildFlow(values, random);
            // one pass of the stream must leave the maps as they were
            final Blackhole blackhole = new Blackhole(
                    "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
            for (int k = 0; k < flowOps.length; k++) {
                bookFlow(blackhole);
            }
            selfCheck();
        }
        System.out.println(profile + ", " + asks.getClass().getSimpleName());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (affinity != null) {
            affinity.close();
        }
    }

    private SortedIndex side(final long key) {
        return key > mid ? asks : bids;
    }

    @Benchmark
    public Object getHit() {
        final long key = present[i++ & RING_MASK];
        return side(key).get(key);
    }

    @Benchmark
    public Object getMiss() {
        final long key = absent[i++ & RING_MASK];
        return side(key).get(key);
    }

    /** the closest level towards the mid: the lower one for an ask, the higher one for a bid */
    @Benchmark
    public Object neighbour() {
        final long key = present[i++ & RING_MASK];
        return key > mid ? asks.lower(key) : bids.higher(key);
    }

    /** a level disappears and comes back: removal and put of the same key, the size stays */
    @Benchmark
    public Object removeInsert() {
        final int j = i++ & RING_MASK;
        final long key = present[j];
        final SortedIndex index = side(key);
        final Object removed = index.remove(key);
        index.put(key, presentValues[j]);
        return removed;
    }

    /**
     * A new price level as an order book adds it: put, and the neighbour towards the mid to link it to
     * (the fused put of the ART). The level is removed first, to keep the size - minus removeInsert, it is the
     * cost of finding the neighbour.
     */
    @Benchmark
    public Object newLevel() {
        final int j = i++ & RING_MASK;
        final long key = present[j];
        if (key > mid) {
            asks.remove(key);
            return asks.putAndGetLower(key, presentValues[j]);
        } else {
            bids.remove(key);
            return bids.putAndGetHigher(key, presentValues[j]);
        }
    }

    /** L2 market data: the 10 best levels of the asks and of the bids in turn (lowest and highest keys otherwise) */
    @Benchmark
    public long top10() {
        return (i++ & 1) == 0 ? asks.topAsc(TOP) : bids.topDesc(TOP);
    }

    /**
     * L2 market data from a price: the 10 levels from a stored key away from the mid - up for an ask, down for a bid
     * (ascending otherwise). The ART walks with forEach from the key.
     */
    @Benchmark
    public long range10() {
        final long key = present[i++ & RING_MASK];
        return key > mid || !profile.twoSided() ? asks.rangeAsc(key, TOP) : bids.rangeDesc(key, TOP);
    }

    /** as range10, with the ART's cursor: a seek and 10 moves (the trees: the same iterators as in range10) */
    @Benchmark
    public long cursor10() {
        final long key = present[i++ & RING_MASK];
        return key > mid || !profile.twoSided() ? asks.cursorAsc(key, TOP) : bids.cursorDesc(key, TOP);
    }

    /**
     * A stream of order-book operations, one per call: 75% lookups of a level (an order joins it), 10% new
     * levels (put + neighbour), 10% removed levels, 5% top 10. The keys come from the distribution, so most
     * activity is near the mid. The stream returns the maps to their initial state and repeats.
     */
    @Benchmark
    public void bookFlow(final Blackhole blackhole) {
        final int j = flowPos;
        flowPos = j + 1 == flowOps.length ? 0 : j + 1;
        final long key = flowKeys[j];
        switch (flowOps[j]) {
            case FLOW_GET -> blackhole.consume(side(key).get(key));
            case FLOW_ADD -> blackhole.consume(key > mid
                    ? asks.putAndGetLower(key, flowValues[j])
                    : bids.putAndGetHigher(key, flowValues[j]));
            case FLOW_REMOVE -> blackhole.consume(side(key).remove(key));
            default -> blackhole.consume(key > mid ? asks.topAsc(TOP) : bids.topDesc(TOP));
        }
    }

    private static final byte FLOW_GET = 0;
    private static final byte FLOW_ADD = 1;
    private static final byte FLOW_REMOVE = 2;
    private static final byte FLOW_TOP = 3;

    /**
     * Simulates the stream on a TreeSet, then appends the operations that restore the initial key set - the
     * levels added and not removed again are removed, the removed ones put back.
     */
    private void buildFlow(final Object[] values, final SplittableRandom random) {
        final TreeSet<Long> initial = new TreeSet<>();
        for (long key : profile.keys) {
            initial.add(key);
        }
        final TreeSet<Long> live = new TreeSet<>(initial);
        final List<Byte> ops = new ArrayList<>();
        final List<Long> opKeys = new ArrayList<>();
        final List<Object> opValues = new ArrayList<>();

        for (int step = 0; step < RING; step++) {
            final int dice = random.nextInt(100);
            if (dice < 75 || (dice >= 85 && dice < 95 && live.size() <= profile.size / 2)) {
                final long key = nearest(live, profile.sample(random));
                add(ops, opKeys, opValues, FLOW_GET, key, null);
            } else if (dice < 85) {
                long key;
                do {
                    key = profile.sample(random);
                } while (live.contains(key));
                live.add(key);
                add(ops, opKeys, opValues, FLOW_ADD, key, valueOf(key, values));
            } else if (dice < 95) {
                final long key = nearest(live, profile.sample(random));
                live.remove(key);
                add(ops, opKeys, opValues, FLOW_REMOVE, key, null);
            } else {
                add(ops, opKeys, opValues, FLOW_TOP, random.nextBoolean() ? mid + 1 : mid - 1, null);
            }
        }
        for (Long key : live) {
            if (!initial.contains(key)) {
                add(ops, opKeys, opValues, FLOW_REMOVE, key, null);
            }
        }
        for (Long key : initial) {
            if (!live.contains(key)) {
                add(ops, opKeys, opValues, FLOW_ADD, key, valueOf(key, values));
            }
        }

        flowOps = new byte[ops.size()];
        flowKeys = new long[ops.size()];
        flowValues = opValues.toArray();
        for (int k = 0; k < flowOps.length; k++) {
            flowOps[k] = ops.get(k);
            flowKeys[k] = opKeys.get(k);
        }
        System.out.println("flow: " + flowOps.length + " operations (" + (flowOps.length - RING) + " restore the keys)");
    }

    private static void add(List<Byte> ops, List<Long> keys, List<Object> values, byte op, long key, Object value) {
        ops.add(op);
        keys.add(key);
        values.add(value);
    }

    /** the stored value object of a stored key, a new one for a new key */
    private Object valueOf(final long key, final Object[] values) {
        final int idx = profile.indexOf(key);
        return idx >= 0 ? values[idx] : (Object) key;
    }

    private static long nearest(final TreeSet<Long> set, final long key) {
        final Long floor = set.floor(key);
        final Long ceiling = set.ceiling(key);
        if (floor == null) {
            return ceiling;
        }
        if (ceiling == null) {
            return floor;
        }
        return Long.compareUnsigned(key - floor, ceiling - key) <= 0 ? floor : ceiling;
    }

    /** the adapters must answer like the sorted key array - fastutil has no navigation of its own */
    private void selfCheck() {
        final long[] sorted = profile.keys;
        for (int k = 0; k < 2000; k++) {
            final long key = (k & 1) == 0 ? present[k] : absent[k];
            final SortedIndex index = side(key);
            final int idx = profile.indexOf(key);
            check(idx >= 0 ? (Object) key : null, index.get(key), "get", key);
            final int ceiling = idx >= 0 ? idx : -idx - 1;
            final int lowerIdx = (idx >= 0 ? idx : ceiling) - 1;
            final int higherIdx = idx >= 0 ? idx + 1 : ceiling;
            // a neighbour across the mid is in the other map
            final boolean lowerHere = lowerIdx >= 0 && side(sorted[lowerIdx]) == index;
            final boolean higherHere = higherIdx < sorted.length && side(sorted[higherIdx]) == index;
            check(lowerHere ? (Object) sorted[lowerIdx] : null, index.lower(key), "lower", key);
            check(higherHere ? (Object) sorted[higherIdx] : null, index.higher(key), "higher", key);
        }
        long ascending = 0;
        long descending = 0;
        final long[] askKeys = java.util.Arrays.stream(sorted).filter(k -> k > mid).toArray();
        final long[] bidKeys = profile.twoSided() ? java.util.Arrays.stream(sorted).filter(k -> k < mid).toArray() : askKeys;
        for (int k = 0; k < TOP; k++) {
            ascending += askKeys[k];
            descending += bidKeys[bidKeys.length - 1 - k];
        }
        check(ascending, asks.topAsc(TOP), "topAsc", TOP);
        check(descending, bids.topDesc(TOP), "topDesc", TOP);
        for (int k = 0; k < 1000; k++) {
            final long from = (k & 1) == 0 ? present[k] : absent[k];
            final SortedIndex index = side(from);
            final int ceiling = ceilingIndex(sorted, from);
            long up = 0;
            for (int j = ceiling, n = 0; j < sorted.length && n < TOP && side(sorted[j]) == index; j++, n++) {
                up += sorted[j];
            }
            check(up, index.rangeAsc(from, TOP), "rangeAsc", from);
            check(up, index.cursorAsc(from, TOP), "cursorAsc", from);
            final int floor = ceiling < sorted.length && sorted[ceiling] == from ? ceiling : ceiling - 1;
            long down = 0;
            for (int j = floor, n = 0; j >= 0 && n < TOP && side(sorted[j]) == index; j--, n++) {
                down += sorted[j];
            }
            check(down, index.rangeDesc(from, TOP), "rangeDesc", from);
            check(down, index.cursorDesc(from, TOP), "cursorDesc", from);
        }
    }

    private static int ceilingIndex(final long[] sorted, final long key) {
        final int found = java.util.Arrays.binarySearch(sorted, key);
        return found >= 0 ? found : -found - 1;
    }

    private static void check(final Object expected, final Object actual, final String op, final long key) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new IllegalStateException(op + "(" + key + "): expected " + expected + ", got " + actual);
        }
    }

    private static SortedIndex newIndex(final String impl) {
        return switch (impl) {
            case "ART" -> new ArtIndex();
            case "TreeMap" -> new TreeMapIndex();
            case "Long2ObjectRBTreeMap" -> new FastutilIndex(new Long2ObjectRBTreeMap<>());
            case "Long2ObjectAVLTreeMap" -> new FastutilIndex(new Long2ObjectAVLTreeMap<>());
            default -> throw new IllegalArgumentException(impl);
        };
    }

    /** The operations an order book needs from its price index. Each fork loads one implementation. */
    interface SortedIndex {

        Object get(long key);

        void put(long key, Object value);

        Object remove(long key);

        /** value of the closest key strictly below, or null */
        Object lower(long key);

        /** value of the closest key strictly above, or null */
        Object higher(long key);

        Object putAndGetLower(long key, Object value);

        Object putAndGetHigher(long key, Object value);

        /** visits the n lowest entries, returns the sum of their keys */
        long topAsc(int n);

        /** visits the n highest entries, returns the sum of their keys */
        long topDesc(int n);

        /** visits the n entries from the key (inclusive) up, returns the sum of their keys */
        long rangeAsc(long from, int n);

        /** visits the n entries from the key (inclusive) down, returns the sum of their keys */
        long rangeDesc(long from, int n);

        /** as rangeAsc, step by step */
        default long cursorAsc(long from, int n) {
            return rangeAsc(from, n);
        }

        /** as rangeDesc, step by step */
        default long cursorDesc(long from, int n) {
            return rangeDesc(from, n);
        }
    }

    static final class ArtIndex implements SortedIndex {

        private final LongAdaptiveRadixTreeMap<Object> map = new LongAdaptiveRadixTreeMap<>();
        private long sum;
        private final LongObjConsumer<Object> summing = (key, value) -> sum += key;
        private final LongAdaptiveRadixTreeMap.Cursor<Object> cursor = map.cursor();

        @Override
        public Object get(long key) {
            return map.get(key);
        }

        @Override
        public void put(long key, Object value) {
            map.put(key, value);
        }

        @Override
        public Object remove(long key) {
            return map.remove(key);
        }

        @Override
        public Object lower(long key) {
            return map.getLowerValue(key);
        }

        @Override
        public Object higher(long key) {
            return map.getHigherValue(key);
        }

        @Override
        public Object putAndGetLower(long key, Object value) {
            return map.putAndGetLowerValue(key, value);
        }

        @Override
        public Object putAndGetHigher(long key, Object value) {
            return map.putAndGetHigherValue(key, value);
        }

        @Override
        public long topAsc(int n) {
            sum = 0;
            map.forEach(summing, n);
            return sum;
        }

        @Override
        public long topDesc(int n) {
            sum = 0;
            map.forEachDesc(summing, n);
            return sum;
        }

        @Override
        public long rangeAsc(long from, int n) {
            sum = 0;
            map.forEach(from, summing, n);
            return sum;
        }

        @Override
        public long rangeDesc(long from, int n) {
            sum = 0;
            map.forEachDesc(from, summing, n);
            return sum;
        }

        @Override
        public long cursorAsc(long from, int n) {
            long total = 0;
            for (boolean on = cursor.seekCeiling(from); on && n-- > 0; on = cursor.next()) {
                total += cursor.key();
            }
            return total;
        }

        @Override
        public long cursorDesc(long from, int n) {
            long total = 0;
            for (boolean on = cursor.seekFloor(from); on && n-- > 0; on = cursor.prev()) {
                total += cursor.key();
            }
            return total;
        }
    }

    static final class TreeMapIndex implements SortedIndex {

        private final TreeMap<Long, Object> map = new TreeMap<>();

        @Override
        public Object get(long key) {
            return map.get(key);
        }

        @Override
        public void put(long key, Object value) {
            map.put(key, value);
        }

        @Override
        public Object remove(long key) {
            return map.remove(key);
        }

        @Override
        public Object lower(long key) {
            final Map.Entry<Long, Object> entry = map.lowerEntry(key);
            return entry != null ? entry.getValue() : null;
        }

        @Override
        public Object higher(long key) {
            final Map.Entry<Long, Object> entry = map.higherEntry(key);
            return entry != null ? entry.getValue() : null;
        }

        @Override
        public Object putAndGetLower(long key, Object value) {
            map.put(key, value);
            return lower(key);
        }

        @Override
        public Object putAndGetHigher(long key, Object value) {
            map.put(key, value);
            return higher(key);
        }

        @Override
        public long topAsc(int n) {
            return sum(map.entrySet().iterator(), n);
        }

        @Override
        public long topDesc(int n) {
            return sum(map.descendingMap().entrySet().iterator(), n);
        }

        @Override
        public long rangeAsc(long from, int n) {
            return sum(map.tailMap(from, true).entrySet().iterator(), n);
        }

        @Override
        public long rangeDesc(long from, int n) {
            return sum(map.headMap(from, true).descendingMap().entrySet().iterator(), n);
        }

        private static long sum(final Iterator<Map.Entry<Long, Object>> it, int n) {
            long sum = 0;
            while (n-- > 0 && it.hasNext()) {
                sum += it.next().getKey();
            }
            return sum;
        }
    }

    /**
     * fastutil's sorted maps have no lower/higher: a head/tail submap and its last/first entry - one descent,
     * plus two small objects the JIT may or may not scalar-replace.
     */
    static final class FastutilIndex implements SortedIndex {

        private final Long2ObjectSortedMap<Object> map;
        private final ProbeEntry probe = new ProbeEntry();

        FastutilIndex(final Long2ObjectSortedMap<Object> map) {
            this.map = map;
        }

        @Override
        public Object get(long key) {
            return map.get(key);
        }

        @Override
        public void put(long key, Object value) {
            map.put(key, value);
        }

        @Override
        public Object remove(long key) {
            return map.remove(key);
        }

        @Override
        public Object lower(long key) {
            if (map.isEmpty() || key <= map.firstLongKey()) {
                return null;
            }
            return map.headMap(key).long2ObjectEntrySet().last().getValue();
        }

        @Override
        public Object higher(long key) {
            if (map.isEmpty() || key >= map.lastLongKey()) {
                return null;
            }
            return map.tailMap(key + 1).long2ObjectEntrySet().first().getValue();
        }

        @Override
        public Object putAndGetLower(long key, Object value) {
            map.put(key, value);
            return lower(key);
        }

        @Override
        public Object putAndGetHigher(long key, Object value) {
            map.put(key, value);
            return higher(key);
        }

        @Override
        public long topAsc(int n) {
            final ObjectBidirectionalIterator<Long2ObjectMap.Entry<Object>> it = map.long2ObjectEntrySet().iterator();
            long sum = 0;
            while (n-- > 0 && it.hasNext()) {
                sum += it.next().getLongKey();
            }
            return sum;
        }

        @Override
        public long topDesc(int n) {
            if (map.isEmpty()) {
                return 0;
            }
            // positioned after the last entry
            probe.set(map.lastLongKey());
            final ObjectBidirectionalIterator<Long2ObjectMap.Entry<Object>> it = map.long2ObjectEntrySet().iterator(probe);
            long sum = 0;
            while (n-- > 0 && it.hasPrevious()) {
                sum += it.previous().getLongKey();
            }
            return sum;
        }

        @Override
        public long rangeAsc(long from, int n) {
            final LongBidirectionalIterator it = map.tailMap(from).keySet().iterator();
            long sum = 0;
            while (n-- > 0 && it.hasNext()) {
                sum += it.nextLong();
            }
            return sum;
        }

        @Override
        public long rangeDesc(long from, int n) {
            // positioned so that previous() is the highest key at or below the starting point
            final LongBidirectionalIterator it = map.keySet().iterator(from);
            long sum = 0;
            while (n-- > 0 && it.hasPrevious()) {
                sum += it.previousLong();
            }
            return sum;
        }

        private static final class ProbeEntry extends AbstractLong2ObjectMap.BasicEntry<Object> {
            void set(final long key) {
                this.key = key;
            }
        }
    }
}
