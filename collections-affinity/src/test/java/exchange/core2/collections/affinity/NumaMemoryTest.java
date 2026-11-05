package exchange.core2.collections.affinity;

import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The real system call where it is supported, the rest everywhere.
 */
public class NumaMemoryTest {

    private static final long MB = 1 << 20;
    private static final long PAGE = 4096;

    private static long[] sample(long address, long bytes) {
        return NumaMemory.samplePages(address, bytes, new SplittableRandom(1));
    }

    @Test
    public void samplesEveryPageOfASmallRange() {
        assertArrayEquals(new long[]{0x10000, 0x11000, 0x12000}, sample(0x10000, 3 * PAGE));
    }

    @Test
    public void samplesThePagesARangeTouches() {
        assertArrayEquals(new long[]{0x10000}, sample(0x10010, 16));
        assertArrayEquals(new long[]{0x10000, 0x11000}, sample(0x10ff8, 16));
        assertArrayEquals(new long[]{0x10000}, sample(0x10000, PAGE));
    }

    @Test
    public void samplesOnePageFromEachPartOfALargeRange() {
        final long[] pages = sample(0, 8 * 4096 * PAGE);
        assertEquals(4096, pages.length);
        for (int i = 0; i < pages.length; i++) {
            assertEquals(0, pages[i] % PAGE);
            assertEquals(i, pages[i] / (8 * PAGE));
        }
    }

    @Test
    public void samplesOtherPagesEachTime() {
        final long bytes = 8 * 4096 * PAGE;
        assertNotEquals(Arrays.toString(NumaMemory.samplePages(0, bytes, new SplittableRandom(1))),
                Arrays.toString(NumaMemory.samplePages(0, bytes, new SplittableRandom(2))));
    }

    /**
     * 16 GB, sampled every 4 MB on average: a regular step would see only one node of 2 MB stripes.
     */
    @Test
    public void stripesDoNotHideANode() {
        for (long stripe : new long[]{2 * MB, 4 * MB, 8 * MB, 32 * MB}) {
            final int[] nodes = Arrays.stream(sample(0, 16L << 30)).mapToInt(page -> (int) (page / stripe % 2)).toArray();
            final NumaMemory.Placement placement = new NumaMemory.Placement(nodes);
            assertFalse(placement.toString(), placement.isOnlyOn(0));
            assertTrue(placement.toString(), placement.pagesOn(0) > 4096 * 2 / 5);
            assertTrue(placement.toString(), placement.pagesOn(1) > 4096 * 2 / 5);
        }
    }

    @Test
    public void placementCountsThePages() {
        final NumaMemory.Placement placement = new NumaMemory.Placement(new int[]{1, 1, 0, -2, 1, -14, 0, 1});
        assertEquals(8, placement.sampledPages());
        assertEquals(2, placement.absentPages());
        assertEquals(2, placement.pagesOn(0));
        assertEquals(4, placement.pagesOn(1));
        assertEquals(0, placement.pagesOn(2));
        assertEquals(0, placement.pagesOn(-1));
        assertFalse(placement.isOnlyOn(1));
        assertEquals("node 0 25%, node 1 50%, absent 25% of 8 sampled pages", placement.toString());
    }

    @Test
    public void allPagesAbsent() {
        final NumaMemory.Placement placement = new NumaMemory.Placement(new int[]{-14, -2});
        assertEquals(2, placement.sampledPages());
        assertEquals(2, placement.absentPages());
        assertFalse(placement.isOnlyOn(0));
        assertEquals("absent 100% of 2 sampled pages", placement.toString());
    }

    @Test
    public void onlyOnANodeMeansAllThePages() {
        assertTrue(new NumaMemory.Placement(new int[]{1, 1}).isOnlyOn(1));
        assertFalse(new NumaMemory.Placement(new int[]{1, 1}).isOnlyOn(0));
        assertFalse(new NumaMemory.Placement(new int[]{1, -2}).isOnlyOn(1));
    }

    @Test
    public void nothingKnown() {
        final NumaMemory.Placement placement = new NumaMemory.Placement(new int[0]);
        assertEquals(0, placement.sampledPages());
        assertFalse(placement.isOnlyOn(0));
        assertEquals("unknown", placement.toString());
    }

    /**
     * The addresses a query sees, one after the other, the last one from then on.
     */
    private static LongSupplier addresses(long... values) {
        final AtomicInteger next = new AtomicInteger();
        return () -> values[Math.min(next.getAndIncrement(), values.length - 1)];
    }

    @Test
    public void memoryInPlaceIsAskedOnce() {
        final AtomicInteger queries = new AtomicInteger();
        final NumaMemory.Placement placement = NumaMemory.query(addresses(PAGE), PAGE, pages -> {
            queries.incrementAndGet();
            assertArrayEquals(new long[]{PAGE}, pages);
            return new int[]{1};
        });
        assertEquals(1, queries.get());
        assertTrue(placement.toString(), placement.isOnlyOn(1));
    }

    @Test
    public void arrayMovedDuringTheQueryIsAskedAgainAtItsNewAddress() {
        final AtomicInteger queries = new AtomicInteger();
        final NumaMemory.Placement placement = NumaMemory.query(addresses(PAGE, 2 * PAGE), PAGE, pages -> {
            // the answer about the old address must not be the result
            final boolean atNewAddress = queries.incrementAndGet() > 1;
            assertArrayEquals(new long[]{atNewAddress ? 2 * PAGE : PAGE}, pages);
            return new int[]{atNewAddress ? 1 : 0};
        });
        assertEquals(2, queries.get());
        assertTrue(placement.toString(), placement.isOnlyOn(1));
    }

    @Test
    public void arrayThatKeepsMovingIsUnknown() {
        final AtomicInteger address = new AtomicInteger();
        final AtomicInteger queries = new AtomicInteger();
        final NumaMemory.Placement placement = NumaMemory.query(() -> address.incrementAndGet() * PAGE, PAGE, pages -> {
            queries.incrementAndGet();
            return new int[]{1};
        });
        assertEquals(0, placement.sampledPages());
        assertEquals(3, queries.get());
    }

    @Test
    public void failedCallIsUnknown() {
        final AtomicInteger queries = new AtomicInteger();
        final NumaMemory.Placement placement = NumaMemory.query(addresses(PAGE), PAGE, pages -> {
            queries.incrementAndGet();
            return null;
        });
        assertEquals(0, placement.sampledPages());
        assertEquals(1, queries.get());
    }

    @Test
    public void unknownAddressIsUnknown() {
        final NumaMemory.Placement placement = NumaMemory.query(addresses(0), PAGE, pages -> {
            throw new AssertionError("no query without an address");
        });
        assertEquals(0, placement.sampledPages());
    }

    @Test
    public void emptySegmentHasNoPages() {
        assertEquals(0, NumaMemory.nodesOf(MemorySegment.ofArray(new long[0])).sampledPages());
    }

    @Test
    public void closedSegmentHasNoPages() {
        final MemorySegment closed;
        try (Arena arena = Arena.ofConfined()) {
            closed = arena.allocate(MB);
        }
        assertEquals(0, NumaMemory.nodesOf(closed).sampledPages());
    }

    @Test
    public void whereNotSupportedNothingIsKnown() {
        assumeTrue("placement can be read here", !NumaMemory.isSupported());
        assertEquals(0, NumaMemory.nodesOf(MemorySegment.ofArray(new long[1024])).sampledPages());
    }

    /**
     * The numbers of the two architectures, on the architecture of the machine.
     */
    @Test
    public void probeTellsTheErrnoOfARefusedCall() throws Throwable {
        assumeTrue("placement can not be read here", NumaMemory.isSupported());
        assertEquals(0, new LinuxPages(LinuxPages.movePagesNumber(System.getProperty("os.arch"))).probe());
        assertEquals("ENOSYS", 38, new LinuxPages(999_999).probe());
    }

    @Test
    public void refusedCallIsNoAnswer() {
        assumeTrue("placement can not be read here", NumaMemory.isSupported());
        assertEquals(null, new LinuxPages(999_999).nodesOf(new long[]{PAGE}));
    }

    @Test
    public void reportsNativeMemory() {
        assumeTrue("placement can not be read here", NumaMemory.isSupported());
        try (Arena arena = Arena.ofConfined()) {
            final NumaMemory.Placement placement = NumaMemory.nodesOf(arena.allocate(MB)); // zeroed = touched
            assertTrue(placement.toString(), placement.sampledPages() >= 256);
            assertEquals(placement.toString(), 0, placement.absentPages());
        }
    }

    @Test
    public void reportsAnArray() {
        assumeTrue("placement can not be read here", NumaMemory.isSupported());
        final long[] array = new long[(int) (16 * MB / 8)];
        Arrays.fill(array, 1);
        final NumaMemory.Placement placement = NumaMemory.nodesOf(MemorySegment.ofArray(array));
        assertTrue(placement.toString(), placement.sampledPages() >= 4096);
        assertEquals(placement.toString(), 0, placement.absentPages());
    }

    @Test
    public void untouchedPagesAreAbsent() throws Throwable {
        assumeTrue("placement can not be read here", NumaMemory.isSupported());
        final MemorySegment memory = mmap(MB);
        assertEquals(256, NumaMemory.nodesOf(memory).absentPages());
        memory.set(JAVA_BYTE, 0, (byte) 1);
        assertEquals(255, NumaMemory.nodesOf(memory).absentPages());
    }

    /**
     * Binds pages to each node in turn and expects to find them there: only on a machine of more than one node.
     */
    @Test
    public void followsABinding() throws Throwable {
        assumeTrue("placement can not be read here", NumaMemory.isSupported());
        assumeTrue("needs x86-64 to bind", "amd64".equals(System.getProperty("os.arch")));
        final NumaTopology topology = NumaTopology.current();
        assumeTrue("needs two nodes", topology.nodes().cardinality() >= 2);
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            final MemorySegment memory = mmap(MB);
            assertEquals(0, mbind(memory, node));
            memory.fill((byte) 1);
            final NumaMemory.Placement placement = NumaMemory.nodesOf(memory);
            assertTrue("node " + node + ": " + placement, placement.isOnlyOn(node));
        }
    }

    /**
     * Fresh anonymous memory, page-aligned, no page present yet; never unmapped, a test process is short.
     */
    private static MemorySegment mmap(long bytes) throws Throwable {
        final Linker linker = Linker.nativeLinker();
        final MethodHandle mmap = linker.downcallHandle(linker.defaultLookup().find("mmap").orElseThrow(),
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_INT, JAVA_INT, JAVA_INT, JAVA_LONG));
        // PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS
        final long address = (long) mmap.invokeExact(0L, bytes, 3, 0x22, -1, 0L);
        assertTrue(address != -1);
        return MemorySegment.ofAddress(address).reinterpret(bytes);
    }

    private static long mbind(MemorySegment memory, int node) throws Throwable {
        final Linker linker = Linker.nativeLinker();
        final MethodHandle syscall = linker.downcallHandle(linker.defaultLookup().find("syscall").orElseThrow(),
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG),
                Linker.Option.firstVariadicArg(1));
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment nodeMask = arena.allocateFrom(JAVA_LONG, 1L << node);
            // long mbind(void *addr, unsigned long len, int mode (MPOL_BIND), nodemask, maxnode, flags)
            return (long) syscall.invokeExact(237L, memory.address(), memory.byteSize(), 2L, nodeMask.address(), 64L, 0L);
        }
    }
}
