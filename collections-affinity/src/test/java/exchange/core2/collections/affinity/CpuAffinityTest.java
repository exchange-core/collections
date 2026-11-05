package exchange.core2.collections.affinity;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Pins real threads: runs where the platform is supported (Linux, Windows).
 */
public class CpuAffinityTest {

    private static final boolean LINUX = System.getProperty("os.name", "").startsWith("Linux");

    @Before
    public void supported() {
        assumeTrue("thread affinity not supported here", CpuAffinity.isSupported());
    }

    @Test
    public void pinsAndRestores() throws IOException {
        final String before = allowedCpus();
        try (CpuAffinity affinity = CpuAffinity.acquireCore(0)) {
            assertTrue(affinity.toString(), affinity.isPinned());
            assertEquals(0, affinity.cpu());
            assertTrue(affinity.core().get(0));
            if (LINUX) assertEquals("0", allowedCpus());
        }
        assertEquals(before, allowedCpus());
    }

    @Test
    public void coreIsReservedUntilClosed() {
        try (CpuAffinity ignore = CpuAffinity.acquireCore(0)) {
            assertThrows(IllegalStateException.class, () -> CpuAffinity.acquireCore(0));
        }
        try (CpuAffinity again = CpuAffinity.acquireCore(0)) {
            assertTrue(again.isPinned());
        }
    }

    @Test
    public void siblingIsReservedWithItsCore() throws InterruptedException {
        try (CpuAffinity affinity = CpuAffinity.acquireCore(0)) {
            final int sibling = affinity.core().nextSetBit(1);
            assumeTrue("no hyper-threading sibling of cpu 0", sibling > 0);
            final Throwable core = inOtherThread(() -> CpuAffinity.acquireCore(sibling));
            assertTrue(String.valueOf(core), core instanceof IllegalStateException);
            final Throwable cpu = inOtherThread(() -> CpuAffinity.acquireCpu(sibling));
            assertTrue(String.valueOf(cpu), cpu instanceof IllegalStateException);
        }
    }

    @Test
    public void cpuPinsAndRestores() throws IOException {
        final String before = allowedCpus();
        final BitSet onlyCpu0 = new BitSet();
        onlyCpu0.set(0);
        try (CpuAffinity affinity = CpuAffinity.acquireCpu(0)) {
            assertTrue(affinity.toString(), affinity.isPinned());
            assertEquals(0, affinity.cpu());
            assertEquals(onlyCpu0, affinity.core());
            if (LINUX) assertEquals("0", allowedCpus());
        }
        assertEquals(before, allowedCpus());
    }

    @Test
    public void cpuIsReservedUntilClosed() {
        try (CpuAffinity ignore = CpuAffinity.acquireCpu(0)) {
            assertThrows(IllegalStateException.class, () -> CpuAffinity.acquireCpu(0));
            assertThrows(IllegalStateException.class, () -> CpuAffinity.acquireCore(0));
        }
        try (CpuAffinity again = CpuAffinity.acquireCore(0)) {
            assertTrue(again.isPinned());
        }
    }

    @Test
    public void siblingsCanBeHeldSeparately() throws InterruptedException {
        final int sibling;
        try (CpuAffinity whole = CpuAffinity.acquireCore(0)) {
            sibling = whole.core().nextSetBit(1);
        }
        assumeTrue("no hyper-threading sibling of cpu 0", sibling > 0);
        try (CpuAffinity first = CpuAffinity.acquireCpu(0)) {
            assertTrue(first.toString(), first.isPinned());
            final Throwable error = inOtherThread(() -> {
                try (CpuAffinity second = CpuAffinity.acquireCpu(sibling)) {
                    assertTrue(second.toString(), second.isPinned());
                    assertEquals(sibling, second.cpu());
                }
            });
            assertNull(String.valueOf(error), error);
            // the sibling is free again, its core is not: cpu 0 is held
            final Throwable core = inOtherThread(() -> CpuAffinity.acquireCore(sibling));
            assertTrue(String.valueOf(core), core instanceof IllegalStateException);
        }
    }

    @Test
    public void notPinnedWhenNothingIsReserved() {
        assumeTrue("reserved CPUs configured", CpuAffinity.reservedCpus().isEmpty());
        try (CpuAffinity affinity = CpuAffinity.acquireCore()) {
            assertFalse(affinity.isPinned());
            assertEquals(-1, affinity.cpu());
        }
    }

    @Test
    public void knowsItsNode() {
        try (CpuAffinity affinity = CpuAffinity.acquireCore(0)) {
            assertEquals(NumaTopology.current().nodeOfCpu(0), affinity.node());
            assertTrue(affinity.toString(), affinity.toString().startsWith("cpu 0 on node " + affinity.node()));
        }
    }

    @Test
    public void notPinnedHasNoNode() {
        try (CpuAffinity affinity = CpuAffinity.acquireCoreOnNode(1 << 16)) {
            assertFalse(affinity.isPinned());
            assertEquals(-1, affinity.node());
            assertTrue(affinity.toString(), affinity.toString().contains("node 65536"));
        }
    }

    @Test
    public void negativeNodeIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> CpuAffinity.acquireCoreOnNode(-1));
    }

    @Test
    public void nearANotPinnedHandleAsAcquireCore() {
        assumeTrue("reserved CPUs configured", CpuAffinity.reservedCpus().isEmpty());
        try (CpuAffinity notPinned = CpuAffinity.acquireCore(); CpuAffinity affinity = CpuAffinity.acquireCoreNear(notPinned)) {
            assertFalse(affinity.isPinned());
        }
    }

    /**
     * CPU 5000 can not be pinned to anywhere.
     */
    @Test
    public void aFailedPinMovesOnToTheNextCpu() {
        try (CpuAffinity affinity = CpuAffinity.firstFree(new int[]{5000, 0})) {
            assertTrue(affinity.toString(), affinity.isPinned());
            assertEquals(0, affinity.cpu());
        }
    }

    @Test
    public void onlyFailedPinsLeaveTheReason() {
        try (CpuAffinity affinity = CpuAffinity.firstFree(new int[]{5000})) {
            assertFalse(affinity.isPinned());
            assertTrue(affinity.toString(), affinity.toString().contains("cpu 5000"));
        }
    }

    @Test
    public void takenCpusAreNotAFailure() throws InterruptedException {
        try (CpuAffinity ignore = CpuAffinity.acquireCore(0)) {
            final Throwable error = inOtherThread(() -> assertNull(CpuAffinity.firstFree(new int[]{0})));
            assertNull(String.valueOf(error), error);
        }
    }

    /**
     * The tests below need reserved CPUs: isolcpus or -Daffinity.reserved.
     */
    @Test
    public void onANodeIsOnThatNode() {
        assumeTrue("no reserved CPUs", !CpuAffinity.reservedCpus().isEmpty());
        final BitSet reserved = CpuAffinity.reservedCpus();
        final NumaTopology topology = NumaTopology.current();
        final int node = topology.nodeOfCpu(reserved.length() - 1);
        try (CpuAffinity affinity = CpuAffinity.acquireCoreOnNode(node)) {
            assertTrue(affinity.toString(), affinity.isPinned());
            assertEquals(node, affinity.node());
            assertTrue(affinity.toString(), reserved.get(affinity.cpu()));
        }
    }

    @Test
    public void nodeWithoutFreeReservedCoreIsNotPinned() {
        assumeTrue("no reserved CPUs", !CpuAffinity.reservedCpus().isEmpty());
        final BitSet reserved = CpuAffinity.reservedCpus();
        final NumaTopology topology = NumaTopology.current();
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            if (topology.cpusOf(node).intersects(reserved)) continue;
            try (CpuAffinity affinity = CpuAffinity.acquireCoreOnNode(node)) {
                assertFalse(affinity.toString(), affinity.isPinned());
            }
        }
    }

    @Test
    public void nearIsOnTheSameNodeWhileItHasCores() throws InterruptedException {
        assumeTrue("no reserved CPUs", !CpuAffinity.reservedCpus().isEmpty());
        try (CpuAffinity first = CpuAffinity.acquireCore()) {
            assertTrue(first.toString(), first.isPinned());
            final BitSet others = NumaTopology.current().cpusOf(first.node());
            others.and(CpuAffinity.reservedCpus());
            others.andNot(first.core());
            assumeTrue("one reserved core on the node", !others.isEmpty());
            final Throwable error = inOtherThread(() -> {
                try (CpuAffinity second = CpuAffinity.acquireCoreNear(first)) {
                    assertTrue(second.toString(), second.isPinned());
                    assertEquals(first.node(), second.node());
                    assertFalse(second.toString(), first.core().get(second.cpu()));
                }
            });
            assertNull(String.valueOf(error), error);
        }
    }

    /**
     * Linux: the mask of the calling thread as the kernel reports it; elsewhere a constant.
     */
    private static String allowedCpus() throws IOException {
        if (!LINUX) return "";
        return Files.readAllLines(Path.of("/proc/thread-self/status")).stream()
                .filter(l -> l.startsWith("Cpus_allowed_list:"))
                .map(l -> l.substring(l.indexOf(':') + 1).trim())
                .findFirst().orElseThrow();
    }

    private static Throwable inOtherThread(Runnable action) throws InterruptedException {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread other = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable e) {
                error.set(e);
            }
        });
        other.start();
        other.join();
        return error.get();
    }
}
