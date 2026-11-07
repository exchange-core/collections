package exchange.core2.collections.affinity;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
        assumeTrue("reserved CPUs configured", nothingAllowed());
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
        assumeTrue("reserved CPUs configured", nothingAllowed());
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

    @Test
    public void warnsOnceAboutANode() {
        CpuAffinity.WARNED_NODES.clear();
        try (CpuAffinity affinity = CpuAffinity.acquireCore(0)) {
            final int elsewhere = affinity.node() + 1;
            final List<String> warnings = warningsDuring(() -> {
                assertSame(affinity, CpuAffinity.warnIfOnOtherNode(elsewhere, affinity));
                CpuAffinity.warnIfOnOtherNode(elsewhere, affinity);
            });
            assertEquals(warnings.toString(), 1, warnings.size());
            assertTrue(warnings.get(0), warnings.get(0).contains(
                    "pinned to cpu 0 on node " + affinity.node() + ": no free core on node " + elsewhere + ", reserved: "));
        }
    }

    @Test
    public void noWarningOnTheNodeAskedFor() {
        CpuAffinity.WARNED_NODES.clear();
        try (CpuAffinity affinity = CpuAffinity.acquireCore(0)) {
            assertEquals(List.of(), warningsDuring(() -> CpuAffinity.warnIfOnOtherNode(affinity.node(), affinity)));
        }
    }

    @Test
    public void noWarningAboutANotPinnedHandle() {
        CpuAffinity.WARNED_NODES.clear();
        try (CpuAffinity notPinned = CpuAffinity.acquireCoreOnNode(1 << 16)) {
            assertEquals(List.of(), warningsDuring(() -> CpuAffinity.warnIfOnOtherNode(0, notPinned)));
        }
    }

    @Test
    public void factoryNamesItsThreadsAndRunsTheTaskPinnedOrNot() throws InterruptedException {
        final ThreadFactory factory = CpuAffinity.threadFactory("group", 1 << 16);
        final AtomicReference<String> name = new AtomicReference<>();
        final Thread first = factory.newThread(() -> name.set(Thread.currentThread().getName()));
        assertEquals("group-0", first.getName());
        assertEquals("group-1", factory.newThread(() -> {
        }).getName());
        first.start();
        first.join();
        assertEquals("group-0", name.get());
    }

    @Test
    public void factoryRefusesANegativeNode() {
        assertThrows(IllegalArgumentException.class, () -> CpuAffinity.threadFactory("group", -1));
    }

    /**
     * The tests below need reserved CPUs: isolcpus or -Daffinity.reserved.
     */
    @Test
    public void factoryPinsItsThreadsOnTheNodeAndReleasesAtTheEnd() throws InterruptedException {
        assumeTrue("no reserved CPUs", !nothingAllowed());
        assumeTrue("reads the mask of the thread", LINUX);
        final NumaTopology topology = NumaTopology.current();
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            if (!topology.cpusOf(node).intersects(allowed())) continue;
            final int cpu = cpuOfAThreadOf(CpuAffinity.threadFactory("group", node));
            assertEquals(node, topology.nodeOfCpu(cpu));
            assertTrue(String.valueOf(cpu), allowed().get(cpu));
            final Throwable error = inOtherThread(() -> {
                try (CpuAffinity again = CpuAffinity.acquireCore(cpu)) {
                    assertTrue(again.toString(), again.isPinned());
                }
            });
            assertNull(String.valueOf(error), error);
        }
    }

    @Test
    public void factoryWithoutANodePinsAsAcquireCore() throws InterruptedException {
        assumeTrue("no reserved CPUs", !nothingAllowed());
        assumeTrue("reads the mask of the thread", LINUX);
        final int cpu = cpuOfAThreadOf(CpuAffinity.threadFactory("group"));
        assertTrue(String.valueOf(cpu), allowed().get(cpu));
    }

    /**
     * Takes every core it is allowed to, so only where the CPUs were named for the test: on a tuned machine isolcpus
     * covers cores someone may be measuring on.
     */
    @Test
    public void fillsOneNodeBeforeTheNext() {
        assumeTrue("needs -Daffinity.reserved", System.getProperty("affinity.reserved") != null);
        assumeTrue("no reserved CPUs", !nothingAllowed());
        CpuAffinity.WARNED_NODES.clear();
        final List<Integer> cpus = new ArrayList<>();
        final List<String> warnings = warningsDuring(() -> cpus.addAll(takeEveryCore()));
        assertFalse("nothing could be taken", cpus.isEmpty());

        final NumaTopology topology = NumaTopology.current();
        final BitSet reserved = CpuAffinity.reservedCpus();
        final int firstNode = firstNode();
        final List<Integer> nodes = cpus.stream().map(topology::nodeOfCpu).toList();
        final List<Integer> visited = nodes.stream().distinct().toList();
        if (topology.cpusOf(firstNode).intersects(allowed())) {
            assertEquals(cpus.toString(), firstNode, (int) nodes.get(0));
        }
        int runs = 1;
        for (int i = 1; i < cpus.size(); i++) {
            if (!nodes.get(i).equals(nodes.get(i - 1))) {
                runs++;
            } else if (reserved.get(cpus.get(i))) {
                assertTrue("a spare CPU before a reserved one: " + cpus, reserved.get(cpus.get(i - 1)));
            }
        }
        assertEquals("a node was come back to: " + cpus, visited.size(), runs);
        final long otherNodes = visited.stream().filter(node -> node != firstNode).count();
        final long left = warnings.stream().filter(warning -> warning.contains(" pinned to cpu ")).count();
        assertEquals(warnings.toString(), otherNodes, left);
    }

    @Test
    public void memoryIsCheckedOnceOnTheNodeFilledFirst() {
        final int node = Math.max(firstNode(), 0);
        CpuAffinity.MEMORY_CHECKED.clear();
        final List<String> expected = ProcessPlacement.current().warningsFor(node);
        final List<String> warnings = warningsDuring(() -> {
            CpuAffinity.warnAboutMemory(node);
            CpuAffinity.warnAboutMemory(node);
        });
        assertEquals(warnings.toString(), expected.size(), warnings.size());
    }

    @Test
    public void otherNodesHearOnlyIfTheyHaveNoMemory() {
        final NumaTopology topology = NumaTopology.current();
        final int first = Math.max(firstNode(), 0);
        CpuAffinity.MEMORY_CHECKED.clear();
        CpuAffinity.warnAboutMemory(first);
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            if (node == first || topology.memoryOf(node).total() == 0) continue;
            final int other = node;
            assertEquals(List.of(), warningsDuring(() -> CpuAffinity.warnAboutMemory(other)));
        }
    }

    @Test
    public void pinningChecksTheMemory() {
        assumeTrue("no reserved CPUs", !nothingAllowed());
        final int node = firstNode();
        assumeTrue("nothing allowed on the first node", NumaTopology.current().cpusOf(node).intersects(allowed()));
        CpuAffinity.MEMORY_CHECKED.clear();
        final List<String> expected = ProcessPlacement.current().warningsFor(node);
        final List<String> warnings = warningsDuring(() -> {
            try (CpuAffinity affinity = CpuAffinity.acquireCoreOnNode(node)) {
                assertTrue(affinity.toString(), affinity.isPinned());
            }
        });
        assertEquals(warnings.toString(), expected.size(), warnings.size());
    }

    /**
     * With -Daffinity.offHeap larger than any node.
     */
    @Test
    public void theDeclaredOffHeapCounts() {
        assumeTrue("needs -Daffinity.offHeap", System.getProperty("affinity.offHeap") != null);
        final ProcessPlacement placement = ProcessPlacement.current();
        final int node = NumaTopology.current().nodeOfCpu(placement.cpus().nextSetBit(0));
        final List<String> warnings = placement.warningsFor(node);
        assertTrue(warnings.toString(), warnings.stream().anyMatch(warning -> warning.contains("has not enough memory")));
    }

    @Test
    public void noNodeNoCheck() {
        CpuAffinity.MEMORY_CHECKED.clear();
        CpuAffinity.warnAboutMemory(-1);
        assertTrue(CpuAffinity.MEMORY_CHECKED.isEmpty());
    }

    @Test
    public void spareCpusAreOutsideTheReservedOnes() {
        assertFalse(CpuAffinity.spareCpus().toString(), CpuAffinity.spareCpus().intersects(CpuAffinity.reservedCpus()));
    }

    /**
     * A thread pinned by number on a node where nothing is allowed: the nearest core is on another node.
     */
    @Test
    public void nearFromANodeOfNoAllowedCpusWarnsAboutWhereItLanded() throws InterruptedException {
        assumeTrue("no reserved CPUs", !nothingAllowed());
        final NumaTopology topology = NumaTopology.current();
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            if (topology.cpusOf(node).intersects(allowed()) || topology.cpusOf(node).isEmpty()) continue;
            CpuAffinity.WARNED_NODES.clear();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final List<String> warnings;
            try (CpuAffinity first = CpuAffinity.acquireCore(topology.cpusOf(node).nextSetBit(0))) {
                warnings = warningsDuring(() -> error.set(nearInOtherThread(first)));
            }
            assertNull(String.valueOf(error.get()), error.get());
            final List<String> left = warnings.stream().filter(warning -> warning.contains(" pinned to cpu ")).toList();
            assertEquals(warnings.toString(), 1, left.size());
            assertTrue(left.get(0), left.get(0).contains("no free core on node " + node));
        }
    }

    private static Throwable nearInOtherThread(CpuAffinity first) {
        try {
            return inOtherThread(() -> {
                try (CpuAffinity near = CpuAffinity.acquireCoreNear(first)) {
                    assertTrue(near.toString(), near.isPinned());
                    assertFalse(near.toString(), first.node() == near.node());
                }
            });
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void nearTakesASpareCoreOfItsNode() throws InterruptedException {
        final NumaTopology topology = NumaTopology.current();
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            final BitSet spare = topology.cpusOf(node);
            spare.and(CpuAffinity.spareCpus());
            if (topology.cpusOf(node).intersects(CpuAffinity.reservedCpus()) || spare.cardinality() < 2) continue;
            try (CpuAffinity first = CpuAffinity.acquireCoreOnNode(node)) {
                spare.andNot(first.core());
                assumeTrue("one spare core on the node", !spare.isEmpty());
                final Throwable error = inOtherThread(() -> {
                    try (CpuAffinity near = CpuAffinity.acquireCoreNear(first)) {
                        assertEquals(near.toString(), first.node(), near.node());
                    }
                });
                assertNull(String.valueOf(error), error);
            }
        }
    }

    @Test
    public void onANodeOfSpareCpusOnlyTakesThem() {
        final NumaTopology topology = NumaTopology.current();
        for (int node = topology.nodes().nextSetBit(0); node >= 0; node = topology.nodes().nextSetBit(node + 1)) {
            final BitSet cpus = topology.cpusOf(node);
            if (cpus.intersects(CpuAffinity.reservedCpus()) || !cpus.intersects(CpuAffinity.spareCpus())) continue;
            try (CpuAffinity affinity = CpuAffinity.acquireCoreOnNode(node)) {
                assertTrue(affinity.toString(), affinity.isPinned());
                assertEquals(node, affinity.node());
                assertTrue(affinity.toString(), CpuAffinity.spareCpus().get(affinity.cpu()));
            }
        }
    }

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
            if (topology.cpusOf(node).intersects(reserved) || topology.cpusOf(node).intersects(CpuAffinity.spareCpus())) continue;
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

    @Test
    public void malformedMaskIsIgnored() {
        final String name = "affinity.test.mask";
        try {
            assertNull(CpuAffinity.maskProperty(name));
            System.setProperty(name, "AAAA");
            assertEquals(CpuSetsTest.bits(1, 3, 5, 7, 9, 11, 13, 15), CpuAffinity.maskProperty(name));
            for (String malformed : new String[]{"5,7,9", "0xZZ"}) {
                System.setProperty(name, malformed);
                final List<String> warnings = warningsDuring(() -> assertNull(CpuAffinity.maskProperty(name)));
                assertEquals(List.of("-D" + name + "=" + malformed + " is not a hex mask such as AAAA: ignored"), warnings);
            }
        } finally {
            System.clearProperty(name);
        }
    }

    @Test
    public void firstNodeMustBeANode() {
        final NumaTopology topology = NumaTopology.current();
        final BitSet cpu0 = CpuSetsTest.bits(0);
        assertEquals(List.of(), warningsDuring(() -> {
            assertEquals(0, CpuAffinity.firstNode(" 0 ", topology, new BitSet(), new BitSet()));
            assertEquals(topology.nodeOfCpu(0), CpuAffinity.firstNode(null, topology, cpu0, new BitSet()));
            assertEquals(topology.nodeOfCpu(0), CpuAffinity.firstNode(null, topology, new BitSet(), cpu0));
            assertEquals(-1, CpuAffinity.firstNode(null, topology, new BitSet(), new BitSet()));
        }));
        for (String malformed : new String[]{"one", "-1", "4096"}) {
            final List<String> warnings = warningsDuring(() ->
                    assertEquals(topology.nodeOfCpu(0), CpuAffinity.firstNode(malformed, topology, cpu0, new BitSet())));
            assertEquals(List.of("-Daffinity.firstNode=" + malformed + " is not a node of this machine ("
                    + CpuSets.toList(topology.nodes()) + "): ignored"), warnings);
        }
    }

    /**
     * The node acquireCore() starts on, -1 if nothing is allowed and no node asked for.
     */
    private static int firstNode() {
        final BitSet reserved = CpuAffinity.reservedCpus();
        final BitSet highestFrom = reserved.isEmpty() ? CpuAffinity.spareCpus() : reserved;
        final Integer asked = Integer.getInteger("affinity.firstNode");
        return asked != null && asked >= 0 && NumaTopology.current().nodes().get(asked)
                ? asked : NumaTopology.current().nodeOfCpu(highestFrom.length() - 1);
    }

    private static boolean nothingAllowed() {
        return allowed().isEmpty();
    }

    private static BitSet allowed() {
        final BitSet allowed = CpuAffinity.reservedCpus();
        allowed.or(CpuAffinity.spareCpus());
        return allowed;
    }

    /**
     * The CPUs acquireCore() gives, in that order, one thread on each until it pins no more. All are released at the end.
     */
    private static List<Integer> takeEveryCore() {
        final CountDownLatch release = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        final List<Integer> cpus = new ArrayList<>();
        try {
            while (true) {
                final BlockingQueue<Integer> taken = new ArrayBlockingQueue<>(1);
                final Thread thread = new Thread(() -> {
                    try (CpuAffinity affinity = CpuAffinity.acquireCore()) {
                        taken.add(affinity.cpu());
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                thread.start();
                threads.add(thread);
                final int cpu = taken.take();
                if (cpu < 0) return cpus;
                cpus.add(cpu);
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            release.countDown();
            threads.forEach(CpuAffinityTest::join);
        }
    }

    private static void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The CPU a thread of this factory runs on: the mask must be that one CPU.
     */
    private static int cpuOfAThreadOf(ThreadFactory factory) throws InterruptedException {
        final AtomicReference<String> mask = new AtomicReference<>();
        final Thread thread = factory.newThread(() -> {
            try {
                mask.set(allowedCpus());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        thread.start();
        thread.join();
        return Integer.parseInt(mask.get());
    }

    /**
     * What the class logs as warnings meanwhile, from any thread.
     */
    private static List<String> warningsDuring(Runnable action) {
        final List<String> warnings = new CopyOnWriteArrayList<>();
        final Handler handler = new Handler() {
            @Override
            public void publish(LogRecord entry) {
                if (entry.getLevel() == Level.WARNING) warnings.add(new SimpleFormatter().formatMessage(entry));
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        final Logger logger = Logger.getLogger(CpuAffinity.class.getName());
        logger.addHandler(handler);
        try {
            action.run();
        } finally {
            logger.removeHandler(handler);
        }
        return warnings;
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
