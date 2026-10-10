package exchange.core2.collections.affinity;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final Thread other = new Thread(() -> error.set(catchThrowable(() -> CpuAffinity.acquireCore(sibling))));
            other.start();
            other.join();
            assertTrue(String.valueOf(error.get()), error.get() instanceof IllegalStateException);
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

    private static Throwable catchThrowable(Runnable action) {
        try {
            action.run();
            return null;
        } catch (Throwable e) {
            return e;
        }
    }
}
