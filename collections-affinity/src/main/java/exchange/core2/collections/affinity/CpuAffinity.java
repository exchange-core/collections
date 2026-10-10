package exchange.core2.collections.affinity;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;

/**
 * Pins the calling thread to one CPU and reserves the whole physical core (all its hyper-threading siblings) for
 * it, within this process and - through lock files - across processes. Linux and Windows; elsewhere, or without
 * native access, nothing is pinned and the returned handle says so. Uses the Foreign Function and Memory API:
 * Java 22+, run with {@code --enable-native-access=ALL-UNNAMED} (or this module's name) to avoid the JDK's
 * restricted-method warning. Works in a GraalVM native image (the jar carries its reachability metadata).
 * <pre>
 * try (CpuAffinity ignore = CpuAffinity.acquireCore()) {
 *     // hot loop
 * }
 * </pre>
 * Reserved CPUs - those {@link #acquireCore()} chooses from, highest first:
 * <ul>
 *     <li>{@code -Daffinity.reserved=<hex mask>}, e.g. {@code AAAA} for CPUs 1,3,..,15 (as OpenHFT Affinity);</li>
 *     <li>otherwise Linux's isolated CPUs ({@code /sys/devices/system/cpu/isolated}, set by isolcpus);
 *     none on Windows.</li>
 * </ul>
 * Lock files go to {@code -Daffinity.lockDir} (default: java.io.tmpdir).
 * <p>
 * Threads created by a pinned thread inherit its single-CPU mask: create helper threads before pinning, or pin
 * them to cores of their own.
 */
public final class CpuAffinity implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CpuAffinity.class.getName());

    private static final AffinityPlatform PLATFORM = AffinityPlatform.detect(LOG);
    private static final BitSet RESERVED = reserved();
    private static final CpuLockFiles LOCK_FILES = new CpuLockFiles(
            Path.of(System.getProperty("affinity.lockDir", System.getProperty("java.io.tmpdir"))));
    /**
     * Logical CPUs held by the handles of this process. Guards the acquisition.
     */
    private static final BitSet TAKEN = new BitSet();

    private final int cpu;
    private final BitSet core;
    private final BitSet previous;
    private final List<FileChannel> lockFiles;
    private final Thread owner;
    private final String notPinnedReason;
    private boolean closed;

    private CpuAffinity(int cpu, BitSet core, BitSet previous, List<FileChannel> lockFiles, String notPinnedReason) {
        this.cpu = cpu;
        this.core = core;
        this.previous = previous;
        this.lockFiles = lockFiles;
        this.owner = Thread.currentThread();
        this.notPinnedReason = notPinnedReason;
    }

    /**
     * Pins the calling thread to the highest reserved CPU whose core is free. Never throws: if no core is free, no
     * CPU is reserved or the platform is not supported, the handle is not pinned ({@link #isPinned()}).
     */
    public static CpuAffinity acquireCore() {
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        synchronized (TAKEN) {
            for (int cpu = RESERVED.length() - 1; cpu >= 0; cpu = RESERVED.previousSetBit(cpu - 1)) {
                final CpuAffinity affinity = tryAcquire(cpu);
                if (affinity != null) return affinity;
            }
        }
        return notPinned(RESERVED.isEmpty()
                ? "no reserved CPUs (isolcpus or -Daffinity.reserved)"
                : "all reserved cores are taken: " + CpuSets.toList(RESERVED));
    }

    /**
     * Pins the calling thread to this CPU (reserved or not) and reserves its core. If pinning fails or the platform
     * is not supported, the handle is not pinned.
     *
     * @throws IllegalStateException if the core is held in this or another process
     */
    public static CpuAffinity acquireCore(int cpu) {
        if (cpu < 0) throw new IllegalArgumentException("cpu " + cpu);
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        synchronized (TAKEN) {
            final CpuAffinity affinity = tryAcquire(cpu);
            if (affinity == null) throw new IllegalStateException("core of cpu " + cpu + " is taken");
            return affinity;
        }
    }

    /**
     * Holding TAKEN: null if the core is taken, a not pinned handle if pinning failed.
     */
    private static CpuAffinity tryAcquire(int cpu) {
        final BitSet core = PLATFORM.coreOf(cpu);
        if (core.intersects(TAKEN)) return null;
        final List<FileChannel> locks = LOCK_FILES.tryLock(core, Thread.currentThread().getName());
        if (locks == null) return null;
        final BitSet previous = PLATFORM.pin(cpu);
        if (previous == null) {
            CpuLockFiles.release(locks);
            return notPinned("cannot pin to cpu " + cpu);
        }
        TAKEN.or(core);
        LOG.log(System.Logger.Level.DEBUG, "{0} pinned to cpu {1}, core {2}",
                Thread.currentThread().getName(), cpu, CpuSets.toList(core));
        return new CpuAffinity(cpu, core, previous, locks, null);
    }

    private static CpuAffinity notPinned(String reason) {
        LOG.log(System.Logger.Level.WARNING, "{0} not pinned: {1}", Thread.currentThread().getName(), reason);
        return new CpuAffinity(-1, new BitSet(), null, List.of(), reason);
    }

    private static BitSet reserved() {
        final String mask = System.getProperty("affinity.reserved");
        if (mask != null) return CpuSets.parseHexMask(mask);
        return PLATFORM != null ? PLATFORM.isolatedCpus() : new BitSet();
    }

    /**
     * The CPU the thread is pinned to, -1 if not pinned.
     */
    public int cpu() {
        return cpu;
    }

    /**
     * The logical CPUs reserved with it (its physical core), empty if not pinned.
     */
    public BitSet core() {
        return (BitSet) core.clone();
    }

    public boolean isPinned() {
        return cpu >= 0;
    }

    /**
     * The CPUs {@link #acquireCore()} chooses from.
     */
    public static BitSet reservedCpus() {
        return (BitSet) RESERVED.clone();
    }

    /**
     * Whether this OS and runtime can pin threads at all.
     */
    public static boolean isSupported() {
        return PLATFORM != null;
    }

    /**
     * Restores the thread's previous affinity and releases the core. Call it from the pinned thread: from another
     * thread the core is released but the pinned thread keeps its mask.
     */
    @Override
    public void close() {
        if (cpu < 0) return;
        synchronized (TAKEN) {
            if (closed) return;
            closed = true;
        }
        if (Thread.currentThread() == owner) {
            PLATFORM.restore(previous);
        } else {
            LOG.log(System.Logger.Level.WARNING, "cpu {0} released by {1}: {2} keeps its mask",
                    cpu, Thread.currentThread().getName(), owner.getName());
        }
        CpuLockFiles.release(lockFiles);
        synchronized (TAKEN) {
            TAKEN.andNot(core);
        }
    }

    @Override
    public String toString() {
        return isPinned() ? "cpu " + cpu + " (core " + CpuSets.toList(core) + ")" : "not pinned: " + notPinnedReason;
    }
}
