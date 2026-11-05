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
 * {@link #acquireCpu(int)} reserves only the logical CPU, for threads that are meant to share a core.
 * {@link #acquireCoreOnNode(int)} and {@link #acquireCoreNear(CpuAffinity)} choose by NUMA node - see
 * {@link NumaTopology}.
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
    /**
     * The order of {@link #acquireCore()}: nearest to the highest reserved CPU, so that threads stay on one NUMA node
     * as long as it has cores, even where the OS numbers the CPUs of the sockets in turn.
     */
    private static final int[] RESERVED_ORDER = NumaTopology.current().nearestFirst(RESERVED.length() - 1, RESERVED);
    private static final CpuLockFiles LOCK_FILES = new CpuLockFiles(
            Path.of(System.getProperty("affinity.lockDir", System.getProperty("java.io.tmpdir"))));
    /**
     * Logical CPUs held by the handles of this process. Guards the acquisition.
     */
    private static final BitSet TAKEN = new BitSet();

    private final int cpu;
    private final BitSet held;
    private final BitSet previous;
    private final List<FileChannel> lockFiles;
    private final Thread owner;
    private final String notPinnedReason;
    private boolean closed;

    private CpuAffinity(int cpu, BitSet held, BitSet previous, List<FileChannel> lockFiles, String notPinnedReason) {
        this.cpu = cpu;
        this.held = held;
        this.previous = previous;
        this.lockFiles = lockFiles;
        this.owner = Thread.currentThread();
        this.notPinnedReason = notPinnedReason;
    }

    /**
     * Pins the calling thread to the first reserved CPU whose core is free - the highest CPU first, one NUMA node
     * before the next. Never throws: if no core is free, no CPU is reserved or the platform is not supported, the
     * handle is not pinned ({@link #isPinned()}).
     */
    public static CpuAffinity acquireCore() {
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        final CpuAffinity affinity = firstFree(RESERVED_ORDER);
        return affinity != null ? affinity : noReservedCore();
    }

    /**
     * Pins the calling thread to the highest reserved CPU of this NUMA node whose core is free. Never throws for that:
     * if there is none or the platform is not supported, the handle is not pinned ({@link #isPinned()}).
     *
     * @throws IllegalArgumentException if the node is negative
     */
    public static CpuAffinity acquireCoreOnNode(int node) {
        if (node < 0) throw new IllegalArgumentException("node " + node);
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        final NumaTopology topology = NumaTopology.current();
        final BitSet cpus = topology.cpusOf(node);
        cpus.and(RESERVED);
        final CpuAffinity affinity = firstFree(topology.nearestFirst(cpus.length() - 1, cpus));
        return affinity != null
                ? affinity
                : notPinned("no free reserved core on node " + node + ", reserved: " + CpuSets.toList(RESERVED));
    }

    /**
     * Pins the calling thread to the free reserved core nearest to this handle's: sharing its last-level cache, then
     * on its NUMA node, then on the other nodes by distance - {@link #node()} tells where it landed. If that handle is
     * not pinned, as {@link #acquireCore()}. Never throws: if no core is free, the handle is not pinned.
     */
    public static CpuAffinity acquireCoreNear(CpuAffinity other) {
        if (!other.isPinned()) return acquireCore();
        final CpuAffinity affinity = firstFree(NumaTopology.current().nearestFirst(other.cpu, RESERVED));
        return affinity != null ? affinity : noReservedCore();
    }

    /**
     * The first of these CPUs whose core is free and which can be pinned to (a cpuset can forbid one). Null if all are
     * taken, a not pinned handle saying why if pinning failed and nothing else was free.
     */
    static CpuAffinity firstFree(int[] cpus) {
        CpuAffinity failed = null;
        synchronized (TAKEN) {
            for (int cpu : cpus) {
                final CpuAffinity affinity = tryAcquire(cpu, PLATFORM.coreOf(cpu));
                if (affinity != null && affinity.isPinned()) return affinity;
                if (affinity != null) failed = affinity;
            }
        }
        return failed;
    }

    private static CpuAffinity noReservedCore() {
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
            final CpuAffinity affinity = tryAcquire(cpu, PLATFORM.coreOf(cpu));
            if (affinity == null) throw new IllegalStateException("core of cpu " + cpu + " is taken");
            return affinity;
        }
    }

    /**
     * Pins the calling thread to this logical CPU (reserved or not) and reserves only it: its hyper-threading sibling
     * stays free for another thread pinned the same way. If pinning fails or the platform is not supported, the
     * handle is not pinned.
     *
     * @throws IllegalStateException if the CPU is held in this or another process
     */
    public static CpuAffinity acquireCpu(int cpu) {
        if (cpu < 0) throw new IllegalArgumentException("cpu " + cpu);
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        final BitSet single = new BitSet();
        single.set(cpu);
        synchronized (TAKEN) {
            final CpuAffinity affinity = tryAcquire(cpu, single);
            if (affinity == null) throw new IllegalStateException("cpu " + cpu + " is taken");
            return affinity;
        }
    }

    /**
     * Holding TAKEN: null if one of the CPUs is taken, a not pinned handle if pinning failed.
     */
    private static CpuAffinity tryAcquire(int cpu, BitSet cpus) {
        if (cpus.intersects(TAKEN)) return null;
        final List<FileChannel> locks = LOCK_FILES.tryLock(cpus, Thread.currentThread().getName());
        if (locks == null) return null;
        final BitSet previous = PLATFORM.pin(cpu);
        if (previous == null) {
            CpuLockFiles.release(locks);
            return notPinned("cannot pin to cpu " + cpu);
        }
        TAKEN.or(cpus);
        LOG.log(System.Logger.Level.DEBUG, "{0} pinned to cpu {1}, holds {2}",
                Thread.currentThread().getName(), cpu, CpuSets.toList(cpus));
        return new CpuAffinity(cpu, cpus, previous, locks, null);
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
     * The NUMA node of that CPU, -1 if not pinned.
     */
    public int node() {
        return NumaTopology.current().nodeOfCpu(cpu);
    }

    /**
     * The logical CPUs reserved with it: its physical core, only the CPU itself after {@link #acquireCpu(int)},
     * empty if not pinned.
     */
    public BitSet core() {
        return (BitSet) held.clone();
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
     * Restores the thread's previous affinity and releases what it reserved. Call it from the pinned thread: from
     * another thread the CPUs are released but the pinned thread keeps its mask.
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
            TAKEN.andNot(held);
        }
    }

    @Override
    public String toString() {
        return isPinned()
                ? "cpu " + cpu + " on node " + node() + " (holds " + CpuSets.toList(held) + ")"
                : "not pinned: " + notPinnedReason;
    }
}
