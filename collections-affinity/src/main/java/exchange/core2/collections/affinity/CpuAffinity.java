package exchange.core2.collections.affinity;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Pins the calling thread to one CPU and reserves the whole physical core (all its hyper-threading siblings) for
 * it, within this process and - through lock files - across the processes that share the lock directory. Linux and
 * Windows; elsewhere, or without native access, nothing is pinned and the returned handle says so. Uses the Foreign
 * Function and Memory API:
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
 * {@code -Daffinity.spare=<hex mask>} adds CPUs outside the reserved ones, taken on each node after its reserved ones:
 * where only one socket is isolated, the CPUs of the other one for the threads that do not fit or do not belong there.
 * None by default, so nothing lands on a CPU that serves interrupts unless allowed to.
 * {@code -Daffinity.firstNode=<node>} is the node {@link #acquireCore()} starts on, by default that of the highest
 * reserved CPU.
 * Lock files go to {@code -Daffinity.lockDir} (default: java.io.tmpdir). A malformed value of these properties is
 * logged as a warning and ignored, as if it was not set.
 * <p>
 * The first thread on the node filled first logs what stands in the way of its memory, see {@link ProcessPlacement}.
 * <p>
 * Threads created by a pinned thread inherit its single-CPU mask: create helper threads before pinning, or pin
 * them to cores of their own.
 */
public final class CpuAffinity implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CpuAffinity.class.getName());

    private static final AffinityPlatform PLATFORM = AffinityPlatform.detect(LOG);
    private static final BitSet RESERVED = reserved();
    private static final BitSet SPARE = spare();
    private static final int FIRST_NODE =
            firstNode(System.getProperty("affinity.firstNode"), NumaTopology.current(), RESERVED, SPARE);
    /**
     * The order of {@link #acquireCore()}: threads stay on one NUMA node as long as it has cores, even where the OS
     * numbers the CPUs of the sockets in turn.
     */
    private static final int[] FILL_ORDER = NumaTopology.current().fillOrder(FIRST_NODE, RESERVED, SPARE);
    private static final CpuLockFiles LOCK_FILES = new CpuLockFiles(
            Path.of(System.getProperty("affinity.lockDir", System.getProperty("java.io.tmpdir"))));
    /**
     * Logical CPUs held by the handles of this process. Guards the acquisition.
     */
    private static final BitSet TAKEN = new BitSet();
    /**
     * The nodes {@link #warnIfOnOtherNode} has warned about. Guarded by TAKEN.
     */
    static final BitSet WARNED_NODES = new BitSet();
    /**
     * The nodes {@link #warnAboutMemory} has looked at. Guarded by TAKEN.
     */
    static final BitSet MEMORY_CHECKED = new BitSet();

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
     * Pins the calling thread to the first free core: on the first node (see the class comment) its reserved CPUs, the
     * highest first, then its spare ones, then the same on the other nodes by distance. The first landing on another
     * node is logged as a warning. Never throws: if no core is free, no CPU is reserved or the platform is not
     * supported, the handle is not pinned ({@link #isPinned()}).
     */
    public static CpuAffinity acquireCore() {
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        final CpuAffinity affinity = firstFree(FILL_ORDER);
        return affinity != null ? warnIfOnOtherNode(FIRST_NODE, affinity) : noFreeCore();
    }

    /**
     * Pins the calling thread to a free core of this NUMA node: its reserved CPUs, the highest first, then its spare
     * ones. Never throws for that: if there is none or the platform is not supported, the handle is not pinned
     * ({@link #isPinned()}).
     *
     * @throws IllegalArgumentException if the node is negative
     */
    public static CpuAffinity acquireCoreOnNode(int node) {
        if (node < 0) throw new IllegalArgumentException("node " + node);
        if (PLATFORM == null) return notPinned("thread affinity not supported here");
        final NumaTopology topology = NumaTopology.current();
        final int[] cpus = Arrays.stream(topology.fillOrder(node, RESERVED, SPARE))
                .filter(cpu -> topology.nodeOfCpu(cpu) == node)
                .toArray();
        final CpuAffinity affinity = firstFree(cpus);
        return affinity != null ? affinity : notPinned("no free core on node " + node + ", " + allowedCpus());
    }

    /**
     * Pins the calling thread to the free core nearest to this handle's: sharing its last-level cache, then on its NUMA
     * node, then on the other nodes by distance, on each node its reserved CPUs before the spare ones - {@link #node()}
     * tells where it landed, and the first landing on another node is logged as a warning. If that handle is not
     * pinned, as {@link #acquireCore()}. Never throws: if no core is free, the handle is not pinned.
     */
    public static CpuAffinity acquireCoreNear(CpuAffinity other) {
        if (!other.isPinned()) return acquireCore();
        final CpuAffinity affinity = firstFree(NumaTopology.current().nearestFirst(other.cpu, RESERVED, SPARE));
        return affinity != null ? warnIfOnOtherNode(other.node(), affinity) : noFreeCore();
    }

    /**
     * Threads that pin themselves as {@link #acquireCore()} when they start and release the core when they end. Named
     * {@code name-0}, {@code name-1}, ... A thread that can not be pinned still runs, on the mask of the thread that
     * created it: see {@link #isPinned()}, the reason is logged as a warning.
     */
    public static ThreadFactory threadFactory(String name) {
        return threadFactory(name, CpuAffinity::acquireCore);
    }

    /**
     * As {@link #threadFactory(String)}, pinned as {@link #acquireCoreOnNode(int)}: a group of threads that belongs on
     * this node, whatever {@link #acquireCore()} is doing elsewhere.
     *
     * @throws IllegalArgumentException if the node is negative
     */
    public static ThreadFactory threadFactory(String name, int node) {
        if (node < 0) throw new IllegalArgumentException("node " + node);
        return threadFactory(name, () -> acquireCoreOnNode(node));
    }

    private static ThreadFactory threadFactory(String name, Supplier<CpuAffinity> acquire) {
        final AtomicInteger number = new AtomicInteger();
        return task -> new Thread(() -> {
            try (CpuAffinity ignore = acquire.get()) {
                task.run();
            }
        }, name + "-" + number.getAndIncrement());
    }

    /**
     * A thread away from the others pays for every cache line it shares with them, and it does not show unless told.
     * Once for each node: the threads after it land there as well.
     */
    static CpuAffinity warnIfOnOtherNode(int node, CpuAffinity affinity) {
        final int landed = affinity.node();
        if (landed < 0 || landed == node) return affinity;
        synchronized (TAKEN) {
            if (WARNED_NODES.get(landed)) return affinity;
            WARNED_NODES.set(landed);
        }
        LOG.log(System.Logger.Level.WARNING, "{0} pinned to cpu {1} on node {2}: no free core on node {3}, {4}",
                Thread.currentThread().getName(), affinity.cpu, landed, node, allowedCpus());
        return affinity;
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

    private static CpuAffinity noFreeCore() {
        return notPinned(RESERVED.isEmpty() && SPARE.isEmpty()
                ? "no reserved CPUs (isolcpus or -Daffinity.reserved)"
                : "all these cores are taken: " + allowedCpus());
    }

    private static String allowedCpus() {
        return "reserved: " + CpuSets.toList(RESERVED) + ", spare: " + CpuSets.toList(SPARE);
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
        warnAboutMemory(NumaTopology.current().nodeOfCpu(cpu));
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

    /**
     * Holding TAKEN, once for each node, before pinning: it can take a while, see {@link ProcessPlacement}, and that is
     * better spent off an isolated core. The node filled first, or without one the first node pinned on, is where the
     * memory of the process is expected: the others hold threads sent there on purpose and hear only if they have no
     * memory at all.
     */
    static void warnAboutMemory(int node) {
        if (node < 0 || MEMORY_CHECKED.get(node)) return;
        final boolean expected = FIRST_NODE >= 0 ? node == FIRST_NODE : MEMORY_CHECKED.isEmpty();
        MEMORY_CHECKED.set(node);
        final List<String> warnings = expected
                ? ProcessPlacement.current().warningsFor(node)
                : ProcessPlacement.ownMemoryWarnings(node, NumaTopology.current());
        warnings.forEach(warning -> LOG.log(System.Logger.Level.WARNING, warning));
    }

    private static CpuAffinity notPinned(String reason) {
        LOG.log(System.Logger.Level.WARNING, "{0} not pinned: {1}", Thread.currentThread().getName(), reason);
        return new CpuAffinity(-1, new BitSet(), null, List.of(), reason);
    }

    private static BitSet reserved() {
        final BitSet mask = maskProperty("affinity.reserved");
        if (mask != null) return mask;
        return PLATFORM != null ? PLATFORM.isolatedCpus() : new BitSet();
    }

    private static BitSet spare() {
        final BitSet mask = maskProperty("affinity.spare");
        final BitSet spare = mask != null ? mask : new BitSet();
        spare.andNot(RESERVED);
        return spare;
    }

    /**
     * The hex mask of this system property, null if it is not set or not a mask: that is logged, the class must load
     * anyway - acquireCore() never throws.
     */
    static BitSet maskProperty(String name) {
        final String value = System.getProperty(name);
        if (value == null) return null;
        try {
            return CpuSets.parseHexMask(value);
        } catch (NumberFormatException e) {
            LOG.log(System.Logger.Level.WARNING, "-D{0}={1} is not a hex mask such as AAAA: ignored", name, value);
            return null;
        }
    }

    /**
     * -Daffinity.firstNode if it is a node of this machine - otherwise that is logged - else the node of the highest
     * reserved CPU, or spare CPU if none is reserved.
     */
    static int firstNode(String value, NumaTopology topology, BitSet reserved, BitSet spare) {
        if (value != null) {
            try {
                final int node = Integer.parseInt(value.trim());
                if (node >= 0 && topology.nodes().get(node)) return node;
            } catch (NumberFormatException e) {
                // logged below
            }
            LOG.log(System.Logger.Level.WARNING, "-Daffinity.firstNode={0} is not a node of this machine ({1}): ignored",
                    value, CpuSets.toList(topology.nodes()));
        }
        return topology.nodeOfCpu((reserved.isEmpty() ? spare : reserved).length() - 1);
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
     * The CPUs {@link #acquireCore()} chooses from first.
     */
    public static BitSet reservedCpus() {
        return (BitSet) RESERVED.clone();
    }

    /**
     * The CPUs outside {@link #reservedCpus()} it can also take: -Daffinity.spare.
     */
    public static BitSet spareCpus() {
        return (BitSet) SPARE.clone();
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
