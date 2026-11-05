package exchange.core2.collections.affinity;

import java.lang.foreign.MemorySegment;
import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.random.RandomGenerator;

/**
 * Where memory is: the NUMA nodes of the pages of a segment, to verify a placement at start-up or in a benchmark. Only
 * reports, it moves nothing, and it is an estimate from a sample of the pages. Linux on x86-64 and AArch64, through the
 * Foreign Function and Memory API, as {@link CpuAffinity}; elsewhere nothing is known ({@link #isSupported()}). A system
 * call: not for a hot path.
 * <pre>
 * long[] table = new long[1 &lt;&lt; 27];
 * Arrays.fill(table, 1);
 * NumaMemory.Placement placement = NumaMemory.nodesOf(MemorySegment.ofArray(table));
 * if (!placement.isOnlyOn(1)) System.out.println("table: " + placement);   // node 0 50%, node 1 50% of 4096 sampled pages
 * </pre>
 */
public final class NumaMemory {

    private static final System.Logger LOG = System.getLogger(NumaMemory.class.getName());

    /**
     * Pages looked at, one from each of as many equal parts of the segment: a few milliseconds whatever its size.
     */
    private static final int MAX_SAMPLES = 4096;
    /**
     * The unit of the samples. A page of a larger size can simply be sampled more than once.
     */
    private static final long PAGE = 4096;
    /**
     * Queries of an array that the GC moved meanwhile, before giving up: each move needs a GC within microseconds.
     */
    private static final int MAX_ATTEMPTS = 3;

    private static final Placement UNKNOWN = new Placement(new int[0]);

    private static final LinuxPages PAGES = detect();

    private NumaMemory() {
    }

    private static LinuxPages detect() {
        final long number = LinuxPages.movePagesNumber(System.getProperty("os.arch", ""));
        if (!System.getProperty("os.name", "").startsWith("Linux") || number < 0) return null;
        try {
            final LinuxPages pages = new LinuxPages(number);
            final int errno = pages.probe();
            if (errno == 0) return pages;
            LOG.log(System.Logger.Level.WARNING, "memory placement not available: move_pages refused, errno {0}", errno);
        } catch (Throwable e) {
            // e.g. --illegal-native-access=deny, or a C library without the functions
            LOG.log(System.Logger.Level.WARNING, "memory placement not available", e);
        }
        return null;
    }

    /**
     * Whether this OS and runtime can tell where memory is: the system call was tried once, it can be refused - the
     * default seccomp profile of Docker does that, the reason is logged.
     */
    public static boolean isSupported() {
        return PAGES != null;
    }

    /**
     * Where the pages of this segment are now. Never throws: if it can not tell, the segment is empty or already
     * closed, no pages are sampled. Do not close its arena from another thread during the call: the answer would be
     * about what replaced the memory.
     * <p>
     * A segment over an array - {@code MemorySegment.ofArray(long[])} - works as well, as long as the GC keeps it in
     * place for the call: the address is read before and after, and the query repeated if it changed. The answer is
     * for that moment: a large array usually stays where it is, a small one moves with every GC.
     */
    public static Placement nodesOf(MemorySegment segment) {
        if (PAGES == null || segment.byteSize() == 0 || !segment.scope().isAlive()) return UNKNOWN;
        try {
            return query(() -> PAGES.addressOf(segment), segment.byteSize(), PAGES::nodesOf);
        } finally {
            Reference.reachabilityFence(segment); // an automatic arena must not free it during the call
        }
    }

    /**
     * @param address      the current address of the memory, 0 if unknown
     * @param nodesOfPages the node of each page, null if the call failed
     */
    static Placement query(LongSupplier address, long bytes, Function<long[], int[]> nodesOfPages) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            final long before = address.getAsLong();
            if (before == 0) return UNKNOWN;
            final int[] nodes = nodesOfPages.apply(samplePages(before, bytes, ThreadLocalRandom.current()));
            if (nodes == null) return UNKNOWN;
            if (address.getAsLong() == before) return new Placement(nodes);
            // moved: those were the pages of something else
        }
        return UNKNOWN;
    }

    /**
     * Page addresses covering the range: a random page from each of the equal parts, as a regular step could always
     * land on the same node of a placement in stripes - 2 MB huge pages interleaved over the nodes, for one. All the
     * pages if there are few.
     */
    static long[] samplePages(long address, long bytes, RandomGenerator random) {
        final long first = address & -PAGE;
        final long count = (((address + bytes - 1) & -PAGE) - first) / PAGE + 1;
        final int samples = (int) Math.min(count, MAX_SAMPLES);
        final long[] pages = new long[samples];
        for (int i = 0; i < samples; i++) {
            pages[i] = first + random.nextLong(count * i / samples, count * (i + 1) / samples) * PAGE;
        }
        return pages;
    }

    /**
     * The nodes of the sampled pages of a segment at one moment.
     */
    public static final class Placement {

        private final int sampled;
        private final int absent;
        /**
         * Pages by node.
         */
        private final int[] onNode;

        /**
         * @param nodes the node of each sampled page, negative for a page that is not there
         */
        Placement(int[] nodes) {
            this.sampled = nodes.length;
            this.onNode = new int[Math.max(Arrays.stream(nodes).max().orElse(-1) + 1, 0)];
            int notThere = 0;
            for (int node : nodes) {
                if (node >= 0) onNode[node]++;
                else notThere++;
            }
            this.absent = notThere;
        }

        /**
         * The pages looked at: 0 means nothing is known - see {@link NumaMemory#isSupported()}.
         */
        public int sampledPages() {
            return sampled;
        }

        /**
         * Sampled pages that have no node yet: never touched, or not mapped. The first touch places them.
         */
        public int absentPages() {
            return absent;
        }

        /**
         * Sampled pages on this node.
         */
        public int pagesOn(int node) {
            return node >= 0 && node < onNode.length ? onNode[node] : 0;
        }

        /**
         * Whether all the sampled pages are on this node. False if nothing is known and if some have no node yet. A
         * small part elsewhere, below about 0.1% of a large segment, can go unnoticed.
         */
        public boolean isOnlyOn(int node) {
            return sampled > 0 && pagesOn(node) == sampled;
        }

        /**
         * For a report: "node 0 50%, node 1 50% of 4096 sampled pages".
         */
        @Override
        public String toString() {
            if (sampled == 0) return "unknown";
            final StringBuilder sb = new StringBuilder();
            for (int node = 0; node < onNode.length; node++) {
                if (onNode[node] > 0) append(sb, "node " + node, onNode[node]);
            }
            if (absent > 0) append(sb, "absent", absent);
            return sb.append(" of ").append(sampled).append(" sampled pages").toString();
        }

        private void append(StringBuilder sb, String name, int pages) {
            if (!sb.isEmpty()) sb.append(", ");
            sb.append(name).append(' ').append(pages * 100 / sampled).append('%');
        }
    }
}
