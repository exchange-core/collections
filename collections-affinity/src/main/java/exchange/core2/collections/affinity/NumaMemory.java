package exchange.core2.collections.affinity;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * Where memory is: the NUMA nodes of the pages of a segment, to verify a placement at start-up or in a benchmark. Only
 * reports, it moves nothing. Linux on x86-64 and AArch64, through the Foreign Function and Memory API, as
 * {@link CpuAffinity}; elsewhere nothing is known ({@link #isSupported()}). A system call: not for a hot path.
 * <pre>
 * long[] table = new long[1 &lt;&lt; 27];
 * Arrays.fill(table, 1);
 * NumaMemory.Placement placement = NumaMemory.nodesOf(MemorySegment.ofArray(table));
 * if (!placement.isOnlyOn(1)) System.out.println("table: " + placement);   // node 0 50%, node 1 50% of 4096 pages
 * </pre>
 */
public final class NumaMemory {

    private static final System.Logger LOG = System.getLogger(NumaMemory.class.getName());

    /**
     * Pages looked at, spread evenly over the segment: a few milliseconds whatever its size.
     */
    private static final int MAX_SAMPLES = 4096;
    /**
     * The step of the samples. A page of a larger size is simply sampled more than once.
     */
    private static final long PAGE = 4096;

    private static final LinuxPages PAGES = detect();

    private NumaMemory() {
    }

    private static LinuxPages detect() {
        final long movePages = LinuxPages.movePagesNumber(System.getProperty("os.arch", ""));
        if (!System.getProperty("os.name", "").startsWith("Linux") || movePages < 0) return null;
        try {
            return new LinuxPages(movePages);
        } catch (Throwable e) {
            // e.g. --illegal-native-access=deny, or a C library without the functions
            LOG.log(System.Logger.Level.WARNING, "memory placement not available", e);
            return null;
        }
    }

    /**
     * Whether this OS and runtime can tell where memory is.
     */
    public static boolean isSupported() {
        return PAGES != null;
    }

    /**
     * Where the pages of this segment are now. Never throws: if it can not tell, or the segment is empty, no pages
     * are sampled. A segment over an array - {@code MemorySegment.ofArray(long[])} - works as well: the answer is for
     * the place the array has at this moment, the GC can move it later.
     */
    public static Placement nodesOf(MemorySegment segment) {
        if (PAGES == null || segment.byteSize() == 0) return new Placement(new int[0]);
        try {
            final int[] nodes = PAGES.nodesOf(samplePages(PAGES.addressOf(segment), segment.byteSize()));
            return new Placement(nodes != null ? nodes : new int[0]);
        } catch (Throwable e) {
            LOG.log(System.Logger.Level.WARNING, "memory placement not available", e);
            return new Placement(new int[0]);
        }
    }

    /**
     * Page addresses covering the range, the first page always among them.
     */
    static long[] samplePages(long address, long bytes) {
        final long first = address & -PAGE;
        final long count = (((address + bytes - 1) & -PAGE) - first) / PAGE + 1;
        final int samples = (int) Math.min(count, MAX_SAMPLES);
        final long[] pages = new long[samples];
        for (int i = 0; i < samples; i++) {
            pages[i] = first + count * i / samples * PAGE;
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
         * Whether all the sampled pages are on this node. False if nothing is known and if some have no node yet.
         */
        public boolean isOnlyOn(int node) {
            return sampled > 0 && pagesOn(node) == sampled;
        }

        /**
         * For a report: "node 0 50%, node 1 50% of 4096 pages".
         */
        @Override
        public String toString() {
            if (sampled == 0) return "unknown";
            final StringBuilder sb = new StringBuilder();
            for (int node = 0; node < onNode.length; node++) {
                if (onNode[node] > 0) append(sb, "node " + node, onNode[node]);
            }
            if (absent > 0) append(sb, "absent", absent);
            return sb.append(" of ").append(sampled).append(" pages").toString();
        }

        private void append(StringBuilder sb, String name, int pages) {
            if (!sb.isEmpty()) sb.append(", ");
            sb.append(name).append(' ').append(pages * 100 / sampled).append('%');
        }
    }
}
