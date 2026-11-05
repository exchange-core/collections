package exchange.core2.collections.affinity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Where this process runs and keeps its memory, and what stands in the way of its threads on a NUMA node. The JVM sizes
 * its thread pools from the CPU mask it starts with and its memory goes by the policy it starts with, so taskset and
 * numactl can not be replaced from inside: this only tells. {@link CpuAffinity} logs {@link #warningsFor(int)} once, when
 * a thread lands on the node it fills first. Only reads what the OS reports, no native access needed. Linux; elsewhere
 * nothing is known. It walks the page tables of the process, at start-up or between runs, not for a hot path: on a Xeon
 * Gold 6244 about 2 ms with a heap of 16 GiB in huge pages, 65 ms in 4 KB pages.
 * <pre>
 * ProcessPlacement placement = ProcessPlacement.current();
 * placement.policy();                  // "bind:1" after numactl --membind=1, "default" without
 * placement.cpus();                    // {1, 3} after taskset -c 1,3
 * placement.bytesOn(1);                // what the process has there
 * placement.warningsFor(1);            // empty, or what to fix: to fail at start-up rather than run slow
 * </pre>
 * {@code -Daffinity.offHeap=<size>}, e.g. {@code 32g}, is the native memory it will need on top of the heap limit.
 */
public final class ProcessPlacement {

    private static final System.Logger LOG = System.getLogger(ProcessPlacement.class.getName());

    private static final double GIB = 1L << 30;

    private static final long OFF_HEAP = offHeap();

    /**
     * The address and the policy, of the kernel's mpol_to_str: "default", "bind=static:1", "prefer (many):0-1".
     */
    private static final Pattern POLICY =
            Pattern.compile("^\\S+ ((?:weighted )?[a-z]+(?: \\(many\\))?(?:=[a-z]+)?(?::[0-9][0-9,-]*)?)(?: |$)");
    private static final Pattern STACK = Pattern.compile(" stack( |$)");
    private static final Pattern PAGES_ON_NODE = Pattern.compile(" N(\\d+)=(\\d+)");
    private static final Pattern PAGE_SIZE = Pattern.compile(" kernelpagesize_kB=(\\d+)");

    private final String policy;
    /**
     * Those the policy names, none for one without nodes.
     */
    private final BitSet policyNodes;
    private final BitSet cpus;
    private final BitSet allowedNodes;
    private final long[] bytesOnNode;

    private ProcessPlacement(String policy, BitSet cpus, BitSet allowedNodes, long[] bytesOnNode) {
        this.policy = policy;
        this.policyNodes = policy.contains(":")
                ? CpuSets.parseList(policy.substring(policy.lastIndexOf(':') + 1))
                : new BitSet();
        this.cpus = cpus;
        this.allowedNodes = allowedNodes;
        this.bytesOnNode = bytesOnNode;
    }

    /**
     * This process now.
     */
    public static ProcessPlacement current() {
        return System.getProperty("os.name", "").startsWith("Linux")
                ? read(Path.of("/proc/self"))
                : new ProcessPlacement("", new BitSet(), new BitSet(), new long[0]);
    }

    /**
     * @param self /proc/self
     */
    static ProcessPlacement read(Path self) {
        final List<String> maps = readLines(self.resolve("numa_maps"));
        final List<String> status = readLines(self.resolve("status"));
        return new ProcessPlacement(policyOf(maps), listOf(status, "Cpus_allowed_list:"),
                listOf(status, "Mems_allowed_list:"), bytesByNode(maps));
    }

    /**
     * A /proc file: empty if it does not exist or can not be read.
     */
    private static List<String> readLines(Path file) {
        try {
            return Files.readAllLines(file, StandardCharsets.ISO_8859_1); // a path in numa_maps can be anything
        } catch (IOException | RuntimeException e) {
            return List.of();
        }
    }

    /**
     * The policy of the process is on a mapping without one of its own, such as the stack: G1 with UseNUMA binds its
     * regions, for one.
     */
    private static String policyOf(List<String> maps) {
        final String line = maps.stream().filter(map -> STACK.matcher(map).find()).findFirst()
                .orElse(maps.isEmpty() ? "" : maps.get(0));
        final Matcher policy = POLICY.matcher(line);
        return policy.find() ? policy.group(1) : "";
    }

    private static BitSet listOf(List<String> status, String field) {
        return status.stream().filter(line -> line.startsWith(field)).findFirst()
                .map(line -> CpuSets.parseList(line.substring(field.length())))
                .orElseGet(BitSet::new);
    }

    /**
     * Anonymous memory, the huge pages of hugetlbfs included. Not the mapped files: libraries are in the page cache
     * wherever something first read them, that says nothing about the data.
     */
    private static long[] bytesByNode(List<String> maps) {
        long[] bytes = new long[0];
        for (String map : maps) {
            if (map.contains(" file=") && !map.contains(" huge ")) continue;
            final Matcher size = PAGE_SIZE.matcher(map);
            if (!size.find()) continue;
            final long page = Long.parseLong(size.group(1)) * 1024;
            final Matcher pages = PAGES_ON_NODE.matcher(map);
            while (pages.find()) {
                final int node = Integer.parseInt(pages.group(1));
                if (node >= bytes.length) bytes = Arrays.copyOf(bytes, node + 1);
                bytes[node] += Long.parseLong(pages.group(2)) * page;
            }
        }
        return bytes;
    }

    private static long offHeap() {
        final String size = System.getProperty("affinity.offHeap");
        if (size == null) return 0;
        try {
            return parseSize(size);
        } catch (NumberFormatException e) {
            LOG.log(System.Logger.Level.WARNING, "-Daffinity.offHeap={0} is not a size such as 32g: not counted", size);
            return 0;
        }
    }

    /**
     * "32g", "512m", "1024".
     *
     * @throws NumberFormatException if it is not a size
     */
    static long parseSize(String size) {
        final String value = size.trim().toLowerCase(Locale.ROOT);
        final int unit = value.isEmpty() ? -1 : "kmgt".indexOf(value.charAt(value.length() - 1));
        return unit < 0
                ? Long.parseLong(value)
                : Long.parseLong(value.substring(0, value.length() - 1)) << (10 * (unit + 1));
    }

    /**
     * The memory policy of the process as the kernel prints it: "default", "bind:1", "prefer:1", "interleave:0-1". Empty
     * if unknown.
     */
    public String policy() {
        return policy;
    }

    /**
     * The CPUs its first thread may run on, which the threads of the JVM inherit: what it was started with. A thread
     * pinned by {@link CpuAffinity} does not change that. Empty if unknown.
     */
    public BitSet cpus() {
        return (BitSet) cpus.clone();
    }

    /**
     * The nodes its cpuset allows memory on, whatever the policy. Empty if unknown.
     */
    public BitSet allowedNodes() {
        return (BitSet) allowedNodes.clone();
    }

    /**
     * Its anonymous memory on this node now, the Java heap and native memory.
     */
    public long bytesOn(int node) {
        return node >= 0 && node < bytesOnNode.length ? bytesOnNode[node] : 0;
    }

    /**
     * What stands in the way of threads of this process on this node: the node has no memory of its own, the policy or
     * the mask of the process sends its memory elsewhere, the memory already is elsewhere, the heap limit and
     * {@code -Daffinity.offHeap} do not fit there. Empty if nothing does, if nothing is known and on a machine of one node.
     */
    public List<String> warningsFor(int node) {
        return warningsFor(node, NumaTopology.current(), Runtime.getRuntime().maxMemory() + OFF_HEAP);
    }

    /**
     * @param needed bytes the process may come to use
     */
    List<String> warningsFor(int node, NumaTopology topology, long needed) {
        final List<String> warnings = new ArrayList<>();
        if (topology.nodes().cardinality() < 2) return warnings;
        if (node < 0 || !topology.nodes().get(node)) {
            warnings.add("there is no node " + node);
            return warnings;
        }
        warnings.addAll(ownMemoryWarnings(node, topology));
        if (!warnings.isEmpty()) return warnings;

        if (!allowedNodes.isEmpty() && !allowedNodes.get(node)) {
            warnings.add("threads on node " + node + ", but the cpuset of this process allows memory only on nodes "
                    + CpuSets.toList(allowedNodes));
        }
        final boolean noPolicy = policy.equals("default") || policy.equals("local");
        if (!policyNodes.isEmpty() && (policyNodes.cardinality() > 1 || !policyNodes.get(node))) {
            warnings.add("threads on node " + node + ", but the memory policy of this process is " + policy
                    + ": start it with numactl --membind=" + node);
        } else if (noPolicy && !isOnNode(cpus, node, topology)) {
            warnings.add("threads on node " + node + ", but no memory policy and this process started on CPUs "
                    + CpuSets.toList(cpus) + ": its GC and compiler threads place pages there. Start it with numactl --membind="
                    + node);
        }
        final long elsewhere = Arrays.stream(bytesOnNode).sum() - bytesOn(node);
        if (warnings.isEmpty() && elsewhere > bytesOn(node)) {
            warnings.add("threads on node " + node + ", but this process has " + gib(bytesOn(node)) + " there and "
                    + gib(elsewhere) + " on other nodes");
        }
        if (policyNodes.isEmpty() || policyNodes.get(node)) addFitWarnings(warnings, node, topology.memoryOf(node), needed);
        return warnings;
    }

    /**
     * The hint when the memory goes to another node before the page cache is reclaimed: that is what zone_reclaim_mode 0,
     * the default, is documented to do unless the process is bound.
     */
    private void addFitWarnings(List<String> warnings, int node, NumaTopology.NodeMemory memory, long needed) {
        if (memory.total() < 0) return;
        final long toPlace = needed - bytesOn(node);
        final long free = memory.free() + memory.freeHugePages();
        final String numbers = "this process may need " + gib(needed) + " with the heap limit and -Daffinity.offHeap and has "
                + gib(bytesOn(node)) + " there, " + gib(free) + " is free and " + gib(memory.reclaimable()) + " reclaimable";
        if (toPlace > free + memory.reclaimable()) {
            warnings.add("node " + node + " has not enough memory: " + numbers);
        } else if (toPlace > free && !policy.startsWith("bind")) {
            warnings.add("node " + node + " has enough memory only without its page cache, otherwise it can come from "
                    + "another node: " + numbers);
        }
    }

    /**
     * The one warning that holds for any thread on the node, whatever it is there for.
     */
    static List<String> ownMemoryWarnings(int node, NumaTopology topology) {
        return topology.memoryOf(node).total() == 0
                ? List.of("node " + node + " has no memory of its own: its threads use the memory of other nodes")
                : List.of();
    }

    private static boolean isOnNode(BitSet cpus, int node, NumaTopology topology) {
        final BitSet outside = (BitSet) cpus.clone();
        outside.andNot(topology.cpusOf(node));
        return outside.isEmpty();
    }

    private static String gib(long bytes) {
        return String.format(Locale.ROOT, "%.1f GiB", bytes / GIB);
    }

    /**
     * For a report: "policy bind:1, cpus 1,3, memory allowed on 0-1, node 1 8.0 GiB".
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("policy ").append(policy.isEmpty() ? "unknown" : policy)
                .append(", cpus ").append(CpuSets.toList(cpus))
                .append(", memory allowed on ").append(CpuSets.toList(allowedNodes));
        for (int node = 0; node < bytesOnNode.length; node++) {
            if (bytesOnNode[node] > 0) sb.append(", node ").append(node).append(' ').append(gib(bytesOnNode[node]));
        }
        return sb.toString();
    }
}
