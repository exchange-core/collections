package exchange.core2.collections.affinity;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The NUMA nodes of this machine: their CPUs, how far apart they are, which node a device is attached to. Only reads
 * what the OS reports, no native access needed. Linux, from sysfs, the nodes read once; elsewhere, and on a Linux
 * without NUMA, the machine is one node 0 with all the CPUs.
 * <pre>
 * NumaTopology topology = NumaTopology.current();
 * topology.nodeOfCpu(13);                       // 1
 * topology.cpusOf(1);                           // {1, 3, 5, ...}
 * topology.distance(0, 1);                      // 21
 * topology.lastLevelCacheOf(13);                // {1, 3, 5, ...} - less than the node on many AMD CPUs
 * topology.nodeOfNetworkInterface("eno1np0");   // 0
 * topology.memoryOf(1).free();                  // bytes, now
 * </pre>
 * What does not exist is -1 or an empty set, it does not throw: an unknown node, a CPU that is offline.
 */
public final class NumaTopology {

    /**
     * The kernel's LOCAL_DISTANCE and REMOTE_DISTANCE, for a node without a readable distance file.
     */
    private static final int LOCAL_DISTANCE = 10;
    private static final int REMOTE_DISTANCE = 20;

    /**
     * The memory of a node in bytes.
     *
     * @param total         0 for a node without memory of its own
     * @param free          not used by anything
     * @param reclaimable   what the OS could free: the file cache and reclaimable kernel objects. An estimate, as there
     *                      is no MemAvailable for a node: a part of it may be in use
     * @param freeHugePages reserved huge pages of all sizes that are free: not a part of free, and only for mappings
     *                      that ask for them
     */
    public record NodeMemory(long total, long free, long reclaimable, long freeHugePages) {
        static final NodeMemory UNKNOWN = new NodeMemory(-1, -1, -1, -1);
    }

    private static final class Current {
        static final NumaTopology INSTANCE = System.getProperty("os.name", "").startsWith("Linux")
                ? read(Path.of("/sys"))
                : oneNode(null, firstCpus(Runtime.getRuntime().availableProcessors()));
    }

    /**
     * /sys, null if there is none.
     */
    private final Path sys;
    private final BitSet nodes;
    /**
     * By node, null for a node that is not online.
     */
    private final BitSet[] cpus;
    /**
     * By node, by node.
     */
    private final int[][] distances;

    private NumaTopology(Path sys, BitSet nodes, BitSet[] cpus, int[][] distances) {
        this.sys = sys;
        this.nodes = nodes;
        this.cpus = cpus;
        this.distances = distances;
    }

    /**
     * The topology of this machine.
     */
    public static NumaTopology current() {
        return Current.INSTANCE;
    }

    static NumaTopology read(Path sys) {
        final Path nodeDir = sys.resolve("devices/system/node");
        final BitSet nodes = CpuSets.readList(nodeDir.resolve("online"));
        if (nodes.isEmpty()) {
            final BitSet online = CpuSets.readList(sys.resolve("devices/system/cpu/online"));
            return oneNode(sys, online.isEmpty() ? firstCpus(Runtime.getRuntime().availableProcessors()) : online);
        }
        final BitSet[] cpus = new BitSet[nodes.length()];
        final int[][] distances = new int[nodes.length()][];
        for (int node = nodes.nextSetBit(0); node >= 0; node = nodes.nextSetBit(node + 1)) {
            final Path dir = nodeDir.resolve("node" + node);
            cpus[node] = CpuSets.readList(dir.resolve("cpulist"));
            distances[node] = readDistances(dir.resolve("distance"), nodes, node);
        }
        return new NumaTopology(sys, nodes, cpus, distances);
    }

    private static NumaTopology oneNode(Path sys, BitSet cpus) {
        final BitSet nodes = new BitSet();
        nodes.set(0);
        return new NumaTopology(sys, nodes, new BitSet[]{cpus}, new int[][]{{LOCAL_DISTANCE}});
    }

    private static BitSet firstCpus(int count) {
        final BitSet cpus = new BitSet();
        cpus.set(0, count);
        return cpus;
    }

    /**
     * The file has one number per online node, in the order of the nodes - not by node id: they can have gaps.
     */
    private static int[] readDistances(Path file, BitSet nodes, int node) {
        final int[] distances = new int[nodes.length()];
        Arrays.fill(distances, REMOTE_DISTANCE);
        distances[node] = LOCAL_DISTANCE;
        try {
            final String[] values = Files.readString(file).trim().split("\\s+");
            if (values.length != nodes.cardinality()) return distances;
            int i = 0;
            for (int other = nodes.nextSetBit(0); other >= 0; other = nodes.nextSetBit(other + 1)) {
                distances[other] = Integer.parseInt(values[i++]);
            }
        } catch (IOException | RuntimeException e) {
            // no file: the kernel's defaults
        }
        return distances;
    }

    private boolean isNode(int node) {
        return node >= 0 && nodes.get(node);
    }

    /**
     * The online nodes. Their ids can have gaps.
     */
    public BitSet nodes() {
        return (BitSet) nodes.clone();
    }

    /**
     * The CPUs of this node: empty for an unknown node and for a node with memory only.
     */
    public BitSet cpusOf(int node) {
        return isNode(node) ? (BitSet) cpus[node].clone() : new BitSet();
    }

    /**
     * The node of this CPU, -1 if unknown.
     */
    public int nodeOfCpu(int cpu) {
        if (cpu < 0) return -1;
        for (int node = nodes.nextSetBit(0); node >= 0; node = nodes.nextSetBit(node + 1)) {
            if (cpus[node].get(cpu)) return node;
        }
        return -1;
    }

    /**
     * The distance the firmware reports: 10 within a node, relative to that further away (21 = about 2.1 times). -1 if
     * a node is unknown.
     */
    public int distance(int nodeA, int nodeB) {
        return isNode(nodeA) && isNode(nodeB) ? distances[nodeA][nodeB] : -1;
    }

    /**
     * The logical CPUs sharing the last-level cache of this CPU, including it: only the CPU itself if the OS does not
     * tell, empty for a negative CPU.
     */
    public BitSet lastLevelCacheOf(int cpu) {
        if (cpu < 0) return new BitSet();
        BitSet shared = new BitSet();
        if (sys != null) {
            final Path cache = sys.resolve("devices/system/cpu").resolve("cpu" + cpu).resolve("cache");
            int highest = -1;
            for (int i = 0; Files.isDirectory(cache.resolve("index" + i)); i++) {
                final int level = readInt(cache.resolve("index" + i).resolve("level"));
                if (level > highest) {
                    highest = level;
                    shared = CpuSets.readList(cache.resolve("index" + i).resolve("shared_cpu_list"));
                }
            }
        }
        shared.set(cpu);
        return shared;
    }

    /**
     * The node a network interface is attached to, -1 if unknown (machines with one node often report that).
     */
    public int nodeOfNetworkInterface(String name) {
        return readNode("class/net/" + name + "/device/numa_node");
    }

    /**
     * The node a PCI device is attached to, by its address as in lspci -D: "0000:18:00.0". -1 if unknown (machines
     * with one node often report that).
     */
    public int nodeOfPciDevice(String address) {
        return readNode("bus/pci/devices/" + address + "/numa_node");
    }

    /**
     * The memory of this node as the OS reports it now: read each time. All -1 for an unknown node, and off Linux.
     */
    public NodeMemory memoryOf(int node) {
        if (sys == null || !isNode(node)) return NodeMemory.UNKNOWN;
        final Path dir = sys.resolve("devices/system/node/node" + node);
        final Map<String, Long> bytes = readMemInfo(dir.resolve("meminfo"));
        if (!bytes.containsKey("MemTotal")) return NodeMemory.UNKNOWN;
        return new NodeMemory(bytes.get("MemTotal"), bytes.getOrDefault("MemFree", 0L),
                bytes.getOrDefault("Active(file)", 0L) + bytes.getOrDefault("Inactive(file)", 0L)
                        + bytes.getOrDefault("SReclaimable", 0L),
                readFreeHugePages(dir.resolve("hugepages")));
    }

    /**
     * Lines such as "Node 1 MemFree:   98015332 kB": those in kB, by name. Empty if there is no such file.
     */
    private static Map<String, Long> readMemInfo(Path file) {
        final Map<String, Long> bytes = new HashMap<>();
        try {
            for (String line : Files.readAllLines(file)) {
                final String[] parts = line.trim().split("\\s+");
                if (parts.length == 5 && parts[4].equals("kB")) {
                    bytes.put(parts[2].replace(":", ""), Long.parseLong(parts[3]) * 1024);
                }
            }
        } catch (IOException | RuntimeException e) {
            // not there or not as expected: as much as was read
        }
        return bytes;
    }

    /**
     * Directories such as hugepages-2048kB, each with its free_hugepages.
     */
    private static long readFreeHugePages(Path dir) {
        long bytes = 0;
        try (DirectoryStream<Path> sizes = Files.newDirectoryStream(dir, "hugepages-*kB")) {
            for (Path size : sizes) {
                final String name = size.getFileName().toString();
                final long pageKb = Long.parseLong(name.substring("hugepages-".length(), name.length() - "kB".length()));
                bytes += Math.max(0, readInt(size.resolve("free_hugepages"))) * pageKb * 1024;
            }
        } catch (IOException | RuntimeException e) {
            // no huge pages on this machine
        }
        return bytes;
    }

    private int readNode(String file) {
        if (sys == null) return -1;
        try {
            final int node = readInt(sys.resolve(file));
            return isNode(node) ? node : -1;
        } catch (RuntimeException e) {
            return -1; // not a path on this OS
        }
    }

    /**
     * -1 if there is no such file or no number in it.
     */
    private static int readInt(Path file) {
        try {
            return Integer.parseInt(Files.readString(file).trim());
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
    }

    /**
     * The CPUs of the set, the nearest to this CPU first: those sharing its last-level cache, then those of its node,
     * then the other nodes by distance, one node at a time. The highest CPU first within each.
     */
    int[] nearestFirst(int cpu, BitSet set) {
        final BitSet cache = lastLevelCacheOf(cpu);
        final int node = nodeOfCpu(cpu);
        return set.stream().boxed()
                .sorted(Comparator.<Integer>comparingInt(other -> cache.get(other) ? 0 : rank(node, nodeOfCpu(other)))
                        .thenComparing(other -> nodeOfCpu(other) != node)
                        .thenComparing(this::nodeOfCpu, Comparator.reverseOrder())
                        .thenComparing(Comparator.reverseOrder()))
                .mapToInt(Integer::intValue)
                .toArray();
    }

    /**
     * As {@link #nearestFirst(int, BitSet)} for both sets, and on each node the CPUs of the first set before the others.
     */
    int[] nearestFirst(int cpu, BitSet first, BitSet then) {
        final BitSet all = (BitSet) first.clone();
        all.or(then);
        final int[] nearest = nearestFirst(cpu, all);
        final List<Integer> nodesInOrder = Arrays.stream(nearest).map(this::nodeOfCpu).distinct().boxed().toList();
        return Arrays.stream(nearest).boxed()
                .sorted(Comparator.<Integer>comparingInt(other -> nodesInOrder.indexOf(nodeOfCpu(other)))
                        .thenComparing(other -> !first.get(other)))
                .mapToInt(Integer::intValue)
                .toArray();
    }

    /**
     * The order threads fill the machine in: the reserved CPUs of this node, then its spare ones, then the same on the
     * other nodes by distance.
     */
    int[] fillOrder(int node, BitSet reserved, BitSet spare) {
        return nearestFirst(highestCpu(node, reserved, spare), reserved, spare);
    }

    /**
     * The highest CPU of this node in the first set, or else in the second, or else at all. -1 if it has no CPUs.
     */
    private int highestCpu(int node, BitSet first, BitSet second) {
        for (BitSet set : new BitSet[]{first, second}) {
            final BitSet onNode = cpusOf(node);
            onNode.and(set);
            if (!onNode.isEmpty()) return onNode.length() - 1;
        }
        return cpusOf(node).length() - 1;
    }

    /**
     * The distance, unknown nodes last.
     */
    private int rank(int nodeA, int nodeB) {
        final int distance = distance(nodeA, nodeB);
        return distance >= 0 ? distance : Integer.MAX_VALUE;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int node = nodes.nextSetBit(0); node >= 0; node = nodes.nextSetBit(node + 1)) {
            if (!sb.isEmpty()) sb.append("; ");
            sb.append("node ").append(node).append(": cpus ").append(CpuSets.toList(cpus[node]));
        }
        return sb.toString();
    }
}
