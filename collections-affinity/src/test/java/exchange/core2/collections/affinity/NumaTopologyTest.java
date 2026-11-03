package exchange.core2.collections.affinity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

import static exchange.core2.collections.affinity.CpuSetsTest.bits;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * On a fake /sys.
 */
public class NumaTopologyTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Path sys() {
        return tmp.getRoot().toPath();
    }

    private void write(String file, String content) throws IOException {
        final Path path = sys().resolve(file);
        Files.createDirectories(path.getParent());
        Files.writeString(path, content);
    }

    private void node(int node, String cpus, String distance) throws IOException {
        write("devices/system/node/node" + node + "/cpulist", cpus + "\n");
        write("devices/system/node/node" + node + "/distance", distance + "\n");
    }

    private void cache(int cpu, int index, int level, String shared) throws IOException {
        final String dir = "devices/system/cpu/cpu" + cpu + "/cache/index" + index + "/";
        write(dir + "level", level + "\n");
        write(dir + "shared_cpu_list", shared + "\n");
    }

    /**
     * Two sockets, the OS numbers their CPUs in turn: 8-15 reserved means 15,13,11,9 and 14,12,10,8.
     */
    private NumaTopology twoSockets() throws IOException {
        write("devices/system/node/online", "0-1\n");
        node(0, "0,2,4,6,8,10,12,14", "10 21");
        node(1, "1,3,5,7,9,11,13,15", "21 10");
        return NumaTopology.read(sys());
    }

    private static BitSet range(int first, int last) {
        final BitSet cpus = new BitSet();
        cpus.set(first, last + 1);
        return cpus;
    }

    @Test
    public void readsNodesAndCpus() throws IOException {
        final NumaTopology topology = twoSockets();
        assertEquals(bits(0, 1), topology.nodes());
        assertEquals(bits(1, 3, 5, 7, 9, 11, 13, 15), topology.cpusOf(1));
        assertEquals(0, topology.nodeOfCpu(14));
        assertEquals(1, topology.nodeOfCpu(13));
        assertEquals("node 0: cpus 0,2,4,6,8,10,12,14; node 1: cpus 1,3,5,7,9,11,13,15", topology.toString());
    }

    @Test
    public void readsDistances() throws IOException {
        final NumaTopology topology = twoSockets();
        assertEquals(10, topology.distance(1, 1));
        assertEquals(21, topology.distance(0, 1));
    }

    @Test
    public void distancesFollowTheOrderOfTheNodesNotTheirIds() throws IOException {
        write("devices/system/node/online", "0,2\n");
        node(0, "0", "10 31");
        node(2, "1", "31 10");
        final NumaTopology topology = NumaTopology.read(sys());
        assertEquals(31, topology.distance(0, 2));
        assertEquals(10, topology.distance(2, 2));
        assertEquals(-1, topology.distance(0, 1));
    }

    @Test
    public void withoutDistanceFilesTheKernelDefaults() throws IOException {
        write("devices/system/node/online", "0-1\n");
        write("devices/system/node/node0/cpulist", "0\n");
        write("devices/system/node/node1/cpulist", "1\n");
        final NumaTopology topology = NumaTopology.read(sys());
        assertEquals(10, topology.distance(0, 0));
        assertEquals(20, topology.distance(0, 1));
    }

    @Test
    public void nodeWithMemoryOnlyHasNoCpus() throws IOException {
        write("devices/system/node/online", "0-1\n");
        node(0, "0-3", "10 40");
        node(1, "", "40 10");
        final NumaTopology topology = NumaTopology.read(sys());
        assertEquals(bits(0, 1), topology.nodes());
        assertEquals(new BitSet(), topology.cpusOf(1));
    }

    @Test
    public void whatDoesNotExistIsMinusOneOrEmpty() throws IOException {
        final NumaTopology topology = twoSockets();
        assertEquals(new BitSet(), topology.cpusOf(7));
        assertEquals(new BitSet(), topology.cpusOf(-1));
        assertEquals(-1, topology.nodeOfCpu(99));
        assertEquals(-1, topology.nodeOfCpu(-1));
        assertEquals(-1, topology.distance(0, 7));
        assertEquals(-1, topology.distance(-1, 0));
        assertEquals(new BitSet(), topology.lastLevelCacheOf(-1));
    }

    @Test
    public void withoutNodesOneNodeWithTheOnlineCpus() throws IOException {
        write("devices/system/cpu/online", "0-3\n");
        final NumaTopology topology = NumaTopology.read(sys());
        assertEquals(bits(0), topology.nodes());
        assertEquals(range(0, 3), topology.cpusOf(0));
        assertEquals(0, topology.nodeOfCpu(3));
        assertEquals(10, topology.distance(0, 0));
    }

    @Test
    public void lastLevelCacheIsTheHighestLevel() throws IOException {
        twoSockets();
        cache(13, 0, 1, "13");
        cache(13, 1, 1, "13");
        cache(13, 2, 2, "13");
        cache(13, 3, 3, "1,3,5,7,9,11,13,15");
        assertEquals(bits(1, 3, 5, 7, 9, 11, 13, 15), NumaTopology.read(sys()).lastLevelCacheOf(13));
    }

    @Test
    public void withoutCacheInfoTheCpuAlone() throws IOException {
        assertEquals(bits(13), twoSockets().lastLevelCacheOf(13));
    }

    @Test
    public void readsTheNodeOfDevices() throws IOException {
        write("class/net/eno1/device/numa_node", "1\n");
        write("class/net/eno2/device/numa_node", "-1\n");
        write("class/net/eno3/device/numa_node", "7\n");
        write("bus/pci/devices/0000-18-00.0/numa_node", "1\n"); // no ":", the tests run on Windows too
        final NumaTopology topology = twoSockets();
        assertEquals(1, topology.nodeOfNetworkInterface("eno1"));
        assertEquals(-1, topology.nodeOfNetworkInterface("eno2"));
        assertEquals(-1, topology.nodeOfNetworkInterface("eno3"));
        assertEquals(-1, topology.nodeOfNetworkInterface("eno4"));
        assertEquals(1, topology.nodeOfPciDevice("0000-18-00.0"));
        assertEquals(-1, topology.nodeOfPciDevice("0000:3b:00.0"));
    }

    @Test
    public void nearestFirstStaysOnOneNodeWhenTheSocketsAreNumberedInTurn() throws IOException {
        assertArrayEquals(new int[]{15, 13, 11, 9, 14, 12, 10, 8}, twoSockets().nearestFirst(15, range(8, 15)));
    }

    @Test
    public void nearestFirstFromTheOtherNode() throws IOException {
        assertArrayEquals(new int[]{14, 12, 10, 8, 15, 13, 11, 9}, twoSockets().nearestFirst(2, range(8, 15)));
    }

    /**
     * One node of two last-level caches, as on many AMD CPUs: 0-3 and 4-7.
     */
    @Test
    public void nearestFirstPrefersTheLastLevelCache() throws IOException {
        write("devices/system/node/online", "0-1\n");
        node(0, "0-7", "10 32");
        node(1, "8-9", "32 10");
        cache(2, 0, 3, "0-3");
        assertArrayEquals(new int[]{3, 2, 1, 0, 7, 6, 5, 4, 9, 8}, NumaTopology.read(sys()).nearestFirst(2, range(0, 9)));
    }

    /**
     * Two sockets of two sub-NUMA clusters each: the other cluster of the socket comes before the other socket.
     */
    @Test
    public void nearestFirstGoesByDistanceOneNodeAtATime() throws IOException {
        write("devices/system/node/online", "0-3\n");
        node(0, "0,4", "10 11 21 21");
        node(1, "1,5", "11 10 21 21");
        node(2, "2,6", "21 21 10 11");
        node(3, "3,7", "21 21 11 10");
        assertArrayEquals(new int[]{4, 0, 5, 1, 7, 3, 6, 2}, NumaTopology.read(sys()).nearestFirst(4, range(0, 7)));
    }

    @Test
    public void nearestFirstPutsCpusOfUnknownNodesLast() throws IOException {
        assertArrayEquals(new int[]{13, 14, 20}, twoSockets().nearestFirst(15, bits(13, 14, 20)));
    }

    @Test
    public void nearestFirstOfNothing() throws IOException {
        assertArrayEquals(new int[0], twoSockets().nearestFirst(-1, new BitSet()));
    }
}
