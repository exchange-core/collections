package exchange.core2.collections.affinity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static exchange.core2.collections.affinity.CpuSetsTest.bits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * On a fake /proc/self and /sys.
 */
public class ProcessPlacementTest {

    private static final long GIB = 1L << 30;
    private static final long PAGES_IN_GIB = GIB / 4096;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private void write(String file, String content) throws IOException {
        final Path path = tmp.getRoot().toPath().resolve(file);
        Files.createDirectories(path.getParent());
        Files.writeString(path, content);
    }

    /**
     * Node 0 the even CPUs, node 1 the odd ones.
     */
    private NumaTopology twoSockets(long freeGib, long fileGib) throws IOException {
        write("sys/devices/system/node/online", "0-1\n");
        for (int node = 0; node < 2; node++) {
            write("sys/devices/system/node/node" + node + "/cpulist", node == 0 ? "0,2\n" : "1,3\n");
            write("sys/devices/system/node/node" + node + "/meminfo", String.format("""
                    Node %1$d MemTotal:       %2$d kB
                    Node %1$d MemFree:        %3$d kB
                    Node %1$d Active(file):   %4$d kB
                    Node %1$d Inactive(file): 0 kB
                    Node %1$d SReclaimable:   0 kB
                    Node %1$d HugePages_Free: 0
                    """, node, 100 * GIB / 1024, freeGib * GIB / 1024, fileGib * GIB / 1024));
        }
        return NumaTopology.read(tmp.getRoot().toPath().resolve("sys"));
    }

    private NumaTopology plenty() throws IOException {
        return twoSockets(90, 0);
    }

    private ProcessPlacement process(String policy, String cpus, String... otherMaps) throws IOException {
        final StringBuilder maps = new StringBuilder();
        for (String map : otherMaps) maps.append(map).append('\n');
        write("proc/numa_maps", maps + "7ffd " + policy + " stack anon=1 dirty=1 N1=1 kernelpagesize_kB=4\n");
        write("proc/status", "Name:\tjava\nCpus_allowed_list:\t" + cpus + "\nMems_allowed_list:\t0-1\n");
        return ProcessPlacement.read(tmp.getRoot().toPath().resolve("proc"));
    }

    private static String anon(long onNode0, long onNode1) {
        return "7f00 default anon=1 N0=" + onNode0 + " N1=" + onNode1 + " kernelpagesize_kB=4";
    }

    private static void assertOneWarning(List<String> warnings, String part) {
        assertEquals(warnings.toString(), 1, warnings.size());
        assertTrue(warnings.get(0), warnings.get(0).contains(part));
    }

    @Test
    public void thePolicyIsThatOfTheProcessNotOfARegion() throws IOException {
        final ProcessPlacement placement = process("default", "1", "f0000000 bind:1 anon=9 N1=9 kernelpagesize_kB=4");
        assertEquals("default", placement.policy());
    }

    @Test
    public void withoutAStackThePolicyOfTheFirstMapping() throws IOException {
        write("proc/numa_maps", "f0000000 bind:1 anon=9 N1=9 kernelpagesize_kB=4\n7f00 default anon=1 N0=1 kernelpagesize_kB=4\n");
        assertEquals("bind:1", ProcessPlacement.read(tmp.getRoot().toPath().resolve("proc")).policy());
    }

    @Test
    public void readsEveryFormOfPolicy() throws IOException {
        for (String policy : new String[]{"default", "local", "bind:1", "bind=static:1", "bind=relative:0", "prefer:1",
                "prefer (many):0-1", "interleave:0-1", "interleave=static:0,2", "weighted interleave:0-1"}) {
            assertEquals(policy, process(policy, "1").policy());
        }
    }

    @Test
    public void aStackWithoutPagesEndsTheLine() throws IOException {
        write("proc/numa_maps", "7ffd prefer:1 stack\n");
        assertEquals("prefer:1", ProcessPlacement.read(tmp.getRoot().toPath().resolve("proc")).policy());
    }

    @Test
    public void readsTheMaskAndTheAllowedNodes() throws IOException {
        final ProcessPlacement placement = process("default", "1,3");
        assertEquals(bits(1, 3), placement.cpus());
        assertEquals(bits(0, 1), placement.allowedNodes());
    }

    @Test
    public void countsAnonymousMemoryAndHugePagesNotMappedFiles() throws IOException {
        final ProcessPlacement placement = process("default", "1",
                "7f00 default anon=512 dirty=512 N0=12 N1=500 kernelpagesize_kB=4",
                "7f01 default file=/usr/lib/libjvm.so mapped=100 N0=100 kernelpagesize_kB=4",
                "7f02 default file=/anon_hugepage\\040(deleted) huge dirty=2 N1=2 kernelpagesize_kB=2048",
                "50dba000 default");
        assertEquals(12 * 4096, placement.bytesOn(0));
        assertEquals((500 + 1) * 4096 + 2 * 2048 * 1024, placement.bytesOn(1));
        assertEquals(0, placement.bytesOn(2));
        assertEquals(0, placement.bytesOn(-1));
    }

    @Test
    public void reportsAllOfItForALog() throws IOException {
        assertEquals("policy bind:1, cpus 1,3, memory allowed on 0-1, node 1 1.0 GiB",
                process("bind:1", "1,3", anon(0, PAGES_IN_GIB)).toString());
    }

    @Test
    public void withoutTheFilesNothingIsKnownAndNothingToSay() throws IOException {
        final ProcessPlacement placement = ProcessPlacement.read(tmp.getRoot().toPath().resolve("proc"));
        assertEquals("", placement.policy());
        assertEquals(bits(), placement.cpus());
        assertEquals(bits(), placement.allowedNodes());
        assertEquals(List.of(), placement.warningsFor(1, plenty(), GIB));
        assertTrue(placement.toString(), placement.toString().startsWith("policy unknown"));
    }

    @Test
    public void onOneNodeThereIsNothingToSay() throws IOException {
        write("sys/devices/system/node/online", "0\n");
        write("sys/devices/system/node/node0/cpulist", "0-3\n");
        write("sys/devices/system/node/node0/meminfo", "Node 0 MemTotal: 1024 kB\nNode 0 MemFree: 1024 kB\n");
        final NumaTopology oneNode = NumaTopology.read(tmp.getRoot().toPath().resolve("sys"));
        assertEquals(List.of(), process("bind:7", "9").warningsFor(0, oneNode, 100 * GIB));
    }

    @Test
    public void thereIsNoSuchNode() throws IOException {
        final ProcessPlacement placement = process("default", "1");
        assertOneWarning(placement.warningsFor(7, plenty(), GIB), "no node 7");
        assertOneWarning(placement.warningsFor(-1, plenty(), GIB), "no node -1");
    }

    @Test
    public void aNodeWithoutMemorySaysOnlyThat() throws IOException {
        final NumaTopology topology = plenty();
        write("sys/devices/system/node/node1/meminfo", "Node 1 MemTotal: 0 kB\nNode 1 MemFree: 0 kB\n");
        assertOneWarning(process("bind:0", "0").warningsFor(1, topology, GIB), "node 1 has no memory of its own");
        assertOneWarning(ProcessPlacement.ownMemoryWarnings(1, topology), "node 1 has no memory of its own");
        assertEquals(List.of(), ProcessPlacement.ownMemoryWarnings(0, topology));
        assertEquals(List.of(), ProcessPlacement.ownMemoryWarnings(7, topology));
    }

    @Test
    public void theCpusetAllowsOtherNodes() throws IOException {
        final ProcessPlacement allowed = process("default", "1");
        write("proc/status", "Cpus_allowed_list:\t1\nMems_allowed_list:\t0\n");
        final ProcessPlacement placement = ProcessPlacement.read(tmp.getRoot().toPath().resolve("proc"));
        assertOneWarning(placement.warningsFor(1, plenty(), GIB), "allows memory only on nodes 0");
        assertEquals(List.of(), placement.warningsFor(0, plenty(), GIB).stream().filter(w -> w.contains("cpuset")).toList());
        assertEquals(List.of(), allowed.warningsFor(1, plenty(), GIB));
    }

    @Test
    public void thePolicySendsTheMemoryElsewhere() throws IOException {
        for (String policy : new String[]{"bind:0", "prefer:0", "interleave:0-1", "bind:0-1", "prefer (many):0-1"}) {
            assertOneWarning(process(policy, "1").warningsFor(1, plenty(), GIB), "policy of this process is " + policy);
        }
    }

    @Test
    public void aPolicyOnTheNodeIsEnoughWhereverTheCpusAre() throws IOException {
        for (String policy : new String[]{"bind:1", "prefer:1", "bind=static:1"}) {
            assertEquals(policy, List.of(), process(policy, "0,2").warningsFor(1, plenty(), GIB));
        }
    }

    @Test
    public void noPolicyAndTheCpusAreElsewhere() throws IOException {
        for (String policy : new String[]{"default", "local"}) {
            assertOneWarning(process(policy, "0,2").warningsFor(1, plenty(), GIB), "started on CPUs 0,2");
            assertOneWarning(process(policy, "0-3").warningsFor(1, plenty(), GIB), "started on CPUs 0-3");
        }
    }

    @Test
    public void noPolicyAndTheCpusAreOnTheNode() throws IOException {
        assertEquals(List.of(), process("default", "1,3").warningsFor(1, plenty(), GIB));
    }

    @Test
    public void theMemoryAlreadyIsElsewhere() throws IOException {
        final ProcessPlacement placement = process("default", "1,3", anon(PAGES_IN_GIB * 2, PAGES_IN_GIB));
        assertOneWarning(placement.warningsFor(1, plenty(), GIB), "1.0 GiB there and 2.0 GiB on other nodes");
    }

    @Test
    public void aLittleElsewhereIsNormal() throws IOException {
        final ProcessPlacement placement = process("default", "1,3", anon(PAGES_IN_GIB, PAGES_IN_GIB * 2));
        assertEquals(List.of(), placement.warningsFor(1, plenty(), GIB));
    }

    @Test
    public void whatIsAlreadyElsewhereIsNotRepeatedAfterTheCause() throws IOException {
        final ProcessPlacement placement = process("default", "0,2", anon(PAGES_IN_GIB * 2, 0));
        assertOneWarning(placement.warningsFor(1, plenty(), GIB), "started on CPUs 0,2");
    }

    @Test
    public void doesNotFit() throws IOException {
        final List<String> warnings = process("bind:1", "1").warningsFor(1, twoSockets(5, 1), 10 * GIB);
        assertOneWarning(warnings, "node 1 has not enough memory");
        assertTrue(warnings.get(0), warnings.get(0).contains("10.0 GiB"));
        assertTrue(warnings.get(0), warnings.get(0).contains("5.0 GiB is free and 1.0 GiB reclaimable"));
    }

    @Test
    public void whatIsAlreadyThereNeedsNoRoom() throws IOException {
        final ProcessPlacement placement = process("bind:1", "1", anon(0, PAGES_IN_GIB * 8));
        assertEquals(List.of(), placement.warningsFor(1, twoSockets(2, 0), 10 * GIB));
    }

    @Test
    public void moreThereThanNeededIsNoProblem() throws IOException {
        final ProcessPlacement placement = process("bind:1", "1", anon(0, PAGES_IN_GIB * 8));
        assertEquals(List.of(), placement.warningsFor(1, twoSockets(0, 0), 2 * GIB));
    }

    @Test
    public void fitsExactly() throws IOException {
        assertEquals(List.of(), process("default", "1").warningsFor(1, twoSockets(10, 0), 10 * GIB + 4096)); // 4096 is on it
    }

    @Test
    public void withoutABindingThePageCacheCounts() throws IOException {
        final NumaTopology cached = twoSockets(5, 10);
        assertOneWarning(process("default", "1").warningsFor(1, cached, 10 * GIB), "only without its page cache");
        assertOneWarning(process("prefer:1", "1").warningsFor(1, cached, 10 * GIB), "only without its page cache");
        assertEquals(List.of(), process("bind:1", "1").warningsFor(1, cached, 10 * GIB));
        assertEquals(List.of(), process("bind=static:1", "1").warningsFor(1, cached, 10 * GIB));
    }

    @Test
    public void reservedHugePagesCountAsFree() throws IOException {
        final NumaTopology topology = twoSockets(5, 0);
        write("sys/devices/system/node/node1/hugepages/hugepages-1048576kB/free_hugepages", "8\n");
        assertEquals(List.of(), process("default", "1").warningsFor(1, topology, 10 * GIB));
    }

    @Test
    public void noCheckOfTheRoomOnANodeTheMemoryDoesNotGoTo() throws IOException {
        final List<String> warnings = process("bind:0", "1").warningsFor(1, twoSockets(1, 0), 10 * GIB);
        assertOneWarning(warnings, "policy of this process is bind:0");
    }

    @Test
    public void unknownMemoryIsNoProblem() throws IOException {
        final NumaTopology topology = plenty();
        Files.delete(tmp.getRoot().toPath().resolve("sys/devices/system/node/node1/meminfo"));
        assertEquals(List.of(), process("default", "1").warningsFor(1, topology, 1000 * GIB));
    }

    @Test
    public void parsesSizes() {
        assertEquals(1024, ProcessPlacement.parseSize("1024"));
        assertEquals(2048, ProcessPlacement.parseSize("2k"));
        assertEquals(512L << 20, ProcessPlacement.parseSize("512m"));
        assertEquals(32L << 30, ProcessPlacement.parseSize("32g"));
        assertEquals(32L << 30, ProcessPlacement.parseSize(" 32G "));
        assertEquals(1L << 40, ProcessPlacement.parseSize("1t"));
        assertThrows(NumberFormatException.class, () -> ProcessPlacement.parseSize(""));
        assertThrows(NumberFormatException.class, () -> ProcessPlacement.parseSize("g"));
        assertThrows(NumberFormatException.class, () -> ProcessPlacement.parseSize("many"));
    }

    @Test
    public void thisProcess() {
        final ProcessPlacement placement = ProcessPlacement.current();
        if (System.getProperty("os.name", "").startsWith("Linux")) {
            assumeTrue("no NUMA in this kernel", !placement.policy().isEmpty());
            assertTrue(placement.toString(), !placement.cpus().isEmpty());
            assertTrue(placement.toString(), !placement.allowedNodes().isEmpty());
            assertTrue(placement.toString(), placement.toString().contains("policy " + placement.policy()));
        } else {
            assertEquals("", placement.policy());
            assertEquals(List.of(), placement.warningsFor(0));
        }
    }
}
