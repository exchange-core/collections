package exchange.core2.collections.affinity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static exchange.core2.collections.affinity.CpuSetsTest.bits;
import static org.junit.Assert.assertEquals;

/**
 * Core detection on a fake /sys/devices/system/cpu.
 */
public class LinuxTopologyTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void readsCoreCpusList() throws IOException {
        final Path sysfs = tmp.getRoot().toPath();
        write(sysfs, 3, "core_cpus_list", "3,19\n");
        assertEquals(bits(3, 19), LinuxAffinity.coreOf(sysfs, 3));
    }

    @Test
    public void fallsBackToThreadSiblingsList() throws IOException {
        final Path sysfs = tmp.getRoot().toPath();
        write(sysfs, 4, "thread_siblings_list", "4-5\n");
        assertEquals(bits(4, 5), LinuxAffinity.coreOf(sysfs, 4));
    }

    @Test
    public void withoutTopologyTheCpuIsItsOwnCore() {
        assertEquals(bits(7), LinuxAffinity.coreOf(tmp.getRoot().toPath(), 7));
    }

    private static void write(Path sysfs, int cpu, String file, String content) throws IOException {
        final Path topology = Files.createDirectories(sysfs.resolve("cpu" + cpu).resolve("topology"));
        Files.writeString(topology.resolve(file), content);
    }
}
