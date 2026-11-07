package exchange.core2.collections.affinity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static exchange.core2.collections.affinity.CpuSetsTest.bits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

public class CpuLockFilesTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void allOrNothing() throws Exception {
        final CpuLockFiles files = new CpuLockFiles(tmp.getRoot().toPath());
        final List<FileChannel> first = files.tryLock(bits(2, 3), "a");
        assertNotNull(first);

        // 3 is held: 4 must not stay locked either
        assertNull(files.tryLock(bits(3, 4), "b"));
        final List<FileChannel> four = files.tryLock(bits(4), "c");
        assertNotNull(four);

        CpuLockFiles.release(first);
        // readable once released (Windows locks are mandatory)
        assertTrue(Files.readString(files.file(2)).contains("thread a"));
        final List<FileChannel> again = files.tryLock(bits(2, 3), "d");
        assertNotNull(again);
        CpuLockFiles.release(again);
        CpuLockFiles.release(four);
    }

    @Test
    public void doesNotFollowALink() throws Exception {
        final Path dir = tmp.getRoot().toPath();
        final Path other = Files.writeString(dir.resolve("other"), "keep");
        final CpuLockFiles files = new CpuLockFiles(dir);
        try {
            Files.createSymbolicLink(files.file(7), other);
        } catch (IOException | UnsupportedOperationException e) {
            assumeNoException("no symbolic links here", e);
        }
        assertThrows(IOException.class, () -> files.tryLock(bits(7), "test"));
        assertEquals("keep", Files.readString(other));
    }

    @Test
    public void createsItsDirectory() throws Exception {
        final CpuLockFiles files = new CpuLockFiles(tmp.getRoot().toPath().resolve("not/there"));
        final List<FileChannel> locked = files.tryLock(bits(1), "test");
        assertNotNull(locked);
        assertTrue(Files.isRegularFile(files.file(1)));
        CpuLockFiles.release(locked);
    }

    @Test
    public void aFileThatCanNotBeUsedRefusesItsCpu() throws Exception {
        // nothing would tell another process the CPU is taken
        final Path notADirectory = tmp.newFile("file").toPath();
        assertThrows(IOException.class, () -> new CpuLockFiles(notADirectory).tryLock(bits(1), "test"));

        final CpuLockFiles files = new CpuLockFiles(tmp.getRoot().toPath());
        Files.createDirectory(files.file(4));
        assertThrows(IOException.class, () -> files.tryLock(bits(3, 4), "test"));
        // all or nothing: 3 is free again
        final List<FileChannel> three = files.tryLock(bits(3), "test");
        assertNotNull(three);
        CpuLockFiles.release(three);
    }

    @Test
    public void heldByAnotherProcessUntilItDies() throws Exception {
        final Path dir = tmp.getRoot().toPath();
        final Process holder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                LockHolder.class.getName(), dir.toString(), "6")
                .redirectErrorStream(true)
                .start();
        try {
            final BufferedReader out = new BufferedReader(new InputStreamReader(holder.getInputStream()));
            assertEquals("locked", out.readLine());

            final CpuLockFiles files = new CpuLockFiles(dir);
            assertNull(files.tryLock(bits(6), "test"));
        } finally {
            holder.destroyForcibly();
            assertTrue(holder.waitFor(30, TimeUnit.SECONDS));
        }
        // the OS releases the lock of a dead process
        final List<FileChannel> locked = new CpuLockFiles(dir).tryLock(bits(6), "test");
        assertNotNull(locked);
        CpuLockFiles.release(locked);
    }

    /**
     * Locks a CPU and waits to be killed.
     */
    public static final class LockHolder {
        public static void main(String[] args) throws Exception {
            final List<FileChannel> locked = new CpuLockFiles(Path.of(args[0])).tryLock(bits(Integer.parseInt(args[1])), "holder");
            System.out.println(locked != null ? "locked" : "busy");
            System.out.flush();
            Thread.sleep(60_000);
        }
    }
}
