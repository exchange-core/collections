package exchange.core2.collections.affinity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Reserves CPUs across processes: one lock file per logical CPU, locked while a {@link CpuAffinity} holds it.
 * The lock is the OS file lock, so it is released when the process dies; the files themselves stay.
 */
final class CpuLockFiles {

    private static final System.Logger LOG = System.getLogger(CpuLockFiles.class.getName());

    private final Path dir;

    CpuLockFiles(Path dir) {
        this.dir = dir;
    }

    Path file(int cpu) {
        return dir.resolve("exchange-core2-cpu-" + cpu + ".lock");
    }

    /**
     * Locks every CPU of the set or none.
     *
     * @return the open locked files, null if a CPU is locked by another process or by another lock in this one
     */
    List<FileChannel> tryLock(BitSet cpus, String owner) {
        final List<FileChannel> locked = new ArrayList<>();
        for (int cpu = cpus.nextSetBit(0); cpu >= 0; cpu = cpus.nextSetBit(cpu + 1)) {
            final Path file = file(cpu);
            final FileChannel channel;
            try {
                createSharedFile(file);
                channel = FileChannel.open(file, StandardOpenOption.WRITE);
            } catch (IOException e) {
                // e.g. a file created by another user: it can not coordinate anything, so it must not block the CPU
                LOG.log(System.Logger.Level.WARNING, "cpu {0} is not reserved across processes: {1}", cpu, e.toString());
                continue;
            }
            if (!lock(channel, owner)) {
                release(locked);
                return null;
            }
            locked.add(channel);
        }
        return locked;
    }

    static void release(List<FileChannel> locked) {
        for (FileChannel channel : locked) {
            try {
                channel.close(); // releases the lock
            } catch (IOException ignore) {
                // the lock goes with the channel anyway
            }
        }
    }

    private static boolean lock(FileChannel channel, String owner) {
        try {
            final FileLock lock = channel.tryLock();
            if (lock != null) {
                channel.truncate(0);
                channel.write(ByteBuffer.wrap(("pid " + ProcessHandle.current().pid() + ", thread " + owner + "\n")
                        .getBytes(StandardCharsets.UTF_8)));
                return true;
            }
        } catch (OverlappingFileLockException | IOException e) {
            // held by this JVM (overlapping) or not lockable
        }
        try {
            channel.close();
        } catch (IOException ignore) {
            // not locked by us
        }
        return false;
    }

    /**
     * Created writable for everyone (not limited by umask), so that processes of other users can lock it later.
     */
    private static void createSharedFile(Path file) throws IOException {
        try {
            Files.createFile(file);
        } catch (FileAlreadyExistsException e) {
            return;
        }
        try {
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-rw-rw-"));
        } catch (UnsupportedOperationException e) {
            // not POSIX (Windows)
        }
    }
}
