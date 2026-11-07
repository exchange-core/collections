package exchange.core2.collections.affinity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Reserves CPUs across processes: one lock file per logical CPU, locked while a {@link CpuAffinity} holds it.
 * The lock is the OS file lock, so it is released when the process dies; the files themselves stay. Only processes
 * with the same directory see each other; it is created if missing. A file is opened without following a symbolic link:
 * in a directory others can write to, a link would point the truncation at a file of their choosing.
 */
final class CpuLockFiles {

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
     * @throws IOException if a lock file can not be created, opened or locked - a file of another user, a symbolic
     *                     link: then the CPU must not be used, nothing would tell another process it is taken
     */
    List<FileChannel> tryLock(BitSet cpus, String owner) throws IOException {
        final List<FileChannel> locked = new ArrayList<>();
        try {
            Files.createDirectories(dir);
            for (int cpu = cpus.nextSetBit(0); cpu >= 0; cpu = cpus.nextSetBit(cpu + 1)) {
                final Path file = file(cpu);
                createSharedFile(file);
                final FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                locked.add(channel);
                if (!lock(channel, owner)) {
                    release(locked);
                    return null;
                }
            }
            return locked;
        } catch (IOException e) {
            release(locked);
            throw e;
        }
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

    /**
     * @return false if another process or another channel of this one holds the lock
     */
    private static boolean lock(FileChannel channel, String owner) throws IOException {
        final FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return false;
        }
        if (lock == null) return false;
        channel.truncate(0);
        channel.write(ByteBuffer.wrap(("pid " + ProcessHandle.current().pid() + ", thread " + owner + "\n")
                .getBytes(StandardCharsets.UTF_8)));
        return true;
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
