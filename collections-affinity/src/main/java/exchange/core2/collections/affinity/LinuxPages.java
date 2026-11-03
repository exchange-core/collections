package exchange.core2.collections.affinity;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * The move_pages system call, only to read where pages are. Through syscall of glibc: it has no move_pages, libnuma has.
 */
final class LinuxPages {

    private final long movePages;
    private final MethodHandle syscall;
    private final MethodHandle memmove;

    /**
     * @param movePages the system call's number, see {@link #movePagesNumber(String)}
     */
    LinuxPages(long movePages) {
        final Linker linker = Linker.nativeLinker();
        this.movePages = movePages;
        // long syscall(long number, ...) - takes up to six arguments
        this.syscall = linker.downcallHandle(linker.defaultLookup().find("syscall").orElseThrow(),
                FunctionDescriptor.of(JAVA_LONG,
                        JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG),
                Linker.Option.firstVariadicArg(1));
        // void *memmove(void *dest, const void *src, size_t n) - only returns dest when n is 0
        this.memmove = linker.downcallHandle(linker.defaultLookup().find("memmove").orElseThrow(),
                FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, JAVA_LONG),
                Linker.Option.critical(true));
    }

    /**
     * -1 for an architecture not known here.
     */
    static long movePagesNumber(String arch) {
        return switch (arch) {
            case "amd64" -> 279;
            case "aarch64" -> 239;
            default -> -1;
        };
    }

    /**
     * The address of the segment. The JDK gives the address of an array only to a native call, for its duration: the
     * value is out of date if the GC moves the array later.
     */
    long addressOf(MemorySegment segment) throws Throwable {
        return segment.isNative() ? segment.address() : (long) memmove.invokeExact(segment, segment, 0L);
    }

    /**
     * The node of each page of this process, a negative errno for a page that is not there; null if the call failed.
     */
    int[] nodesOf(long[] pages) {
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment addresses = arena.allocateFrom(JAVA_LONG, pages);
            final MemorySegment status = arena.allocate(JAVA_INT, pages.length);
            // long move_pages(pid_t pid (0 = this process), count, void **pages, int *nodes (NULL = only report),
            // int *status, int flags)
            final long rc = (long) syscall.invokeExact(movePages,
                    0L, (long) pages.length, addresses.address(), 0L, status.address(), 0L);
            return rc == 0 ? status.toArray(JAVA_INT) : null;
        } catch (Throwable e) {
            return null;
        }
    }
}
