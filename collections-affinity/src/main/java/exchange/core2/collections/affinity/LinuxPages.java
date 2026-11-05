package exchange.core2.collections.affinity;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * The move_pages system call, only to read where pages are. Through syscall of glibc: it has no move_pages, libnuma has.
 */
final class LinuxPages {

    private static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
    private static final long ERRNO = CALL_STATE.byteOffset(MemoryLayout.PathElement.groupElement("errno"));

    private final long number;
    private final MethodHandle syscall;
    private final MethodHandle memmove;

    /**
     * @param number the system call's number, see {@link #movePagesNumber(String)}
     */
    LinuxPages(long number) {
        final Linker linker = Linker.nativeLinker();
        this.number = number;
        // long syscall(long number, ...) - takes up to six arguments
        this.syscall = linker.downcallHandle(linker.defaultLookup().find("syscall").orElseThrow(),
                FunctionDescriptor.of(JAVA_LONG,
                        JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG),
                Linker.Option.firstVariadicArg(1), Linker.Option.captureCallState("errno"));
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
     * The address of the segment, 0 if it can not tell. The JDK gives the address of an array only to a native call,
     * for its duration: here it is a number to compare and to ask the kernel about, nothing reads memory through it.
     */
    long addressOf(MemorySegment segment) {
        if (segment.isNative()) return segment.address();
        try {
            return (long) memmove.invokeExact(segment, segment, 0L);
        } catch (Throwable e) {
            return 0;
        }
    }

    /**
     * Tries the call: 0 if it works here, otherwise its errno - 1 (EPERM) under the default seccomp profile of Docker.
     */
    int probe() throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            return movePages(arena, new long[]{arena.allocate(8).address()}, arena.allocate(JAVA_INT));
        }
    }

    /**
     * The node of each page of this process, a negative errno for a page that is not there; null if the call failed.
     */
    int[] nodesOf(long[] pages) {
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment status = arena.allocate(JAVA_INT, pages.length);
            return movePages(arena, pages, status) == 0 ? status.toArray(JAVA_INT) : null;
        } catch (Throwable e) {
            return null;
        }
    }

    /**
     * @return 0, or the errno of the call
     */
    private int movePages(Arena arena, long[] pages, MemorySegment status) throws Throwable {
        final MemorySegment addresses = arena.allocateFrom(JAVA_LONG, pages);
        final MemorySegment callState = arena.allocate(CALL_STATE);
        // long move_pages(pid_t pid (0 = this process), count, void **pages, int *nodes (NULL = only report),
        // int *status, int flags)
        final long rc = (long) syscall.invokeExact(callState, number,
                0L, (long) pages.length, addresses.address(), 0L, status.address(), 0L);
        return rc == 0 ? 0 : callState.get(JAVA_INT, ERRNO);
    }
}
