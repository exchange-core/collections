package exchange.core2.collections.affinity;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.BitSet;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * sched_setaffinity / sched_getaffinity of glibc, topology from sysfs.
 */
final class LinuxAffinity implements AffinityPlatform {

    /**
     * cpu_set_t of glibc: 1024 CPUs. On little-endian x86/ARM its bit n is bit n%8 of byte n/8 - the layout of
     * {@link BitSet#valueOf(byte[])}.
     */
    private static final int MASK_BYTES = 128;

    private final MethodHandle setAffinity;
    private final MethodHandle getAffinity;
    /**
     * /sys/devices/system/cpu
     */
    private final Path sysfs;

    LinuxAffinity(Path sysfs) {
        final Linker linker = Linker.nativeLinker();
        // int sched_[gs]etaffinity(pid_t pid (0 = calling thread), size_t size, cpu_set_t *mask)
        final FunctionDescriptor descriptor = FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, ADDRESS);
        this.setAffinity = linker.downcallHandle(linker.defaultLookup().find("sched_setaffinity").orElseThrow(), descriptor);
        this.getAffinity = linker.downcallHandle(linker.defaultLookup().find("sched_getaffinity").orElseThrow(), descriptor);
        this.sysfs = sysfs;
    }

    @Override
    public BitSet isolatedCpus() {
        return CpuSets.readList(sysfs.resolve("isolated"));
    }

    @Override
    public BitSet coreOf(int cpu) {
        return coreOf(sysfs, cpu);
    }

    /**
     * core_cpus_list since kernel 5.7, thread_siblings_list before.
     */
    static BitSet coreOf(Path sysfs, int cpu) {
        final Path topology = sysfs.resolve("cpu" + cpu).resolve("topology");
        BitSet core = CpuSets.readList(topology.resolve("core_cpus_list"));
        if (core.isEmpty()) core = CpuSets.readList(topology.resolve("thread_siblings_list"));
        core.set(cpu);
        return core;
    }

    @Override
    public BitSet pin(int cpu) {
        if (cpu >= MASK_BYTES * 8) return null;
        final BitSet previous = call(getAffinity, new BitSet());
        if (previous == null) return null;
        final BitSet mask = new BitSet();
        mask.set(cpu);
        return call(setAffinity, mask) != null ? previous : null;
    }

    @Override
    public boolean restore(BitSet mask) {
        return call(setAffinity, mask) != null;
    }

    /**
     * Calls get/set with a copy of the mask; returns the mask after the call, null on error.
     */
    private static BitSet call(MethodHandle function, BitSet mask) {
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment segment = arena.allocate(MASK_BYTES);
            final byte[] bytes = mask.toByteArray();
            MemorySegment.copy(bytes, 0, segment, JAVA_BYTE, 0, Math.min(bytes.length, MASK_BYTES));
            final int rc = (int) function.invokeExact(0, (long) MASK_BYTES, segment);
            return rc == 0 ? BitSet.valueOf(segment.toArray(JAVA_BYTE)) : null;
        } catch (Throwable e) {
            return null;
        }
    }
}
