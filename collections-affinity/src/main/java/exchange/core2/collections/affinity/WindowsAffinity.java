package exchange.core2.collections.affinity;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * SetThreadAffinityMask and GetLogicalProcessorInformation of kernel32. Processor group 0 only (up to 64 CPUs).
 * Windows has no isolated CPUs: the reserved set comes from -Daffinity.reserved, or the CPU is given explicitly.
 */
final class WindowsAffinity implements AffinityPlatform {

    private static final int RELATION_PROCESSOR_CORE = 0;
    /**
     * SYSTEM_LOGICAL_PROCESSOR_INFORMATION on x64: ULONG_PTR ProcessorMask, enum Relationship (+ padding), 16-byte union.
     */
    private static final int INFO_SIZE = 32;
    private static final int RELATIONSHIP_OFFSET = 8;

    private final MethodHandle getCurrentThread;
    private final MethodHandle setThreadAffinityMask;
    /**
     * Masks of the physical cores.
     */
    private final List<BitSet> cores;

    WindowsAffinity() {
        final Linker linker = Linker.nativeLinker();
        final SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32", Arena.global());
        // HANDLE GetCurrentThread() - a pseudo handle, valid in the calling thread
        this.getCurrentThread = linker.downcallHandle(kernel32.find("GetCurrentThread").orElseThrow(),
                FunctionDescriptor.of(ADDRESS));
        // DWORD_PTR SetThreadAffinityMask(HANDLE thread, DWORD_PTR mask) - returns the previous mask, 0 on error
        this.setThreadAffinityMask = linker.downcallHandle(kernel32.find("SetThreadAffinityMask").orElseThrow(),
                FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG));
        // BOOL GetLogicalProcessorInformation(PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer, PDWORD length)
        final MethodHandle processorInformation = linker.downcallHandle(
                kernel32.find("GetLogicalProcessorInformation").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        this.cores = readCores(processorInformation);
    }

    @Override
    public BitSet isolatedCpus() {
        return new BitSet();
    }

    @Override
    public BitSet coreOf(int cpu) {
        for (BitSet core : cores) {
            if (core.get(cpu)) return (BitSet) core.clone();
        }
        final BitSet single = new BitSet();
        single.set(cpu);
        return single;
    }

    @Override
    public BitSet pin(int cpu) {
        if (cpu >= 64) return null;
        final long previous = setMask(1L << cpu);
        return previous != 0 ? BitSet.valueOf(new long[]{previous}) : null;
    }

    @Override
    public boolean restore(BitSet mask) {
        final long[] words = mask.toLongArray();
        return words.length > 0 && setMask(words[0]) != 0;
    }

    private long setMask(long mask) {
        try {
            return (long) setThreadAffinityMask.invokeExact((MemorySegment) getCurrentThread.invokeExact(), mask);
        } catch (Throwable e) {
            return 0;
        }
    }

    private static List<BitSet> readCores(MethodHandle processorInformation) {
        final List<BitSet> cores = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment length = arena.allocate(JAVA_INT);
            // first call fails and reports the buffer size
            final int sizing = (int) processorInformation.invokeExact(MemorySegment.NULL, length);
            final int size = length.get(JAVA_INT, 0);
            if (sizing != 0 || size <= 0) return cores;
            final MemorySegment buffer = arena.allocate(size, 8);
            if ((int) processorInformation.invokeExact(buffer, length) == 0) return cores;
            final int filled = length.get(JAVA_INT, 0);
            for (long offset = 0; offset + INFO_SIZE <= filled; offset += INFO_SIZE) {
                if (buffer.get(JAVA_INT, offset + RELATIONSHIP_OFFSET) == RELATION_PROCESSOR_CORE) {
                    cores.add(BitSet.valueOf(new long[]{buffer.get(JAVA_LONG, offset)}));
                }
            }
        } catch (Throwable e) {
            // no topology: every CPU is its own core
        }
        return cores;
    }
}
