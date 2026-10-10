package exchange.core2.collections.affinity;

import java.nio.file.Path;
import java.util.BitSet;

/**
 * Native side of {@link CpuAffinity}. All methods act on the calling thread.
 */
interface AffinityPlatform {

    /**
     * CPUs the OS keeps free for pinned threads (Linux: isolcpus), the default reserved set.
     */
    BitSet isolatedCpus();

    /**
     * Logical CPUs of the physical core that contains this CPU (hyper-threading siblings), including it.
     */
    BitSet coreOf(int cpu);

    /**
     * Pins the calling thread to one CPU.
     *
     * @return the thread's previous mask, null if pinning failed
     */
    BitSet pin(int cpu);

    /**
     * Sets the calling thread's mask back.
     */
    boolean restore(BitSet mask);

    /**
     * The implementation for this OS; null if there is none or native access is not available.
     */
    static AffinityPlatform detect(System.Logger log) {
        final String os = System.getProperty("os.name", "");
        try {
            if (os.startsWith("Linux")) return new LinuxAffinity(Path.of("/sys/devices/system/cpu"));
            if (os.startsWith("Windows")) return new WindowsAffinity();
        } catch (Throwable e) {
            // e.g. --illegal-native-access=deny, or a C library without the functions
            log.log(System.Logger.Level.WARNING, "thread affinity not available on " + os, e);
            return null;
        }
        log.log(System.Logger.Level.DEBUG, "thread affinity not supported on {0}", os);
        return null;
    }
}
