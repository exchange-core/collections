package exchange.core2.collections.affinity;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

/**
 * CPU set formats of the kernel and of the configuration.
 */
final class CpuSets {

    private CpuSets() {
    }

    /**
     * List format of sysfs and of the kernel command line: "0-3,8,10-11". Blank means empty.
     */
    static BitSet parseList(String list) {
        final BitSet cpus = new BitSet();
        for (String part : list.trim().split(",")) {
            if (part.isBlank()) continue;
            final String[] range = part.trim().split("-");
            final int first = Integer.parseInt(range[0].trim());
            final int last = Integer.parseInt(range[range.length - 1].trim());
            cpus.set(first, last + 1);
        }
        return cpus;
    }

    /**
     * Hex mask, most significant digit first: "AAAA" (as OpenHFT's affinity.reserved) or the kernel's bitmap
     * format of comma-separated 32-bit words "00000100,00000000". An optional "0x" prefix is ignored.
     */
    static BitSet parseHexMask(String mask) {
        String hex = mask.trim().replace(",", "");
        if (hex.startsWith("0x") || hex.startsWith("0X")) hex = hex.substring(2);
        final BitSet cpus = new BitSet();
        if (hex.isEmpty()) return cpus;
        final BigInteger bits = new BigInteger(hex, 16);
        for (int i = 0; i < bits.bitLength(); i++) {
            if (bits.testBit(i)) cpus.set(i);
        }
        return cpus;
    }

    /**
     * List format, for messages: "0-3,8".
     */
    static String toList(BitSet cpus) {
        final StringBuilder sb = new StringBuilder();
        for (int first = cpus.nextSetBit(0); first >= 0; ) {
            final int end = cpus.nextClearBit(first);
            if (!sb.isEmpty()) sb.append(',');
            sb.append(first);
            if (end - 1 > first) sb.append('-').append(end - 1);
            first = cpus.nextSetBit(end);
        }
        return sb.toString();
    }

    /**
     * A file in list format; empty if it does not exist or can not be read.
     */
    static BitSet readList(Path file) {
        try {
            return parseList(Files.readString(file));
        } catch (IOException | RuntimeException e) {
            return new BitSet();
        }
    }
}
