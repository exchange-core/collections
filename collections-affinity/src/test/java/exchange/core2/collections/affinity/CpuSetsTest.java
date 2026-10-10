package exchange.core2.collections.affinity;

import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CpuSetsTest {

    @Test
    public void parsesListFormat() {
        assertEquals(bits(0, 1, 2, 3, 8, 10, 11), CpuSets.parseList("0-3,8,10-11\n"));
        assertEquals(bits(5), CpuSets.parseList(" 5 "));
        assertTrue(CpuSets.parseList("\n").isEmpty());
    }

    @Test
    public void parsesHexMasks() {
        assertEquals(bits(1, 3, 5, 7, 9, 11, 13, 15), CpuSets.parseHexMask("AAAA"));
        assertEquals(bits(0, 4), CpuSets.parseHexMask("0x11"));
        // kernel bitmap format: 32-bit words, most significant first
        assertEquals(bits(40), CpuSets.parseHexMask("00000100,00000000"));
        assertTrue(CpuSets.parseHexMask("0").isEmpty());
    }

    @Test
    public void printsListFormat() {
        assertEquals("0-3,8,10-11", CpuSets.toList(bits(0, 1, 2, 3, 8, 10, 11)));
        assertEquals("5", CpuSets.toList(bits(5)));
        assertEquals("", CpuSets.toList(new BitSet()));
    }

    static BitSet bits(int... cpus) {
        final BitSet set = new BitSet();
        for (int cpu : cpus) set.set(cpu);
        return set;
    }
}
