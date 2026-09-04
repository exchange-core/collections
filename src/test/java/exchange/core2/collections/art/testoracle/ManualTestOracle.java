package exchange.core2.collections.art.testoracle;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMapTest;
import org.junit.Test;

import java.util.*;

public class ManualTestOracle {

    @Test
    public void shouldMaintainOrder(){
        List<Long> arrayOfLong = Arrays.asList(-4429196230935817217L, 2967034165565055791L, 3969304394476946051L, 5502623383577302445L);
//        List<Long> arrayOfLong = Arrays.asList(1L, 2L, -3L, 4L, 2967034165565055791L, 3969304394476946051L, 5502623383577302445L);

        LongAdaptiveRadixTreeMap<String> testedMap = new LongAdaptiveRadixTreeMap<>();
        TreeMap<Long, String> oracleMap = new TreeMap<>();

        for (Long key : arrayOfLong) {
            String value = String.valueOf(key);
            oracleMap.put(key, value);
            testedMap.put(key, value);
        }

        System.out.println("\norig");
        printEntries(oracleMap.entrySet());

        System.out.println("\ntested");
        printEntries(testedMap.entriesList());

        System.out.println("\n" + testedMap.printDiagram());

        LongAdaptiveRadixTreeMapTest.checkStreamsEqual(testedMap.entriesList().stream(), oracleMap.entrySet().stream());
    }

    private void printEntries(Collection<Map.Entry<Long, String>> entries) {
        for (Map.Entry<Long, String> longStringEntry : entries) {
            System.out.println(longStringEntry.getKey() + ": " + longStringEntry.getValue());
        }
    }
}
