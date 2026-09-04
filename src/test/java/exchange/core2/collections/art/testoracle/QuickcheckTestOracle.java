package exchange.core2.collections.art.testoracle;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMapTest;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.TreeMap;

@RunWith(JUnitQuickcheck.class)
public class QuickcheckTestOracle {

    @Property(trials = 1500)
    public void shouldEntriesMaintainCorrectOrderMixedSignValues(List<Long> arrayOfLong){
        shouldEntriesMaintainCorrectOrder(arrayOfLong);
    }

    @Property(trials = 1500)
    public void shouldEntriesMaintainCorrectOrderAllNegativeValues(List<@InRange(max = "0") Long> arrayOfLong){
        shouldEntriesMaintainCorrectOrder(arrayOfLong);
    }

    @Property(trials = 1500)
    public void shouldEntriesMaintainCorrectOrderAllPositiveValues(List<@InRange(min = "0") Long> arrayOfLong){
        shouldEntriesMaintainCorrectOrder(arrayOfLong);
    }

    private void shouldEntriesMaintainCorrectOrder(List<Long> arrayOfLong) {
        LongAdaptiveRadixTreeMap<String> testedMap = new LongAdaptiveRadixTreeMap<>();
        TreeMap<Long, String> oracleMap = new TreeMap<>();

        for (Long key : arrayOfLong) {
            String value = String.valueOf(key);
            oracleMap.put(key, value);
            testedMap.put(key, value);
        }

        LongAdaptiveRadixTreeMapTest.checkStreamsEqual(testedMap.entriesList().stream(), oracleMap.entrySet().stream());
    }

}
