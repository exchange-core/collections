package tests.pref;

import tests.common.LatencyTools;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;

public class LatencySideChannelAttackTest {

    private static final Logger log = LoggerFactory.getLogger(LatencySideChannelAttackTest.class);

    @Test
    public void testLatencyAttach() {


        try (AffinityLock ignore = AffinityLock.acquireCore()) {

            Random rand = new Random(-761253);

            TreeSet<KeyLatency> latencyCache = new TreeSet<>(Comparator.comparingLong(KeyLatency::latency).thenComparing(KeyLatency::key));

            Long2LongHashMap map = new Long2LongHashMap(0L); // agrona
//             LongLongHashMap map = new LongLongHashMap(); // eclipse
//            LongLongHashtable map = new LongLongHashtable(); // exchange collections
//            LongLongLL2Hashtable map = new LongLongLL2Hashtable();

//            Map<Long,Long> map  = new HashMap<>();
//            Map<Long,Long> map  = new ConcurrentHashMap<>();
//            Map<Long,Long> map  = new TreeMap<>();
//            Map<Long, Long> map = HashLongLongMaps.newMutableMap();



//            Map<Long, Long> map = new FastMap<>(); // javolution


            /*
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
             */
//                    .name("long-long-benchmark-map").entries(4_000_000);

//            LongAdaptiveRadixTreeMap<Long> map = new LongAdaptiveRadixTreeMap<>();


            final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

            final long startTimeNs = System.nanoTime();
            long nextPublishTimeNs = startTimeNs + 1_000_000_000L;

            int size = 0;

            for (int i = 0; i < 190_000_000; i++) {

                int key = rand.nextInt();
//
//                if (size > 10000 && i % 20000 == 0) {
//                    //log.debug("refresh");
//                    TreeSet<KeyLatency> latencyCache2 = new TreeSet<>(Comparator.comparingLong(KeyLatency::latency).thenComparing(KeyLatency::key));
//
//                    latencyCache.stream().map(KeyLatency::key).forEach(k -> {
//                                final long nanoTime = System.nanoTime();
//                                map.get((long) k);
//                                final long getNs = System.nanoTime() - nanoTime;
//
//
//                                latencyCache2.add(new KeyLatency(key, getNs));
//                            }
//                    );
//
//                    latencyCache = latencyCache2;
//                }


                final long nanoTime = System.nanoTime();
                map.put((long) key, (long) key);
                final long putNs = System.nanoTime() - nanoTime;

                histogramPut.recordValue(putNs);

                latencyCache.add(new KeyLatency(key, putNs));

                if (size > 100 && i % 16 != 0) {
                    map.remove((long) latencyCache.removeFirst().key);
                } else {
                    size++;
                }

                if (nanoTime > nextPublishTimeNs) {
                    nextPublishTimeNs = nanoTime + 1_000_000_000L;
                    log.info("size={} {}", size, LatencyTools.createLatencyReportFast(histogramPut));
                    histogramPut.reset();
                }
            }

        }
    }


    record KeyLatency(int key, long latency) {
    }


}
