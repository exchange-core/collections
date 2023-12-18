package tests.pref;

import exchange.core2.collections.hashtable.LongLongHashtable;
import org.agrona.collections.Long2LongHashMap;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class PerfThroughputTests {
    private static final Logger log = LoggerFactory.getLogger(PerfThroughputTests.class);

    /*
21:43:36.166 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=43000000, avgPut=1425, avgGet=46, avgRemove=73, acc=-1672554397643506140]
21:43:36.449 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=44000000, avgPut=69, avgGet=48, avgRemove=78, acc=3589811011092885449]
21:43:36.721 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=45000000, avgPut=56, avgGet=45, avgRemove=88, acc=-7514733801871352032]
21:43:36.995 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=46000000, avgPut=57, avgGet=46, avgRemove=88, acc=-3905013898375721640]
21:43:37.269 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=47000000, avgPut=56, avgGet=46, avgRemove=89, acc=-7750427877667391157]
21:43:37.555 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=48000000, avgPut=57, avgGet=47, avgRemove=98, acc=-2237202425244425261]
21:43:37.842 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=49000000, avgPut=57, avgGet=47, avgRemove=99, acc=-5355392788359869314]
21:43:38.123 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=50000000, avgPut=57, avgGet=48, avgRemove=93, acc=3888111686528119637]
21:43:38.411 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=51000000, avgPut=58, avgGet=48, avgRemove=98, acc=-6764141314531764086]
21:43:38.704 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=52000000, avgPut=55, avgGet=52, avgRemove=95, acc=6307035932539845607]
21:43:39.007 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=53000000, avgPut=56, avgGet=54, avgRemove=105, acc=-3423215513503818337]
21:43:39.302 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=54000000, avgPut=62, avgGet=52, avgRemove=102, acc=-6603104935152108163]
21:43:39.601 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=55000000, avgPut=59, avgGet=53, avgRemove=103, acc=-8684659675454317671]
21:43:39.917 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=56000000, avgPut=59, avgGet=59, avgRemove=104, acc=5416501473881684006]
21:43:40.225 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=57000000, avgPut=63, avgGet=53, avgRemove=103, acc=-2465799239140633944]
21:43:40.547 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=58000000, avgPut=60, avgGet=59, avgRemove=107, acc=5378910590455819188]
21:43:40.876 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=59000000, avgPut=65, avgGet=58, avgRemove=115, acc=-5036208243399769395]
21:43:41.213 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=60000000, avgPut=62, avgGet=62, avgRemove=117, acc=8178651245940594756]
21:43:41.551 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=61000000, avgPut=69, avgGet=60, avgRemove=116, acc=-3230740535618016306]
21:43:41.899 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=62000000, avgPut=68, avgGet=74, avgRemove=107, acc=6804754093297892821]
21:43:42.245 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=63000000, avgPut=65, avgGet=68, avgRemove=115, acc=-5789758513936101914]
21:43:42.589 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=64000000, avgPut=69, avgGet=66, avgRemove=113, acc=-9212532176367149992]
21:43:42.952 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=65000000, avgPut=71, avgGet=70, avgRemove=118, acc=-6465640422869784324]
21:43:43.318 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=66000000, avgPut=69, avgGet=73, avgRemove=120, acc=1538516993917531138]
21:43:43.701 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=67000000, avgPut=70, avgGet=80, avgRemove=127, acc=-2469532780730794746]
21:43:44.086 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=68000000, avgPut=71, avgGet=80, avgRemove=124, acc=3223890057870361736]
21:43:44.476 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=69000000, avgPut=73, avgGet=82, avgRemove=126, acc=5528653120771508780]
21:43:44.875 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=70000000, avgPut=78, avgGet=81, avgRemove=127, acc=-6555708771745772031]
21:43:45.280 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=71000000, avgPut=79, avgGet=84, avgRemove=129, acc=-5046502625251176360]
21:43:45.700 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=72000000, avgPut=80, avgGet=88, avgRemove=136, acc=3235146399636377728]
21:43:46.122 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=73000000, avgPut=79, avgGet=90, avgRemove=137, acc=-4888950476501364877]
21:43:46.558 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=74000000, avgPut=84, avgGet=89, avgRemove=143, acc=4832899208090569750]
21:43:46.996 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=75000000, avgPut=85, avgGet=95, avgRemove=143, acc=-5044896014816524428]
21:43:47.446 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=76000000, avgPut=87, avgGet=93, avgRemove=146, acc=3908704143882371258]
21:43:47.904 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=77000000, avgPut=86, avgGet=100, avgRemove=149, acc=-1572561514517710728]
21:43:48.388 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=78000000, avgPut=93, avgGet=110, avgRemove=152, acc=2674954797940223585]
21:43:48.881 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=79000000, avgPut=97, avgGet=105, avgRemove=159, acc=6011065319546397313]
21:43:49.380 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=80000000, avgPut=98, avgGet=103, avgRemove=160, acc=-114726295742212881]
21:43:49.894 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=81000000, avgPut=102, avgGet=108, avgRemove=166, acc=-3716514070329784941]
21:43:50.415 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=82000000, avgPut=105, avgGet=109, avgRemove=168, acc=-9165465450830171007]
21:43:50.951 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=83000000, avgPut=112, avgGet=113, avgRemove=166, acc=-296331734718651986]
21:43:51.490 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=84000000, avgPut=112, avgGet=114, avgRemove=170, acc=4252319330582285564]
21:43:52.050 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=85000000, avgPut=116, avgGet=118, avgRemove=180, acc=6378332487476590290]
21:43:52.623 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=86000000, avgPut=116, avgGet=125, avgRemove=182, acc=-8377522509209559396]
*/

    @Test
    public void benchmarkBasic() {
        benchmarkAbstract(
            (long[] kv) -> {
                final LongLongHashtable hashtable = new LongLongHashtable();
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (LongLongHashtable hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    /*
21:00:18.920 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=43000000, avgPut=1709, avgGet=46, avgRemove=76, acc=-1672554397643506140]
21:00:19.193 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=44000000, avgPut=55, avgGet=47, avgRemove=86, acc=3589811011092885449]
21:00:19.468 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=45000000, avgPut=55, avgGet=50, avgRemove=82, acc=-7514733801871352032]
21:00:19.750 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=46000000, avgPut=56, avgGet=51, avgRemove=88, acc=-3905013898375721640]
21:00:20.029 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=47000000, avgPut=60, avgGet=49, avgRemove=87, acc=-7750427877667391157]
21:00:20.312 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=48000000, avgPut=62, avgGet=48, avgRemove=90, acc=-2237202425244425261]
21:00:20.597 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=49000000, avgPut=60, avgGet=50, avgRemove=89, acc=-5355392788359869314]
21:00:20.895 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=50000000, avgPut=60, avgGet=56, avgRemove=92, acc=3888111686528119637]
21:00:21.198 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=51000000, avgPut=67, avgGet=52, avgRemove=97, acc=-6764141314531764086]
21:00:21.499 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=52000000, avgPut=63, avgGet=55, avgRemove=92, acc=6307035932539845607]
21:00:21.813 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=53000000, avgPut=67, avgGet=56, avgRemove=97, acc=-3423215513503818337]
21:00:22.117 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=54000000, avgPut=66, avgGet=56, avgRemove=94, acc=-6603104935152108163]
21:00:22.437 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=55000000, avgPut=63, avgGet=62, avgRemove=99, acc=-8684659675454317671]
21:00:22.757 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=56000000, avgPut=70, avgGet=60, avgRemove=99, acc=5416501473881684006]
21:00:23.090 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=57000000, avgPut=66, avgGet=65, avgRemove=103, acc=-2465799239140633944]
21:00:23.419 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=58000000, avgPut=73, avgGet=65, avgRemove=99, acc=5378910590455819188]
21:00:23.765 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=59000000, avgPut=73, avgGet=69, avgRemove=104, acc=-5036208243399769395]
21:00:24.112 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=60000000, avgPut=70, avgGet=69, avgRemove=108, acc=8178651245940594756]
21:00:24.453 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=61000000, avgPut=74, avgGet=71, avgRemove=100, acc=-3230740535618016306]
21:00:24.810 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=62000000, avgPut=75, avgGet=71, avgRemove=110, acc=6804754093297892821]
21:00:25.179 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=63000000, avgPut=78, avgGet=78, avgRemove=109, acc=-5789758513936101914]
21:00:25.561 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=64000000, avgPut=75, avgGet=80, avgRemove=123, acc=-9212532176367149992]
21:00:25.949 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=65000000, avgPut=79, avgGet=84, avgRemove=117, acc=-6465640422869784324]
21:00:26.344 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=66000000, avgPut=82, avgGet=86, avgRemove=118, acc=1538516993917531138]
21:00:26.740 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=67000000, avgPut=83, avgGet=87, avgRemove=117, acc=-2469532780730794746]
21:00:27.141 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=68000000, avgPut=84, avgGet=92, avgRemove=114, acc=3223890057870361736]
21:00:27.556 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=69000000, avgPut=87, avgGet=95, avgRemove=122, acc=5528653120771508780]
21:00:27.967 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=70000000, avgPut=85, avgGet=94, avgRemove=123, acc=-6555708771745772031]
21:00:28.388 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=71000000, avgPut=87, avgGet=98, avgRemove=125, acc=-5046502625251176360]
21:00:28.833 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=72000000, avgPut=94, avgGet=101, avgRemove=131, acc=3235146399636377728]
21:00:29.279 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=73000000, avgPut=90, avgGet=106, avgRemove=136, acc=-4888950476501364877]
21:00:29.738 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=74000000, avgPut=96, avgGet=106, avgRemove=137, acc=4832899208090569750]
21:00:30.196 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=75000000, avgPut=94, avgGet=107, avgRemove=139, acc=-5044896014816524428]
21:00:30.679 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=76000000, avgPut=102, avgGet=115, avgRemove=139, acc=3908704143882371258]
21:00:31.168 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=77000000, avgPut=106, avgGet=111, avgRemove=145, acc=-1572561514517710728]
21:00:31.677 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=78000000, avgPut=115, avgGet=114, avgRemove=146, acc=2674954797940223585]
21:00:32.188 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=79000000, avgPut=111, avgGet=118, avgRemove=152, acc=6011065319546397313]
21:00:32.715 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=80000000, avgPut=123, avgGet=120, avgRemove=149, acc=-114726295742212881]
21:00:33.257 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=81000000, avgPut=120, avgGet=127, avgRemove=157, acc=-3716514070329784941]
21:00:33.810 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=82000000, avgPut=125, avgGet=125, avgRemove=162, acc=-9165465450830171007]
21:00:34.370 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=83000000, avgPut=124, avgGet=128, avgRemove=166, acc=-296331734718651986]
21:00:34.939 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=84000000, avgPut=130, avgGet=131, avgRemove=165, acc=4252319330582285564]
21:00:35.520 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=85000000, avgPut=133, avgGet=135, avgRemove=169, acc=6378332487476590290]
21:00:36.107 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=86000000, avgPut=129, avgGet=140, avgRemove=174, acc=-8377522509209559396]
21:00:39.695 [main] INFO  tests.pref.PerfThroughputTests - SingleResult[size=87000000, avgPut=3294, avgGet=55, avgRemove=89, acc=2955427837856908500]     */
    @Test
    public void benchmarkAgrona() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Long2LongHashMap hashtable = new Long2LongHashMap(0L);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Long2LongHashMap hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    @Test
    public void benchmarkStdHashMap() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Map<Long,Long> hashtable = new HashMap<>();
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Map<Long,Long> hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }



    private <T> void benchmarkAbstract(Function<long[], T> factory,
                                       BiFunction<T, long[], SingleResult> singleTest,
                                       BiConsumer<T, long[]> extraLoader) {
        int n = 4_000_000;
        long seed = 2918723469278364978L;


        log.debug("Pre-filling {} random k/v pairs...", n);
        Random rand = new Random(seed);
        final long[] prefillKeys = new long[n];
        for (int i = 0; i < n; i++) prefillKeys[i] = rand.nextLong();
//
//
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            hashtable.put(key, value);
//        }

        int n2 = 1_000_000;
        log.debug("Allocating {} random k/v pairs...", n2);

        final T hashtable = factory.apply(prefillKeys);
        log.debug("Benchmarking...");

        final long[] keys = new long[n2];

        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();
            final SingleResult benchmark = singleTest.apply(hashtable, keys);
            log.info("{}", benchmark);
            extraLoader.accept(hashtable, keys);
        }



//
//        log.info("done");
    }

    private SingleResult benchmark(LongLongHashtable hashtable, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) hashtable.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += hashtable.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) hashtable.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;

//        log.info("validating remove...");
//        Random rand = new Random(keys[0]);
//        for (int i = 0; i < keys.length; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertThat(hashtable.remove(key), Is.is(value));
//        }
//
//        log.info("confirm empty...");
//        rand = new Random(keys[0]);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertFalse(hashtable.containsKey(key));
//        }


        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);
    }

    private SingleResult benchmark(Long2LongHashMap hashtable, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) hashtable.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += hashtable.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) hashtable.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;
        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);
    }

    private SingleResult benchmark(Map<Long, Long> hashtable, long[] keys) {
        long t = System.nanoTime();
        for (long key : keys) hashtable.put(key, key);
        long putNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        long acc = 0;
        for (long key : keys) acc += hashtable.get(key);
        long getNs = (System.nanoTime() - t) / keys.length;

        t = System.nanoTime();
        for (long key : keys) hashtable.remove(key);
        long removNs = (System.nanoTime() - t) / keys.length;
        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);
    }


    record SingleResult(long size, long avgPut, long avgGet, long avgRemove, long acc) {

    }


}
