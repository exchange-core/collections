package tests.pref;

import exchange.core2.collections.hashtable.LongLongHashtable;
import org.agrona.collections.Long2LongHashMap;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class PerfThroughputTests {
    private static final Logger log = LoggerFactory.getLogger(PerfThroughputTests.class);

    /*
18:24:32.185 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=43000000, avgPut=1472, avgGet=45, avgRemove=79, acc=-1672554397643506140]
18:24:32.468 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=44000000, avgPut=76, avgGet=50, avgRemove=80, acc=3589811011092885449]
18:24:32.740 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=45000000, avgPut=52, avgGet=47, avgRemove=95, acc=-7514733801871352032]
18:24:33.020 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=46000000, avgPut=51, avgGet=56, avgRemove=88, acc=-3905013898375721640]
18:24:33.304 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=47000000, avgPut=52, avgGet=53, avgRemove=96, acc=-7750427877667391157]
18:24:33.581 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=48000000, avgPut=58, avgGet=49, avgRemove=91, acc=-2237202425244425261]
18:24:33.869 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=49000000, avgPut=60, avgGet=50, avgRemove=98, acc=-5355392788359869314]
18:24:34.160 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=50000000, avgPut=62, avgGet=49, avgRemove=95, acc=3888111686528119637]
18:24:34.450 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=51000000, avgPut=61, avgGet=51, avgRemove=95, acc=-6764141314531764086]
18:24:34.743 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=52000000, avgPut=54, avgGet=56, avgRemove=96, acc=6307035932539845607]
18:24:35.047 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=53000000, avgPut=62, avgGet=58, avgRemove=98, acc=-3423215513503818337]
18:24:35.346 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=54000000, avgPut=60, avgGet=52, avgRemove=105, acc=-6603104935152108163]
18:24:35.654 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=55000000, avgPut=57, avgGet=58, avgRemove=102, acc=-8684659675454317671]
18:24:35.966 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=56000000, avgPut=62, avgGet=53, avgRemove=110, acc=5416501473881684006]
18:24:36.287 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=57000000, avgPut=57, avgGet=60, avgRemove=112, acc=-2465799239140633944]
18:24:36.620 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=58000000, avgPut=66, avgGet=64, avgRemove=110, acc=5378910590455819188]
18:24:36.962 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=59000000, avgPut=62, avgGet=66, avgRemove=118, acc=-5036208243399769395]
18:24:37.283 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=60000000, avgPut=61, avgGet=59, avgRemove=110, acc=8178651245940594756]
18:24:37.615 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=61000000, avgPut=61, avgGet=66, avgRemove=109, acc=-3230740535618016306]
18:24:37.961 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=62000000, avgPut=67, avgGet=63, avgRemove=125, acc=6804754093297892821]
18:24:38.318 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=63000000, avgPut=64, avgGet=73, avgRemove=118, acc=-5789758513936101914]
18:24:38.690 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=64000000, avgPut=72, avgGet=79, avgRemove=125, acc=-9212532176367149992]
18:24:39.074 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=65000000, avgPut=72, avgGet=84, avgRemove=125, acc=-6465640422869784324]
18:24:39.477 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=66000000, avgPut=74, avgGet=80, avgRemove=143, acc=1538516993917531138]
18:24:39.885 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=67000000, avgPut=81, avgGet=83, avgRemove=119, acc=-2469532780730794746]
18:24:40.263 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=68000000, avgPut=74, avgGet=76, avgRemove=121, acc=3223890057870361736]
18:24:40.654 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=69000000, avgPut=76, avgGet=84, avgRemove=122, acc=5528653120771508780]
18:24:41.157 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=70000000, avgPut=108, avgGet=119, avgRemove=154, acc=-6555708771745772031]
18:24:41.562 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=71000000, avgPut=74, avgGet=88, avgRemove=131, acc=-5046502625251176360]
18:24:41.980 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=72000000, avgPut=74, avgGet=89, avgRemove=142, acc=3235146399636377728]
18:24:42.401 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=73000000, avgPut=80, avgGet=91, avgRemove=134, acc=-4888950476501364877]
18:24:42.824 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=74000000, avgPut=80, avgGet=92, avgRemove=139, acc=4832899208090569750]
18:24:43.276 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=75000000, avgPut=86, avgGet=96, avgRemove=150, acc=-5044896014816524428]
18:24:43.723 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=76000000, avgPut=83, avgGet=99, avgRemove=145, acc=3908704143882371258]
18:24:44.209 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=77000000, avgPut=94, avgGet=103, avgRemove=158, acc=-1572561514517710728]
18:24:44.681 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=78000000, avgPut=95, avgGet=99, avgRemove=149, acc=2674954797940223585]
18:24:45.160 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=79000000, avgPut=88, avgGet=112, avgRemove=156, acc=6011065319546397313]
18:24:45.673 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=80000000, avgPut=100, avgGet=109, avgRemove=170, acc=-114726295742212881]
18:24:46.197 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=81000000, avgPut=98, avgGet=116, avgRemove=173, acc=-3716514070329784941]
18:24:46.732 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=82000000, avgPut=103, avgGet=116, avgRemove=177, acc=-9165465450830171007]
18:24:47.275 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=83000000, avgPut=109, avgGet=117, avgRemove=181, acc=-296331734718651986]
18:24:47.824 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=84000000, avgPut=106, avgGet=122, avgRemove=180, acc=4252319330582285564]
18:24:48.395 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=85000000, avgPut=114, avgGet=127, avgRemove=181, acc=6378332487476590290]
18:24:48.963 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=86000000, avgPut=109, avgGet=129, avgRemove=190, acc=-8377522509209559396]
18:24:49.143 [main] DEBUG exchange.core2.collections.hashtable.LongLongHashtable - RESIZE 268435456->536870912 elements=87241520 ...
18:24:49.143 [main] DEBUG exchange.core2.collections.hashtable.LongLongHashtable - Sync resizing...
18:24:49.143 [main] INFO exchange.core2.collections.hashtable.HashtableResizer - (A) Allocating array: long[536870912] ... -----------------
18:24:50.882 [main] INFO exchange.core2.collections.hashtable.HashtableResizer - (A) Copying data (up to 134217728 elements) ...
18:24:51.909 [main] INFO exchange.core2.collections.hashtable.HashtableResizer - (A) Copying completed ----------------------
18:24:51.909 [main] DEBUG exchange.core2.collections.hashtable.LongLongHashtable - RESIZE done, upsizeThreshold=174483040
18:24:52.107 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=87000000, avgPut=2840, avgGet=60, avgRemove=90, acc=2955427837856908500]
     */

    @Test
    public void benchmark() {
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
    18:19:24.984 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=43000000, avgPut=1703, avgGet=50, avgRemove=71, acc=-1672554397643506140]
18:19:25.271 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=44000000, avgPut=67, avgGet=50, avgRemove=84, acc=3589811011092885449]
18:19:25.543 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=45000000, avgPut=60, avgGet=47, avgRemove=83, acc=-7514733801871352032]
18:19:25.817 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=46000000, avgPut=60, avgGet=48, avgRemove=85, acc=-3905013898375721640]
18:19:26.098 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=47000000, avgPut=56, avgGet=49, avgRemove=91, acc=-7750427877667391157]
18:19:26.387 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=48000000, avgPut=55, avgGet=56, avgRemove=84, acc=-2237202425244425261]
18:19:26.681 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=49000000, avgPut=62, avgGet=52, avgRemove=90, acc=-5355392788359869314]
18:19:26.970 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=50000000, avgPut=63, avgGet=51, avgRemove=88, acc=3888111686528119637]
18:19:27.291 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=51000000, avgPut=75, avgGet=66, avgRemove=92, acc=-6764141314531764086]
18:19:27.593 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=52000000, avgPut=64, avgGet=57, avgRemove=90, acc=6307035932539845607]
18:19:27.892 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=53000000, avgPut=63, avgGet=55, avgRemove=93, acc=-3423215513503818337]
18:19:28.214 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=54000000, avgPut=62, avgGet=69, avgRemove=97, acc=-6603104935152108163]
18:19:28.527 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=55000000, avgPut=67, avgGet=60, avgRemove=96, acc=-8684659675454317671]
18:19:28.850 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=56000000, avgPut=65, avgGet=65, avgRemove=95, acc=5416501473881684006]
18:19:29.177 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=57000000, avgPut=68, avgGet=65, avgRemove=102, acc=-2465799239140633944]
18:19:29.509 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=58000000, avgPut=70, avgGet=66, avgRemove=99, acc=5378910590455819188]
18:19:29.844 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=59000000, avgPut=68, avgGet=73, avgRemove=97, acc=-5036208243399769395]
18:19:30.191 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=60000000, avgPut=75, avgGet=71, avgRemove=106, acc=8178651245940594756]
18:19:30.554 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=61000000, avgPut=75, avgGet=74, avgRemove=113, acc=-3230740535618016306]
18:19:30.930 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=62000000, avgPut=80, avgGet=81, avgRemove=105, acc=6804754093297892821]
18:19:31.377 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=63000000, avgPut=92, avgGet=114, avgRemove=137, acc=-5789758513936101914]
18:19:31.759 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=64000000, avgPut=81, avgGet=82, avgRemove=115, acc=-9212532176367149992]
18:19:32.169 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=65000000, avgPut=87, avgGet=88, avgRemove=121, acc=-6465640422869784324]
18:19:32.565 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=66000000, avgPut=83, avgGet=89, avgRemove=118, acc=1538516993917531138]
18:19:32.991 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=67000000, avgPut=95, avgGet=90, avgRemove=130, acc=-2469532780730794746]
18:19:33.447 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=68000000, avgPut=107, avgGet=100, avgRemove=125, acc=3223890057870361736]
18:19:33.875 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=69000000, avgPut=93, avgGet=93, avgRemove=127, acc=5528653120771508780]
18:19:34.302 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=70000000, avgPut=83, avgGet=106, avgRemove=122, acc=-6555708771745772031]
18:19:34.732 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=71000000, avgPut=93, avgGet=98, avgRemove=124, acc=-5046502625251176360]
18:19:35.183 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=72000000, avgPut=99, avgGet=100, avgRemove=138, acc=3235146399636377728]
18:19:35.658 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=73000000, avgPut=104, avgGet=106, avgRemove=141, acc=-4888950476501364877]
18:19:36.137 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=74000000, avgPut=100, avgGet=111, avgRemove=143, acc=4832899208090569750]
18:19:36.681 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=75000000, avgPut=121, avgGet=131, avgRemove=162, acc=-5044896014816524428]
18:19:37.215 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=76000000, avgPut=106, avgGet=118, avgRemove=181, acc=3908704143882371258]
18:19:37.727 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=77000000, avgPut=107, avgGet=110, avgRemove=146, acc=-1572561514517710728]
18:19:38.301 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=78000000, avgPut=132, avgGet=137, avgRemove=159, acc=2674954797940223585]
18:19:38.824 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=79000000, avgPut=112, avgGet=120, avgRemove=160, acc=6011065319546397313]
18:19:39.360 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=80000000, avgPut=117, avgGet=118, avgRemove=165, acc=-114726295742212881]
18:19:39.943 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=81000000, avgPut=135, avgGet=124, avgRemove=169, acc=-3716514070329784941]
18:19:40.494 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=82000000, avgPut=128, avgGet=130, avgRemove=156, acc=-9165465450830171007]
18:19:41.051 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=83000000, avgPut=125, avgGet=132, avgRemove=165, acc=-296331734718651986]
18:19:41.635 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=84000000, avgPut=127, avgGet=134, avgRemove=175, acc=4252319330582285564]
18:19:42.265 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=85000000, avgPut=144, avgGet=142, avgRemove=184, acc=6378332487476590290]
18:19:42.853 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=86000000, avgPut=124, avgGet=140, avgRemove=180, acc=-8377522509209559396]
18:19:46.573 [main] INFO tests.pref.PerfThroughputTests - SingleResult[size=87000000, avgPut=3426, avgGet=58, avgRemove=85, acc=2955427837856908500]
     */
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

    record SingleResult(long size, long avgPut, long avgGet, long avgRemove, long acc) {

    }


}
