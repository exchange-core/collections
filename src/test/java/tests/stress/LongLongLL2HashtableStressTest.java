package tests.stress;

import exchange.core2.collections.hashtable.LongLongHashtableAbstractTest;
import exchange.core2.collections.hashtable.LongLongLL2Hashtable;
import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Currency;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongLongLL2HashtableStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongHashtableAbstractTest.class);

    @Test
    public void should_upsize_single_thread() {

        final long seed = Integer.hashCode(441);
        runTestSingleThread(seed, 1000, 500000);
    }

    @Test
    public void should_upsize_multi_threaded() throws InterruptedException {

        final Map<Thread, Throwable> exceptions = new ConcurrentHashMap<>();
        final Thread.UncaughtExceptionHandler ueh = exceptions::put;

        final int numThreads = 30;

        final Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            final long seed = Integer.hashCode(i);
            final Thread thread = new Thread(() -> runTestSingleThread(seed, 1000, 200_000));
            thread.setName("TEST-" + i);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(ueh);
            threads[i] = thread;
            thread.start();
        }

        while (Arrays.stream(threads).anyMatch(Thread::isAlive) && exceptions.isEmpty()) {
            Thread.sleep(1000);
            log.debug("Waiting {} threads", Arrays.stream(threads).filter(Thread::isAlive).count());
        }

        exceptions.forEach((t, ex) -> {
            log.error("Exception in thread {}", t, ex);
        });

        assertTrue(exceptions.isEmpty());
    }


    private static void runTestSingleThread(long seed, int iterations, long size) {

        final Random rand = new Random(seed);

        for (int iteration = 0; iteration < iterations; iteration++) {

            final LongLongLL2Hashtable hashtable = new LongLongLL2Hashtable();
//            final LongLongHashtable hashtable = new LongLongHashtable();
            //final Map<Long, Long> refMap = new HashMap<>();
            final Long2LongHashMap refMap = new Long2LongHashMap(0L);

            final Long2LongHashMap timesMap = new Long2LongHashMap(0L);



            for (long i = 0; i < size; i++) {
                final long key = rand.nextLong();
                final long value = rand.nextLong();

                //log.info("=============== put {}={}", key, value);
                Long prevRef = refMap.put(key, value);
                long prev = hashtable.put(key, value);

                timesMap.put(key, System.currentTimeMillis());

                try {
                    // check previous value (GET function)
                    assertThat(prev, is(prevRef == null ? 0L : prevRef));

                    //log.info("--------------- get {} expected {}", key, value);
                    assertThat(hashtable.get(key), is(value));
                    assertThat(hashtable.remove(key), is(value));
                    assertThat(hashtable.get(key), is(0L));
                    assertThat(hashtable.remove(key), is(0L));

                    assertThat(hashtable.size(), is(i));
                    hashtable.put(key, value);
//                    agronaMap.put(key, value);


                } catch (Throwable er) {

                    int mask = hashtable.mask() >> 1;
                    final int hash = Hashing.hash(key);
                    int pos = (hash & mask) << 1;

                    // String actions = hashtable.getActions(key);

                    log.error("ERR INSTANT: KEY={} VALUE={} i={} iter={} pos={} act={}", key, value, i, iteration, pos, "");

                    throw er;
                }
                //refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

                if (BitUtil.isPowerOfTwo(i)) {

                    //hashtable.printLayout();

                    assertThat(hashtable.size(), is(i + 1L));
                    //log.info("Periodic validating get - size={} iteration={}...", hashtable.size(), iteration);

//                     log.debug("refMap size = {}", refMap.size());
//                    log.debug("agronaMap size = {}", agronaMap.size());
//                    log.debug("hashtable size = {}", hashtable.size());

                    MutableInteger errCnt = new MutableInteger(0);

                    int mask = hashtable.mask() >> 1;

                    MutableLong timeFrom = new MutableLong(Long.MAX_VALUE);
                    MutableLong timeTo = new MutableLong(0L);
                    MutableLong posFrom = new MutableLong(Long.MAX_VALUE);
                    MutableLong posTo = new MutableLong(0L);


                    refMap.forEach((k, v) -> {

                        try {
                            assertThat(hashtable.get(k), is(v));
                        } catch (Throwable er) {

                            final int hash = Hashing.hash(k);
                            int pos = (hash & mask) << 1;

                            posFrom.set(Math.min(posFrom.get(), pos));
                            posTo.set(Math.max(posTo.get(), pos));

                            Instant originalPut = Instant.ofEpochMilli(timesMap.get(k));


                            log.error("PERIODIC: KEY={} VALUE={} pos={} tPut={} {} {}", k, v, pos, originalPut, er.getClass(), er.getMessage());
                            if (errCnt.incrementAndGet() > 200) {

                                log.error("PERIOD: {} ... {}", Instant.ofEpochMilli(timeFrom.get()), Instant.ofEpochMilli(timeTo.get()));
                                log.error("POS: {} ... {}", (posFrom.get()), (posTo.get()));

                                throw er;
                            }
                        }
                    });


                    if (errCnt.get() != 0) {
                        throw new IllegalStateException();
                    }
                }

            }

            assertThat(hashtable.size(), is(size));
            log.info("DONE iteration: {}", iteration);
            //log.info("validating get...");
            refMap.forEach((k, v) -> {
                try {
                    assertThat(hashtable.get(k), is(v));
                } catch (Throwable er) {
                    log.info("PERIODIC: KEY={} VALUE={} {} {}", k, v, er.getClass(), er.getMessage());
                    throw er;
                }
            });
            //log.info("validating remove...");
            refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(v)));
            assertThat(hashtable.size(), is(0L));
            //log.info("validating remove 0...");
            refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(0L)));
            //log.info("confirm empty...");
            refMap.forEach((k, v) -> assertFalse(hashtable.containsKey(k)));
            assertThat(hashtable.size(), is(0L));

            //log.info("done");
        }
    }

}
