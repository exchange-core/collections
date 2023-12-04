package tests.stress;

import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtableAbstractTest;
import exchange.core2.collections.hashtable.LongLongLLHashtable;
import org.agrona.BitUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.MutableInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class LongLongLLHashtableStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongLongHashtableAbstractTest.class);

    @Test
    public void should_upsize() {

        final Random rand = new Random(14232313L);
        for (int iteration = 0; iteration < 1000; iteration++) {

            final LongLongLLHashtable hashtable = new LongLongLLHashtable();
            final Map<Long, Long> refMap = new HashMap<>();

//            final long size = 5000000L;
            final long size = 500000L;
            for (long i = 0; i < size; i++) {
                final long key = rand.nextLong();
                final long value = rand.nextLong();

                //log.info("=============== put {}={}", key, value);
                Long prevRef = refMap.put(key, value);
                long prev = hashtable.put(key, value);

                try {
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
                    log.error("ERR: KEY={} VALUE={} i={} iter={}", key, value, i, iteration);

                    throw er;
                }
                //refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

                if (BitUtil.isPowerOfTwo(i)) {

                    //hashtable.printLayout();

                    assertThat(hashtable.size(), is(i + 1L));
                    log.info("Periodic validating get - size={} iteration={}...", hashtable.size(), iteration);

                    log.debug("refMap size = {}", refMap.size());
//                    log.debug("agronaMap size = {}", agronaMap.size());
                    log.debug("hashtable size = {}", hashtable.size());

                    MutableInteger errCnt = new MutableInteger(0);


                    refMap.forEach((k, v) -> {

                        try {
                            assertThat(hashtable.get(k), is(v));
                        } catch (Throwable er) {
                            log.info("PERIODIC: KEY={} VALUE={} {} {}", k, v, er.getClass(), er.getMessage());
                            if (errCnt.incrementAndGet() > 40) {
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
            log.info("validating get...");
            refMap.forEach((k, v) -> {
                try {
                    assertThat(hashtable.get(k), is(v));
                } catch (Throwable er) {
                    log.info("PERIODIC: KEY={} VALUE={} {} {}", k, v, er.getClass(), er.getMessage());
                    throw er;
                }
            });
            log.info("validating remove...");
            refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(v)));
            assertThat(hashtable.size(), is(0L));
            log.info("validating remove 0...");
            refMap.forEach((k, v) -> assertThat(hashtable.remove(k), is(0L)));
            log.info("confirm empty...");
            refMap.forEach((k, v) -> assertFalse(hashtable.containsKey(k)));
            assertThat(hashtable.size(), is(0L));

            log.info("done");
        }
    }

}
