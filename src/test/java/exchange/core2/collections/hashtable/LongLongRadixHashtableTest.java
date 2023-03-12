package exchange.core2.collections.hashtable;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tests.pref.RandomDataSetsProvider;

import java.util.Random;

public class LongLongRadixHashtableTest {


    private static final Logger log = LoggerFactory.getLogger(LongLongRadixHashtableTest.class);

    LongLongRadixHashtable hashtable;

    @Before
    public void before() {
        hashtable = new LongLongRadixHashtable();
    }

    @Test
    public void put() {

        Random rand = new Random(1L);

        for (int i = 0; i < 32; i++) {
            final long key = rand.nextLong();
            hashtable.put(key, 1L);
        }
    }


    @Test
    public void should_upsize_throughput() {

        RandomDataSetsProvider randomDataSetsProvider = RandomDataSetsProvider.create();

        long t = System.currentTimeMillis();
        final int mask = (1 << 23) - 1;
        log.debug("mask=" + mask);

        int j = 0;
        long[] dataset = randomDataSetsProvider.next();

        for (long i = 0; i < 4_000_000_000L; i++) {

            if (j > dataset.length - 2) {
                dataset = randomDataSetsProvider.next();
                j = 0;
            }

            final long key = dataset[j++];
            final long value = dataset[j++];

//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
            hashtable.put(key, value);

            //refMap.forEach((k, v) -> assertThat(hashtable.get(k), is(v)));

            if ((i & mask) == 0) {
                final long t2 = System.currentTimeMillis();
                log.debug("i={} t={}ms", i, t2 - t);
                t = t2;
            }
        }
    }

    @Test
    public void tmp() {
        final int newMask = -(1 << 29);
        log.debug("newMask = {}", Integer.toHexString(newMask));

        int bitsShift = 29;

        for (int i = 0; i < 4; i++) {
            final int destBits = (i * 2 + 1) << bitsShift;

            log.debug("destBits = {}", String.format("%8X", destBits));
        }
    }

}