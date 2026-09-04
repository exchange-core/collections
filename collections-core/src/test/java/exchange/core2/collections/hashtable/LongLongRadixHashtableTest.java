package exchange.core2.collections.hashtable;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    public void tmp() {

        int subtables = 256;

        for (int i = 1; i <= 256; i++) {

            double x = (double) subtables / i;

            int r = (int) x;

            //int s = r * i;
            double f = x - r;
            long k = Math.round(f * i);

            // (i-k) * r / subtables + k * (r + 1) / subtables = 1

            log.debug("{}: {} * {}/{} + {} * {}/{} = 1", i, i - k, r, subtables, k, r + 1, subtables);


            if ((i - k) * r + k * (r + 1) != subtables) {
                throw new IllegalStateException();
            }

            //log.debug("i={} x={} xRounded={} f={} k={}", i, x, r, f, k);


        }

    }

}