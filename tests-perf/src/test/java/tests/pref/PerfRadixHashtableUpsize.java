package tests.pref;

import exchange.core2.collections.hashtable.LongLongRadixHashtable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tests.common.RandomDataSetsProvider;

/**
 * Endurance benchmark: keeps inserting into a growing radix hashtable and reports the time per
 * 2^23 insertions. Runs until the heap gives out - it is not a unit test and has no assertions,
 * which is why it lives here and not next to the correctness tests.
 */
public class PerfRadixHashtableUpsize {

    private static final Logger log = LoggerFactory.getLogger(PerfRadixHashtableUpsize.class);

    @Test
    public void should_upsize_throughput() {

        final LongLongRadixHashtable hashtable = new LongLongRadixHashtable();

        final RandomDataSetsProvider randomDataSetsProvider = RandomDataSetsProvider.create();

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

            hashtable.put(key, value);

            if ((i & mask) == 0) {
                final long t2 = System.currentTimeMillis();
                log.debug("i={} t={}ms", i, t2 - t);
                t = t2;
            }
        }
    }
}
