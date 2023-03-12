package tests.pref;

import exchange.core2.collections.hashtable.HashingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

public class RandomDataSetsProvider {

    private static final Logger log = LoggerFactory.getLogger(HashingUtils.class);

    final int size = 10_000_000;
    final Random random = new Random(1L);

    final private BlockingQueue<long[]> queue = new LinkedBlockingQueue<>(2);

    public static RandomDataSetsProvider create() {
        final RandomDataSetsProvider provider = new RandomDataSetsProvider();
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor
            .scheduleWithFixedDelay(provider::generate, 1, 1, TimeUnit.MILLISECONDS);
        return provider;
    }

    private RandomDataSetsProvider() {

    }

    public void stop() {
        //TODO
    }

    private void generate() {

//        log.debug("Generating dataset, size = {}", size);
        final long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = random.nextLong();
        }

        try {
//            log.debug("Dataset created, inserting into the queue...");
            queue.put(data);
//            log.debug("Dataset inserted");
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long[] next() {
        try {
//            log.debug("Waiting dataset...");
            final long[] dataset = queue.take();
//            log.debug("Dataset taken");
            return dataset;
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }


}
