package exchange.core2.collections.hashtable;

import org.junit.Test;

import java.util.Random;
import java.util.function.LongToIntFunction;

import static org.junit.Assert.assertTrue;

/**
 * Guards the quality of {@link HashingUtils#hash(long)}.
 * <p>
 * A weak mixer does not break anything visibly: every test still passes, keys are still found. It
 * shows up only as clustering, and clustering in a linear-probing table shows up only as a latency
 * tail under load - which is expensive to notice and expensive to diagnose. This test makes a bad
 * mixer fail immediately instead, so the constants cannot be "tidied up" by accident.
 * <p>
 * Everything here is deterministic (fixed seed / fixed key ranges), so a failure is always
 * reproducible and never a flake.
 */
public class HashingUtilsAvalancheTest {

    private static final int AVALANCHE_SAMPLES = 50_000;

    /**
     * Strict avalanche criterion: flipping one input bit must flip each output bit with p ~ 0.5.
     * At this sample count the sampling noise is about 0.0022, so the bound below sits ~9 sigma
     * away - tight enough to catch a bad constant, loose enough never to flake.
     */
    private static final double MAX_ACCEPTABLE_BIAS = 0.02;

    @Test
    public void hash_shouldSatisfyStrictAvalancheCriterion() {

        final double bias = worstAvalancheBias(HashingUtils::hash);

        assertTrue("hash(long) is biased: worst deviation from 0.5 is " + bias
                        + " (limit " + MAX_ACCEPTABLE_BIAS + ")",
                bias < MAX_ACCEPTABLE_BIAS);
    }

    /**
     * Control: proves the check above can actually fail. Without this the avalanche test could
     * silently become vacuous - for instance if the measurement itself were broken.
     */
    @Test
    public void avalancheCheck_shouldRejectWeakMixers() {

        // no mixing at all
        final double identityBias = worstAvalancheBias(v -> (int) v);
        assertTrue("the avalanche check failed to reject the identity function",
                identityBias > MAX_ACCEPTABLE_BIAS);

        // single multiplication: fine for the high bits, poor for the low ones
        final double singleMulBias = worstAvalancheBias(v -> (int) (v * 0x9E3779B97F4A7C15L));
        assertTrue("the avalanche check failed to reject a single-multiply mixer",
                singleMulBias > MAX_ACCEPTABLE_BIAS);
    }

    /**
     * The property the hashtables actually depend on: they index with {@code hash(key) & mask}, so
     * the LOW bits have to be uniform. Sequential keys are the realistic hard case here - order ids
     * are counters, not random longs.
     */
    @Test
    public void hash_shouldSpreadSequentialKeysAcrossBuckets() {

        final int buckets = 1024;
        final int keys = buckets * 1024;
        final int[] histogram = new int[buckets];

        for (long key = 1; key <= keys; key++) {
            histogram[HashingUtils.hash(key) & (buckets - 1)]++;
        }

        final int expected = keys / buckets;
        // expected 1024 per bucket, sigma ~32; 25% leaves a wide margin over normal fluctuation
        // while still catching the order-of-magnitude skew a broken mixer produces
        final int allowedDeviation = expected / 4;

        int worstBucket = -1;
        int worstCount = expected;
        for (int i = 0; i < buckets; i++) {
            if (Math.abs(histogram[i] - expected) > Math.abs(worstCount - expected)) {
                worstCount = histogram[i];
                worstBucket = i;
            }
        }

        assertTrue("sequential keys cluster: bucket " + worstBucket + " got " + worstCount
                        + " entries, expected ~" + expected + " (+/- " + allowedDeviation + ")",
                Math.abs(worstCount - expected) <= allowedDeviation);
    }

    /**
     * @return the largest deviation from 0.5 over all 64x32 (input bit, output bit) pairs
     */
    private static double worstAvalancheBias(LongToIntFunction mixer) {

        final int[][] flips = new int[Long.SIZE][Integer.SIZE];
        final Random rand = new Random(12345L);

        for (int s = 0; s < AVALANCHE_SAMPLES; s++) {
            final long key = rand.nextLong();
            final int base = mixer.applyAsInt(key);

            for (int inBit = 0; inBit < Long.SIZE; inBit++) {
                final int diff = mixer.applyAsInt(key ^ (1L << inBit)) ^ base;
                for (int outBit = 0; outBit < Integer.SIZE; outBit++) {
                    if (((diff >>> outBit) & 1) != 0) {
                        flips[inBit][outBit]++;
                    }
                }
            }
        }

        double worst = 0;
        for (int inBit = 0; inBit < Long.SIZE; inBit++) {
            for (int outBit = 0; outBit < Integer.SIZE; outBit++) {
                final double p = (double) flips[inBit][outBit] / AVALANCHE_SAMPLES;
                worst = Math.max(worst, Math.abs(p - 0.5));
            }
        }
        return worst;
    }
}
