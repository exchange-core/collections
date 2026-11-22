package exchange.core2.collections.hashtable;

/**
 * Function of a UUID key given as its two halves, as {@link java.util.UUID#getMostSignificantBits()} and
 * {@link java.util.UUID#getLeastSignificantBits()}.
 */
@FunctionalInterface
public interface UuidFunction<R> {

    R apply(long mostSigBits, long leastSigBits);
}
