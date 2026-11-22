package exchange.core2.collections.hashtable;

/**
 * Consumer of a UUID key - its two halves, as {@link java.util.UUID#getMostSignificantBits()} and
 * {@link java.util.UUID#getLeastSignificantBits()} - and an object value.
 */
@FunctionalInterface
public interface UuidObjectConsumer<V> {

    void accept(long mostSigBits, long leastSigBits, V value);
}
