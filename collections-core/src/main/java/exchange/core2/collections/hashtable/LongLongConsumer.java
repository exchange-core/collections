package exchange.core2.collections.hashtable;

/**
 * Consumer of a primitive long key / long value pair.
 * <p>
 * Inlined from {@code org.agrona.collections.LongLongConsumer} so that this artifact carries no
 * runtime dependencies.
 */
@FunctionalInterface
public interface LongLongConsumer {

    void accept(long key, long value);
}
