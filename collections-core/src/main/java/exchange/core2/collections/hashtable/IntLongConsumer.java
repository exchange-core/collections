package exchange.core2.collections.hashtable;

/**
 * Consumer of a primitive int key / long value pair.
 */
@FunctionalInterface
public interface IntLongConsumer {

    void accept(int key, long value);
}
