package exchange.core2.collections.hashtable;

/**
 * Consumer of a primitive int key / object value pair.
 */
@FunctionalInterface
public interface IntObjectConsumer<V> {

    void accept(int key, V value);
}
