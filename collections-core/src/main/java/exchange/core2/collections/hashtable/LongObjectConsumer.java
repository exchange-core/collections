package exchange.core2.collections.hashtable;

/**
 * Consumer of a primitive long key / object value pair.
 */
@FunctionalInterface
public interface LongObjectConsumer<V> {

    void accept(long key, V value);
}
