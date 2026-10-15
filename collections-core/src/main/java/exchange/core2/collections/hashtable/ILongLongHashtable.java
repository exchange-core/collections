package exchange.core2.collections.hashtable;


import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

/**
 * Primitive {@code long} to {@code long} map. Any key can be stored, {@code 0} included.
 * <p>
 * A missing key reads as {@link #missingValue()} ({@code 0} unless set in the constructor), so a key stored with that
 * value looks the same through {@link #get} - use {@link #containsKey} to tell them apart.
 * <p>
 * The default methods are compositions of the basic operations, the implementations do them in one probe.
 */
public interface ILongLongHashtable {

    /**
     * @return the previous value, or missingValue if the key was absent
     */
    long put(long key, long value);

    /**
     * Puts the value only if the key is absent.
     *
     * @return the value the key already has (then the table is unchanged), or missingValue if it was absent
     */
    default long putIfAbsent(long key, long value) {
        return containsKey(key) ? get(key) : put(key, value);
    }

    /**
     * Adds delta to the value of the key. A missing key counts as 0 (not as missingValue): it is put with the value
     * delta. Overflow wraps around, as with {@code +}.
     *
     * @return the new value
     */
    default long addAndGet(long key, long delta) {
        final long value = (containsKey(key) ? get(key) : 0L) + delta;
        put(key, value);
        return value;
    }

    /**
     * The value of the key; a missing key is first put with the value the function computes for it. The function
     * should not modify the table - if it does, its result still ends up under the key.
     *
     * @return the existing or the computed value
     */
    default long computeIfAbsent(long key, LongUnaryOperator mappingFunction) {
        if (containsKey(key)) {
            return get(key);
        }
        final long value = mappingFunction.applyAsLong(key);
        put(key, value);
        return value;
    }

    /**
     * @return the value, or missingValue if the key is absent
     */
    long get(long key);

    /**
     * @return true if the key is present, whatever its value - missingValue included
     */
    boolean containsKey(long key);

    /**
     * @return the removed value, or missingValue if the key was absent
     */
    long remove(long key);

    /**
     * Removes the key only if it has the given value.
     *
     * @return true if the entry was removed
     */
    default boolean remove(long key, long value) {
        if (containsKey(key) && get(key) == value) {
            remove(key);
            return true;
        }
        return false;
    }

    void clear();

    LongStream keysStream();

    LongStream valuesStream();

    void forEach(LongLongConsumer consumer);

    long size();

    /**
     * @return what {@link #get}, {@link #put} and {@link #remove} return for a missing key
     */
    long missingValue();

    /**
     * @return the number of slots; the table grows when its size reaches 65% of them
     */
    int capacity();

    /**
     * Grows the table, if needed, to take expectedSize entries in total without a resize. One rehash on the calling
     * thread instead of a series of resizes - for the set-up, not for the latency-critical path.
     *
     * @throws IllegalArgumentException if expectedSize is negative or needs more than the maximum capacity
     */
    void ensureCapacity(int expectedSize);

}
