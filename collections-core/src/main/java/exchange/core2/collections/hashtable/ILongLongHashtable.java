package exchange.core2.collections.hashtable;


import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import static exchange.core2.collections.hashtable.HashingUtils.NOT_ALLOWED_KEY;

/**
 * Primitive {@code long} to {@code long} map.
 * <p>
 * Key {@code 0} marks empty cells and can not be stored. A missing key reads as value {@code 0}, so a key stored
 * with value {@code 0} looks the same through {@link #get} - use {@link #containsKey} to tell them apart.
 * <p>
 * The default methods are compositions of the basic operations, the implementations do them in one probe.
 */
public interface ILongLongHashtable {

    /**
     * @return the previous value, or 0 if the key was absent
     * @throws IllegalArgumentException for key 0
     */
    long put(long key, long value);

    /**
     * Puts the value only if the key is absent.
     *
     * @return the value the key already has (then the table is unchanged), or 0 if it was absent
     * @throws IllegalArgumentException for key 0
     */
    default long putIfAbsent(long key, long value) {
        return containsKey(key) ? get(key) : put(key, value);
    }

    /**
     * Adds delta to the value of the key. A missing key counts as 0: it is put with the value delta. Overflow wraps
     * around, as with {@code +}.
     *
     * @return the new value
     * @throws IllegalArgumentException for key 0
     */
    default long addAndGet(long key, long delta) {
        final long value = get(key) + delta;
        put(key, value);
        return value;
    }

    /**
     * The value of the key; a missing key is first put with the value the function computes for it. The function
     * should not modify the table - if it does, its result still ends up under the key.
     *
     * @return the existing or the computed value
     * @throws IllegalArgumentException for key 0, without calling the function
     */
    default long computeIfAbsent(long key, LongUnaryOperator mappingFunction) {
        if (key == NOT_ALLOWED_KEY) {
            throw new IllegalArgumentException("Not allowed key " + NOT_ALLOWED_KEY);
        }
        if (containsKey(key)) {
            return get(key);
        }
        final long value = mappingFunction.applyAsLong(key);
        put(key, value);
        return value;
    }

    /**
     * @return the value, or 0 if the key is absent (always 0 for key 0)
     */
    long get(long key);

    /**
     * @return true if the key is present, whatever its value - including 0 (always false for key 0)
     */
    boolean containsKey(long key);

    /**
     * @return the removed value, or 0 if the key was absent (key 0 is always absent)
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
