package exchange.core2.collections.hashtable;


import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

/**
 * Primitive {@code long} to {@code long} map. Any key can be stored, {@code 0} included.
 * <p>
 * A missing key reads as {@link #missingValue()} ({@code 0} unless set in the builder), so a key stored with that
 * value looks the same through {@link #get} - {@link #containsKey} or {@link #getOrDefault} tell them apart.
 * <p>
 * The default methods are compositions of the basic operations, two or three probes. The hashtables override them
 * with one probe - {@code computeIfAbsent} of a missing key takes two, the function may change the table - except
 * {@link LongLongLL2Hashtable} during a migration, which uses the compositions.
 * <p>
 * Cost. A single-key operation probes the cluster of its key: the longer, the fuller the table and the more its keys
 * collide - short at load 0.65 with keys nobody picked to collide (see the hash seed). A removal also shifts back the
 * rest of the cluster. A put of a new key may resize the table (see the implementations). {@link #clear},
 * {@link #forEach}, the streams and {@code toString} walk the whole array; {@link #ensureCapacity} rehashes it.
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
     * @return the value, or defaultValue if the key is absent - presence and value in one probe, where {@link #get}
     * can not tell a missing key from one stored with missingValue
     */
    default long getOrDefault(long key, long defaultValue) {
        return containsKey(key) ? get(key) : defaultValue;
    }

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
     * @throws IllegalArgumentException if expectedSize is negative or needs more than the maximum capacity (with a
     *                                  fixed capacity: more than the table has)
     */
    void ensureCapacity(int expectedSize);

}
