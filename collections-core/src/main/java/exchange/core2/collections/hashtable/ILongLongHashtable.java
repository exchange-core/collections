package exchange.core2.collections.hashtable;


import java.util.stream.LongStream;

/**
 * Primitive {@code long} to {@code long} map.
 * <p>
 * Key {@code 0} marks empty cells and can not be stored. A missing key reads as value {@code 0}, so a key stored
 * with value {@code 0} looks the same through {@link #get} - use {@link #containsKey} to tell them apart.
 */
public interface ILongLongHashtable {

    /**
     * @return the previous value, or 0 if the key was absent
     * @throws IllegalArgumentException for key 0
     */
    long put(long key, long value);

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

    void clear();

    LongStream keysStream();

    LongStream valuesStream();

    void forEach(LongLongConsumer consumer);

    long size();

}
