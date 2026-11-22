package exchange.core2.collections.hashtable;

import java.util.function.IntToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Primitive {@code int} to {@code long} hashtable: a {@link LongLongHashtable} under int keys, widened to long - the
 * same layout (a key and its value side by side in one {@code long[]}, one cache line for a lookup), the same
 * operations and costs, 16 bytes per slot.
 * <ul>
 *     <li>Any key, {@code 0} included. A missing key reads as {@code missingValue} ({@code 0} by default).</li>
 *     <li>The hash seed of the JVM ({@code -Dexchange.hashtable.seed}), the load factor, the resize, the maximum
 *     capacity and {@link Builder#maxLoad} as in {@link LongLongHashtable}.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 */
public final class IntLongHashtable {

    public static final int DEFAULT_ARRAY_SIZE = LongLongHashtable.DEFAULT_ARRAY_SIZE;

    private final LongLongHashtable table;

    public IntLongHashtable() {
        this(DEFAULT_ARRAY_SIZE);
    }

    /**
     * @param size expected number of entries - that many fit without a resize
     */
    public IntLongHashtable(int size) {
        this.table = new LongLongHashtable(size);
    }

    private IntLongHashtable(LongLongHashtable table) {
        this.table = table;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of an {@link IntLongHashtable}, all optional - those of {@link LongLongHashtable.Builder}.
     */
    public static final class Builder {

        private final LongLongHashtable.Builder builder = LongLongHashtable.builder();

        private Builder() {
        }

        /**
         * Entries that fit without a resize (default {@value IntLongHashtable#DEFAULT_ARRAY_SIZE}).
         */
        public Builder expectedSize(int expectedSize) {
            builder.expectedSize(expectedSize);
            return this;
        }

        /**
         * What get, put and remove return for a missing key (default 0), see
         * {@link LongLongHashtable.Builder#missingValue}.
         */
        public Builder missingValue(long missingValue) {
            builder.missingValue(missingValue);
            return this;
        }

        /**
         * Capacity in slots the table grows up to, see {@link LongLongHashtable.Builder#maxCapacity}.
         */
        public Builder maxCapacity(int maxCapacity) {
            builder.maxCapacity(maxCapacity);
            return this;
        }

        /**
         * No resize, ever, see {@link LongLongHashtable.Builder#fixedCapacity}.
         */
        public Builder fixedCapacity() {
            builder.fixedCapacity();
            return this;
        }

        /**
         * Load up to which a table that can not grow any more takes new keys, see
         * {@link LongLongHashtable.Builder#maxLoad}.
         */
        public Builder maxLoad(double maxLoad) {
            builder.maxLoad(maxLoad);
            return this;
        }

        public IntLongHashtable build() {
            return new IntLongHashtable(builder.build());
        }
    }

    /**
     * @return the previous value, or missingValue if the key was absent
     */
    public long put(int key, long value) {
        return table.put(key, value);
    }

    /**
     * Puts the value only if the key is absent.
     *
     * @return the value the key already has (then the table is unchanged), or missingValue if it was absent
     */
    public long putIfAbsent(int key, long value) {
        return table.putIfAbsent(key, value);
    }

    /**
     * Adds delta to the value of the key. A missing key counts as 0 (not as missingValue).
     *
     * @return the new value
     */
    public long addAndGet(int key, long delta) {
        return table.addAndGet(key, delta);
    }

    /**
     * The value of the key; a missing key is first put with the value the function computes for it.
     *
     * @return the existing or the computed value
     */
    public long computeIfAbsent(int key, IntToLongFunction mappingFunction) {
        return table.computeIfAbsentInt(key, mappingFunction);
    }

    /**
     * @return the value, or missingValue if the key is absent
     */
    public long get(int key) {
        return table.get(key);
    }

    /**
     * @return the value, or defaultValue if the key is absent - in one probe
     */
    public long getOrDefault(int key, long defaultValue) {
        return table.getOrDefault(key, defaultValue);
    }

    /**
     * @return true if the key is present, whatever its value
     */
    public boolean containsKey(int key) {
        return table.containsKey(key);
    }

    /**
     * @return the removed value, or missingValue if the key was absent
     */
    public long remove(int key) {
        return table.remove(key);
    }

    /**
     * Removes the key only if it has the given value.
     *
     * @return true if the entry was removed
     */
    public boolean remove(int key, long value) {
        return table.remove(key, value);
    }

    /**
     * Removes all entries, keeping the current capacity.
     */
    public void clear() {
        table.clear();
    }

    /**
     * Keys in the table order. The stream reads the table lazily - do not modify the table until it is consumed.
     */
    public IntStream keysStream() {
        return table.keysStream().mapToInt(k -> (int) k);
    }

    /**
     * Values in the order of {@link #keysStream()}, with the same restriction.
     */
    public LongStream valuesStream() {
        return table.valuesStream();
    }

    /**
     * Allocation-free iteration over all entries, in the order of {@link #keysStream()}. The consumer must not modify
     * the table.
     */
    public void forEach(IntLongConsumer consumer) {
        table.forEachInt(consumer);
    }

    public long size() {
        return table.size();
    }

    /**
     * @return what {@link #get}, {@link #put} and {@link #remove} return for a missing key
     */
    public long missingValue() {
        return table.missingValue();
    }

    /**
     * @return the number of slots; the table grows when its size reaches 65% of them
     */
    public int capacity() {
        return table.capacity();
    }

    /**
     * Grows the table, if needed, to take expectedSize entries in total without a resize, see
     * {@link LongLongHashtable#ensureCapacity}.
     */
    public void ensureCapacity(int expectedSize) {
        table.ensureCapacity(expectedSize);
    }

    /**
     * @return the hash seed - one for all tables in the JVM, see {@link LongLongHashtable#seed()}
     */
    public long seed() {
        return table.seed();
    }

    /**
     * All entries as {@code {key=value, ...}} - for tests and debugging, it walks the whole table.
     */
    @Override
    public String toString() {
        return table.toString();
    }

    void integrityCheck() {
        table.integrityCheck();
    }
}
