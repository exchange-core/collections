package exchange.core2.collections.hashtable;

import exchange.core2.collections.records.RecordStore;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

/**
 * {@code long} keys to records of {@code long} fields: a {@link LongLongHashtable} maps every key to the handle of its
 * record in a {@link RecordStore}.
 * <ul>
 *     <li>One probe for a key: {@link #find}, {@link #getOrCreate}, {@link #remove}. The fields are reached through the
 *     handle: keep it, and the next access needs no probe - it stays valid until the key is removed, however the
 *     index grows, since the records never move.</li>
 *     <li>A removed key frees its record: the handle is stale, an access through it throws
 *     {@link IllegalArgumentException} - so does {@link #NO_RECORD}, what {@link #find} returns for a missing key.</li>
 *     <li>A record holds its key too, one more {@code long}: {@link #keyOf}, and {@link #forEach} in the order of the
 *     records - the same from run to run, whatever the hash seed.</li>
 *     <li>Growing: the index resizes as {@link LongLongHashtable} does - a copy of the index (16 bytes per slot) on the
 *     calling thread; the store adds a page. Pre-size the table ({@link Builder#expectedSize},
 *     {@link #ensureCapacity}), or make it fixed ({@link Builder#fixedCapacity}).</li>
 *     <li>The hash seed of the JVM, as for the hashtables.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 * Cost: an operation on a key is a probe of the index and an access to the record - two cache misses where neither
 * is cached; an operation on a handle is the access to the record alone.
 */
public final class LongRecordTable {

    /**
     * Not a handle: what {@link #find} returns for a missing key.
     */
    public static final long NO_RECORD = RecordStore.NO_RECORD;

    private final LongLongHashtable index;
    private final RecordStore store;
    private final int fields;
    /** the field of the store with the key: after the fields of the table */
    private final int keyField;
    /** allocates the record of a new key - getOrCreate, one probe */
    private final LongUnaryOperator newRecord;
    private final LongConsumer forEachRecord;
    private LongLongConsumer forEachConsumer;

    private LongRecordTable(Builder builder) {
        if (builder.fields < 1 || builder.fields == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("fields must be from 1 to " + (Integer.MAX_VALUE - 1) + ": "
                    + builder.fields);
        }
        this.fields = builder.fields;
        this.keyField = builder.fields;

        final RecordStore.Builder storeBuilder = RecordStore.builder()
                .fields(fields + 1)
                .expectedSize(builder.expectedSize)
                .pageSize(builder.pageSize)
                .maxCapacity(builder.maxCapacity);
        final LongLongHashtable.Builder indexBuilder = LongLongHashtable.builder()
                .expectedSize(builder.expectedSize);
        if (builder.fixedCapacity) {
            storeBuilder.fixedCapacity();
            indexBuilder.fixedCapacity();
        }
        this.store = storeBuilder.build();
        this.index = indexBuilder.build();

        this.newRecord = key -> {
            final long handle = store.allocate();
            store.set(handle, keyField, key);
            return handle;
        };
        this.forEachRecord = handle -> forEachConsumer.accept(store.get(handle, keyField), handle);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link LongRecordTable}: {@link #fields} is required, the rest optional.
     */
    public static final class Builder {

        private int fields;
        private int expectedSize;
        private int pageSize = RecordStore.DEFAULT_PAGE_SIZE;
        private int maxCapacity = RecordStore.MAX_CAPACITY;
        private boolean fixedCapacity;

        private Builder() {
        }

        /**
         * Number of {@code long} fields of a record, at least 1. A record takes two more {@code long}s: its header in
         * the store and the key.
         */
        public Builder fields(int fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Keys that fit without a resize of the index or a new page of the store (default 0).
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * Records per page of the store, see {@link RecordStore.Builder#pageSize}.
         */
        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Keys the table holds at most, see {@link RecordStore.Builder#maxCapacity}: beyond them
         * {@link LongRecordTable#getOrCreate} of a new key throws {@link IllegalStateException}.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        /**
         * No allocation after the build: neither the index nor the store grows. The table takes at least
         * {@link #expectedSize} keys - the index up to its maximum load, the store up to its whole pages - then
         * {@link LongRecordTable#getOrCreate} of a new key throws {@link IllegalStateException}.
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        public LongRecordTable build() {
            return new LongRecordTable(this);
        }
    }

    /**
     * @return the handle of the key's record, or {@link #NO_RECORD} if the key is absent
     */
    public long find(long key) {
        return index.get(key);
    }

    public boolean containsKey(long key) {
        return index.containsKey(key);
    }

    /**
     * The record of the key; a missing key gets a new one, all its fields 0. One probe.
     *
     * @return the handle of the record
     * @throws IllegalStateException if the key is missing and the table can not take another - nothing changes then
     */
    public long getOrCreate(long key) {
        return index.getOrInsert(key, newRecord);
    }

    /**
     * Removes the key and frees its record: the handle is stale from now on.
     *
     * @return true if the key was present
     */
    public boolean remove(long key) {
        final long handle = index.remove(key);
        if (handle == NO_RECORD) {
            return false;
        }
        store.free(handle);
        return true;
    }

    /**
     * @return the key of the record
     * @throws IllegalArgumentException if the handle is stale
     */
    public long keyOf(long handle) {
        return store.get(handle, keyField);
    }

    /**
     * @return true if the handle is of a record of this table - false once its key is removed
     */
    public boolean isValid(long handle) {
        return store.isValid(handle);
    }

    /**
     * @return the value of the field
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public long get(long handle, int field) {
        return store.get(handle, Objects.checkIndex(field, fields));
    }

    /**
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public void set(long handle, int field, long value) {
        store.set(handle, Objects.checkIndex(field, fields), value);
    }

    /**
     * Adds delta to the field. Overflow wraps around, as with {@code +} - see {@link #addAndGetExact}.
     *
     * @return the new value
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public long addAndGet(long handle, int field, long delta) {
        return store.addAndGet(handle, Objects.checkIndex(field, fields), delta);
    }

    /**
     * Adds delta to the field, unless the sum overflows a {@code long}.
     *
     * @return the new value
     * @throws ArithmeticException       if the sum overflows - the field keeps its value
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public long addAndGetExact(long handle, int field, long delta) {
        return store.addAndGetExact(handle, Objects.checkIndex(field, fields), delta);
    }

    /**
     * Visits the keys with the handles of their records, in the order of the records - the same from run to run,
     * whatever the hash seed. The consumer may remove keys, the one visited included; a key added meanwhile may or may
     * not be visited.
     */
    public void forEach(LongLongConsumer keyHandleConsumer) {
        final LongLongConsumer outer = forEachConsumer;
        forEachConsumer = keyHandleConsumer;
        try {
            store.forEach(forEachRecord);
        } finally {
            forEachConsumer = outer;
        }
    }

    /**
     * Removes all keys, keeping the capacity: every handle is stale afterwards.
     */
    public void clear() {
        index.clear();
        store.clear();
    }

    /**
     * Makes the index and the store take expectedSize keys in total without a resize or a new page - for the set-up,
     * not for the latency-critical path.
     *
     * @throws IllegalArgumentException if expectedSize is negative or more than the table can hold
     */
    public void ensureCapacity(int expectedSize) {
        store.ensureCapacity(expectedSize);
        index.ensureCapacity(expectedSize);
    }

    public int size() {
        return store.size();
    }

    /**
     * @return number of {@code long} fields of a record, the key not counted
     */
    public int fields() {
        return fields;
    }

    @Override
    public String toString() {
        return "LongRecordTable{size=" + size() + ", fields=" + fields + '}';
    }

    /** for tests: the integrity check of the store is package-private there */
    RecordStore store() {
        return store;
    }

    /**
     * Verifies the index ({@link LongLongHashtable#integrityCheck}) and that it matches the store: every key maps to a
     * live record holding that key, every live record is mapped by its key. For tests - walks both.
     */
    void integrityCheck() {
        index.integrityCheck();
        if (index.size() != store.size()) {
            throw new IllegalStateException("integrityCheck: " + index.size() + " keys, " + store.size()
                    + " records");
        }
        index.forEach((key, handle) -> {
            if (!store.isValid(handle) || store.get(handle, keyField) != key) {
                throw new IllegalStateException("integrityCheck: key " + key + " maps to handle 0x"
                        + Long.toHexString(handle) + (store.isValid(handle)
                        ? " of key " + store.get(handle, keyField) : ", not a live record"));
            }
        });
        store.forEach(handle -> {
            final long key = store.get(handle, keyField);
            if (index.get(key) != handle) {
                throw new IllegalStateException("integrityCheck: the record 0x" + Long.toHexString(handle)
                        + " of key " + key + " is not in the index");
            }
        });
    }
}
