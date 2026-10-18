package exchange.core2.collections.hashtable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntConsumer;

import static exchange.core2.collections.hashtable.HashingUtils.SEED;

/**
 * Keys of a fixed number of {@code long}s to values of a fixed number of {@code long}s, side by side in the slots of
 * one {@code long[]}: open addressing with linear probing, backward-shift deletion (no tombstones).
 * <ul>
 *     <li>A slot is a tag, the key and the values: {@code 1 + keyLongs + valueLongs} longs. The tag is the hash of the
 *     key with the top bit set - never 0, which marks a free slot - so any key can be stored, all zeros included; a
 *     probe compares the tags before the keys, and a resize or a removal never hashes a key again.</li>
 *     <li>An operation on a key returns the position of its slot, and the values are read and written through it:
 *     one probe reaches them, no second structure. A position is valid until the next insert or removal - a resize or
 *     the backward shift of a removal moves the entries: find the key again after one.</li>
 *     <li>Keys of 1 to 4 longs have methods of their own, without an array; keys of any length go through a
 *     {@code long[]}. The methods for a length check it against {@link Builder#keyLongs}.</li>
 *     <li>Grows by doubling at load 0.65, on the calling thread - pre-size the table ({@link Builder#expectedSize},
 *     {@link #ensureCapacity}). {@link Builder#maxCapacity}, {@link Builder#fixedCapacity} and {@link Builder#maxLoad}
 *     as in {@link LongLongHashtable}: at the maximum capacity a new key is refused with
 *     {@link IllegalStateException}, the table unchanged.</li>
 *     <li>The hash seed of the JVM ({@code -Dexchange.hashtable.seed}) is mixed into every tag.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 */
public final class LongTupleHashtable {

    /**
     * Not a position: what {@link #find} returns for a missing key. An access through it throws
     * {@link ArrayIndexOutOfBoundsException}.
     */
    public static final int NO_SLOT = Integer.MIN_VALUE;

    public static final int DEFAULT_EXPECTED_SIZE = 16;

    private static final float UPSIZE_LOAD = 0.65f;

    private final int keyLongs;
    private final int valueLongs;
    private final int stride;
    /** the offset of the first value in a slot */
    private final int valueBase;
    private final int maxCapacity;
    private final double maxLoad;

    private long[] data;
    private int mask;
    private int size;
    private long upsizeThreshold;

    private LongTupleHashtable(Builder builder) {
        if (builder.keyLongs < 1 || builder.valueLongs < 0
                || 1L + builder.keyLongs + builder.valueLongs > Integer.MAX_VALUE / 2) {
            throw new IllegalArgumentException("a key of at least 1 long and values of at least 0, not "
                    + builder.keyLongs + " and " + builder.valueLongs);
        }
        this.keyLongs = builder.keyLongs;
        this.valueLongs = builder.valueLongs;
        this.stride = 1 + keyLongs + valueLongs;
        this.valueBase = 1 + keyLongs;

        final int largest = largestCapacity(stride);
        if (builder.maxCapacity > largest) {
            throw new IllegalArgumentException("max capacity " + builder.maxCapacity + " is above " + largest
                    + ", the largest for slots of " + stride + " longs");
        }
        final int limit = builder.maxCapacity == 0 ? largest : builder.maxCapacity;
        final int capacity = HashingUtils.capacityFor(builder.expectedSize, UPSIZE_LOAD, limit);
        this.maxCapacity = builder.fixedCapacity ? capacity : limit;
        this.maxLoad = builder.maxLoad;
        switchTo(new long[capacity * stride], capacity);
    }

    /**
     * @return the largest capacity, a power of two, whose slots fit a {@code long[]}
     */
    private static int largestCapacity(int stride) {
        return Math.min(Integer.highestOneBit((Integer.MAX_VALUE - 8) / stride), HashingUtils.MAX_CAPACITY);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link LongTupleHashtable}: {@link #keyLongs} and {@link #valueLongs} are required, the rest
     * optional.
     */
    public static final class Builder {

        private int keyLongs;
        private int valueLongs = -1;
        private int expectedSize = DEFAULT_EXPECTED_SIZE;
        /** 0: the largest for the slot */
        private int maxCapacity;
        private boolean fixedCapacity;
        private double maxLoad = HashingUtils.DEFAULT_MAX_LOAD;

        private Builder() {
        }

        /**
         * Number of {@code long}s of a key, at least 1.
         */
        public Builder keyLongs(int keyLongs) {
            this.keyLongs = keyLongs;
            return this;
        }

        /**
         * Number of {@code long} values of a key, at least 0 (a set of keys).
         */
        public Builder valueLongs(int valueLongs) {
            this.valueLongs = valueLongs;
            return this;
        }

        /**
         * Keys that fit without a resize (default {@value LongTupleHashtable#DEFAULT_EXPECTED_SIZE}).
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * Capacity in slots the table grows up to: a power of two, at most 2^29 and what fits a {@code long[]} for the
         * slot. There it takes new keys up to {@link #maxLoad}, then refuses them.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = HashingUtils.checkMaxCapacity(maxCapacity);
            return this;
        }

        /**
         * No resize, ever: the capacity for {@link #expectedSize} is also the maximum.
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        /**
         * Load up to which a table that can not grow takes new keys: from 0.65 to below 1, default 0.95 - see
         * {@link LongLongHashtable.Builder#maxLoad}.
         */
        public Builder maxLoad(double maxLoad) {
            this.maxLoad = HashingUtils.checkMaxLoad(maxLoad);
            return this;
        }

        public LongTupleHashtable build() {
            return new LongTupleHashtable(this);
        }
    }

    /*
     * The tag: the key mixed long by long - h = mix(h + k) from the seed, mix the SplitMix64 finalizer (a bijection) -
     * with the top bit set. Its low bits are the home slot.
     */

    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    private static long tag(long k1) {
        return mix(SEED + k1) | Long.MIN_VALUE;
    }

    private static long tag(long k1, long k2) {
        return mix(mix(SEED + k1) + k2) | Long.MIN_VALUE;
    }

    private static long tag(long k1, long k2, long k3) {
        return mix(mix(mix(SEED + k1) + k2) + k3) | Long.MIN_VALUE;
    }

    private static long tag(long k1, long k2, long k3, long k4) {
        return mix(mix(mix(mix(SEED + k1) + k2) + k3) + k4) | Long.MIN_VALUE;
    }

    private static long tag(long[] key, int offset, int length) {
        long h = SEED;
        for (int i = 0; i < length; i++) {
            h = mix(h + key[offset + i]);
        }
        return h | Long.MIN_VALUE;
    }

    private void checkKeyLongs(int length) {
        if (length != keyLongs) {
            throw new IllegalArgumentException("a key of " + length + " longs, the table has keys of " + keyLongs);
        }
    }

    /*
     * The probes: the position of the key's slot, or ~position of the free slot the probe stopped at.
     */

    private int probe(long tag, long k1) {
        final long[] data = this.data;
        int slot = (int) tag & mask;
        for (int i = 0; ; i++) {
            final int pos = slot * stride;
            final long t = data[pos];
            if (t == 0L) {
                return ~pos;
            }
            if (t == tag && data[pos + 1] == k1) {
                return pos;
            }
            slot = nextSlot(slot, i);
        }
    }

    private int probe(long tag, long k1, long k2) {
        final long[] data = this.data;
        int slot = (int) tag & mask;
        for (int i = 0; ; i++) {
            final int pos = slot * stride;
            final long t = data[pos];
            if (t == 0L) {
                return ~pos;
            }
            if (t == tag && data[pos + 1] == k1 && data[pos + 2] == k2) {
                return pos;
            }
            slot = nextSlot(slot, i);
        }
    }

    private int probe(long tag, long k1, long k2, long k3) {
        final long[] data = this.data;
        int slot = (int) tag & mask;
        for (int i = 0; ; i++) {
            final int pos = slot * stride;
            final long t = data[pos];
            if (t == 0L) {
                return ~pos;
            }
            if (t == tag && data[pos + 1] == k1 && data[pos + 2] == k2 && data[pos + 3] == k3) {
                return pos;
            }
            slot = nextSlot(slot, i);
        }
    }

    private int probe(long tag, long k1, long k2, long k3, long k4) {
        final long[] data = this.data;
        int slot = (int) tag & mask;
        for (int i = 0; ; i++) {
            final int pos = slot * stride;
            final long t = data[pos];
            if (t == 0L) {
                return ~pos;
            }
            if (t == tag && data[pos + 1] == k1 && data[pos + 2] == k2 && data[pos + 3] == k3
                    && data[pos + 4] == k4) {
                return pos;
            }
            slot = nextSlot(slot, i);
        }
    }

    private int probe(long tag, long[] key, int offset) {
        final long[] data = this.data;
        int slot = (int) tag & mask;
        for (int i = 0; ; i++) {
            final int pos = slot * stride;
            final long t = data[pos];
            if (t == 0L) {
                return ~pos;
            }
            if (t == tag && Arrays.equals(data, pos + 1, pos + 1 + keyLongs, key, offset, offset + keyLongs)) {
                return pos;
            }
            slot = nextSlot(slot, i);
        }
    }

    /**
     * The probe loop ends at a free slot - there is always one; without it (a corrupted table) fail instead of
     * spinning.
     */
    private int nextSlot(int slot, int probed) {
        if (probed >= mask) {
            throw new IllegalStateException("probe: no free slot, capacity=" + (mask + 1) + " size=" + size);
        }
        return (slot + 1) & mask;
    }

    /*
     * The operations on a key, for each length of it.
     */

    /**
     * @return the position of the key's slot, or {@link #NO_SLOT} if the key is absent
     */
    public int find(long k1) {
        checkKeyLongs(1);
        final int r = probe(tag(k1), k1);
        return r >= 0 ? r : NO_SLOT;
    }

    public int find(long k1, long k2) {
        checkKeyLongs(2);
        final int r = probe(tag(k1, k2), k1, k2);
        return r >= 0 ? r : NO_SLOT;
    }

    public int find(long k1, long k2, long k3) {
        checkKeyLongs(3);
        final int r = probe(tag(k1, k2, k3), k1, k2, k3);
        return r >= 0 ? r : NO_SLOT;
    }

    public int find(long k1, long k2, long k3, long k4) {
        checkKeyLongs(4);
        final int r = probe(tag(k1, k2, k3, k4), k1, k2, k3, k4);
        return r >= 0 ? r : NO_SLOT;
    }

    /**
     * The key is {@code key[offset .. offset + keyLongs)}.
     */
    public int find(long[] key, int offset) {
        Objects.checkFromIndexSize(offset, keyLongs, key.length);
        final int r = probe(tag(key, offset, keyLongs), key, offset);
        return r >= 0 ? r : NO_SLOT;
    }

    /**
     * The slot of the key; a missing key gets one, all its values 0. One probe - two if the table resizes.
     *
     * @return the position of the key's slot
     * @throws IllegalStateException if the key is missing and the table is full - nothing changes then
     */
    public int getOrInsert(long k1) {
        checkKeyLongs(1);
        final long tag = tag(k1);
        int r = probe(tag, k1);
        if (r >= 0) {
            return r;
        }
        if (size + 1 >= upsizeThreshold && makeRoomForOneMore()) {
            r = probe(tag, k1);
        }
        final int pos = ~r;
        data[pos + 1] = k1;
        return inserted(pos, tag);
    }

    public int getOrInsert(long k1, long k2) {
        checkKeyLongs(2);
        final long tag = tag(k1, k2);
        int r = probe(tag, k1, k2);
        if (r >= 0) {
            return r;
        }
        if (size + 1 >= upsizeThreshold && makeRoomForOneMore()) {
            r = probe(tag, k1, k2);
        }
        final int pos = ~r;
        data[pos + 1] = k1;
        data[pos + 2] = k2;
        return inserted(pos, tag);
    }

    public int getOrInsert(long k1, long k2, long k3) {
        checkKeyLongs(3);
        final long tag = tag(k1, k2, k3);
        int r = probe(tag, k1, k2, k3);
        if (r >= 0) {
            return r;
        }
        if (size + 1 >= upsizeThreshold && makeRoomForOneMore()) {
            r = probe(tag, k1, k2, k3);
        }
        final int pos = ~r;
        data[pos + 1] = k1;
        data[pos + 2] = k2;
        data[pos + 3] = k3;
        return inserted(pos, tag);
    }

    public int getOrInsert(long k1, long k2, long k3, long k4) {
        checkKeyLongs(4);
        final long tag = tag(k1, k2, k3, k4);
        int r = probe(tag, k1, k2, k3, k4);
        if (r >= 0) {
            return r;
        }
        if (size + 1 >= upsizeThreshold && makeRoomForOneMore()) {
            r = probe(tag, k1, k2, k3, k4);
        }
        final int pos = ~r;
        data[pos + 1] = k1;
        data[pos + 2] = k2;
        data[pos + 3] = k3;
        data[pos + 4] = k4;
        return inserted(pos, tag);
    }

    /**
     * The key is {@code key[offset .. offset + keyLongs)}.
     */
    public int getOrInsert(long[] key, int offset) {
        Objects.checkFromIndexSize(offset, keyLongs, key.length);
        final long tag = tag(key, offset, keyLongs);
        int r = probe(tag, key, offset);
        if (r >= 0) {
            return r;
        }
        if (size + 1 >= upsizeThreshold && makeRoomForOneMore()) {
            r = probe(tag, key, offset);
        }
        final int pos = ~r;
        System.arraycopy(key, offset, data, pos + 1, keyLongs);
        return inserted(pos, tag);
    }

    /**
     * The tag written last: the key and the zeroed values are in place. Free slots hold zeros only.
     */
    private int inserted(int pos, long tag) {
        data[pos] = tag;
        size++;
        return pos;
    }

    /**
     * Removes the key and its values; the positions of the other keys may change.
     *
     * @return true if the key was present
     */
    public boolean remove(long k1) {
        checkKeyLongs(1);
        return removeAt(probe(tag(k1), k1));
    }

    public boolean remove(long k1, long k2) {
        checkKeyLongs(2);
        return removeAt(probe(tag(k1, k2), k1, k2));
    }

    public boolean remove(long k1, long k2, long k3) {
        checkKeyLongs(3);
        return removeAt(probe(tag(k1, k2, k3), k1, k2, k3));
    }

    public boolean remove(long k1, long k2, long k3, long k4) {
        checkKeyLongs(4);
        return removeAt(probe(tag(k1, k2, k3, k4), k1, k2, k3, k4));
    }

    /**
     * The key is {@code key[offset .. offset + keyLongs)}.
     */
    public boolean remove(long[] key, int offset) {
        Objects.checkFromIndexSize(offset, keyLongs, key.length);
        return removeAt(probe(tag(key, offset, keyLongs), key, offset));
    }

    private boolean removeAt(int r) {
        if (r < 0) {
            return false;
        }
        deleteAt(r / stride);
        size--;
        return true;
    }

    /**
     * Backward-shift deletion (Knuth's Algorithm R), in slots: every entry of the rest of the cluster that the gap
     * would cut off from its home moves back into it; the last gap is zeroed.
     */
    private void deleteAt(int gap) {
        int slot = gap;
        for (int i = 0; ; i++) {
            slot = (slot + 1) & mask;
            final int pos = slot * stride;
            final long t = data[pos];
            if (t == 0L) {
                break;
            }
            if (LinearProbing.canFillGap(gap, (int) t & mask, slot, mask)) {
                System.arraycopy(data, pos, data, gap * stride, stride);
                gap = slot;
            }
            if (i >= mask) {
                throw new IllegalStateException("deleteAt: no free slot, capacity=" + (mask + 1));
            }
        }
        final int pos = gap * stride;
        Arrays.fill(data, pos, pos + stride, 0L);
    }

    /*
     * The values and the key of a slot, through its position.
     */

    /**
     * @return value i of the slot
     * @throws IndexOutOfBoundsException if i is not from 0 to {@link #valueLongs()} - 1, or the position is
     *                                   {@link #NO_SLOT}
     */
    public long getValue(int position, int i) {
        return data[position + valueBase + Objects.checkIndex(i, valueLongs)];
    }

    /**
     * @throws IndexOutOfBoundsException if i is not from 0 to {@link #valueLongs()} - 1, or the position is
     *                                   {@link #NO_SLOT}
     */
    public void setValue(int position, int i, long value) {
        data[position + valueBase + Objects.checkIndex(i, valueLongs)] = value;
    }

    /**
     * Adds delta to value i. Overflow wraps around, as with {@code +} - see {@link #addAndGetValueExact}.
     *
     * @return the new value
     * @throws IndexOutOfBoundsException if i is not from 0 to {@link #valueLongs()} - 1, or the position is
     *                                   {@link #NO_SLOT}
     */
    public long addAndGetValue(int position, int i, long delta) {
        return data[position + valueBase + Objects.checkIndex(i, valueLongs)] += delta;
    }

    /**
     * Adds delta to value i, unless the sum overflows a {@code long}.
     *
     * @return the new value
     * @throws ArithmeticException       if the sum overflows - the value stays
     * @throws IndexOutOfBoundsException if i is not from 0 to {@link #valueLongs()} - 1, or the position is
     *                                   {@link #NO_SLOT}
     */
    public long addAndGetValueExact(int position, int i, long delta) {
        final int at = position + valueBase + Objects.checkIndex(i, valueLongs);
        return data[at] = Math.addExact(data[at], delta);
    }

    /**
     * @return long i of the slot's key
     * @throws IndexOutOfBoundsException if i is not from 0 to {@link #keyLongs()} - 1, or the position is
     *                                   {@link #NO_SLOT}
     */
    public long getKey(int position, int i) {
        return data[position + 1 + Objects.checkIndex(i, keyLongs)];
    }

    /*
     * The whole table.
     */

    /**
     * Visits the positions of all keys, in the table order. The consumer reads and writes the values through them;
     * it must not insert or remove keys.
     */
    public void forEach(IntConsumer positionConsumer) {
        final long[] data = this.data;
        for (int pos = 0; pos < data.length; pos += stride) {
            if (data[pos] != 0L) {
                positionConsumer.accept(pos);
            }
        }
    }

    /**
     * Removes all keys, keeping the capacity.
     */
    public void clear() {
        Arrays.fill(data, 0L);
        size = 0;
    }

    /**
     * Grows the table, if needed, to take expectedSize keys in total without a resize - one rehash, for the set-up.
     *
     * @throws IllegalArgumentException if expectedSize is negative or needs more than the maximum capacity
     */
    public void ensureCapacity(int expectedSize) {
        final int capacity = HashingUtils.capacityFor(expectedSize, UPSIZE_LOAD, maxCapacity);
        if (capacity > mask + 1) {
            rehash(capacity);
        }
    }

    /**
     * Before the insert that crosses the threshold: grows the table, or raises the threshold up to maxLoad at the
     * maximum capacity, or throws there if the table is full.
     *
     * @return true if the entries moved - the probe has to be done again
     */
    private boolean makeRoomForOneMore() {
        if (mask + 1 >= maxCapacity) {
            final long limit = HashingUtils.maxEntries(mask + 1, maxLoad);
            if (size + 1 <= limit) {
                upsizeThreshold = limit + 1;
                return false;
            }
            throw new IllegalStateException("table is full: " + size + " keys at the maximum capacity of "
                    + (mask + 1) + " slots");
        }
        rehash((mask + 1) * 2);
        return true;
    }

    /**
     * Moves every entry to its slot in a new array - the tag gives the home, no key is hashed.
     */
    private void rehash(int capacity) {
        final long[] old = data;
        final long[] fresh = new long[capacity * stride];
        final int newMask = capacity - 1;
        for (int from = 0; from < old.length; from += stride) {
            final long t = old[from];
            if (t == 0L) {
                continue;
            }
            int slot = (int) t & newMask;
            while (fresh[slot * stride] != 0L) {
                slot = (slot + 1) & newMask;
            }
            System.arraycopy(old, from, fresh, slot * stride, stride);
        }
        switchTo(fresh, capacity);
    }

    private void switchTo(long[] array, int capacity) {
        this.data = array;
        this.mask = capacity - 1;
        this.upsizeThreshold = (int) (capacity * UPSIZE_LOAD);
    }

    public int size() {
        return size;
    }

    /**
     * @return the number of slots; the table grows when its size reaches 65% of them
     */
    public int capacity() {
        return mask + 1;
    }

    public int keyLongs() {
        return keyLongs;
    }

    public int valueLongs() {
        return valueLongs;
    }

    /**
     * @return the hash seed - one for all tables in the JVM, {@code -Dexchange.hashtable.seed=random|<number>}
     */
    public long seed() {
        return SEED;
    }

    /**
     * All entries as {@code {[k1, k2]=[v1, v2], ...}} in the table order - for tests and debugging.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach(pos -> {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(Arrays.toString(Arrays.copyOfRange(data, pos + 1, pos + valueBase))).append('=')
                    .append(Arrays.toString(Arrays.copyOfRange(data, pos + valueBase, pos + stride)));
        });
        return sb.append('}').toString();
    }

    /**
     * Verifies the invariants: every tag is the hash of its key and the key is found by probing from its home (no free
     * slot in the way, no duplicate before it), free slots hold zeros only, the size matches, a slot is free. For tests
     * - scans the whole array.
     */
    void integrityCheck() {
        int occupied = 0;
        final long[] key = new long[keyLongs];
        for (int pos = 0; pos < data.length; pos += stride) {
            final long t = data[pos];
            if (t == 0L) {
                for (int i = 1; i < stride; i++) {
                    if (data[pos + i] != 0L) {
                        throw new IllegalStateException("integrityCheck: free slot at " + pos + " holds "
                                + data[pos + i] + " at " + i);
                    }
                }
                continue;
            }
            occupied++;
            System.arraycopy(data, pos + 1, key, 0, keyLongs);
            if (t != tag(key, 0, keyLongs)) {
                throw new IllegalStateException("integrityCheck: the tag at " + pos + " is not the hash of "
                        + Arrays.toString(key));
            }
            final int r = probe(t, key, 0);
            if (r != pos) {
                throw new IllegalStateException("integrityCheck: " + Arrays.toString(key) + " stored at " + pos
                        + " but the probe from its home reaches " + (r >= 0 ? r : ~r)
                        + (r >= 0 ? " (duplicate)" : " (unreachable)"));
            }
        }
        if (occupied != size) {
            throw new IllegalStateException("integrityCheck: size=" + size + " but " + occupied + " occupied slots");
        }
        if (occupied > mask) {
            throw new IllegalStateException("integrityCheck: no free slot left, capacity=" + (mask + 1));
        }
    }
}
