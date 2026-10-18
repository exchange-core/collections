package exchange.core2.collections.ledger;

import exchange.core2.collections.hashtable.LongTupleHashtable;

import java.util.Objects;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Balances - an available and a reserved amount - under keys of a fixed number of {@code long}s: (account,
 * currency), (user UUID, token, subaccount)... in a {@link LongTupleHashtable}.
 * <ul>
 *     <li>Neither amount ever goes below 0, and no sum overflows: an operation checks before it writes, and returns a
 *     status - {@link #OK}, {@link #INSUFFICIENT_AVAILABLE}, {@link #INSUFFICIENT_RESERVED}, {@link #OVERFLOW}... -
 *     the balance unchanged unless it is {@code OK}. No exception for a refusal, no allocation.</li>
 *     <li>The operations on one balance go through a reference: {@link #find} or {@link #getOrCreate} it by the key,
 *     then {@link #deposit}, {@link #withdraw}, {@link #reserve}, {@link #release}, {@link #settle} or
 *     {@link #post}. A reference is valid until the entries move - a resize, a removal: one used after that throws
 *     {@link IllegalArgumentException} instead of reaching another balance. {@link #NO_REF}, what {@link #find}
 *     returns for a missing key, stands for a zero balance.</li>
 *     <li>A {@link Batch} of postings to several balances applies all or nothing: in their order, the same key any
 *     number of times; the first one refused undoes those before it.</li>
 *     <li>A balance is created by a deposit-like posting only - one that takes nothing - and removed only when it is
 *     zero.</li>
 *     <li>The hash seed of the JVM, {@code maxCapacity} / {@code fixedCapacity} / {@code maxLoad} as for
 *     {@link LongTupleHashtable}. Not thread-safe.</li>
 * </ul>
 */
public final class BalanceTable {

    /** the operation is done */
    public static final int OK = 0;
    /** the available amount would go below 0 - a missing balance has none */
    public static final int INSUFFICIENT_AVAILABLE = 1;
    /** the reserved amount would go below 0 - a missing balance has none */
    public static final int INSUFFICIENT_RESERVED = 2;
    /** an amount would overflow a {@code long} */
    public static final int OVERFLOW = 3;
    /** a deposit to a missing balance - {@link #getOrCreate} it - or the removal of a missing one */
    public static final int NO_BALANCE = 4;
    /** a negative amount for an operation that takes a positive one */
    public static final int INVALID_AMOUNT = 5;
    /** the removal of a balance that is not zero */
    public static final int NOT_ZERO = 6;

    private static final String[] STATUS_NAMES = {"OK", "INSUFFICIENT_AVAILABLE", "INSUFFICIENT_RESERVED",
            "OVERFLOW", "NO_BALANCE", "INVALID_AMOUNT", "NOT_ZERO"};

    /**
     * Not a reference: what {@link #find} returns for a missing key. The operations take it for a zero balance.
     */
    public static final long NO_REF = -1L;

    private static final int AVAILABLE = 0;
    private static final int RESERVED = 1;

    private final LongTupleHashtable balances;
    private final int keyLongs;
    /** a key read from a slot - the removal through a reference, the undo of a batch */
    private final long[] scratchKey;
    /** grows whenever the entries move - a resize, a removal, a clear: the high half of a reference */
    private int epoch;

    private final IntConsumer forEachSlot;
    private LongConsumer forEachConsumer;

    private BalanceTable(Builder builder) {
        if (builder.keyLongs < 1) {
            throw new IllegalArgumentException("a key of at least 1 long: " + builder.keyLongs);
        }
        final LongTupleHashtable.Builder tableBuilder = LongTupleHashtable.builder()
                .keyLongs(builder.keyLongs)
                .valueLongs(2)
                .expectedSize(builder.expectedSize)
                .maxLoad(builder.maxLoad);
        if (builder.maxCapacity != 0) {
            tableBuilder.maxCapacity(builder.maxCapacity);
        }
        if (builder.fixedCapacity) {
            tableBuilder.fixedCapacity();
        }
        this.balances = tableBuilder.build();
        this.keyLongs = builder.keyLongs;
        this.scratchKey = new long[keyLongs];
        this.forEachSlot = pos -> forEachConsumer.accept(ref(pos));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link BalanceTable}: {@link #keyLongs} is required, the rest optional.
     */
    public static final class Builder {

        private int keyLongs;
        private int expectedSize = LongTupleHashtable.DEFAULT_EXPECTED_SIZE;
        private int maxCapacity;
        private boolean fixedCapacity;
        private double maxLoad = 0.95;

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
         * Balances that fit without a resize - a resize makes every reference stale.
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * See {@link LongTupleHashtable.Builder#maxCapacity}.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        /**
         * No resize, ever - see {@link LongTupleHashtable.Builder#fixedCapacity}: a reference then stays valid until
         * a removal.
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        /**
         * See {@link LongTupleHashtable.Builder#maxLoad}.
         */
        public Builder maxLoad(double maxLoad) {
            this.maxLoad = maxLoad;
            return this;
        }

        public BalanceTable build() {
            return new BalanceTable(this);
        }
    }

    /**
     * @return the name of a status, {@code "OK"} for {@link #OK} - for messages and logs
     */
    public static String statusName(int status) {
        return status >= 0 && status < STATUS_NAMES.length ? STATUS_NAMES[status] : "UNKNOWN(" + status + ")";
    }

    /*
     * References: the epoch in the high half, the position of the slot in the low one.
     */

    private long ref(int position) {
        return (long) epoch << 32 | position;
    }

    private int position(long ref) {
        if ((int) (ref >>> 32) != epoch || ref == NO_REF) {
            throw new IllegalArgumentException("stale balance reference 0x" + Long.toHexString(ref)
                    + ": the table has resized, or a balance was removed, since it was taken - find the key again");
        }
        return (int) ref;
    }

    /** after an insert: the entries moved if the table grew */
    private long created(int position, int capacityBefore) {
        if (balances.capacity() != capacityBefore) {
            epoch++;
        }
        return ref(position);
    }

    private long refOrNone(int position) {
        return position == LongTupleHashtable.NO_SLOT ? NO_REF : ref(position);
    }

    /*
     * The balance of a key, for each length of it.
     */

    /**
     * @return the reference of the key's balance, or {@link #NO_REF} if there is none
     */
    public long find(long k1) {
        return refOrNone(balances.find(k1));
    }

    public long find(long k1, long k2) {
        return refOrNone(balances.find(k1, k2));
    }

    public long find(long k1, long k2, long k3) {
        return refOrNone(balances.find(k1, k2, k3));
    }

    public long find(long k1, long k2, long k3, long k4) {
        return refOrNone(balances.find(k1, k2, k3, k4));
    }

    /**
     * The key is {@code key[offset .. offset + keyLongs)}.
     */
    public long find(long[] key, int offset) {
        return refOrNone(balances.find(key, offset));
    }

    /**
     * The balance of the key; a missing key gets a zero one. A new key may resize the table: the references taken
     * before are stale then.
     *
     * @return the reference of the balance
     * @throws IllegalStateException if the key is missing and the table is full - nothing changes then
     */
    public long getOrCreate(long k1) {
        final int capacity = balances.capacity();
        return created(balances.getOrInsert(k1), capacity);
    }

    public long getOrCreate(long k1, long k2) {
        final int capacity = balances.capacity();
        return created(balances.getOrInsert(k1, k2), capacity);
    }

    public long getOrCreate(long k1, long k2, long k3) {
        final int capacity = balances.capacity();
        return created(balances.getOrInsert(k1, k2, k3), capacity);
    }

    public long getOrCreate(long k1, long k2, long k3, long k4) {
        final int capacity = balances.capacity();
        return created(balances.getOrInsert(k1, k2, k3, k4), capacity);
    }

    /**
     * The key is {@code key[offset .. offset + keyLongs)}.
     */
    public long getOrCreate(long[] key, int offset) {
        final int capacity = balances.capacity();
        return created(balances.getOrInsert(key, offset), capacity);
    }

    /*
     * The operations on a balance.
     */

    /**
     * @return the available amount, 0 for {@link #NO_REF}
     * @throws IllegalArgumentException if the reference is stale
     */
    public long available(long ref) {
        return ref == NO_REF ? 0L : balances.getValue(position(ref), AVAILABLE);
    }

    /**
     * @return the reserved amount, 0 for {@link #NO_REF}
     * @throws IllegalArgumentException if the reference is stale
     */
    public long reserved(long ref) {
        return ref == NO_REF ? 0L : balances.getValue(position(ref), RESERVED);
    }

    /**
     * @return long i of the balance's key
     * @throws IllegalArgumentException if the reference is stale or {@link #NO_REF}
     */
    public long key(long ref, int i) {
        return balances.getKey(position(ref), i);
    }

    /**
     * Adds to the available amount.
     *
     * @return {@link #OK}, {@link #OVERFLOW}, {@link #INVALID_AMOUNT} for a negative amount, {@link #NO_BALANCE} for
     * {@link #NO_REF}
     */
    public int deposit(long ref, long amount) {
        return amount < 0 ? INVALID_AMOUNT : post(ref, amount, 0L);
    }

    /**
     * Takes from the available amount.
     *
     * @return {@link #OK}, {@link #INSUFFICIENT_AVAILABLE}, {@link #INVALID_AMOUNT} for a negative amount
     */
    public int withdraw(long ref, long amount) {
        return amount < 0 ? INVALID_AMOUNT : post(ref, -amount, 0L);
    }

    /**
     * Moves an amount from available to reserved - the hold of an order.
     *
     * @return {@link #OK}, {@link #INSUFFICIENT_AVAILABLE}, {@link #OVERFLOW} of the reserved amount,
     * {@link #INVALID_AMOUNT} for a negative amount
     */
    public int reserve(long ref, long amount) {
        return amount < 0 ? INVALID_AMOUNT : post(ref, -amount, amount);
    }

    /**
     * Moves an amount from reserved back to available - a cancelled order, the unused part of a hold.
     *
     * @return {@link #OK}, {@link #INSUFFICIENT_RESERVED}, {@link #OVERFLOW} of the available amount,
     * {@link #INVALID_AMOUNT} for a negative amount
     */
    public int release(long ref, long amount) {
        return amount < 0 ? INVALID_AMOUNT : post(ref, amount, -amount);
    }

    /**
     * Takes from the reserved amount - what a trade spends of a hold.
     *
     * @return {@link #OK}, {@link #INSUFFICIENT_RESERVED}, {@link #INVALID_AMOUNT} for a negative amount
     */
    public int settle(long ref, long amount) {
        return amount < 0 ? INVALID_AMOUNT : post(ref, 0L, -amount);
    }

    /**
     * Adds the deltas to the available and the reserved amounts, both or neither. {@link #NO_REF} is a zero balance:
     * a negative delta is refused, a positive one has no balance to go to.
     *
     * @return {@link #OK}; {@link #OVERFLOW}; {@link #INSUFFICIENT_AVAILABLE} or {@link #INSUFFICIENT_RESERVED} if an
     * amount would go below 0; {@link #NO_BALANCE} for a positive delta to {@link #NO_REF}
     * @throws IllegalArgumentException if the reference is stale
     */
    public int post(long ref, long availableDelta, long reservedDelta) {
        if (ref == NO_REF) {
            return postToNone(availableDelta, reservedDelta);
        }
        final int pos = position(ref);
        final long available = balances.getValue(pos, AVAILABLE);
        final long reserved = balances.getValue(pos, RESERVED);
        final int status = check(available, availableDelta, reserved, reservedDelta);
        if (status == OK) {
            balances.setValue(pos, AVAILABLE, available + availableDelta);
            balances.setValue(pos, RESERVED, reserved + reservedDelta);
        }
        return status;
    }

    private static int postToNone(long availableDelta, long reservedDelta) {
        if (availableDelta < 0) {
            return INSUFFICIENT_AVAILABLE;
        }
        if (reservedDelta < 0) {
            return INSUFFICIENT_RESERVED;
        }
        return availableDelta == 0 && reservedDelta == 0 ? OK : NO_BALANCE;
    }

    /**
     * @return the status of adding the deltas: an overflow first, then an amount below 0
     */
    private static int check(long available, long availableDelta, long reserved, long reservedDelta) {
        final long newAvailable = available + availableDelta;
        final long newReserved = reserved + reservedDelta;
        if (((available ^ newAvailable) & (availableDelta ^ newAvailable)) < 0
                || ((reserved ^ newReserved) & (reservedDelta ^ newReserved)) < 0) {
            return OVERFLOW;
        }
        if (newAvailable < 0) {
            return INSUFFICIENT_AVAILABLE;
        }
        if (newReserved < 0) {
            return INSUFFICIENT_RESERVED;
        }
        return OK;
    }

    /**
     * Removes the balance if it is zero; every reference is stale afterwards.
     *
     * @return {@link #OK}, {@link #NOT_ZERO}, or {@link #NO_BALANCE} for {@link #NO_REF}
     * @throws IllegalArgumentException if the reference is stale
     */
    public int remove(long ref) {
        if (ref == NO_REF) {
            return NO_BALANCE;
        }
        final int pos = position(ref);
        if (balances.getValue(pos, AVAILABLE) != 0L || balances.getValue(pos, RESERVED) != 0L) {
            return NOT_ZERO;
        }
        for (int i = 0; i < keyLongs; i++) {
            scratchKey[i] = balances.getKey(pos, i);
        }
        balances.remove(scratchKey, 0);
        epoch++;
        return OK;
    }

    /*
     * Batches.
     */

    /**
     * @return an empty batch of up to maxPostings postings for this table - made once, applied and cleared many times
     */
    public Batch newBatch(int maxPostings) {
        return new Batch(keyLongs, maxPostings);
    }

    /**
     * Applies the postings of the batch in their order, all or none: the first one refused - or a new key that does
     * not fit - undoes those before it, the balances created by them included. A posting to a missing key creates its
     * balance if it takes nothing (no negative delta). New balances may resize the table: the references taken
     * before are stale then.
     *
     * @return {@link #OK}, or the status of the posting refused - {@link Batch#failedPosting()} tells which
     * @throws IllegalStateException if a new key does not fit the table - nothing changes then
     */
    public int apply(Batch batch) {
        if (batch.keyLongs != keyLongs) {
            throw new IllegalArgumentException("a batch of keys of " + batch.keyLongs + " longs, the table has keys of "
                    + keyLongs);
        }
        batch.failed = -1;
        for (int i = 0; i < batch.size; i++) {
            final int status;
            try {
                status = applyPosting(batch, i);
            } catch (IllegalStateException full) {
                undo(batch, i - 1);
                batch.failed = i;
                throw full;
            }
            if (status != OK) {
                undo(batch, i - 1);
                batch.failed = i;
                return status;
            }
        }
        return OK;
    }

    private static final long UNDO_CHANGED = 0;
    private static final long UNDO_CREATED = 1;
    private static final long UNDO_NOTHING = 2;

    private int applyPosting(Batch batch, int i) {
        final int keyAt = i * keyLongs;
        final long availableDelta = batch.deltas[i * 2];
        final long reservedDelta = batch.deltas[i * 2 + 1];
        int pos = balances.find(batch.keys, keyAt);
        if (pos == LongTupleHashtable.NO_SLOT) {
            final int status = postToNone(availableDelta, reservedDelta);
            if (status == OK) {
                batch.undo[i * 3 + 2] = UNDO_NOTHING;
                return OK;
            }
            if (status != NO_BALANCE) {
                return status;
            }
            // takes nothing: a new balance holds the deltas, it can not overflow
            final int capacity = balances.capacity();
            pos = balances.getOrInsert(batch.keys, keyAt);
            created(pos, capacity);
            balances.setValue(pos, AVAILABLE, availableDelta);
            balances.setValue(pos, RESERVED, reservedDelta);
            batch.undo[i * 3 + 2] = UNDO_CREATED;
            return OK;
        }
        final long available = balances.getValue(pos, AVAILABLE);
        final long reserved = balances.getValue(pos, RESERVED);
        final int status = check(available, availableDelta, reserved, reservedDelta);
        if (status != OK) {
            return status;
        }
        balances.setValue(pos, AVAILABLE, available + availableDelta);
        balances.setValue(pos, RESERVED, reserved + reservedDelta);
        batch.undo[i * 3] = available;
        batch.undo[i * 3 + 1] = reserved;
        batch.undo[i * 3 + 2] = UNDO_CHANGED;
        return OK;
    }

    /**
     * Undoes postings last..0, the last applied first - by key: a balance created by the batch may have resized the
     * table, and every removal moves entries.
     */
    private void undo(Batch batch, int last) {
        for (int i = last; i >= 0; i--) {
            final long what = batch.undo[i * 3 + 2];
            if (what == UNDO_NOTHING) {
                continue;
            }
            final int keyAt = i * keyLongs;
            if (what == UNDO_CREATED) {
                balances.remove(batch.keys, keyAt);
                epoch++;
            } else {
                final int pos = balances.find(batch.keys, keyAt);
                balances.setValue(pos, AVAILABLE, batch.undo[i * 3]);
                balances.setValue(pos, RESERVED, batch.undo[i * 3 + 1]);
            }
        }
    }

    /**
     * Postings to several balances, applied together by {@link BalanceTable#apply}: each a key and the deltas of its
     * available and reserved amounts - a deposit is (amount, 0), a withdrawal (-amount, 0), a reservation (-amount,
     * amount), a release (amount, -amount), a settlement (0, -amount). Reusable: {@link #clear} it after the apply.
     * No allocation after it is made.
     */
    public static final class Batch {

        private final int keyLongs;
        private final int capacity;
        private final long[] keys;
        private final long[] deltas;
        /** per posting: the amounts before it, and what it did (changed, created, nothing) */
        private final long[] undo;
        private int size;
        private int failed = -1;

        private Batch(int keyLongs, int capacity) {
            if (capacity < 1) {
                throw new IllegalArgumentException("a batch of at least 1 posting: " + capacity);
            }
            this.keyLongs = keyLongs;
            this.capacity = capacity;
            this.keys = new long[Math.multiplyExact(capacity, keyLongs)];
            this.deltas = new long[Math.multiplyExact(capacity, 2)];
            this.undo = new long[Math.multiplyExact(capacity, 3)];
        }

        private int next(int length) {
            if (length != keyLongs) {
                throw new IllegalArgumentException("a key of " + length + " longs, the table has keys of "
                        + keyLongs);
            }
            if (size == capacity) {
                throw new IllegalStateException("the batch is full: " + capacity + " postings");
            }
            return size++;
        }

        private Batch deltas(int i, long availableDelta, long reservedDelta) {
            deltas[i * 2] = availableDelta;
            deltas[i * 2 + 1] = reservedDelta;
            return this;
        }

        public Batch add(long k1, long availableDelta, long reservedDelta) {
            final int i = next(1);
            keys[i] = k1;
            return deltas(i, availableDelta, reservedDelta);
        }

        public Batch add(long k1, long k2, long availableDelta, long reservedDelta) {
            final int i = next(2);
            keys[i * 2] = k1;
            keys[i * 2 + 1] = k2;
            return deltas(i, availableDelta, reservedDelta);
        }

        public Batch add(long k1, long k2, long k3, long availableDelta, long reservedDelta) {
            final int i = next(3);
            keys[i * 3] = k1;
            keys[i * 3 + 1] = k2;
            keys[i * 3 + 2] = k3;
            return deltas(i, availableDelta, reservedDelta);
        }

        public Batch add(long k1, long k2, long k3, long k4, long availableDelta, long reservedDelta) {
            final int i = next(4);
            keys[i * 4] = k1;
            keys[i * 4 + 1] = k2;
            keys[i * 4 + 2] = k3;
            keys[i * 4 + 3] = k4;
            return deltas(i, availableDelta, reservedDelta);
        }

        /**
         * The key is {@code key[offset .. offset + keyLongs)}.
         */
        public Batch add(long[] key, int offset, long availableDelta, long reservedDelta) {
            Objects.checkFromIndexSize(offset, keyLongs, key.length);
            final int i = next(keyLongs);
            System.arraycopy(key, offset, keys, i * keyLongs, keyLongs);
            return deltas(i, availableDelta, reservedDelta);
        }

        public int size() {
            return size;
        }

        /**
         * @return the index of the posting the last {@link BalanceTable#apply} refused, -1 if it applied them all
         */
        public int failedPosting() {
            return failed;
        }

        public void clear() {
            size = 0;
            failed = -1;
        }
    }

    /*
     * The whole table.
     */

    /**
     * Visits the references of all balances. The consumer may read them and post to them; it must not create or
     * remove balances.
     */
    public void forEach(LongConsumer refConsumer) {
        final LongConsumer outer = forEachConsumer;
        forEachConsumer = refConsumer;
        try {
            balances.forEach(forEachSlot);
        } finally {
            forEachConsumer = outer;
        }
    }

    /**
     * Grows the table, if needed, to take expectedSize balances in total without a resize - for the set-up. A growth
     * makes every reference stale.
     *
     * @throws IllegalArgumentException if expectedSize is negative or needs more than the maximum capacity
     */
    public void ensureCapacity(int expectedSize) {
        final int capacity = balances.capacity();
        balances.ensureCapacity(expectedSize);
        if (balances.capacity() != capacity) {
            epoch++;
        }
    }

    /**
     * Removes all balances, whatever they hold; every reference is stale afterwards.
     */
    public void clear() {
        balances.clear();
        epoch++;
    }

    public int size() {
        return balances.size();
    }

    public int keyLongs() {
        return keyLongs;
    }

    @Override
    public String toString() {
        return "BalanceTable{size=" + size() + ", keyLongs=" + keyLongs + '}';
    }

    /**
     * Verifies that no amount is below 0. For tests - walks the whole table.
     */
    void integrityCheck() {
        forEach(ref -> {
            if (available(ref) < 0 || reserved(ref) < 0) {
                throw new IllegalStateException("integrityCheck: a balance below 0: available " + available(ref)
                        + ", reserved " + reserved(ref));
            }
        });
    }
}
