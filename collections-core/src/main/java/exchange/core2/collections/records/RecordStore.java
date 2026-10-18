package exchange.core2.collections.records;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * Records of a fixed number of {@code long} fields, addressed by a handle - no keys: {@link #allocate()} hands out a
 * handle, the fields are read and written through it, {@link #free} ends the record.
 * <ul>
 *     <li>A record never moves: the handle stays valid until the record is freed, whatever is allocated or freed
 *     meanwhile. It is a {@code long}, so it fits a value of a {@code long} hashtable or a field of another record.</li>
 *     <li>A freed record can not be reached through its handle any more: the slot is reused under a new generation,
 *     and every access checks the generation - a stale handle throws {@link IllegalArgumentException} instead of
 *     reaching the record allocated after it. So does {@link #NO_RECORD} ({@code 0}), which is never a handle. A
 *     value this store never handed out may also throw {@link IndexOutOfBoundsException}, where its slot is beyond
 *     the page directory.</li>
 *     <li>Pages of {@link Builder#pageSize} records, each a {@code long[]}: the store grows by a page, nothing is
 *     copied but the page directory (a reference per page). A new page is an allocation on the calling thread (24 KB
 *     to zero for 2 fields and 1024 records) - pre-size the store ({@link Builder#expectedSize},
 *     {@link #ensureCapacity}) where that matters, or make it fixed ({@link Builder#fixedCapacity}).</li>
 *     <li>Deterministic: the same sequence of allocations and frees hands out the same handles. A freed slot is the
 *     next one allocated (last freed, first reused), then the slots never used, in their order.</li>
 *     <li>Not thread-safe.</li>
 * </ul>
 * Layout: a record is a header {@code long} and the fields, side by side in its page. The header of a live record is
 * its handle, so the check of a handle is one compare in the cache line of the fields; the header of a free record
 * holds its generation and the next free slot. Slot 0 is never allocated: its header stops {@link #NO_RECORD}.
 * <p>
 * Handle: the generation in the high 32 bits, the index of the slot in the low 32. A slot is free at an even
 * generation and live at an odd one: the allocation and the free both increment it, so a slot is reused up to 2^31 - 1
 * times; then it is retired, never allocated again.
 */
public final class RecordStore {

    /**
     * Not a handle: what a {@code long} hashtable returns for a missing key unless told otherwise, so it can map keys
     * to handles as it is.
     */
    public static final long NO_RECORD = 0L;

    /** records per page unless set in the builder */
    public static final int DEFAULT_PAGE_SIZE = 1024;

    /** the largest page, in records */
    public static final int MAX_PAGE_SIZE = 1 << 20;

    /** the largest number of records */
    public static final int MAX_CAPACITY = 1 << 30;

    private static final int NO_SLOT = -1;
    private static final long INDEX_MASK = 0xFFFF_FFFFL;
    private static final long GENERATION_ONE = 1L << 32;
    /** a slot freed at the last odd generation gets this one: even (free), but in no free list */
    private static final long RETIRED_GENERATION = 0xFFFF_FFFEL;
    /** the header of a retired slot - its index part points nowhere, so no handle equals it */
    private static final long RETIRED_HEADER = RETIRED_GENERATION << 32 | INDEX_MASK;
    /** the header of slot 0: not 0, which a slot never used has and NO_RECORD equals */
    private static final long RESERVED_HEADER = INDEX_MASK;

    private final int fields;
    private final int stride;
    private final int pageShift;
    private final int pageMask;
    /** slot 0 included */
    private final int maxSlots;
    /** stands in for the pages not allocated yet: no handle equals one of its headers */
    private final long[] emptyPage;

    private long[][] pages;
    /** pages allocated, from the start of the directory */
    private int allocatedPages;
    /** slots ever used, slot 0 included: those below are live, free or retired, those above never allocated */
    private int used = 1;
    private int freeHead = NO_SLOT;
    private int size;
    private int retired;

    private RecordStore(Builder builder) {
        if (builder.fields < 1) {
            throw new IllegalArgumentException("fields must be at least 1: " + builder.fields);
        }
        final int pageSize = builder.pageSize;
        if (pageSize < 1 || pageSize > MAX_PAGE_SIZE || Integer.bitCount(pageSize) != 1) {
            throw new IllegalArgumentException("page size must be a power of two from 1 to " + MAX_PAGE_SIZE + ": "
                    + pageSize);
        }
        if (pageSize * (builder.fields + 1L) > Integer.MAX_VALUE - 8) {
            throw new IllegalArgumentException("a page of " + pageSize + " records of " + builder.fields
                    + " fields does not fit a long[]");
        }
        if (builder.maxCapacity < 1 || builder.maxCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("max capacity must be from 1 to " + MAX_CAPACITY + ": "
                    + builder.maxCapacity);
        }
        if (builder.expectedSize < 0 || builder.expectedSize > builder.maxCapacity) {
            throw new IllegalArgumentException("expected size must be from 0 to the max capacity "
                    + builder.maxCapacity + ": " + builder.expectedSize);
        }

        this.fields = builder.fields;
        this.stride = fields + 1;
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = pageSize - 1;
        this.emptyPage = new long[pageSize * stride];
        emptyPage[0] = RESERVED_HEADER;

        // slot 0 is in the first page
        final int pagesNeeded = builder.expectedSize == 0 ? 0 : pagesFor(builder.expectedSize + 1L);
        this.maxSlots = builder.fixedCapacity
                ? Math.max(Math.min(pagesNeeded << pageShift, MAX_CAPACITY + 1), 1)
                : builder.maxCapacity + 1;
        // never empty: NO_RECORD reaches the header of slot 0 in the empty page, not the end of the directory
        this.pages = new long[Math.max(pagesNeeded, 1)][];
        Arrays.fill(pages, emptyPage);
        for (int p = 0; p < pagesNeeded; p++) {
            addPage();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link RecordStore}: {@link #fields} is required, the rest optional.
     */
    public static final class Builder {

        private int fields;
        private int expectedSize;
        private int pageSize = DEFAULT_PAGE_SIZE;
        private int maxCapacity = MAX_CAPACITY;
        private boolean fixedCapacity;

        private Builder() {
        }

        /**
         * Number of {@code long} fields of a record, at least 1. A record takes one more {@code long}, its header.
         */
        public Builder fields(int fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Records that fit without a new page: their pages are allocated when the store is built (default 0).
         */
        public Builder expectedSize(int expectedSize) {
            this.expectedSize = expectedSize;
            return this;
        }

        /**
         * Records per page: a power of two up to {@value RecordStore#MAX_PAGE_SIZE}, default
         * {@value RecordStore#DEFAULT_PAGE_SIZE}. Larger pages - fewer and longer pauses to allocate them.
         */
        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Records the store holds at most, default and at most {@value RecordStore#MAX_CAPACITY} (fewer the retired
         * slots, see {@link RecordStore}): beyond them {@link RecordStore#allocate()} throws
         * {@link IllegalStateException}.
         */
        public Builder maxCapacity(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        /**
         * No allocation after the build: the pages for {@link #expectedSize} are all the store has, so it holds that
         * many records, rounded up to whole pages - beyond them {@link RecordStore#allocate()} throws
         * {@link IllegalStateException}.
         */
        public Builder fixedCapacity() {
            this.fixedCapacity = true;
            return this;
        }

        public RecordStore build() {
            return new RecordStore(this);
        }
    }

    /**
     * A new record, all its fields 0.
     *
     * @return its handle
     * @throws IllegalStateException if the store holds its maximum number of records - nothing changes then
     */
    public long allocate() {
        final int index;
        if (freeHead != NO_SLOT) {
            index = freeHead;
            final long[] page = pages[index >>> pageShift];
            final int offset = (index & pageMask) * stride;
            final long header = page[offset];
            freeHead = (int) header;
            // the free generation (even) + 1; a free record has its fields zeroed already
            final long handle = (header & ~INDEX_MASK) + GENERATION_ONE | index;
            page[offset] = handle;
            size++;
            return handle;
        }
        if (used == maxSlots) {
            throw new IllegalStateException("store is full: " + size + " records, " + retired
                    + " retired slots, max capacity " + maxCapacity());
        }
        index = used;
        // two pages at the first allocation if a page is one record: slot 0 takes the first
        while ((index >>> pageShift) >= allocatedPages) {
            addPage();
        }
        used++;
        size++;
        // a slot never used before: generation 0, so the handle is of generation 1
        final long handle = GENERATION_ONE | index;
        pages[index >>> pageShift][(index & pageMask) * stride] = handle;
        return handle;
    }

    /**
     * Ends the record: its handle is stale from now on, its slot goes to the next allocation.
     *
     * @throws IllegalArgumentException if the handle is stale
     */
    public void free(long handle) {
        final long[] page = pages[(int) handle >>> pageShift];
        final int offset = ((int) handle & pageMask) * stride;
        if (page[offset] != handle) {
            throw staleHandle(handle);
        }
        Arrays.fill(page, offset + 1, offset + stride, 0L);
        size--;
        final long generation = (handle >>> 32) + 1;
        if (generation >= RETIRED_GENERATION) {
            page[offset] = RETIRED_HEADER;
            retired++;
            return;
        }
        page[offset] = generation << 32 | (freeHead & INDEX_MASK);
        freeHead = (int) handle;
    }

    /**
     * @return true if the handle is of a live record of this store; false for a stale handle and for
     * {@link #NO_RECORD}
     */
    public boolean isValid(long handle) {
        final int index = (int) handle;
        if (index < 0 || index >= used) {
            return false;
        }
        return pages[index >>> pageShift][(index & pageMask) * stride] == handle;
    }

    /**
     * @return the value of the field
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public long get(long handle, int field) {
        final long[] page = pages[(int) handle >>> pageShift];
        final int offset = ((int) handle & pageMask) * stride;
        if (page[offset] != handle) {
            throw staleHandle(handle);
        }
        return page[offset + 1 + Objects.checkIndex(field, fields)];
    }

    /**
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public void set(long handle, int field, long value) {
        final long[] page = pages[(int) handle >>> pageShift];
        final int offset = ((int) handle & pageMask) * stride;
        if (page[offset] != handle) {
            throw staleHandle(handle);
        }
        page[offset + 1 + Objects.checkIndex(field, fields)] = value;
    }

    /**
     * Adds delta to the field. Overflow wraps around, as with {@code +} - see {@link #addAndGetExact}.
     *
     * @return the new value
     * @throws IllegalArgumentException  if the handle is stale
     * @throws IndexOutOfBoundsException if the field is not from 0 to {@link #fields()} - 1
     */
    public long addAndGet(long handle, int field, long delta) {
        final long[] page = pages[(int) handle >>> pageShift];
        final int offset = ((int) handle & pageMask) * stride;
        if (page[offset] != handle) {
            throw staleHandle(handle);
        }
        return page[offset + 1 + Objects.checkIndex(field, fields)] += delta;
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
        final long[] page = pages[(int) handle >>> pageShift];
        final int offset = ((int) handle & pageMask) * stride;
        if (page[offset] != handle) {
            throw staleHandle(handle);
        }
        final int position = offset + 1 + Objects.checkIndex(field, fields);
        return page[position] = Math.addExact(page[position], delta);
    }

    /**
     * Visits the live records in the order of their slots - the same from run to run. The consumer may free records,
     * the one visited included; a record allocated meanwhile may or may not be visited.
     */
    public void forEach(LongConsumer handleConsumer) {
        // page by page: the slots used when the page is reached; slot 0 is in the first one
        for (int p = 0, first = 1; ((long) p << pageShift) < used; p++, first = 0) {
            final long[] page = pages[p];
            final int slots = Math.min(pageMask + 1, used - (p << pageShift));
            for (int offset = first * stride, end = slots * stride; offset < end; offset += stride) {
                final long header = page[offset];
                if ((header & GENERATION_ONE) != 0) {
                    handleConsumer.accept(header);
                }
            }
        }
    }

    /**
     * Frees all records, keeping the pages: every handle is stale afterwards. The slots are allocated again in the
     * order of a new store - under new generations.
     */
    public void clear() {
        freeHead = NO_SLOT;
        retired = 0;
        // from the last slot down, so that the free list starts at the first one
        for (int index = used - 1; index >= 1; index--) {
            final long[] page = pages[index >>> pageShift];
            final int offset = (index & pageMask) * stride;
            long generation = page[offset] >>> 32;
            if ((generation & 1) != 0) {
                Arrays.fill(page, offset + 1, offset + stride, 0L);
                generation++;
            }
            if (generation >= RETIRED_GENERATION) {
                page[offset] = RETIRED_HEADER;
                retired++;
                continue;
            }
            page[offset] = generation << 32 | (freeHead & INDEX_MASK);
            freeHead = index;
        }
        size = 0;
    }

    /**
     * Allocates the pages, if needed, for expectedSize records in total to fit without a new page - for the set-up,
     * not for the latency-critical path.
     *
     * @throws IllegalArgumentException if expectedSize is negative or above the maximum capacity
     */
    public void ensureCapacity(int expectedSize) {
        if (expectedSize < 0 || expectedSize > maxCapacity()) {
            throw new IllegalArgumentException("expected size must be from 0 to the max capacity " + maxCapacity()
                    + ": " + expectedSize);
        }
        if (expectedSize == 0) {
            return;
        }
        // slot 0 and the retired slots take no records
        final int pagesNeeded = Math.min(pagesFor(expectedSize + 1L + retired), pagesFor(maxSlots));
        while (allocatedPages < pagesNeeded) {
            addPage();
        }
    }

    private int pagesFor(long slots) {
        return (int) ((slots + pageMask) >>> pageShift);
    }

    private void addPage() {
        if (allocatedPages == pages.length) {
            final int length = Math.min(Math.max(pages.length * 2, 1), pagesFor(maxSlots));
            final long[][] directory = Arrays.copyOf(pages, length);
            Arrays.fill(directory, pages.length, length, emptyPage);
            pages = directory;
        }
        final long[] page = new long[emptyPage.length];
        if (allocatedPages == 0) {
            page[0] = RESERVED_HEADER;
        }
        pages[allocatedPages++] = page;
    }

    /**
     * @return the index of the record's slot: from 1 to {@link #capacity()}, the same for the life of the record -
     * for arrays kept beside the store (a field that is an object, say)
     */
    public static int index(long handle) {
        return (int) handle;
    }

    /**
     * @return live records
     */
    public int size() {
        return size;
    }

    /**
     * @return records the allocated pages hold (fewer the retired slots): up to that many fit without another page
     */
    public int capacity() {
        return allocatedPages == 0 ? 0 : (allocatedPages << pageShift) - 1;
    }

    /**
     * @return the largest number of records: the maximum capacity of the builder, or for a fixed capacity the
     * expected size rounded up to whole pages
     */
    public int maxCapacity() {
        return maxSlots - 1;
    }

    /**
     * @return number of {@code long} fields of a record
     */
    public int fields() {
        return fields;
    }

    @Override
    public String toString() {
        return "RecordStore{size=" + size + ", fields=" + fields + ", capacity=" + capacity() + '}';
    }

    private IllegalArgumentException staleHandle(long handle) {
        return new IllegalArgumentException("stale handle 0x" + Long.toHexString(handle) + " (slot " + (int) handle
                + ", generation " + (handle >>> 32) + "): the record was freed, or is not of this store");
    }

    /**
     * Verifies the invariants: live headers are the handles of their slots, free and retired records have zeroed
     * fields, the free list holds every free slot once, slot 0 and the slots never used are untouched, the counters
     * match. For tests - scans every page.
     */
    void integrityCheck() {
        final long slots = (long) allocatedPages << pageShift;
        if (used > Math.max(slots, 1) || used > maxSlots || allocatedPages > pages.length) {
            throw new IllegalStateException("integrityCheck: used=" + used + " slots=" + slots
                    + " directory=" + pages.length + " maxSlots=" + maxSlots);
        }
        for (int p = allocatedPages; p < pages.length; p++) {
            if (pages[p] != emptyPage) {
                throw new IllegalStateException("integrityCheck: page " + p + " beyond the allocated ones");
            }
        }
        for (int i = 1; i < emptyPage.length; i++) {
            if (emptyPage[i] != 0L) {
                throw new IllegalStateException("integrityCheck: the empty page was written at " + i);
            }
        }
        if (emptyPage[0] != RESERVED_HEADER || (allocatedPages > 0 && pages[0][0] != RESERVED_HEADER)) {
            throw new IllegalStateException("integrityCheck: the header of slot 0 was overwritten");
        }
        final boolean[] inFreeList = new boolean[used];
        int free = 0;
        for (int index = freeHead; index != NO_SLOT; ) {
            if (index < 1 || index >= used || inFreeList[index]) {
                throw new IllegalStateException("integrityCheck: free list reaches slot " + index
                        + (index >= 1 && index < used ? " twice" : ", used=" + used));
            }
            inFreeList[index] = true;
            free++;
            final long header = pages[index >>> pageShift][(index & pageMask) * stride];
            final long generation = header >>> 32;
            if ((generation & 1) != 0 || generation >= RETIRED_GENERATION) {
                throw new IllegalStateException("integrityCheck: slot " + index + " in the free list at generation "
                        + generation);
            }
            index = (int) header;
        }
        int live = 0;
        int retiredSlots = 0;
        for (int index = 0; index < slots; index++) {
            final long[] page = pages[index >>> pageShift];
            final int offset = (index & pageMask) * stride;
            final long header = page[offset];
            final long generation = header >>> 32;
            final boolean isLive = index > 0 && (generation & 1) != 0;
            if (index == 0 || index >= used) {
                if (header != (index == 0 ? RESERVED_HEADER : 0L)) {
                    throw new IllegalStateException("integrityCheck: slot " + index + " never used has header 0x"
                            + Long.toHexString(header));
                }
            } else if (isLive) {
                if ((int) header != index) {
                    throw new IllegalStateException("integrityCheck: live slot " + index + " has the handle of slot "
                            + (int) header);
                }
                live++;
            } else if (header == RETIRED_HEADER) {
                retiredSlots++;
            } else if (!inFreeList[index]) {
                throw new IllegalStateException("integrityCheck: free slot " + index + " is not in the free list");
            }
            if (!isLive) {
                for (int f = 1; f < stride; f++) {
                    if (page[offset + f] != 0L) {
                        throw new IllegalStateException("integrityCheck: free slot " + index + " field " + (f - 1)
                                + " holds " + page[offset + f]);
                    }
                }
            }
        }
        if (live != size || retiredSlots != retired || 1 + live + free + retiredSlots != used) {
            throw new IllegalStateException("integrityCheck: size=" + size + " live=" + live + " free=" + free
                    + " retired=" + retired + "/" + retiredSlots + " used=" + used);
        }
    }
}
