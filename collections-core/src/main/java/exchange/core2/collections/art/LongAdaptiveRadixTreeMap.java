/*
 * Copyright 2019-2020 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.collections.art;

import exchange.core2.collections.objpool.ObjectsPool;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A sorted map of {@code long} keys to object values: an Adaptive Radix Tree (V. Leis, A. Kemper, T. Neumann,
 * "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases", https://db.in.tum.de/~leis/papers/ART.pdf).
 * <p>
 * The keys are ordered as signed longs, as in {@code TreeMap<Long, V>}. Values must not be null: a missing key reads
 * as null. A lookup, put or removal walks at most the 8 bytes of the key and never rebalances or rehashes; the nodes
 * come from an {@link ObjectsPool} and go back to it, so a map of a steady size does not allocate. Ordered walks: from
 * the ends ({@link #forEach(LongObjConsumer, int)}), from a key ({@link #forEach(long, LongObjConsumer, int)}), step by
 * step ({@link #cursor()}), as streams ({@link #keysStream()}, {@link #valuesStream()}).
 * <p>
 * Not thread-safe.
 */
public final class LongAdaptiveRadixTreeMap<V> {

    private static final int INITIAL_LEVEL = 56;

    // the streams: ascending, of the size the map has when a stream is consumed
    private static final int KEYS_STREAM = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED
            | Spliterator.NONNULL | Spliterator.SIZED | Spliterator.SUBSIZED;
    private static final int VALUES_STREAM = Spliterator.ORDERED | Spliterator.NONNULL
            | Spliterator.SIZED | Spliterator.SUBSIZED;

    private ArtNode<V> root = null;

    private final ObjectsPool objectsPool;

    private int size = 0;

    /** counts the changes of the key set - a cursor fails fast after a change it did not make */
    int modCount;

    /**
     * Out-parameter of the node put/remove: the value a put replaced or a remove took out, null if the key was
     * absent. The map resets it as soon as it has read it, so it never keeps a value alive.
     */
    V previousValue;

    /**
     * A put that also looks for a neighbour (putAndGetLowerValue / putAndGetHigherValue) asks the node of the last
     * level where the key ends up to remember itself here: the neighbour is most often in the same node, next to the
     * key - in an order book the levels near the market share their 256-tick blocks.
     * <p>
     * Every node on the way down offering its closest child on that side was measured too: as slow as the second
     * descent it saves, which runs over warm cache lines (a new level in a 20k-level book +7-9%).
     */
    private boolean rememberPut;
    private ArtNode<V> lastPutNode;

    /** the cursor of removeRange, created at its first call */
    private Cursor<V> rangeCursor;

    /**
     * @param objectsPool where the nodes come from and go back to - one pool can serve several maps of the same thread
     */
    public LongAdaptiveRadixTreeMap(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    /** A map with a pool of its own, of the default capacity - see {@link Builder#poolCapacity(int)}. */
    public LongAdaptiveRadixTreeMap() {
        this(ObjectsPool.forArt(Builder.DEFAULT_POOL_CAPACITY));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parameters of a {@link LongAdaptiveRadixTreeMap}, all optional.
     */
    public static final class Builder {

        static final int DEFAULT_POOL_CAPACITY = 256;

        private int poolCapacity = -1;
        private ObjectsPool sharedPool;

        private Builder() {
        }

        /**
         * Free nodes the map keeps in a pool of its own for its next puts (default
         * {@value LongAdaptiveRadixTreeMap.Builder#DEFAULT_POOL_CAPACITY}): up to that many leaves and 4-key nodes, fewer
         * of the bigger ones - see {@link ObjectsPool#forArt(int)}. A map that shrinks and grows back by more than that
         * allocates the rest; 0 keeps none.
         */
        public Builder poolCapacity(int nodes) {
            if (nodes < 0) {
                throw new IllegalArgumentException("pool capacity must not be negative: " + nodes);
            }
            this.poolCapacity = nodes;
            return this;
        }

        /**
         * A pool shared with other maps of the same thread instead of a pool of the map's own: the nodes one map frees,
         * another takes. The pool has its sizes - not together with {@link #poolCapacity(int)}.
         */
        public Builder sharedPool(ObjectsPool pool) {
            this.sharedPool = Objects.requireNonNull(pool, "pool");
            return this;
        }

        /**
         * @throws IllegalStateException if both a pool capacity and a shared pool are set
         */
        public <V> LongAdaptiveRadixTreeMap<V> build() {
            if (sharedPool == null) {
                return new LongAdaptiveRadixTreeMap<>(ObjectsPool.forArt(poolCapacity < 0 ? DEFAULT_POOL_CAPACITY : poolCapacity));
            }
            if (poolCapacity >= 0) {
                throw new IllegalStateException("a pool capacity and a shared pool: the shared pool has sizes of its own");
            }
            return new LongAdaptiveRadixTreeMap<>(sharedPool);
        }
    }

    /**
     * A radix tree walks keys byte by byte, so it inevitably orders them as UNSIGNED - which puts
     * every negative key after every positive one. Flipping the sign bit maps the signed order onto
     * the unsigned one (it is an order-isomorphism), so the nodes can keep working with plain
     * unsigned bytes while the public API behaves like {@code TreeMap<Long, V>}.
     * <p>
     * The conversion is its own inverse, hence one method used in both directions - also by the nodes,
     * which hand out map keys in forEach and entries.
     */
    static long flipSignBit(final long key) {
        return key ^ Long.MIN_VALUE;
    }

    /**
     * @return the value of the key, or null if the key is absent
     */
    public V get(final long key) {
        // A recursion through the nodes. A loop over the levels (the node class told apart by instanceof, the child
        // lookups inlined) was measured on rhel-lat: misses on 1M random keys -12%, but hits in a 20k-level order book
        // +14-19% (patch kept outside the repository) - the recursion stays.
        return root != null
                ? root.getValue(flipSignBit(key), INITIAL_LEVEL)
                : null;
    }

    /**
     * @return the value of the key, or defaultValue if the key is absent
     */
    public V getOrDefault(final long key, final V defaultValue) {
        final V value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * @return true if the key is present - the same as {@code get(key) != null}, a value is never null
     */
    public boolean containsKey(final long key) {
        return get(key) != null;
    }

    /**
     * @return the previous value, or null if the key was absent
     * @throws NullPointerException if the value is null - a missing key reads as null, so null can not be stored
     */
    public V put(final long key, final V value) {
        Objects.requireNonNull(value, "value");
        final long internalKey = flipSignBit(key);
        if (root == null) {
            root = ArtLeaf.create(objectsPool, internalKey, value);
            size = 1;
            modCount++;
            return null;
        }

        final ArtNode<V> upSizedNode = root.put(internalKey, INITIAL_LEVEL, value, this);
        if (upSizedNode != null) {
            root = upSizedNode;
        }
        final V previous = takePreviousValue();
        if (previous == null) {
            size++;
            modCount++;
        }
        return previous;
    }

    /**
     * Puts the value only if the key is absent. Two descents when it is absent (a get and a put).
     *
     * @return the existing value, or null if the key was absent and the value has been put
     */
    public V putIfAbsent(final long key, final V value) {
        Objects.requireNonNull(value, "value");
        final V existing = get(key);
        if (existing == null) {
            put(key, value);
        }
        return existing;
    }

    /**
     * Returns the value of the key, creating it if absent. The function is called only for an absent key; if it
     * returns null, nothing is put. Two descents when the key is absent (a get and a put).
     *
     * @return the existing or the created value, null if the function returned null
     */
    public V computeIfAbsent(final long key, final LongFunction<? extends V> mappingFunction) {
        final V existing = get(key);
        if (existing != null) {
            return existing;
        }
        final V created = mappingFunction.apply(key);
        if (created != null) {
            put(key, created);
        }
        return created;
    }

    /**
     * Puts the value and returns the value of the closest key below - how an order book links a new ask price
     * level to the better level next to it. One descent when the neighbour is in the same 256-key block (the node of
     * the last level remembers the put), otherwise one more.
     *
     * @return the value of the closest key strictly below the key, or null if there is none
     */
    public V putAndGetLowerValue(final long key, final V value) {
        return putAndGetNeighbour(key, value, true);
    }

    /**
     * Puts the value and returns the value of the closest key above - for a new bid price level.
     * One descent or two, as {@link #putAndGetLowerValue}.
     *
     * @return the value of the closest key strictly above the key, or null if there is none
     */
    public V putAndGetHigherValue(final long key, final V value) {
        return putAndGetNeighbour(key, value, false);
    }

    private V putAndGetNeighbour(final long key, final V value, final boolean lower) {
        Objects.requireNonNull(value, "value");
        rememberPut = true;
        put(key, value);
        rememberPut = false;
        final ArtNode<V> node = lastPutNode;
        if (node != null) {
            lastPutNode = null;
            final V next = node.valueNextTo((int) (key & 0xFF), lower);
            if (next != null) {
                return next;
            }
        }
        // a key alone in its 256-key block, or at its edge: the neighbour is elsewhere, if anywhere
        return lower ? lowerValue(key) : higherValue(key);
    }

    /** a node of the last level that has just put the key - see lastPutNode */
    void rememberPut(final ArtNode<V> node) {
        if (rememberPut) {
            lastPutNode = node;
        }
    }

    /**
     * @return the removed value, or null if the key was absent
     */
    public V remove(final long key) {
        if (root == null) {
            return null;
        }
        // the new root: shrunk, collapsed, or null when the last key is gone
        root = root.remove(flipSignBit(key), INITIAL_LEVEL, this);
        final V removed = takePreviousValue();
        if (removed != null) {
            size--;
            modCount++;
        }
        return removed;
    }

    private V takePreviousValue() {
        final V value = previousValue;
        previousValue = null;
        return value;
    }

    /**
     * Removes all keys and returns the nodes into the pool (as many as it takes) - O(number of nodes), not O(1).
     */
    public void clear() {
        if (root != null) {
            root.recycle();
            root = null;
        }
        size = 0;
        modCount++;
    }

    /**
     * @return a new cursor over this map - keep it and reuse it: seeking and moving allocate nothing
     */
    public Cursor<V> cursor() {
        return new Cursor<>(this);
    }

    /**
     * Removes the keys from keyFromInclusive up to keyToExclusive - e.g. the stop orders up to a price. One removal per
     * key, the cursor moves on from it: O(number of keys removed), each step a descent.
     *
     * @return the number of keys removed; 0 if keyToExclusive is not above keyFromInclusive
     */
    public int removeRange(final long keyFromInclusive, final long keyToExclusive) {
        if (root == null || keyFromInclusive >= keyToExclusive) {
            return 0;
        }
        // no user code runs in between, so the map can keep a cursor for it - nothing allocated per call
        if (rangeCursor == null) {
            rangeCursor = cursor();
        }
        final Cursor<V> c = rangeCursor;
        int removed = 0;
        for (boolean on = c.seekCeiling(keyFromInclusive); on && c.key() < keyToExclusive; on = c.isValid()) {
            c.remove();
            removed++;
        }
        return removed;
    }

    /**
     * @return the value of the closest key strictly above the key, or null if there is none
     */
    public V higherValue(final long key) {
        // Long.MAX_VALUE is the top of the signed range - nothing can be above it.
        if (root != null && key != Long.MAX_VALUE) {
            return root.getCeilingValue(flipSignBit(key) + 1, INITIAL_LEVEL);
        } else {
            return null;
        }
    }

    /**
     * @return the value of the closest key strictly below the key, or null if there is none
     */
    public V lowerValue(final long key) {
        // Long.MIN_VALUE is the bottom of the signed range. The guard used to be "key != 0", which
        // was the bottom of the UNSIGNED range - correct for the old, unsigned-ordered map only.
        if (root != null && key != Long.MIN_VALUE) {
            return root.getFloorValue(flipSignBit(key) - 1, INITIAL_LEVEL);
        } else {
            return null;
        }
    }

    /**
     * @return the value of the key, or else of the closest key above it; null if there is none
     */
    public V ceilingValue(final long key) {
        return root != null
                ? root.getCeilingValue(flipSignBit(key), INITIAL_LEVEL)
                : null;
    }

    /**
     * @return the value of the key, or else of the closest key below it; null if there is none
     */
    public V floorValue(final long key) {
        return root != null
                ? root.getFloorValue(flipSignBit(key), INITIAL_LEVEL)
                : null;
    }

    /**
     * @return the value of the lowest key, or null if the map is empty
     */
    public V firstValue() {
        return ceilingValue(Long.MIN_VALUE);
    }

    /**
     * @return the value of the highest key, or null if the map is empty
     */
    public V lastValue() {
        return floorValue(Long.MAX_VALUE);
    }

    /**
     * @return the lowest key
     * @throws NoSuchElementException if the map is empty
     */
    public long firstKey() {
        if (root == null) {
            throw new NoSuchElementException();
        }
        return flipSignBit(root.firstKey());
    }

    /**
     * @return the highest key
     * @throws NoSuchElementException if the map is empty
     */
    public long lastKey() {
        if (root == null) {
            throw new NoSuchElementException();
        }
        return flipSignBit(root.lastKey());
    }

    /**
     * Removes the lowest key. Two descents (the first key, then its removal).
     *
     * @return the value of the removed key, or null if the map is empty
     */
    public V pollFirstValue() {
        return root != null ? remove(firstKey()) : null;
    }

    /**
     * Removes the highest key. Two descents (the last key, then its removal).
     *
     * @return the value of the removed key, or null if the map is empty
     */
    public V pollLastValue() {
        return root != null ? remove(lastKey()) : null;
    }

    /**
     * Visits all keys, ascending - allocation-free, as {@link #forEach(LongObjConsumer, int)}.
     */
    public void forEach(final LongObjConsumer<V> consumer) {
        forEach(consumer, Integer.MAX_VALUE);
    }

    /**
     * Visits the lowest keys, ascending - e.g. the best asks for L2 market data.
     *
     * @return the number of keys visited, at most limit
     */
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        return root != null ? root.forEach(consumer, limit) : 0;
    }

    /**
     * Visits the highest keys, descending.
     *
     * @return the number of keys visited, at most limit
     */
    public int forEachDesc(LongObjConsumer<V> consumer, int limit) {
        return root != null ? root.forEachDesc(consumer, limit) : 0;
    }

    /**
     * Visits the keys from fromInclusive up, ascending - an L2 snapshot from a price. One descent to the key, then the
     * subtrees above it are walked as {@link #forEach(LongObjConsumer, int)} does.
     *
     * @return the number of keys visited, at most limit
     */
    public int forEach(final long fromInclusive, final LongObjConsumer<V> consumer, final int limit) {
        return root != null && limit > 0
                ? root.forEachFrom(flipSignBit(fromInclusive), INITIAL_LEVEL, consumer, limit)
                : 0;
    }

    /**
     * Visits the keys from fromInclusive down, descending.
     *
     * @return the number of keys visited, at most limit
     */
    public int forEachDesc(final long fromInclusive, final LongObjConsumer<V> consumer, final int limit) {
        return root != null && limit > 0
                ? root.forEachDescFrom(flipSignBit(fromInclusive), INITIAL_LEVEL, consumer, limit)
                : 0;
    }

    /**
     * @return number of keys - a counter, O(1)
     */
    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * The keys, ascending. The stream reads the map lazily, through a cursor of its own made when the stream is
     * consumed: a change of the key set during the walk makes it throw ConcurrentModificationException.
     */
    public LongStream keysStream() {
        return StreamSupport.longStream(() -> {
            final Cursor<V> c = cursor();
            final PrimitiveIterator.OfLong keys = new PrimitiveIterator.OfLong() {
                private boolean on = c.seekFirst();

                @Override
                public boolean hasNext() {
                    return on;
                }

                @Override
                public long nextLong() {
                    if (!on) {
                        throw new NoSuchElementException();
                    }
                    final long key = c.key();
                    on = c.next();
                    return key;
                }
            };
            return Spliterators.spliterator(keys, size, KEYS_STREAM);
        }, KEYS_STREAM, false);
    }

    /**
     * The values in the order of {@link #keysStream()}, with the same restriction.
     */
    public Stream<V> valuesStream() {
        return StreamSupport.stream(() -> {
            final Cursor<V> c = cursor();
            final Iterator<V> values = new Iterator<>() {
                private boolean on = c.seekFirst();

                @Override
                public boolean hasNext() {
                    return on;
                }

                @Override
                public V next() {
                    if (!on) {
                        throw new NoSuchElementException();
                    }
                    final V value = c.value();
                    on = c.next();
                    return value;
                }
            };
            return Spliterators.spliterator(values, size, VALUES_STREAM);
        }, VALUES_STREAM, false);
    }

    /**
     * A snapshot of all entries, ascending - allocates them all, for tests and diagnostics, not for hot paths.
     */
    public List<Map.Entry<Long, V>> entriesList() {
        return root != null ? root.entries() : Collections.emptyList();
    }

    /**
     * All entries as {@code {key=value, ...}}, ascending - the format of {@code TreeMap}; for tests and debugging, it
     * walks the whole map.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach((key, value) -> {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(key).append('=').append(value == this ? "(this Map)" : value);
        });
        return sb.append('}').toString();
    }

    /**
     * Diagnostics: checks the invariants of the tree (node sizes and order, occupancy bitmaps, the size counter) -
     * O(number of nodes).
     *
     * @throws IllegalStateException if one does not hold
     */
    public void validateInternalState() {
        if (previousValue != null || lastPutNode != null || rememberPut) {
            throw new IllegalStateException("previousValue / lastPutNode retained");
        }
        if (root != null) {
            root.validateInternalState(INITIAL_LEVEL);
            final int counted = root.size(Integer.MAX_VALUE);
            if (counted != size) {
                throw new IllegalStateException("size is " + size + ", but the tree holds " + counted);
            }
        } else if (size != 0) {
            throw new IllegalStateException("size is " + size + ", but the tree is empty");
        }
    }

    /**
     * Diagnostics: the nodes of the tree as text - the format may change from version to version.
     */
    public String printDiagram() {
        if (root != null) {
            return root.printDiagram("", INITIAL_LEVEL);
        } else {
            return "";
        }
    }


    static <V> ArtNode<V> branchIfRequired(final long key, final V value, final long nodeKey, final int nodeLevel, final ArtNode<V> caller) {

        final long keyDiff = key ^ nodeKey;

        // check if there is common part
        if ((keyDiff & (-1L << nodeLevel)) == 0) {
            return null;
        }

        // on which level
        final int newLevel = (63 - Long.numberOfLeadingZeros(keyDiff)) & 0xF8;
        if (newLevel == nodeLevel) {
            return null;
        }

        final ObjectsPool objectsPool = caller.getObjectsPool();
        final ArtLeaf<V> newSubNode = ArtLeaf.create(objectsPool, key, value);

        final ArtNode4<V> newNode = objectsPool.get(ObjectsPool.ART_NODE_4, ArtNode4::new);
        newNode.initTwoKeys(nodeKey, caller, key, newSubNode, newLevel);

        return newNode;
    }

//    static boolean keyNotMatches(long key, int level, long nodeKey, int nodeLevel) {
//        return (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0);
//    }
    // TODO remove based on leaf  (having reference) ?

    static String printDiagram(String prefix,
                               int level,
                               int nodeLevel,
                               long nodeKey,
                               short numChildren,
                               Function<Short, Short> subKeys,
                               Function<Short, Object> nodes) {

        final String baseKeyPrefix;
        final String baseKeyPrefix1;
        final int lvlDiff = level - nodeLevel;
//        log.debug("nodeKey={} level={} nodeLevel={} lvlDiff={}", String.format("%X", nodeKey), level, nodeLevel, lvlDiff);

        if (lvlDiff != 0) {
            int chars = lvlDiff >> 2;
//            baseKeyPrefix = String.format("[%0" + chars + "X]", nodeKey & ((1L << lvlDiff) - 1L) << nodeLevel);
            long mask = ((1L << lvlDiff) - 1L);
//            log.debug("       mask={}", String.format("%X", mask));
//            log.debug("       nodeKey >> level = {}", String.format("%X", nodeKey >> (nodeLevel + 8)));
//            log.debug("       nodeKey >> level  & mask= {}", String.format("%X", (nodeKey >> (nodeLevel + 8)) & mask));
            baseKeyPrefix = charRepeat('─', chars - 2) + String.format("[%0" + chars + "X]", (nodeKey >> (nodeLevel + 8)) & mask);
            baseKeyPrefix1 = charRepeat(' ', chars * 2);
        } else {
            baseKeyPrefix = "";
            baseKeyPrefix1 = "";
        }
        //       log.debug("baseKeyPrefix={}", baseKeyPrefix);


        StringBuilder sb = new StringBuilder();
        for (short i = 0; i < numChildren; i++) {
            Object node = nodes.apply(i);
            String key = String.format("%s%02X", baseKeyPrefix, subKeys.apply(i));
            String x = (i == 0 ? (numChildren == 1 ? "──" : "┬─") : (i + 1 == numChildren ? (prefix + "└─") : (prefix + "├─")));

            if (nodeLevel == 0) {
                sb.append(x + key + " = " + node);
            } else {
                sb.append(x + key + "" + (((ArtNode<?>) node).printDiagram(prefix + (i + 1 == numChildren ? "    " : "│   ") + baseKeyPrefix1, nodeLevel - 8)));
            }
            if (i < numChildren - 1) {
                sb.append("\n");
            } else if (nodeLevel == 0) {
                sb.append("\n" + prefix);
            }
        }
        return sb.toString();
    }

    private static String charRepeat(char x, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(x);
        }
        return sb.toString();
    }


    /**
     * A position in the map, reusable and allocation-free: seek a key (ceiling, higher, floor, lower, first, last),
     * then move to the next or the previous one - an L2 snapshot from a price, a sweep over the price levels, the stop
     * orders up to the last trade price.
     * <pre>
     * for (boolean on = cursor.seekCeiling(price); on &amp;&amp; n &lt; 10; on = cursor.next()) {
     *     consume(cursor.key(), cursor.value());
     * }
     * </pre>
     * A seek or a move returns false when there is no such key; the cursor is then not positioned. The map can be
     * changed only through the cursor ({@link #remove()}) while it is in use - any other change of the key set makes
     * the next call of the cursor throw ConcurrentModificationException, until the next seek. Value updates of existing
     * keys are not changes of the key set. Each seek is one descent, a move is O(1) on average.
     */
    public static final class Cursor<V> {

        // inner nodes above the last level: 56, 48, ... 8
        private static final int MAX_DEPTH = 7;

        private final LongAdaptiveRadixTreeMap<V> map;

        // the inner nodes on the path to the current key and the slot of the child taken in each
        @SuppressWarnings("unchecked")
        private final ArtNode<V>[] ancestors = new ArtNode[MAX_DEPTH];
        private final int[] ancestorSlots = new int[MAX_DEPTH];
        private int depth;

        // the current key - fields, not the arrays above, so that a move within a node touches nothing else: a node of
        // the last level with the slot of the value and its node key (the high bytes), or a leaf with its key
        private ArtNode<V> top;
        private int topSlot;
        private long topKey;
        private boolean topIsLeaf;

        private int expectedModCount;

        Cursor(final LongAdaptiveRadixTreeMap<V> map) {
            this.map = map;
        }

        /** @return true if the cursor is at a key */
        public boolean isValid() {
            return top != null;
        }

        /** at the lowest key; false if the map is empty */
        public boolean seekFirst() {
            start();
            if (map.root == null) {
                return false;
            }
            map.root.pushFirst(this);
            return true;
        }

        /** at the highest key; false if the map is empty */
        public boolean seekLast() {
            start();
            if (map.root == null) {
                return false;
            }
            map.root.pushLast(this);
            return true;
        }

        /** at the key, or else at the closest one above it */
        public boolean seekCeiling(final long key) {
            start();
            return map.root != null && map.root.seekCeiling(this, flipSignBit(key), INITIAL_LEVEL);
        }

        /** at the key, or else at the closest one below it */
        public boolean seekFloor(final long key) {
            start();
            return map.root != null && map.root.seekFloor(this, flipSignBit(key), INITIAL_LEVEL);
        }

        /** at the closest key above it */
        public boolean seekHigher(final long key) {
            if (key == Long.MAX_VALUE) {
                start();
                return false;
            }
            return seekCeiling(key + 1);
        }

        /** at the closest key below it */
        public boolean seekLower(final long key) {
            if (key == Long.MIN_VALUE) {
                start();
                return false;
            }
            return seekFloor(key - 1);
        }

        /** @return false at the end - the cursor is then not positioned */
        public boolean next() {
            checkModCount();
            final ArtNode<V> node = top;
            if (node == null) {
                return false;
            }
            if (!topIsLeaf) {
                // the dense nodes - most of the moves in an order book - without a virtual call
                final int next = node instanceof ArtNode256<V> n256
                        ? n256.nextOccupied(topSlot + 1)
                        : node.slotNext(topSlot);
                if (next >= 0) {
                    topSlot = next;
                    return true;
                }
            }
            return ascend(true);
        }

        /** @return false at the beginning - the cursor is then not positioned */
        public boolean prev() {
            checkModCount();
            final ArtNode<V> node = top;
            if (node == null) {
                return false;
            }
            if (!topIsLeaf) {
                final int prev = node instanceof ArtNode256<V> n256
                        ? n256.prevOccupied(topSlot - 1)
                        : node.slotPrev(topSlot);
                if (prev >= 0) {
                    topSlot = prev;
                    return true;
                }
            }
            return ascend(false);
        }

        /**
         * @return the key the cursor is at
         * @throws NoSuchElementException if the cursor is not positioned
         */
        public long key() {
            checkPosition();
            return topIsLeaf
                    ? flipSignBit(topKey)
                    : flipSignBit((topKey & -256L) | (topSlot & 0xFF));
        }

        /**
         * @return the value of the key the cursor is at
         * @throws NoSuchElementException if the cursor is not positioned
         */
        @SuppressWarnings("unchecked")
        public V value() {
            checkPosition();
            return topIsLeaf
                    ? ((ArtLeaf<V>) top).value
                    : (V) top.childAtSlot(topSlot);
        }

        /**
         * Removes the key the cursor is at and moves to the next one above - a seek, one descent.
         *
         * @return the removed value
         */
        public V remove() {
            final long key = key();
            final V removed = map.remove(key);
            seekCeiling(key);
            return removed;
        }

        /** the node of the current key is done: up to the first ancestor with a child on that side, then down */
        @SuppressWarnings("unchecked")
        private boolean ascend(final boolean forward) {
            while (depth > 0) {
                final ArtNode<V> node = ancestors[depth - 1];
                final int slot = forward
                        ? node.slotNext(ancestorSlots[depth - 1])
                        : node.slotPrev(ancestorSlots[depth - 1]);
                if (slot >= 0) {
                    ancestorSlots[depth - 1] = slot;
                    final ArtNode<V> child = (ArtNode<V>) node.childAtSlot(slot);
                    if (forward) {
                        child.pushFirst(this);
                    } else {
                        child.pushLast(this);
                    }
                    return true;
                }
                depth--;
            }
            top = null;
            return false;
        }

        private void start() {
            depth = 0;
            top = null;
            expectedModCount = map.modCount;
        }

        void pushInner(final ArtNode<V> node, final int slot) {
            ancestors[depth] = node;
            ancestorSlots[depth] = slot;
            depth++;
        }

        void popInner() {
            depth--;
        }

        void setTop(final ArtNode<V> node, final int slot, final long nodeKey) {
            top = node;
            topSlot = slot;
            topKey = nodeKey;
            topIsLeaf = false;
        }

        void setTopLeaf(final ArtLeaf<V> leaf) {
            top = leaf;
            topKey = leaf.key;
            topIsLeaf = true;
        }

        private void checkModCount() {
            if (expectedModCount != map.modCount) {
                throw new ConcurrentModificationException();
            }
        }

        private void checkPosition() {
            checkModCount();
            if (top == null) {
                throw new NoSuchElementException();
            }
        }
    }

    /** An entry of {@link #entriesList()} - a copy, not a view of the map. */
    public static final class Entry<V> implements Map.Entry<Long, V> {

        final long key;

        V value;

        public Entry(long key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Long getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            final V v = this.value;
            this.value = value;
            return v;
        }

        /** as {@link Map.Entry#equals} defines it - equal to any entry with the same key and value */
        @Override
        public boolean equals(Object o) {
            return o instanceof Map.Entry<?, ?> e
                    && Long.valueOf(key).equals(e.getKey())
                    && Objects.equals(value, e.getValue());
        }

        /** as {@link Map.Entry#hashCode} defines it */
        @Override
        public int hashCode() {
            return Long.hashCode(key) ^ Objects.hashCode(value);
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

}
