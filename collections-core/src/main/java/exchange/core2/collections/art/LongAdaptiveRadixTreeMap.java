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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * Adaptive Radix Tree (ART) Java implementation
 * <p>
 * based on original paper:
 * <p>
 * The Adaptive Radix Tree:
 * ARTful Indexing for Main-Memory Databases
 * <p>
 * Viktor Leis, Alfons Kemper, Thomas Neumann
 * Fakultat fur Informatik
 * Technische Universitat Munchen
 * Boltzmannstrae 3, D-85748 Garching
 * <p>
 * https://db.in.tum.de/~leis/papers/ART.pdf
 * <p>
 * Target operations:
 * - GET or (PUT + GET_LOWER/HIGHER) - placing/moving/bulkload order - often GET, more rare PUT ??cache
 * - REMOVE - cancel or move - last order in the bucket
 * - TRAVERSE from LOWER - filling L2 market data, in hot area (Node256 or Node48).
 * - REMOVE price during matching - !! can use RANGE removal operation - rare, but latency critical
 * - GET or PUT if not exists - inserting back own orders, very rare
 */
public final class LongAdaptiveRadixTreeMap<V> {

    private static final int INITIAL_LEVEL = 56;

    private IArtNode<V> root = null;

    private final ObjectsPool objectsPool;

    private int size = 0;

    /**
     * Out-parameter of the node put/remove: the value a put replaced or a remove took out, null if the key was
     * absent. The map resets it as soon as it has read it, so it never keeps a value alive.
     */
    V previousValue;

    public LongAdaptiveRadixTreeMap(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    public LongAdaptiveRadixTreeMap() {
        objectsPool = ObjectsPool.createDefaultTestPool();
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

    public V get(final long key) {
        return root != null
                ? root.getValue(flipSignBit(key), INITIAL_LEVEL)
                : null;
    }

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
            return null;
        }

        final IArtNode<V> upSizedNode = root.put(internalKey, INITIAL_LEVEL, value, this);
        if (upSizedNode != null) {
            root = upSizedNode;
        }
        final V previous = takePreviousValue();
        if (previous == null) {
            size++;
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
     * level to the better level next to it. Currently a put and a {@link #getLowerValue} - two descents.
     *
     * @return the value of the closest key strictly below the key, or null if there is none
     */
    public V putAndGetLowerValue(final long key, final V value) {
        put(key, value);
        return getLowerValue(key);
    }

    /**
     * Puts the value and returns the value of the closest key above - for a new bid price level.
     * Currently a put and a {@link #getHigherValue} - two descents.
     *
     * @return the value of the closest key strictly above the key, or null if there is none
     */
    public V putAndGetHigherValue(final long key, final V value) {
        put(key, value);
        return getHigherValue(key);
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
    }

    /**
     * remove keys range
     *
     * @param keyFromInclusive from key inclusive
     * @param keyToExclusive   to key exclusive
     */
    public void removeRange(final long keyFromInclusive, final long keyToExclusive) {
        // TODO
        throw new UnsupportedOperationException();
    }

    // TODO moveToAnotherKey(long oldKey, long newKey) - throw exception if not found

    /**
     * @return the value of the closest key strictly above the key, or null if there is none
     */
    public V getHigherValue(final long key) {
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
    public V getLowerValue(final long key) {
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
    public V getCeilingValue(final long key) {
        return root != null
                ? root.getCeilingValue(flipSignBit(key), INITIAL_LEVEL)
                : null;
    }

    /**
     * @return the value of the key, or else of the closest key below it; null if there is none
     */
    public V getFloorValue(final long key) {
        return root != null
                ? root.getFloorValue(flipSignBit(key), INITIAL_LEVEL)
                : null;
    }

    /**
     * @return the value of the lowest key, or null if the map is empty
     */
    public V firstValue() {
        return getCeilingValue(Long.MIN_VALUE);
    }

    /**
     * @return the value of the highest key, or null if the map is empty
     */
    public V lastValue() {
        return getFloorValue(Long.MAX_VALUE);
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

    public int forEach(LongObjConsumer<V> consumer, int limit) {
        return root != null ? root.forEach(consumer, limit) : 0;
    }

    public int forEachDesc(LongObjConsumer<V> consumer, int limit) {
        return root != null ? root.forEachDesc(consumer, limit) : 0;
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
     * Same as {@code Math.min(size(), limit)}. The size used to be counted by walking the tree, the limit bounded
     * that walk - now the size is a counter.
     */
    public int size(int limit) {
        return Math.min(size, limit);
    }

    /**
     * Materializes all entries - produces garbage, avoid on hot paths.
     */
    public List<Map.Entry<Long, V>> entriesList() {
        return root != null ? root.entries() : Collections.emptyList();
    }

    public void validateInternalState() {
        if (previousValue != null) {
            throw new IllegalStateException("previousValue retained");
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

    public String printDiagram() {
        if (root != null) {
            return root.printDiagram("", INITIAL_LEVEL);
        } else {
            return "";
        }
    }


    static <V> IArtNode<V> branchIfRequired(final long key, final V value, final long nodeKey, final int nodeLevel, final IArtNode<V> caller) {

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
                sb.append(x + key + "" + (((IArtNode<?>) node).printDiagram(prefix + (i + 1 == numChildren ? "    " : "│   ") + baseKeyPrefix1, nodeLevel - 8)));
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
