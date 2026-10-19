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

import java.util.List;
import java.util.Map;

/**
 * A node of the tree: ArtNode4 / 16 / 48 / 256, or an ArtLeaf in place of a child. An abstract class, not an
 * interface: a call on a node whose class the JIT can not predict (most of them - five classes meet at every
 * level) goes through the vtable, one indexed load, where an interface call searches the itable of the class.
 */
public abstract class ArtNode<V> {

    public abstract V getValue(long key, int level);

    /**
     * @param map receives the value the put replaced in {@code map.previousValue} (stays null for a new key)
     * @return the node that replaces this one (grown or branched), or null if this node stays
     */
    public abstract ArtNode<V> put(long key, int level, V value, LongAdaptiveRadixTreeMap<V> map);

    /**
     * @param map receives the removed value in {@code map.previousValue} (stays null if the key is absent)
     * @return the node that replaces this one (shrunk or collapsed), this node, or null if it became empty
     */
    public abstract ArtNode<V> remove(long key, int level, LongAdaptiveRadixTreeMap<V> map);

    public abstract V getCeilingValue(long key, int level);

    public abstract V getFloorValue(long key, int level);

    /**
     * For a node of the last level (its children are values): the value next to the key byte in this node.
     *
     * @param lower the closest one below, otherwise above
     * @return the value, or null if this node has none on that side
     */
    public abstract V valueNextTo(int idx, boolean lower);

    /** @return the lowest internal key of this subtree (the subtree is never empty) */
    public abstract long firstKey();

    /** @return the highest internal key of this subtree (the subtree is never empty) */
    public abstract long lastKey();

    /** hands out map keys (signed), not the internal ones */
    public abstract int forEach(LongObjConsumer<V> consumer, int limit);

    /** hands out map keys (signed), not the internal ones */
    public abstract int forEachDesc(LongObjConsumer<V> consumer, int limit);

    /**
     * Visits the keys at or above the internal key, ascending - map keys (signed) to the consumer.
     *
     * @return the number of keys visited, at most limit (positive)
     */
    public abstract int forEachFrom(long key, int level, LongObjConsumer<V> consumer, int limit);

    /** Visits the keys at or below the internal key, descending - as {@link #forEachFrom}. */
    public abstract int forEachDescFrom(long key, int level, LongObjConsumer<V> consumer, int limit);

    /**
     * Returns this node and its whole subtree into the pool. The node must be unreachable afterwards.
     */
    public abstract void recycle();

    /**
     * Get number of elements
     * Slow operation - O(n) complexity
     *
     * @param limit - can provide value to operation increase performance
     * @return if returned value less than limit - it is precise size of the node
     */
    public abstract int size(int limit);

    /**
     * For testing only
     *
     * @param level level
     */
    public abstract void validateInternalState(int level);

    /**
     * For testing only
     *
     * @param prefix prefix
     * @param level  level
     * @return internal diagram part
     */
    public abstract String printDiagram(String prefix, int level);

    /**
     * For testing only
     *
     * @return list of entries, map keys (signed)
     */
    public abstract List<Map.Entry<Long, V>> entries();


    public abstract ObjectsPool getObjectsPool();

    /*
     * Slots, for the cursor: the children in key order. The low byte of a slot is the key byte of the child (ArtNode48 /
     * 256 use the key byte alone), ArtNode4 / 16 put the position above it - so the cursor gets the key byte without
     * asking the node. A leaf has no slots - the cursor tells it apart and does not ask.
     */

    long nodeKey() {
        throw new UnsupportedOperationException();
    }

    int nodeLevel() {
        throw new UnsupportedOperationException();
    }

    /** @return the slot of the lowest key byte at or above idx, or -1 */
    int slotCeiling(int idx) {
        throw new UnsupportedOperationException();
    }

    /** @return the slot of the highest key byte at or below idx, or -1 */
    int slotFloor(int idx) {
        throw new UnsupportedOperationException();
    }

    /** @return the slot after this one, or -1 */
    int slotNext(int slot) {
        throw new UnsupportedOperationException();
    }

    /** @return the slot before this one, or -1 */
    int slotPrev(int slot) {
        throw new UnsupportedOperationException();
    }

    /** @return the child at the slot: a node, or a value at level 0 */
    Object childAtSlot(int slot) {
        throw new UnsupportedOperationException();
    }
}
