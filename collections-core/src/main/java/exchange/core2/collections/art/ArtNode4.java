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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The smallest node type stores 2 to 4 children (a single key is an {@link ArtLeaf}), the keys sorted.
 * <p>
 * No arrays: the key bytes are packed into one int and the children are four fields - one object of 56 bytes
 * (48 with compact object headers) and no dependent load, where the paper's layout (an array of keys, an array
 * of pointers) took three objects (96 bytes) and two more loads on every visit.
 */
final class ArtNode4<V> extends ArtNode<V> {

    /** highest possible key in the unsigned key space the nodes work in */
    private static final long UNSIGNED_MAX_KEY = -1L;

    private static final int ONES = 0x01010101;
    private static final int HIGHS = 0x80808080;

    // key byte of child i in byte i, ascending; the bytes from numChildren up are not used
    int keys;

    // child i - in the order of the keys; null from numChildren up
    Object node0;
    Object node1;
    Object node2;
    Object node3;

    private final ObjectsPool objectsPool;

    long nodeKey;
    int nodeLevel;

    byte numChildren;

    ArtNode4(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    /** bulk load: an empty node for 2-4 children, see append - a node from the pool has no children left */
    void initForAppend(final long nodeKey, final int nodeLevel) {
        this.nodeKey = nodeKey;
        this.nodeLevel = nodeLevel;
        this.keys = 0;
        this.numChildren = 0;
    }

    @Override
    void append(final int keyByte, final Object child) {
        keys |= keyByte << (numChildren << 3);
        setChild(numChildren, child);
        numChildren++;
    }

    // split-compact operation constructor
    void initTwoKeys(final long key1, final Object value1, final long key2, final Object value2, final int level) {
        // create compact node
        this.numChildren = 2;
        final int idx1 = (int) ((key1 >>> level) & 0xFF);
        final int idx2 = (int) ((key2 >>> level) & 0xFF);
        // The keys are ordered by BRANCH INDEX, not by the full key - every lookup and
        // traversal relies on that.
        //
        // Comparing the full keys here used to invert the two children whenever the branch was on
        // the sign bit: for key1=-5, key2=1 the signed comparison says key1 < key2, but the indices
        // are 0xFF and 0x00, so -5 has to go SECOND. The result was an unsorted node - wrong
        // iteration order, and get() returning null for keys that were present.
        // Branch indices are masked to 0..255, so an ordinary comparison is the unsigned one.
        if (idx1 < idx2) {
            this.keys = idx1 | (idx2 << 8);
            this.node0 = value1;
            this.node1 = value2;
        } else {
            this.keys = idx2 | (idx1 << 8);
            this.node0 = value2;
            this.node1 = value1;
        }
        this.nodeKey = key1; // leading part the same for both keys
        this.nodeLevel = level;
    }

    // downsize 16->4
    void initFromNode16(ArtNode16<V> artNode16) {
        // put original node back into pool
        objectsPool.put(ObjectsPool.ART_NODE_16, artNode16);

        this.numChildren = artNode16.numChildren;
        int packed = 0;
        for (int i = 0; i < numChildren; i++) {
            packed |= artNode16.keys[i] << (i << 3);
            setChild(i, artNode16.nodes[i]);
        }
        this.keys = packed;
        this.nodeLevel = artNode16.nodeLevel;
        this.nodeKey = artNode16.nodeKey;

        java.util.Arrays.fill(artNode16.nodes, null);
    }

    /** @return the key byte of child i */
    int keyAt(final int i) {
        return (keys >>> (i << 3)) & 0xFF;
    }

    /** @return child i */
    Object childAt(final int i) {
        switch (i) {
            case 0:
                return node0;
            case 1:
                return node1;
            case 2:
                return node2;
            default:
                return node3;
        }
    }

    private void setChild(final int i, final Object child) {
        switch (i) {
            case 0 -> node0 = child;
            case 1 -> node1 = child;
            case 2 -> node2 = child;
            default -> node3 = child;
        }
    }

    void clearChildren() {
        node0 = node1 = node2 = node3 = null;
    }

    /**
     * @return position of the key byte, or -1 - all four bytes compared at once: a byte of keys ^ idx that is zero
     * is a match. The zero-byte test flags the lowest zero byte exactly (a borrow can flag the bytes above it, never
     * below), so the lowest flag among the used bytes is the answer.
     */
    private int positionOf(final int idx) {
        final int x = keys ^ (idx * ONES);
        final int used = (int) ((1L << (numChildren << 3)) - 1);
        final int matches = (x - ONES) & ~x & HIGHS & used;
        return matches == 0 ? -1 : Integer.numberOfTrailingZeros(matches) >>> 3;
    }

    /** inserts a key byte and its child at pos, the ones from pos up move one place up (numChildren < 4) */
    private void insertAt(final int pos, final int idx, final Object child) {
        final int shift = pos << 3;
        final int below = (1 << shift) - 1;
        keys = (keys & below) | (idx << shift) | ((keys & ~below) << 8);
        switch (pos) {
            case 0 -> {
                node3 = node2;
                node2 = node1;
                node1 = node0;
                node0 = child;
            }
            case 1 -> {
                node3 = node2;
                node2 = node1;
                node1 = child;
            }
            case 2 -> {
                node3 = node2;
                node2 = child;
            }
            default -> node3 = child;
        }
        numChildren++;
    }

    /** removes the key byte and the child at pos, the ones above it move one place down */
    private void removeAt(final int pos) {
        final int below = (1 << (pos << 3)) - 1;
        keys = (keys & below) | ((keys >>> 8) & ~below);
        switch (pos) {
            case 0 -> {
                node0 = node1;
                node1 = node2;
                node2 = node3;
            }
            case 1 -> {
                node1 = node2;
                node2 = node3;
            }
            case 2 -> node2 = node3;
            default -> {
            }
        }
        numChildren--;
        setChild(numChildren, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {

        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return null;
        }
        final int pos = positionOf((int) ((key >>> nodeLevel) & 0xFF));
        if (pos < 0) {
            return null;
        }
        final Object node = childAt(pos);
        return nodeLevel == 0
                ? (V) node
                : ((ArtNode<V>) node).getValue(key, nodeLevel - 8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> put(final long key, final int level, final V value, final LongAdaptiveRadixTreeMap<V> map) {

        if (level != nodeLevel) {
            final ArtNode<V> branch = LongAdaptiveRadixTreeMap.branchIfRequired(key, value, nodeKey, nodeLevel, this);
            if (branch != null) {
                return branch;
            }
        }

        final int nodeIndex = (int) ((key >>> nodeLevel) & 0xFF);
        final int found = positionOf(nodeIndex);
        if (found >= 0) {
            // just update
            if (nodeLevel == 0) {
                map.previousValue = (V) childAt(found);
                setChild(found, value);
                map.rememberPut(this);
            } else {
                final ArtNode<V> resizedNode = ((ArtNode<V>) childAt(found)).put(key, nodeLevel - 8, value, map);
                if (resizedNode != null) {
                    // update resized node if capacity has increased
                    setChild(found, resizedNode);
                }
            }
            return null;
        }

        // new element
        final Object newElement = nodeLevel == 0 ? value : ArtLeaf.create(objectsPool, key, value);
        if (numChildren != 4) {
            // capacity less than 4 - can simply insert node; its place: after the keys below it
            int pos = 0;
            while (pos < numChildren && keyAt(pos) < nodeIndex) {
                pos++;
            }
            insertAt(pos, nodeIndex, newElement);
            if (nodeLevel == 0) {
                map.rememberPut(this);
            }
            return null;
        } else {
            // no space left, create a Node16 with new item
            final ArtNode16<V> node16 = objectsPool.get(ObjectsPool.ART_NODE_16, ArtNode16::new);
            node16.initFromNode4(this, (short) nodeIndex, newElement);
            if (node16.nodeLevel == 0) {
                map.rememberPut(node16);
            }
            return node16;
        }
    }

    @Override
    public String toString() {
        return "ArtNode4{" +
                "nodeKey=" + nodeKey +
                ", nodeLevel=" + nodeLevel +
                ", numChildren=" + numChildren +
                '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> remove(final long key, final int level, final LongAdaptiveRadixTreeMap<V> map) {

        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return this;
        }
        final int pos = positionOf((int) ((key >>> nodeLevel) & 0xFF));
        if (pos < 0) {
            // not found
            return this;
        }
        final Object node = childAt(pos);

        // removing
        if (nodeLevel == 0) {
            map.previousValue = (V) node;
            removeAt(pos);
            if (numChildren == 1) {
                // the last key goes into a leaf, this node to the pool
                final ArtLeaf<V> leaf = ArtLeaf.create(objectsPool, (nodeKey & -256L) | keyAt(0), (V) node0);
                node0 = null;
                numChildren = 0;
                objectsPool.put(ObjectsPool.ART_NODE_4, this);
                return leaf;
            }
        } else {
            final ArtNode<V> resizedNode = ((ArtNode<V>) node).remove(key, nodeLevel - 8, map);
            if (resizedNode != node) {
                // update resized node if capacity has decreased
                if (resizedNode != null) {
                    setChild(pos, resizedNode);
                } else {
                    removeAt(pos);
                    if (numChildren == 1) {
                        // The parent links the last child directly: the child keeps the full key, so its own
                        // prefix check covers the byte this node used to branch on. This node goes to the pool.
                        final ArtNode<V> lastNode = (ArtNode<V>) node0;
                        node0 = null;
                        numChildren = 0;
                        objectsPool.put(ObjectsPool.ART_NODE_4, this);
                        return lastNode;
                    }
                }
            }
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getCeilingValue(long key, int level) {

        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
            final long keyWithMask = key & mask;
            final long nodeKeyWithMask = nodeKey & mask;
            // unsigned: the node layer orders keys by their bytes, and the map flips the sign
            // bit on the way in, so the top bit here is data - not a sign
            if (Long.compareUnsigned(nodeKeyWithMask, keyWithMask) < 0) {
                // compacted part is lower - no need to search for ceiling entry here
                return null;
            } else if (keyWithMask != nodeKeyWithMask) {
                // accept first existing entry, because compacted nodekey is higher
                key = 0;
            }
        }

        final int nodeIndex = (int) ((key >>> nodeLevel) & 0xFF);

        for (int i = 0; i < numChildren; i++) {
            final int index = keyAt(i);
            // any equal or higher is ok
            if (index == nodeIndex) {
                final V res = nodeLevel == 0
                        ? (V) childAt(i)
                        : ((ArtNode<V>) childAt(i)).getCeilingValue(key, nodeLevel - 8);
                if (res != null) {
                    // return if found ceiling, otherwise will try next one
                    return res;
                }
            }
            if (index > nodeIndex) {
                // exploring first higher key
                return nodeLevel == 0
                        ? (V) childAt(i)
                        : ((ArtNode<V>) childAt(i)).getCeilingValue(0, nodeLevel - 8); // take lowest existing key
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getFloorValue(long key, int level) {

        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
            final long keyWithMask = key & mask;
            final long nodeKeyWithMask = nodeKey & mask;
            // unsigned - see the note in getCeilingValue
            if (Long.compareUnsigned(nodeKeyWithMask, keyWithMask) > 0) {
                // compacted part is higher - no need to search for floor entry here
                return null;
            } else if (keyWithMask != nodeKeyWithMask) {
                // find highest value, because compacted nodekey is lower
                key = UNSIGNED_MAX_KEY;
            }
        }

        final int nodeIndex = (int) ((key >>> nodeLevel) & 0xFF);

        for (int i = numChildren - 1; i >= 0; i--) {
            final int index = keyAt(i);
            if (index == nodeIndex) {
                final V res = nodeLevel == 0
                        ? (V) childAt(i)
                        : ((ArtNode<V>) childAt(i)).getFloorValue(key, nodeLevel - 8);
                if (res != null) {
                    // return if found floor, otherwise will try previous one
                    return res;
                }
            }
            if (index < nodeIndex) {
                // exploring first lower key
                return nodeLevel == 0
                        ? (V) childAt(i)
                        : ((ArtNode<V>) childAt(i)).getFloorValue(UNSIGNED_MAX_KEY, nodeLevel - 8); // take highest existing key
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V valueNextTo(final int idx, final boolean lower) {
        if (lower) {
            for (int i = numChildren - 1; i >= 0; i--) {
                if (keyAt(i) < idx) {
                    return (V) childAt(i);
                }
            }
        } else {
            for (int i = 0; i < numChildren; i++) {
                if (keyAt(i) > idx) {
                    return (V) childAt(i);
                }
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    boolean seekCeiling(final LongAdaptiveRadixTreeMap.Cursor<V> cursor, final long key, final int level) {
        if (level != nodeLevel) {
            // the bytes this node skips (path compression): its keys are all above or all below the key
            final long mask = -1L << (nodeLevel + 8);
            final int cmp = Long.compareUnsigned(nodeKey & mask, key & mask);
            if (cmp != 0) {
                if (cmp > 0) {
                    pushFirst(cursor);
                    return true;
                }
                return false;
            }
        }
        final int idx = (int) ((key >>> nodeLevel) & 0xFF);
        int pos = 0;
        while (pos < numChildren && keyAt(pos) < idx) {
            pos++;
        }
        if (pos == numChildren) {
            return false;
        }
        if (nodeLevel == 0) {
            cursor.setTop(this, (pos << 8) | keyAt(pos), nodeKey);
            return true;
        }
        if (keyAt(pos) == idx) {
            cursor.pushInner(this, (pos << 8) | idx);
            if (((ArtNode<V>) childAt(pos)).seekCeiling(cursor, key, nodeLevel - 8)) {
                return true;
            }
            cursor.popInner();
            if (++pos == numChildren) {
                return false;
            }
        }
        cursor.pushInner(this, (pos << 8) | keyAt(pos));
        ((ArtNode<V>) childAt(pos)).pushFirst(cursor);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    boolean seekFloor(final LongAdaptiveRadixTreeMap.Cursor<V> cursor, final long key, final int level) {
        if (level != nodeLevel) {
            final long mask = -1L << (nodeLevel + 8);
            final int cmp = Long.compareUnsigned(nodeKey & mask, key & mask);
            if (cmp != 0) {
                if (cmp < 0) {
                    pushLast(cursor);
                    return true;
                }
                return false;
            }
        }
        final int idx = (int) ((key >>> nodeLevel) & 0xFF);
        int pos = numChildren - 1;
        while (pos >= 0 && keyAt(pos) > idx) {
            pos--;
        }
        if (pos < 0) {
            return false;
        }
        if (nodeLevel == 0) {
            cursor.setTop(this, (pos << 8) | keyAt(pos), nodeKey);
            return true;
        }
        if (keyAt(pos) == idx) {
            cursor.pushInner(this, (pos << 8) | idx);
            if (((ArtNode<V>) childAt(pos)).seekFloor(cursor, key, nodeLevel - 8)) {
                return true;
            }
            cursor.popInner();
            if (--pos < 0) {
                return false;
            }
        }
        cursor.pushInner(this, (pos << 8) | keyAt(pos));
        ((ArtNode<V>) childAt(pos)).pushLast(cursor);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    void pushFirst(final LongAdaptiveRadixTreeMap.Cursor<V> cursor) {
        if (nodeLevel == 0) {
            cursor.setTop(this, keyAt(0), nodeKey);
        } else {
            cursor.pushInner(this, keyAt(0));
            ((ArtNode<V>) childAt(0)).pushFirst(cursor);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    void pushLast(final LongAdaptiveRadixTreeMap.Cursor<V> cursor) {
        final int last = numChildren - 1;
        if (nodeLevel == 0) {
            cursor.setTop(this, (last << 8) | keyAt(last), nodeKey);
        } else {
            cursor.pushInner(this, (last << 8) | keyAt(last));
            ((ArtNode<V>) childAt(last)).pushLast(cursor);
        }
    }

    @Override
    int slotNext(final int slot) {
        final int i = (slot >>> 8) + 1;
        return i < numChildren ? (i << 8) | keyAt(i) : -1;
    }

    @Override
    int slotPrev(final int slot) {
        final int i = (slot >>> 8) - 1;
        return i >= 0 ? (i << 8) | keyAt(i) : -1;
    }

    @Override
    Object childAtSlot(final int slot) {
        return childAt(slot >>> 8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public long firstKey() {
        return nodeLevel == 0
                ? (nodeKey & -256L) | keyAt(0)
                : ((ArtNode<V>) node0).firstKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public long lastKey() {
        final int last = numChildren - 1;
        return nodeLevel == 0
                ? (nodeKey & -256L) | keyAt(last)
                : ((ArtNode<V>) childAt(last)).lastKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            // map key of index 0: the low byte is zero, so adding an index never reaches the flipped sign bit
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            final int n = Math.min(numChildren, limit);
            for (int i = 0; i < n; i++) {
                consumer.accept(keyBase + keyAt(i), (V) childAt(i));
            }
            return n;
        } else {
            int numLeft = limit;
            for (int i = 0; i < numChildren && numLeft > 0; i++) {
                numLeft -= ((ArtNode<V>) childAt(i)).forEach(consumer, numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEachFrom(final long key, final int level, final LongObjConsumer<V> consumer, final int limit) {
        if (level != nodeLevel) {
            // the bytes this node skips (path compression): its keys are all above or all below the key
            final long mask = -1L << (nodeLevel + 8);
            final int cmp = Long.compareUnsigned(nodeKey & mask, key & mask);
            if (cmp != 0) {
                return cmp > 0 ? forEach(consumer, limit) : 0;
            }
        }
        final int idx = (int) ((key >>> nodeLevel) & 0xFF);
        int pos = 0;
        while (pos < numChildren && keyAt(pos) < idx) {
            pos++;
        }
        int found = 0;
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; pos < numChildren && found < limit; pos++) {
                consumer.accept(keyBase + keyAt(pos), (V) childAt(pos));
                found++;
            }
            return found;
        }
        if (pos < numChildren && keyAt(pos) == idx) {
            found += ((ArtNode<V>) childAt(pos)).forEachFrom(key, nodeLevel - 8, consumer, limit);
            pos++;
        }
        for (; pos < numChildren && found < limit; pos++) {
            found += ((ArtNode<V>) childAt(pos)).forEach(consumer, limit - found);
        }
        return found;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEachDescFrom(final long key, final int level, final LongObjConsumer<V> consumer, final int limit) {
        if (level != nodeLevel) {
            final long mask = -1L << (nodeLevel + 8);
            final int cmp = Long.compareUnsigned(nodeKey & mask, key & mask);
            if (cmp != 0) {
                return cmp < 0 ? forEachDesc(consumer, limit) : 0;
            }
        }
        final int idx = (int) ((key >>> nodeLevel) & 0xFF);
        int pos = numChildren - 1;
        while (pos >= 0 && keyAt(pos) > idx) {
            pos--;
        }
        int found = 0;
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; pos >= 0 && found < limit; pos--) {
                consumer.accept(keyBase + keyAt(pos), (V) childAt(pos));
                found++;
            }
            return found;
        }
        if (pos >= 0 && keyAt(pos) == idx) {
            found += ((ArtNode<V>) childAt(pos)).forEachDescFrom(key, nodeLevel - 8, consumer, limit);
            pos--;
        }
        for (; pos >= 0 && found < limit; pos--) {
            found += ((ArtNode<V>) childAt(pos)).forEachDesc(consumer, limit - found);
        }
        return found;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEachDesc(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            int numFound = 0;
            for (int i = numChildren - 1; i >= 0 && numFound < limit; i--) {
                consumer.accept(keyBase + keyAt(i), (V) childAt(i));
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = numChildren - 1; i >= 0 && numLeft > 0; i--) {
                numLeft -= ((ArtNode<V>) childAt(i)).forEachDesc(consumer, numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int size(int limit) {
        if (nodeLevel == 0) {
            return numChildren;
        } else {
            int numLeft = limit;
            for (int i = numChildren - 1; i >= 0 && numLeft > 0; i--) {
                numLeft -= ((ArtNode<V>) childAt(i)).size(numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void recycle() {
        if (nodeLevel != 0) {
            for (int i = 0; i < numChildren; i++) {
                ((ArtNode<V>) childAt(i)).recycle();
            }
        }
        clearChildren();
        numChildren = 0;
        objectsPool.put(ObjectsPool.ART_NODE_4, this);
    }

    @Override
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
        // a single key is a leaf, a single child is linked to the parent directly
        if (numChildren > 4 || numChildren < 2) throw new IllegalStateException("unexpected numChildren");
        int last = -1;
        for (int i = 0; i < 4; i++) {
            final Object node = childAt(i);
            if (i < numChildren) {
                if (node == null) throw new IllegalStateException("null node");
                final int key = keyAt(i);
                if (key == last) throw new IllegalStateException("duplicate key");
                if (key < last) throw new IllegalStateException("wrong key order");
                if (positionOf(key) != i) throw new IllegalStateException("key " + key + " not found at " + i);
                last = key;
                if (node instanceof ArtNode) {
                    if (nodeLevel == 0) throw new IllegalStateException("unexpected node type");
                    ArtNode artNode = (ArtNode) node;
                    artNode.validateInternalState(nodeLevel - 8);
                } else {
                    if (nodeLevel != 0) throw new IllegalStateException("unexpected node type");
                }
            } else {
                if (node != null) throw new IllegalStateException("not released node");
            }
        }
    }

    @Override
    public String printDiagram(String prefix, int level) {
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren,
                idx -> (short) keyAt(idx), idx -> childAt(idx));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries() {
        final long keyPrefix = LongAdaptiveRadixTreeMap.flipSignBit(nodeKey & (-1L << 8)); // map key of index 0

        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        for (int i = 0; i < numChildren; i++) {
            if (nodeLevel == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefix + keyAt(i), (V) childAt(i)));
            } else {
                list.addAll(((ArtNode<V>) childAt(i)).entries());
            }
        }
        return list;
    }

    @Override
    public ObjectsPool getObjectsPool() {
        return objectsPool;
    }

}
