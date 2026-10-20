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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This node type is used for storing between 5 and
 * 16 child pointers. Like the Node4, the keys and pointers
 * are stored in separate arrays at corresponding positions, but
 * both arrays have space for 16 entries. A key can be found
 * efficiently with binary search or, on modern hardware, with
 * parallel comparisons using SIMD instructions.
 */
public final class ArtNode16<V> extends ArtNode<V> {

    /** highest possible key in the unsigned key space the nodes work in */
    private static final long UNSIGNED_MAX_KEY = -1L;

    private static final int NODE4_SWITCH_THRESHOLD = 3;

    // keys are ordered
    final short[] keys = new short[16];
    final Object[] nodes = new Object[16];

    long nodeKey;
    int nodeLevel;

    byte numChildren;

    private final ObjectsPool objectsPool;

    public ArtNode16(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    void initFromNode4(ArtNode4<V> node4, short subKey, Object newElement) {

        final byte sourceSize = node4.numChildren;
        this.nodeLevel = node4.nodeLevel;
        this.nodeKey = node4.nodeKey;
        this.numChildren = (byte) (sourceSize + 1);
        int inserted = 0;
        for (int i = 0; i < sourceSize; i++) {
            final int key = node4.keyAt(i);
            if (inserted == 0 && key > subKey) {
                keys[i] = subKey;
                nodes[i] = newElement;
                inserted = 1;
            }
            keys[i + inserted] = (short) key;
            nodes[i + inserted] = node4.childAt(i);
        }
        if (inserted == 0) {
            keys[sourceSize] = subKey;
            nodes[sourceSize] = newElement;
        }

        // put original node back into pool
        node4.clearChildren();
        objectsPool.put(ObjectsPool.ART_NODE_4, node4);
    }

    void initFromNode48(ArtNode48<V> node48) {
//        log.debug("48->16 nodeLevel={} (nodekey={})", node48.nodeLevel, node48.nodeKey);
        this.numChildren = node48.numChildren;
        this.nodeLevel = node48.nodeLevel;
        this.nodeKey = node48.nodeKey;
        byte idx = 0;
        for (int i = node48.nextOccupied(0); i >= 0; i = node48.nextOccupied(i + 1)) {
            this.keys[idx] = (short) i;
            this.nodes[idx] = node48.nodes[node48.indexes[i]];
            idx++;
        }

        Arrays.fill(node48.nodes, null);
        Arrays.fill(node48.indexes, (byte) -1);
        objectsPool.put(ObjectsPool.ART_NODE_48, node48);
    }


    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return null;
        }
        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);
        for (int i = 0; i < numChildren; i++) {
            final short index = keys[i];
            if (index == nodeIndex) {
                final Object node = nodes[i];
                return nodeLevel == 0
                        ? (V) node
                        : ((ArtNode<V>) node).getValue(key, nodeLevel - 8);
            }
            if (nodeIndex < index) {
                // can give up searching because keys are in sorted order
                break;
            }
        }
        return null;
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
        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);
        int pos = 0;
        while (pos < numChildren) {
            if (nodeIndex == keys[pos]) {
                // just update
                if (nodeLevel == 0) {
                    map.previousValue = (V) nodes[pos];
                    nodes[pos] = value;
                    map.rememberPut(this);
                } else {
                    final ArtNode<V> resizedNode = ((ArtNode<V>) nodes[pos]).put(key, nodeLevel - 8, value, map);
                    if (resizedNode != null) {
                        // update resized node if capacity has increased
                        nodes[pos] = resizedNode;
                    }
                }
                return null;
            }
            if (nodeIndex < keys[pos]) {
                // can give up searching because keys are in sorted order
                break;
            }
            pos++;
        }

        // not found, put new element
        if (numChildren != 16) {
            // capacity less than 16 - can simply insert node
            final int copyLength = numChildren - pos;
            if (copyLength != 0) {
                System.arraycopy(keys, pos, keys, pos + 1, copyLength);
                System.arraycopy(nodes, pos, nodes, pos + 1, copyLength);
            }
            keys[pos] = nodeIndex;
            if (nodeLevel == 0) {
                nodes[pos] = value;
                map.rememberPut(this);
            } else {
                final ArtLeaf<V> newSubNode = ArtLeaf.create(objectsPool, key, value);
                nodes[pos] = newSubNode;
            }
            numChildren++;
            return null;
        } else {
            // no space left, create a Node48 with new element
            final Object newElement;
            if (nodeLevel == 0) {
                newElement = value;
            } else {
                final ArtLeaf<V> newSubNode = ArtLeaf.create(objectsPool, key, value);
                newElement = newSubNode;
            }

            ArtNode48<V> node48 = objectsPool.get(ObjectsPool.ART_NODE_48, ArtNode48::new);
            node48.initFromNode16(this, nodeIndex, newElement);
            if (node48.nodeLevel == 0) {
                map.rememberPut(node48);
            }
            return node48;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> remove(final long key, final int level, final LongAdaptiveRadixTreeMap<V> map) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return this;
        }
        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);
        Object node = null;
        int pos = 0;
        while (pos < numChildren) {
            if (nodeIndex == keys[pos]) {
                // found
                node = nodes[pos];
                break;
            }
            if (nodeIndex < keys[pos]) {
                // can give up searching because keys are in sorted order
                return this;
            }
            pos++;
        }

        if (node == null) {
            // not found
            return this;
        }

        // removing
        if (nodeLevel == 0) {
            map.previousValue = (V) node;
            removeElementAtPos(pos);
        } else {
            final ArtNode<V> resizedNode = ((ArtNode<V>) node).remove(key, nodeLevel - 8, map);
            if (resizedNode != node) {
                // update resized node if capacity has decreased
                nodes[pos] = resizedNode;
                if (resizedNode == null) {
                    removeElementAtPos(pos);
                }
            }
        }

        // switch to ArtNode4 if too small
        if (numChildren == NODE4_SWITCH_THRESHOLD) {
            final ArtNode4<V> newNode = objectsPool.get(ObjectsPool.ART_NODE_4, ArtNode4::new);
            newNode.initFromNode16(this);
            return newNode;
        } else {
            return this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getCeilingValue(long key, int level) {
//        log.debug("key = {}", String.format("%Xh", key));
//        log.debug("level={} nodeLevel={} nodekey={} looking for key={} mask={}",
//                level, nodeLevel, String.format("%Xh", nodeKey), String.format("%Xh", key), String.format("%Xh", mask));
//
//        log.debug("key & mask = {} > nodeKey & mask = {}",
//                String.format("%Xh", key & mask), String.format("%Xh", nodeKey & mask));

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
                // can reset key, because compacted nodekey is higher
                key = 0;
            }
        }

        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);

        for (int i = 0; i < numChildren; i++) {
            final short index = keys[i];
//            log.debug("try index={} (looking for {}) key={}", String.format("%X", index), String.format("%X", nodeIndex), String.format("%X", key));
            // any equal or higher is ok
            if (index == nodeIndex) {
                final V res = nodeLevel == 0
                        ? (V) nodes[i]
                        : ((ArtNode<V>) nodes[i]).getCeilingValue(key, nodeLevel - 8);
                if (res != null) {
                    // return if found ceiling, otherwise will try next one
                    return res;
                }
            }
            if (index > nodeIndex) {
                // exploring first higher key
                return nodeLevel == 0
                        ? (V) nodes[i]
                        : ((ArtNode<V>) nodes[i]).getCeilingValue(0, nodeLevel - 8); // take lowest existing key
            }
        }
        return null;
    }


    @Override
    @SuppressWarnings("unchecked")
    public V getFloorValue(long key, int level) {
        //        log.debug("key = {}", String.format("%Xh", key));
//        log.debug("level={} nodeLevel={} nodekey={} looking for key={} mask={}",
//                level, nodeLevel, String.format("%Xh", nodeKey), String.format("%Xh", key), String.format("%Xh", mask));

        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
//            log.debug("key & mask = {} > nodeKey & mask = {}",
//                    String.format("%Xh", key & mask), String.format("%Xh", nodeKey & mask));
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

        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);

        for (int i = numChildren - 1; i >= 0; i--) {
            final short index = keys[i];
            if (index == nodeIndex) {
                final V res = nodeLevel == 0
                        ? (V) nodes[i]
                        : ((ArtNode<V>) nodes[i]).getFloorValue(key, nodeLevel - 8);
                if (res != null) {
                    // return if found ceiling, otherwise will try next one
                    return res;
                }
            }
            if (index < nodeIndex) {
                // exploring first lower key
                return nodeLevel == 0
                        ? (V) nodes[i]
                        : ((ArtNode<V>) nodes[i]).getFloorValue(UNSIGNED_MAX_KEY, nodeLevel - 8); // take highest existing key
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V valueNextTo(final int idx, final boolean lower) {
        if (lower) {
            for (int i = numChildren - 1; i >= 0; i--) {
                if (keys[i] < idx) {
                    return (V) nodes[i];
                }
            }
        } else {
            for (int i = 0; i < numChildren; i++) {
                if (keys[i] > idx) {
                    return (V) nodes[i];
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
        while (pos < numChildren && keys[pos] < idx) {
            pos++;
        }
        if (pos == numChildren) {
            return false;
        }
        if (nodeLevel == 0) {
            cursor.setTop(this, (pos << 8) | keys[pos], nodeKey);
            return true;
        }
        if (keys[pos] == idx) {
            cursor.pushInner(this, (pos << 8) | idx);
            if (((ArtNode<V>) nodes[pos]).seekCeiling(cursor, key, nodeLevel - 8)) {
                return true;
            }
            cursor.popInner();
            if (++pos == numChildren) {
                return false;
            }
        }
        cursor.pushInner(this, (pos << 8) | keys[pos]);
        ((ArtNode<V>) nodes[pos]).pushFirst(cursor);
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
        while (pos >= 0 && keys[pos] > idx) {
            pos--;
        }
        if (pos < 0) {
            return false;
        }
        if (nodeLevel == 0) {
            cursor.setTop(this, (pos << 8) | keys[pos], nodeKey);
            return true;
        }
        if (keys[pos] == idx) {
            cursor.pushInner(this, (pos << 8) | idx);
            if (((ArtNode<V>) nodes[pos]).seekFloor(cursor, key, nodeLevel - 8)) {
                return true;
            }
            cursor.popInner();
            if (--pos < 0) {
                return false;
            }
        }
        cursor.pushInner(this, (pos << 8) | keys[pos]);
        ((ArtNode<V>) nodes[pos]).pushLast(cursor);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    void pushFirst(final LongAdaptiveRadixTreeMap.Cursor<V> cursor) {
        if (nodeLevel == 0) {
            cursor.setTop(this, keys[0], nodeKey);
        } else {
            cursor.pushInner(this, keys[0]);
            ((ArtNode<V>) nodes[0]).pushFirst(cursor);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    void pushLast(final LongAdaptiveRadixTreeMap.Cursor<V> cursor) {
        final int last = numChildren - 1;
        if (nodeLevel == 0) {
            cursor.setTop(this, (last << 8) | keys[last], nodeKey);
        } else {
            cursor.pushInner(this, (last << 8) | keys[last]);
            ((ArtNode<V>) nodes[last]).pushLast(cursor);
        }
    }

    @Override
    int slotNext(final int slot) {
        final int i = (slot >>> 8) + 1;
        return i < numChildren ? (i << 8) | keys[i] : -1;
    }

    @Override
    int slotPrev(final int slot) {
        final int i = (slot >>> 8) - 1;
        return i >= 0 ? (i << 8) | keys[i] : -1;
    }

    @Override
    Object childAtSlot(final int slot) {
        return nodes[slot >>> 8];
    }

    @Override
    @SuppressWarnings("unchecked")
    public long firstKey() {
        return nodeLevel == 0
                ? (nodeKey & -256L) | keys[0]
                : ((ArtNode<V>) nodes[0]).firstKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public long lastKey() {
        final int last = numChildren - 1;
        return nodeLevel == 0
                ? (nodeKey & -256L) | keys[last]
                : ((ArtNode<V>) nodes[last]).lastKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            // map key of index 0: the low byte is zero, so adding an index never reaches the flipped sign bit
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            final int n = Math.min(numChildren, limit);
            for (int i = 0; i < n; i++) {
                consumer.accept(keyBase + keys[i], (V) nodes[i]);
            }
            return n;
        } else {
            int numLeft = limit;
            for (int i = 0; i < numChildren && numLeft > 0; i++) {
                numLeft -= ((ArtNode<V>) nodes[i]).forEach(consumer, numLeft);
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
        while (pos < numChildren && keys[pos] < idx) {
            pos++;
        }
        int found = 0;
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; pos < numChildren && found < limit; pos++) {
                consumer.accept(keyBase + keys[pos], (V) nodes[pos]);
                found++;
            }
            return found;
        }
        if (pos < numChildren && keys[pos] == idx) {
            found += ((ArtNode<V>) nodes[pos]).forEachFrom(key, nodeLevel - 8, consumer, limit);
            pos++;
        }
        for (; pos < numChildren && found < limit; pos++) {
            found += ((ArtNode<V>) nodes[pos]).forEach(consumer, limit - found);
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
        while (pos >= 0 && keys[pos] > idx) {
            pos--;
        }
        int found = 0;
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; pos >= 0 && found < limit; pos--) {
                consumer.accept(keyBase + keys[pos], (V) nodes[pos]);
                found++;
            }
            return found;
        }
        if (pos >= 0 && keys[pos] == idx) {
            found += ((ArtNode<V>) nodes[pos]).forEachDescFrom(key, nodeLevel - 8, consumer, limit);
            pos--;
        }
        for (; pos >= 0 && found < limit; pos--) {
            found += ((ArtNode<V>) nodes[pos]).forEachDesc(consumer, limit - found);
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
                consumer.accept(keyBase + keys[i], (V) nodes[i]);
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = numChildren - 1; i >= 0 && numLeft > 0; i--) {
                numLeft -= ((ArtNode<V>) nodes[i]).forEachDesc(consumer, numLeft);
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
                numLeft -= ((ArtNode<V>) nodes[i]).size(numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
        if (numChildren > 16 || numChildren <= NODE4_SWITCH_THRESHOLD)
            throw new IllegalStateException("unexpected numChildren");
        short last = -1;
        for (int i = 0; i < 16; i++) {
            Object node = nodes[i];
            if (i < numChildren) {
                if (node == null) throw new IllegalStateException("null node");
                if (keys[i] < 0 || keys[i] >= 256) throw new IllegalStateException("key out of range");
                if (keys[i] == last) throw new IllegalStateException("duplicate key");
                if (keys[i] < last) throw new IllegalStateException("wrong key order");
                last = keys[i];
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
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren, idx -> keys[idx], idx -> nodes[idx]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries() {
        final long keyPrefix = LongAdaptiveRadixTreeMap.flipSignBit(nodeKey & (-1L << 8)); // map key of index 0
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        for (int i = 0; i < numChildren; i++) {
            if (nodeLevel == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefix + keys[i], (V) nodes[i]));
            } else {
                list.addAll(((ArtNode<V>) nodes[i]).entries());
            }
        }
        return list;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void recycle() {
        if (nodeLevel != 0) {
            for (int i = 0; i < numChildren; i++) {
                ((ArtNode<V>) nodes[i]).recycle();
            }
        }
        Arrays.fill(nodes, null);
        numChildren = 0;
        objectsPool.put(ObjectsPool.ART_NODE_16, this);
    }

    private void removeElementAtPos(final int pos) {
        final int ppos = pos + 1;
        final int copyLength = numChildren - ppos;
        if (copyLength != 0) {
            System.arraycopy(keys, ppos, keys, pos, copyLength);
            System.arraycopy(nodes, ppos, nodes, pos, copyLength);
        }
        numChildren--;
        nodes[numChildren] = null;
    }

    @Override
    public ObjectsPool getObjectsPool() {
        return objectsPool;
    }

    @Override
    public String toString() {
        return "ArtNode16{" +
                "nodeKey=" + nodeKey +
                ", nodeLevel=" + nodeLevel +
                ", numChildren=" + numChildren +
                '}';
    }
}
