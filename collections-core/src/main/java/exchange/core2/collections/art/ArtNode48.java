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

import java.util.*;

/**
 * As the number of entries in a node increases,
 * searching the key array becomes expensive. Therefore, nodes
 * with more than 16 pointers do not store the keys explicitly.
 * Instead, a 256-element array is used, which can be indexed
 * with key bytes directly. If a node has between 17 and 48 child
 * pointers, this array stores indexes into a second array which
 * contains up to 48 pointers. This indirection saves space in
 * comparison to 256 pointers of 8 bytes, because the indexes
 * only require 6 bits (we use 1 byte for simplicity).
 */
public final class ArtNode48<V> extends ArtNode<V> {

    /** highest possible key in the unsigned key space the nodes work in */
    private static final long UNSIGNED_MAX_KEY = -1L;

    private static final int NODE16_SWITCH_THRESHOLD = 12;

    // just keep indexes
    final byte[] indexes = new byte[256];
    final Object[] nodes = new Object[48];
    private long freeBitMask;

    // slots of indexes that have a child - see Bitmap256
    final long[] occupied = new long[4];

    long nodeKey;
    int nodeLevel;

    byte numChildren;

    private final ObjectsPool objectsPool;

    public ArtNode48(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    void initFromNode16(ArtNode16<V> node16, short subKey, Object newElement) {

        final byte sourceSize = 16;
        Arrays.fill(this.indexes, (byte) -1);
        this.numChildren = sourceSize + 1;
        this.nodeLevel = node16.nodeLevel;
        this.nodeKey = node16.nodeKey;

        Arrays.fill(occupied, 0);
        for (byte i = 0; i < sourceSize; i++) {
            this.indexes[node16.keys[i]] = i;
            this.nodes[i] = node16.nodes[i];
            setOccupied(node16.keys[i]);
        }

        this.indexes[subKey] = sourceSize;
        this.nodes[sourceSize] = newElement;
        setOccupied(subKey);
        this.freeBitMask = (1L << (sourceSize + 1)) - 1;

        Arrays.fill(node16.nodes, null);
        objectsPool.put(ObjectsPool.ART_NODE_16, node16);
    }

    void initFromNode256(ArtNode256<V> node256) {
        Arrays.fill(this.indexes, (byte) -1);
        this.numChildren = (byte) node256.numChildren;
        this.nodeLevel = node256.nodeLevel;
        this.nodeKey = node256.nodeKey;
        byte idx = 0;
        for (int i = node256.nextOccupied(0); i >= 0; i = node256.nextOccupied(i + 1)) {
            this.indexes[i] = idx;
            this.nodes[idx] = node256.nodes[i];
            idx++;
        }
        System.arraycopy(node256.occupied, 0, this.occupied, 0, 4);
        this.freeBitMask = (1L << (numChildren)) - 1;

        Arrays.fill(node256.nodes, null);
        objectsPool.put(ObjectsPool.ART_NODE_256, node256);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return null;
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex != -1) {
            final Object node = nodes[nodeIndex];
            return nodeLevel == 0
                    ? (V) node
                    : ((ArtNode<V>) node).getValue(key, nodeLevel - 8);
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
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex != -1) {
            // found
            if (nodeLevel == 0) {
                map.previousValue = (V) nodes[nodeIndex];
                nodes[nodeIndex] = value;
                map.rememberPut(this);
            } else {
                final ArtNode<V> resizedNode = ((ArtNode<V>) nodes[nodeIndex]).put(key, nodeLevel - 8, value, map);
                if (resizedNode != null) {
                    // update resized node if capacity has increased
                    nodes[nodeIndex] = resizedNode;
                }
            }
            return null;
        }

        // not found, put new element

        if (numChildren != 48) {
            // capacity less than 48 - can simply insert node
            final byte freePosition = (byte) Long.numberOfTrailingZeros(~freeBitMask);
            indexes[idx] = freePosition;
            setOccupied(idx);

            if (nodeLevel == 0) {
                nodes[freePosition] = value;
                map.rememberPut(this);
            } else {
                final ArtLeaf<V> newSubNode = ArtLeaf.create(objectsPool, key, value);
                nodes[freePosition] = newSubNode;
            }
            numChildren++;
            freeBitMask = freeBitMask ^ (1L << freePosition);
            return null;

        } else {
            // no space left, create a ArtNode256 containing a new item
            final Object newElement;
            if (nodeLevel == 0) {
                newElement = value;
            } else {
                final ArtLeaf<V> newSubNode = ArtLeaf.create(objectsPool, key, value);
                newElement = newSubNode;
            }

            ArtNode256<V> node256 = objectsPool.get(ObjectsPool.ART_NODE_256, ArtNode256::new);
            node256.initFromNode48(this, idx, newElement);
            if (node256.nodeLevel == 0) {
                map.rememberPut(node256);
            }
            return node256;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> remove(final long key, final int level, final LongAdaptiveRadixTreeMap<V> map) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return this;
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex == -1) {
            return this;
        }

        if (nodeLevel == 0) {
            map.previousValue = (V) nodes[nodeIndex];
            nodes[nodeIndex] = null;
            indexes[idx] = -1;
            clearOccupied(idx);
            numChildren--;
            freeBitMask = freeBitMask ^ (1L << nodeIndex);
        } else {
            final ArtNode<V> node = (ArtNode<V>) nodes[nodeIndex];
            final ArtNode<V> resizedNode = node.remove(key, nodeLevel - 8, map);
            if (resizedNode != node) {
                // update resized node if capacity has decreased
                nodes[nodeIndex] = resizedNode;
                if (resizedNode == null) {
                    numChildren--;
                    indexes[idx] = -1;
                    clearOccupied(idx);
                    freeBitMask = freeBitMask ^ (1L << nodeIndex);
                }
            }
        }

        if (numChildren == NODE16_SWITCH_THRESHOLD) {
            final ArtNode16<V> newNode = objectsPool.get(ObjectsPool.ART_NODE_16, ArtNode16::new);
            newNode.initFromNode48(this);
            return newNode;
        } else {
            return this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getCeilingValue(long key, int level) {
//        log.debug("key = {}", String.format("%Xh", key));
        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
//            log.debug("key & mask = {} > nodeKey & mask = {}", String.format("%Xh", key & mask), String.format("%Xh", nodeKey & mask));
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

        final int idx = (int) ((key >>> nodeLevel) & 0xFF);

        final byte index = indexes[idx];
        if (index != -1) {
            // if exact key found
            final V res = nodeLevel == 0 ? (V) nodes[index] : ((ArtNode<V>) nodes[index]).getCeilingValue(key, nodeLevel - 8);
            if (res != null) {
                // return if found ceiling, otherwise will try next one
                return res;
            }
        }

        // if exact key not found - the first higher key
        final int next = nextOccupied(idx + 1);
        if (next >= 0) {
            final Object node = nodes[indexes[next]];
            return nodeLevel == 0
                    ? (V) node
                    : ((ArtNode<V>) node).getCeilingValue(0, nodeLevel - 8); // find first lowest key
        }

        // no keys found
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getFloorValue(long key, int level) {
        //        log.debug("key = {}", String.format("%Xh", key));
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

        final int idx = (int) ((key >>> nodeLevel) & 0xFF);

        final byte index = indexes[idx];
        if (index != -1) {
            // if exact key found
            final V res = nodeLevel == 0 ? (V) nodes[index] : ((ArtNode<V>) nodes[index]).getFloorValue(key, nodeLevel - 8);
            if (res != null) {
                // return if found ceiling, otherwise will try prev one
                return res;
            }
        }

        // if exact key not found - the first lower key
        final int prev = prevOccupied(idx - 1);
        if (prev >= 0) {
            final Object node = nodes[indexes[prev]];
            return nodeLevel == 0
                    ? (V) node
                    : ((ArtNode<V>) node).getFloorValue(UNSIGNED_MAX_KEY, nodeLevel - 8); // find first highest key
        }

        // no keys found
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V valueNextTo(final int idx, final boolean lower) {
        final int slot = lower ? prevOccupied(idx - 1) : nextOccupied(idx + 1);
        return slot < 0 ? null : (V) nodes[indexes[slot]];
    }

    @Override
    long nodeKey() {
        return nodeKey;
    }

    @Override
    int nodeLevel() {
        return nodeLevel;
    }

    @Override
    int slotCeiling(final int idx) {
        return nextOccupied(idx);
    }

    @Override
    int slotFloor(final int idx) {
        return prevOccupied(idx);
    }

    @Override
    int slotNext(final int slot) {
        return nextOccupied(slot + 1);
    }

    @Override
    int slotPrev(final int slot) {
        return prevOccupied(slot - 1);
    }

    @Override
    Object childAtSlot(final int slot) {
        return nodes[indexes[slot]];
    }

    @Override
    @SuppressWarnings("unchecked")
    public long firstKey() {
        final int idx = nextOccupied(0);
        return nodeLevel == 0
                ? (nodeKey & -256L) | idx
                : ((ArtNode<V>) nodes[indexes[idx]]).firstKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public long lastKey() {
        final int idx = prevOccupied(255);
        return nodeLevel == 0
                ? (nodeKey & -256L) | idx
                : ((ArtNode<V>) nodes[indexes[idx]]).lastKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            // map key of index 0: the low byte is zero, so adding an index never reaches the flipped sign bit
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            int numFound = 0;
            for (int i = nextOccupied(0); i >= 0 && numFound < limit; i = nextOccupied(i + 1)) {
                consumer.accept(keyBase + i, (V) nodes[indexes[i]]);
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = nextOccupied(0); i >= 0 && numLeft > 0; i = nextOccupied(i + 1)) {
                numLeft -= ((ArtNode<V>) nodes[indexes[i]]).forEach(consumer, numLeft);
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
        int found = 0;
        int i = nextOccupied(idx);
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; i >= 0 && found < limit; i = nextOccupied(i + 1)) {
                consumer.accept(keyBase + i, (V) nodes[indexes[i]]);
                found++;
            }
            return found;
        }
        if (i == idx) {
            found += ((ArtNode<V>) nodes[indexes[i]]).forEachFrom(key, nodeLevel - 8, consumer, limit);
            i = nextOccupied(i + 1);
        }
        for (; i >= 0 && found < limit; i = nextOccupied(i + 1)) {
            found += ((ArtNode<V>) nodes[indexes[i]]).forEach(consumer, limit - found);
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
        int found = 0;
        int i = prevOccupied(idx);
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; i >= 0 && found < limit; i = prevOccupied(i - 1)) {
                consumer.accept(keyBase + i, (V) nodes[indexes[i]]);
                found++;
            }
            return found;
        }
        if (i == idx) {
            found += ((ArtNode<V>) nodes[indexes[i]]).forEachDescFrom(key, nodeLevel - 8, consumer, limit);
            i = prevOccupied(i - 1);
        }
        for (; i >= 0 && found < limit; i = prevOccupied(i - 1)) {
            found += ((ArtNode<V>) nodes[indexes[i]]).forEachDesc(consumer, limit - found);
        }
        return found;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEachDesc(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            int numFound = 0;
            for (int i = prevOccupied(255); i >= 0 && numFound < limit; i = prevOccupied(i - 1)) {
                consumer.accept(keyBase + i, (V) nodes[indexes[i]]);
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = prevOccupied(255); i >= 0 && numLeft > 0; i = prevOccupied(i - 1)) {
                numLeft -= ((ArtNode<V>) nodes[indexes[i]]).forEachDesc(consumer, numLeft);
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
            for (int i = nextOccupied(0); i >= 0 && numLeft > 0; i = nextOccupied(i + 1)) {
                numLeft -= ((ArtNode<V>) nodes[indexes[i]]).size(numLeft);
            }
            return limit - numLeft;
        }
    }


    @Override
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
        int found = 0;
        final Set<Integer> keysSet = new HashSet<>();
        long expectedBitMask = 0;
        for (int i = 0; i < 256; i++) {
            byte idx = indexes[i];
            if ((idx != -1) != (nextOccupied(i) == i)) {
                throw new IllegalStateException("occupancy bit " + i + " is wrong");
            }
            if (idx != -1) {
                if (idx > 47 || idx < -1) throw new IllegalStateException("wrong index");
                keysSet.add((int) idx);
                found++;
                if (nodes[idx] == null) throw new IllegalStateException("null node");
                expectedBitMask ^= (1L << idx);
            }
        }
        if (freeBitMask != expectedBitMask) throw new IllegalStateException("freeBitMask is wrong");
        if (found != numChildren) throw new IllegalStateException("wrong numChildren");
        if (keysSet.size() != numChildren) {
            throw new IllegalStateException("duplicate keys keysSet=" + keysSet + " numChildren=" + numChildren);
        }
        if (numChildren > 48 || numChildren <= NODE16_SWITCH_THRESHOLD)
            throw new IllegalStateException("unexpected numChildren");
        for (int i = 0; i < 48; i++) {
            Object node = nodes[i];
            if (keysSet.contains(i)) {
                if (node == null) {
                    throw new IllegalStateException("null node");
                } else {
                    if (node instanceof ArtNode) {
                        if (nodeLevel == 0) throw new IllegalStateException("unexpected node type");
                        ArtNode artNode = (ArtNode) node;
                        artNode.validateInternalState(nodeLevel - 8);
                    } else {
                        if (nodeLevel != 0) throw new IllegalStateException("unexpected node type");
                    }
                }
            } else {
                if (node != null) throw new IllegalStateException("not released node");
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries() {
        final long keyPrefix = LongAdaptiveRadixTreeMap.flipSignBit(nodeKey & (-1L << 8)); // map key of index 0
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        short[] keys = createKeysArray();
        for (int i = 0; i < numChildren; i++) {
            if (nodeLevel == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefix + keys[i], (V) nodes[indexes[keys[i]]]));
            } else {
                list.addAll(((ArtNode<V>) nodes[indexes[keys[i]]]).entries());
            }
        }
        return list;
    }

    @Override
    public String printDiagram(String prefix, int level) {
        final short[] keys = createKeysArray();
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren, idx -> keys[idx], idx -> nodes[indexes[keys[idx]]]);
    }

    @Override
    public ObjectsPool getObjectsPool() {
        return objectsPool;
    }

    @Override
    public String toString() {
        return "ArtNode48{" +
                "nodeKey=" + nodeKey +
                ", nodeLevel=" + nodeLevel +
                ", numChildren=" + numChildren +
                '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    public void recycle() {
        if (nodeLevel != 0) {
            for (Object node : nodes) {
                if (node != null) {
                    ((ArtNode<V>) node).recycle();
                }
            }
        }
        Arrays.fill(nodes, null);
        Arrays.fill(indexes, (byte) -1);
        freeBitMask = 0;
        Arrays.fill(occupied, 0);
        numChildren = 0;
        objectsPool.put(ObjectsPool.ART_NODE_48, this);
    }

    void setOccupied(final int idx) {
        occupied[idx >>> 6] |= 1L << idx;
    }

    private void clearOccupied(final int idx) {
        occupied[idx >>> 6] &= ~(1L << idx);
    }

    /** @return the lowest occupied slot at or above from (0..256), or -1 */
    int nextOccupied(final int from) {
        return Bitmap256.next(occupied[0], occupied[1], occupied[2], occupied[3], from);
    }

    /** @return the highest occupied slot at or below from (-1..255), or -1 */
    int prevOccupied(final int from) {
        return Bitmap256.prev(occupied[0], occupied[1], occupied[2], occupied[3], from);
    }

    private short[] createKeysArray() {
        short[] keys = new short[numChildren];
        int j = 0;
        for (short i = 0; i < 256; i++) {
            if (indexes[i] != -1) {
                keys[j++] = i;
            }
        }
        return keys;
    }
}
