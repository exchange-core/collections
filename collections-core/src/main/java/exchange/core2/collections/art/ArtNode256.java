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
 * The largest node type is simply an array of 256
 * pointers and is used for storing between 49 and 256 entries.
 * With this representation, the next node can be found very
 * efficiently using a single lookup of the key byte in that array.
 * No additional indirection is necessary. If most entries are not
 * null, this representation is also very space efficient because
 * only pointers need to be stored.
 */
public final class ArtNode256<V> extends ArtNode<V> {

    /** highest possible key in the unsigned key space the nodes work in */
    private static final long UNSIGNED_MAX_KEY = -1L;

    private static final int NODE48_SWITCH_THRESHOLD = 37;

    // direct addressing
    final Object[] nodes = new Object[256];

    // occupied slots - see Bitmap256
    final long[] occupied = new long[4];

    long nodeKey;
    int nodeLevel;
    short numChildren;

    private final ObjectsPool objectsPool;

    public ArtNode256(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    void initFromNode48(ArtNode48<V> artNode48, short subKey, Object newElement) {

        this.nodeLevel = artNode48.nodeLevel;
        this.nodeKey = artNode48.nodeKey;
        final int sourceSize = 48;
        for (int i = artNode48.nextOccupied(0); i >= 0; i = artNode48.nextOccupied(i + 1)) {
            this.nodes[i] = artNode48.nodes[artNode48.indexes[i]];
        }
        System.arraycopy(artNode48.occupied, 0, this.occupied, 0, 4);
        this.nodes[subKey] = newElement;
        setOccupied(subKey);
        this.numChildren = sourceSize + 1;

        Arrays.fill(artNode48.nodes, null);
        Arrays.fill(artNode48.indexes, (byte) -1);
        objectsPool.put(ObjectsPool.ART_NODE_48, artNode48);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return null;
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final Object node = nodes[idx];
        if (node != null) {
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
        if (nodes[idx] == null) {
            // new object will be inserted
            numChildren++;
            setOccupied(idx);
        }

        if (nodeLevel == 0) {
            map.previousValue = (V) nodes[idx];
            nodes[idx] = value;
            map.rememberPut(this);
        } else {
            ArtNode<V> node = (ArtNode<V>) nodes[idx];
            if (node != null) {
                final ArtNode<V> resizedNode = node.put(key, nodeLevel - 8, value, map);
                if (resizedNode != null) {
                    // update resized node if capacity has increased
                    nodes[idx] = resizedNode;
                }
            } else {
                final ArtLeaf<V> newSubNode = ArtLeaf.create(objectsPool, key, value);
                nodes[idx] = newSubNode;
            }
        }

        // never need to increase the size
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> remove(final long key, final int level, final LongAdaptiveRadixTreeMap<V> map) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return this;
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        if (nodes[idx] == null) {
            return this;
        }

        if (nodeLevel == 0) {
            map.previousValue = (V) nodes[idx];
            nodes[idx] = null;
            numChildren--;
            clearOccupied(idx);
        } else {
            final ArtNode<V> node = (ArtNode<V>) nodes[idx];
            final ArtNode<V> resizedNode = node.remove(key, nodeLevel - 8, map);
            if (resizedNode != node) {
                // update resized node if capacity has decreased
                nodes[idx] = resizedNode;
                if (resizedNode == null) {
                    numChildren--;
                    clearOccupied(idx);
                }
            }
        }

        if (numChildren == NODE48_SWITCH_THRESHOLD) {
            final ArtNode48<V> newNode = objectsPool.get(ObjectsPool.ART_NODE_48, ArtNode48::new);
            newNode.initFromNode256(this);
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
                // find first lowest key, because compacted nodekey is higher
                key = 0;
            }
        }

        final int idx = (int) ((key >>> nodeLevel) & 0xFF);
        final Object node = nodes[idx];
        if (node != null) {
            // if exact key found
            final V res = nodeLevel == 0 ? (V) node : ((ArtNode<V>) node).getCeilingValue(key, nodeLevel - 8);
            if (res != null) {
                // return if found ceiling, otherwise will try next one
                return res;
            }
        }

        // if exact key not found - the first higher key
        final int next = nextOccupied(idx + 1);
        if (next >= 0) {
            return (nodeLevel == 0)
                    ? (V) nodes[next]
                    : ((ArtNode<V>) nodes[next]).getCeilingValue(0, nodeLevel - 8);// find first lowest key
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
        final Object node = nodes[idx];
        if (node != null) {
            // if exact key found
            final V res = nodeLevel == 0 ? (V) node : ((ArtNode<V>) node).getFloorValue(key, nodeLevel - 8);
            if (res != null) {
                // return if found floor, otherwise will try prev one
                return res;
            }
        }

        // if exact key not found - the first lower key
        final int prev = prevOccupied(idx - 1);
        if (prev >= 0) {
            return (nodeLevel == 0)
                    ? (V) nodes[prev]
                    : ((ArtNode<V>) nodes[prev]).getFloorValue(UNSIGNED_MAX_KEY, nodeLevel - 8);// find first highest key
        }
        // no keys found
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V valueNextTo(final int idx, final boolean lower) {
        final int slot = lower ? prevOccupied(idx - 1) : nextOccupied(idx + 1);
        return slot < 0 ? null : (V) nodes[slot];
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
        return nodes[slot];
    }

    @Override
    @SuppressWarnings("unchecked")
    public long firstKey() {
        final int idx = nextOccupied(0);
        return nodeLevel == 0
                ? (nodeKey & -256L) | idx
                : ((ArtNode<V>) nodes[idx]).firstKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public long lastKey() {
        final int idx = prevOccupied(255);
        return nodeLevel == 0
                ? (nodeKey & -256L) | idx
                : ((ArtNode<V>) nodes[idx]).lastKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            // map key of index 0: the low byte is zero, so adding an index never reaches the flipped sign bit
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            int numFound = 0;
            for (int i = nextOccupied(0); i >= 0 && numFound < limit; i = nextOccupied(i + 1)) {
                consumer.accept(keyBase + i, (V) nodes[i]);
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = nextOccupied(0); i >= 0 && numLeft > 0; i = nextOccupied(i + 1)) {
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
        int found = 0;
        int i = nextOccupied(idx);
        if (nodeLevel == 0) {
            final long keyBase = LongAdaptiveRadixTreeMap.flipSignBit((nodeKey >>> 8) << 8);
            for (; i >= 0 && found < limit; i = nextOccupied(i + 1)) {
                consumer.accept(keyBase + i, (V) nodes[i]);
                found++;
            }
            return found;
        }
        if (i == idx) {
            found += ((ArtNode<V>) nodes[i]).forEachFrom(key, nodeLevel - 8, consumer, limit);
            i = nextOccupied(i + 1);
        }
        for (; i >= 0 && found < limit; i = nextOccupied(i + 1)) {
            found += ((ArtNode<V>) nodes[i]).forEach(consumer, limit - found);
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
                consumer.accept(keyBase + i, (V) nodes[i]);
                found++;
            }
            return found;
        }
        if (i == idx) {
            found += ((ArtNode<V>) nodes[i]).forEachDescFrom(key, nodeLevel - 8, consumer, limit);
            i = prevOccupied(i - 1);
        }
        for (; i >= 0 && found < limit; i = prevOccupied(i - 1)) {
            found += ((ArtNode<V>) nodes[i]).forEachDesc(consumer, limit - found);
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
                consumer.accept(keyBase + i, (V) nodes[i]);
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = prevOccupied(255); i >= 0 && numLeft > 0; i = prevOccupied(i - 1)) {
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
            for (int i = nextOccupied(0); i >= 0 && numLeft > 0; i = nextOccupied(i + 1)) {
                numLeft -= ((ArtNode<V>) nodes[i]).size(numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
        int found = 0;
        for (int i = 0; i < 256; i++) {
            Object node = nodes[i];
            if ((node != null) != (nextOccupied(i) == i)) {
                throw new IllegalStateException("occupancy bit " + i + " is wrong");
            }
            if (node != null) {
                if (node instanceof ArtNode) {
                    if (nodeLevel == 0) throw new IllegalStateException("unexpected node type");
                    ArtNode artNode = (ArtNode) node;
                    artNode.validateInternalState(nodeLevel - 8);
                } else {
                    if (nodeLevel != 0) throw new IllegalStateException("unexpected node type");
                }
                found++;
            }
        }
        if (found != numChildren) {
            throw new IllegalStateException("wrong numChildren");
        }
        if (numChildren <= NODE48_SWITCH_THRESHOLD || numChildren > 256) {
            throw new IllegalStateException("unexpected numChildren");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries() {
        final long keyPrefix = LongAdaptiveRadixTreeMap.flipSignBit(nodeKey & (-1L << 8)); // map key of index 0
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        final short[] keys = createKeysArray();
        for (int i = 0; i < numChildren; i++) {
            if (nodeLevel == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefix + keys[i], (V) nodes[keys[i]]));
            } else {
                list.addAll(((ArtNode<V>) nodes[keys[i]]).entries());
            }
        }
        return list;
    }

    @Override
    public String printDiagram(String prefix, int level) {
        final short[] keys = createKeysArray();
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren, idx -> keys[idx], idx -> nodes[keys[idx]]);
    }

    @Override
    public ObjectsPool getObjectsPool() {
        return objectsPool;
    }

    @Override
    public String toString() {
        return "ArtNode256{" +
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
        Arrays.fill(occupied, 0);
        numChildren = 0;
        objectsPool.put(ObjectsPool.ART_NODE_256, this);
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
            if (nodes[i] != null) {
                keys[j++] = i;
            }
        }
        return keys;
    }
}
