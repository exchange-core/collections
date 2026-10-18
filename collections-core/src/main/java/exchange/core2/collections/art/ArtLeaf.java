/*
 * Copyright 2026 Maksim Zheravin
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
 * A key alone below a branch - the lazy expansion of the ART paper: the full key and the value in one object of
 * 24 bytes, one load, where an ArtNode4 with a single key took three objects (96 bytes) and three dependent loads.
 * Price levels far from the market and old orders among cancelled ones are mostly alone in their 256-key block.
 * <p>
 * A leaf takes the place of a child node. A node at level 0 (the last key byte) holds its values directly and
 * never points to a leaf; a second key turns the leaf into an ArtNode4, and the last key of an ArtNode4 at level 0
 * goes back into a leaf.
 */
final class ArtLeaf<V> extends ArtNode<V> {

    /** internal key: the sign bit flipped, see LongAdaptiveRadixTreeMap.flipSignBit */
    long key;
    V value;

    private final ObjectsPool objectsPool;

    ArtLeaf(ObjectsPool objectsPool) {
        this.objectsPool = objectsPool;
    }

    static <V> ArtLeaf<V> create(final ObjectsPool objectsPool, final long key, final V value) {
        final ArtLeaf<V> leaf = objectsPool.get(ObjectsPool.ART_LEAF, ArtLeaf::new);
        leaf.key = key;
        leaf.value = value;
        return leaf;
    }

    @Override
    public V getValue(final long key, final int level) {
        return key == this.key ? value : null;
    }

    @Override
    public ArtNode<V> put(final long key, final int level, final V value, final LongAdaptiveRadixTreeMap<V> map) {
        if (key == this.key) {
            map.previousValue = this.value;
            this.value = value;
            return null;
        }
        // a second key: a Node4 at the highest byte where the two keys differ - not above the level of this slot,
        // as both keys came through the same parents
        final int newLevel = (63 - Long.numberOfLeadingZeros(key ^ this.key)) & 0xF8;
        final ArtNode4<V> node = objectsPool.get(ObjectsPool.ART_NODE_4, ArtNode4::new);
        if (newLevel == 0) {
            // only the last byte differs: a node at level 0 holds the values itself
            node.initTwoKeys(this.key, this.value, key, value, 0);
            recycle();
        } else {
            node.initTwoKeys(this.key, this, key, create(objectsPool, key, value), newLevel);
        }
        return node;
    }

    @Override
    public ArtNode<V> remove(final long key, final int level, final LongAdaptiveRadixTreeMap<V> map) {
        if (key != this.key) {
            return this;
        }
        map.previousValue = value;
        recycle();
        return null;
    }

    @Override
    public V getCeilingValue(final long key, final int level) {
        // unsigned: the internal keys are ordered by their bytes
        return Long.compareUnsigned(this.key, key) >= 0 ? value : null;
    }

    @Override
    public V getFloorValue(final long key, final int level) {
        return Long.compareUnsigned(this.key, key) <= 0 ? value : null;
    }

    @Override
    public long firstKey() {
        return key;
    }

    @Override
    public long lastKey() {
        return key;
    }

    @Override
    public int forEach(final LongObjConsumer<V> consumer, final int limit) {
        if (limit <= 0) {
            return 0;
        }
        consumer.accept(LongAdaptiveRadixTreeMap.flipSignBit(key), value);
        return 1;
    }

    @Override
    public int forEachDesc(final LongObjConsumer<V> consumer, final int limit) {
        return forEach(consumer, limit);
    }

    @Override
    public void recycle() {
        value = null;
        objectsPool.put(ObjectsPool.ART_LEAF, this);
    }

    @Override
    public int size(final int limit) {
        return 1;
    }

    @Override
    public void validateInternalState(final int level) {
        if (value == null) {
            throw new IllegalStateException("leaf without a value");
        }
    }

    @Override
    public String printDiagram(final String prefix, final int level) {
        return String.format("─[%016X] = %s", key, value) + "\n" + prefix;
    }

    @Override
    public List<Map.Entry<Long, V>> entries() {
        final List<Map.Entry<Long, V>> list = new ArrayList<>(1);
        list.add(new LongAdaptiveRadixTreeMap.Entry<>(LongAdaptiveRadixTreeMap.flipSignBit(key), value));
        return list;
    }

    @Override
    public ObjectsPool getObjectsPool() {
        return objectsPool;
    }

    @Override
    public String toString() {
        return "ArtLeaf{key=" + key + '}';
    }
}
