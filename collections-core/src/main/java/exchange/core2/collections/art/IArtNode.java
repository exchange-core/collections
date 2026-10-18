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

public interface IArtNode<V> {

    V getValue(long key, int level);

    /**
     * @param map receives the value the put replaced in {@code map.previousValue} (stays null for a new key)
     * @return the node that replaces this one (grown or branched), or null if this node stays
     */
    IArtNode<V> put(long key, int level, V value, LongAdaptiveRadixTreeMap<V> map);

    /**
     * @param map receives the removed value in {@code map.previousValue} (stays null if the key is absent)
     * @return the node that replaces this one (shrunk or collapsed), this node, or null if it became empty
     */
    IArtNode<V> remove(long key, int level, LongAdaptiveRadixTreeMap<V> map);

    V getCeilingValue(long key, int level);

    V getFloorValue(long key, int level);

    /** @return the lowest internal key of this subtree (the subtree is never empty) */
    long firstKey();

    /** @return the highest internal key of this subtree (the subtree is never empty) */
    long lastKey();

    /** hands out map keys (signed), not the internal ones */
    int forEach(LongObjConsumer<V> consumer, int limit);

    /** hands out map keys (signed), not the internal ones */
    int forEachDesc(LongObjConsumer<V> consumer, int limit);

    /**
     * Returns this node and its whole subtree into the pool. The node must be unreachable afterwards.
     */
    void recycle();

    /**
     * Get number of elements
     * Slow operation - O(n) complexity
     *
     * @param limit - can provide value to operation increase performance
     * @return if returned value less than limit - it is precise size of the node
     */
    int size(int limit);

    /**
     * For testing only
     *
     * @param level level
     */
    void validateInternalState(int level);

    /**
     * For testing only
     *
     * @param prefix prefix
     * @param level  level
     * @return internal diagram part
     */
    String printDiagram(String prefix, int level);

    /**
     * For testing only
     *
     * @return list of entries, map keys (signed)
     */
    List<Map.Entry<Long, V>> entries();


    ObjectsPool getObjectsPool();
}
