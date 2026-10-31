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
package exchange.core2.collections.objpool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class ObjectsPool {

    public static final int ORDER = 0;

    public static final int DIRECT_ORDER = 1;
    public static final int DIRECT_BUCKET = 2;
    public static final int ART_NODE_4 = 8;
    public static final int ART_NODE_16 = 9;
    public static final int ART_NODE_48 = 10;
    public static final int ART_NODE_256 = 11;
    public static final int SYMBOL_POSITION_RECORD = 12;
    /** a single key of a LongAdaptiveRadixTreeMap; unless configured, as many as ART_NODE_4 */
    public static final int ART_LEAF = 13;

    /** every type up to this one gets a stack, an empty one if not configured */
    private static final int LAST_KNOWN_TYPE = ART_LEAF;

    private final ArrayStack[] pools;


    /**
     * A pool for the nodes of {@link exchange.core2.collections.art.LongAdaptiveRadixTreeMap} only: up to
     * {@code nodes} leaves and 4-key nodes, half as many 16-key nodes, a quarter as many 48-key and an eighth as many
     * 256-key ones - the bigger the node, the fewer of them a tree has. One pool can serve several maps of one thread.
     *
     * @param nodes 0 keeps none: every node a map takes is new, every node it frees is garbage
     */
    public static ObjectsPool forArt(final int nodes) {
        if (nodes < 0) {
            throw new IllegalArgumentException("pool capacity must not be negative: " + nodes);
        }
        final HashMap<Integer, Integer> config = new HashMap<>();
        config.put(ART_LEAF, nodes);
        config.put(ART_NODE_4, nodes);
        config.put(ART_NODE_16, nodes / 2);
        config.put(ART_NODE_48, nodes / 4);
        config.put(ART_NODE_256, nodes / 8);
        return new ObjectsPool(config);
    }

    /**
     * The pool of the exchange-core tests: orders, buckets and the ART nodes. A pool for ART maps alone:
     * {@link #forArt(int)}.
     */
    public static ObjectsPool createDefaultTestPool() {

        // initialize object pools
        final HashMap<Integer, Integer> objectsPoolConfig = new HashMap<>();
        objectsPoolConfig.put(ObjectsPool.DIRECT_ORDER, 512);
        objectsPoolConfig.put(ObjectsPool.DIRECT_BUCKET, 256);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_4, 256);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_16, 128);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_48, 64);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_256, 32);
        objectsPoolConfig.put(ObjectsPool.ART_LEAF, 256);

        return new ObjectsPool(objectsPoolConfig);
    }

    /**
     * @param sizesConfig capacity per type; a type left out gets an empty stack (get allocates, put drops),
     *                    except ART_LEAF, which gets the capacity of ART_NODE_4 - configurations written before the
     *                    leaves existed keep recycling them
     */
    public ObjectsPool(final Map<Integer, Integer> sizesConfig) {
        final int maxType = Math.max(LAST_KNOWN_TYPE, sizesConfig.keySet().stream().max(Integer::compareTo).orElse(0));
        this.pools = new ArrayStack[maxType + 1];
        for (int type = 0; type <= maxType; type++) {
            this.pools[type] = new ArrayStack(sizesConfig.getOrDefault(type, 0));
        }
        if (!sizesConfig.containsKey(ART_LEAF)) {
            this.pools[ART_LEAF] = new ArrayStack(sizesConfig.getOrDefault(ART_NODE_4, 0));
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final int type, final Supplier<T> supplier) {
        final T obj = (T) pools[type].pop();  // pollFirst is cheaper for empty pool

        if (obj == null) {
//            log.debug("MISS {}", type);
            return supplier.get();
        } else {
//            log.debug("HIT {} (count={})", type, pools[type].count);
            return obj;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final int type, final Function<ObjectsPool, T> constructor) {
        final T obj = (T) pools[type].pop();  // pollFirst is cheaper for empty pool

        if (obj == null) {
//            log.debug("MISS {}", type);
            return constructor.apply(this);
        } else {
//            log.debug("HIT {} (count={})", type, pools[type].count);
            return obj;
        }
    }

    public void put(final int type, Object object) {
//        log.debug("RETURN {} (count={})", type, pools[type].count);
        pools[type].add(object);
    }

    private final static class ArrayStack {
        private int count;
        private Object[] objects;

        ArrayStack(int fixedSize) {
            this.objects = new Object[fixedSize];
            this.count = 0;
        }

        void add(Object element) {
            if (count != objects.length) {
                objects[count] = element;
                count++;
            }
        }

        Object pop() {
            if (count != 0) {
                count--;
                Object object = objects[count];
                objects[count] = null;
                return object;
            }
            return null;
        }
    }


}
