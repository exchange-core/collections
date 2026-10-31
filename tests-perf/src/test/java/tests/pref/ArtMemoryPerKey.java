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
package tests.pref;

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.objpool.ObjectsPool;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Heap per key of a filled map - ART, TreeMap, fastutil's red-black tree - for the README: the heap after a full GC
 * with the maps, less the heap before. Small maps are built in copies, 1M keys in all, so the GC noise is small
 * against them. One value object for all keys: the values are not counted. The ART maps share one pool that keeps
 * nothing: no free nodes counted. Run alone, with a heap of 1 GB or more.
 */
public class ArtMemoryPerKey {

    private static final Logger log = LoggerFactory.getLogger(ArtMemoryPerKey.class);

    private static final int KEYS = 1_000_000;

    private static final Object VALUE = new Object();

    public static void main(String[] args) {
        new ArtMemoryPerKey().bytesPerKey();
    }

    @Test
    public void bytesPerKey() {
        final Map<String, long[]> profiles = new LinkedHashMap<>();
        // as the art-keys group of CollectionsLatencySamples
        final Random rand = new Random(11);
        final long[] sequential = new long[KEYS];
        final long[] sparse = new long[KEYS];
        final long[] clustered = new long[KEYS];
        final long[] clusterNext = new long[16];
        for (int c = 0; c < clusterNext.length; c++) clusterNext[c] = (rand.nextLong() & Long.MAX_VALUE) >>> 1;
        for (int i = 0; i < KEYS; i++) {
            sequential[i] = i + 1;
            sparse[i] = (i + 1) * 256L;
            clustered[i] = clusterNext[rand.nextInt(clusterNext.length)]++;
        }
        profiles.put("sequential-1m", sequential);
        profiles.put("clustered-1m", clustered);
        profiles.put("sparse-1m", sparse);
        for (String name : new String[]{"book-1k", "book-20k", "ids-1m", "random-1m"}) {
            profiles.put(name, ArtKeyProfiles.parse(name, 1).keys);
        }

        log.info(String.format("%-14s %8s %8s %8s  (bytes per key)", "keys", "ART", "TreeMap", "fastutil"));
        for (Map.Entry<String, long[]> p : profiles.entrySet()) {
            final long[] keys = p.getValue();
            final int copies = Math.max(1, KEYS / keys.length);
            log.info(String.format("%-14s %8.1f %8.1f %8.1f", p.getKey(),
                    perKey(keys, copies, ArtMemoryPerKey::art),
                    perKey(keys, copies, ArtMemoryPerKey::treeMap),
                    perKey(keys, copies, ArtMemoryPerKey::rbTree)));
        }
        log.info(String.format("an empty ART map: %.0f bytes with a pool of its own, %.0f with poolCapacity(0)",
                perKey(new long[0], KEYS / 10, keys -> new LongAdaptiveRadixTreeMap<>()),
                perKey(new long[0], KEYS / 10, keys -> LongAdaptiveRadixTreeMap.builder().poolCapacity(0).build())));
    }

    private interface MapFactory {
        Object filled(long[] keys);
    }

    private static final ObjectsPool NO_FREE_NODES = ObjectsPool.forArt(0);

    private static Object art(final long[] keys) {
        final LongAdaptiveRadixTreeMap<Object> map = LongAdaptiveRadixTreeMap.builder().sharedPool(NO_FREE_NODES).build();
        for (long key : keys) map.put(key, VALUE);
        return map;
    }

    private static Object treeMap(final long[] keys) {
        final TreeMap<Long, Object> map = new TreeMap<>();
        for (long key : keys) map.put(key, VALUE);
        return map;
    }

    private static Object rbTree(final long[] keys) {
        final Long2ObjectRBTreeMap<Object> map = new Long2ObjectRBTreeMap<>();
        for (long key : keys) map.put(key, VALUE);
        return map;
    }

    /** bytes per key; for no keys, bytes per map */
    private static double perKey(final long[] keys, final int copies, final MapFactory factory) {
        final Object[] maps = new Object[copies];
        final long before = usedAfterGc();
        for (int c = 0; c < copies; c++) {
            maps[c] = factory.filled(keys);
        }
        final long after = usedAfterGc();
        // the maps must be alive at the second measurement
        Reference.reachabilityFence(maps);
        return (double) (after - before) / ((long) copies * Math.max(1, keys.length));
    }

    private static long usedAfterGc() {
        for (int i = 0; i < 4; i++) {
            System.gc();
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}
