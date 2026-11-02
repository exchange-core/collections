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

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap.Cursor;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * {@link LongAdaptiveRadixTreeMap#asNavigableMap()}: the map, or a range of its keys, in either order, as a
 * {@code NavigableMap<Long, V>} - the bounds, their checks and the views as in {@code TreeMap}'s sub-maps.
 * <p>
 * The keys are longs, so a range is also kept as its lowest and its highest key, both inclusive: a navigation clamps
 * the key to them and seeks once. The bounds as they were given only decide which nested views are allowed - TreeMap's
 * rule, under which an exclusive bound can bound a nested view too. Each navigation and each iterator takes a new
 * cursor of the map, so a view keeps nothing of the map but the map.
 */
final class ArtNavigableMap<V> extends AbstractMap<Long, V> implements NavigableMap<Long, V> {

    private final LongAdaptiveRadixTreeMap<V> map;

    // the bounds as given; the whole map is [MIN_VALUE, MAX_VALUE], both inclusive
    private final long lo;
    private final boolean loInclusive;
    private final long hi;
    private final boolean hiInclusive;

    // the same range as its lowest and its highest key; min > max if no key fits
    private final long min;
    private final long max;

    private final boolean descending;

    ArtNavigableMap(final LongAdaptiveRadixTreeMap<V> map) {
        this(map, Long.MIN_VALUE, true, Long.MAX_VALUE, true, false);
    }

    private ArtNavigableMap(final LongAdaptiveRadixTreeMap<V> map,
                            final long lo,
                            final boolean loInclusive,
                            final long hi,
                            final boolean hiInclusive,
                            final boolean descending) {
        this.map = map;
        this.lo = lo;
        this.loInclusive = loInclusive;
        this.hi = hi;
        this.hiInclusive = hiInclusive;
        this.descending = descending;
        // nothing above MAX_VALUE, nothing below MIN_VALUE
        final boolean none = !loInclusive && lo == Long.MAX_VALUE || !hiInclusive && hi == Long.MIN_VALUE;
        this.min = none ? Long.MAX_VALUE : loInclusive ? lo : lo + 1;
        this.max = none ? Long.MIN_VALUE : hiInclusive ? hi : hi - 1;
    }

    private boolean inRange(final long key) {
        return key >= min && key <= max;
    }

    private boolean whole() {
        return min == Long.MIN_VALUE && max == Long.MAX_VALUE;
    }

    // ---- lookups and changes ----

    @Override
    public V get(final Object key) {
        final long k = (Long) key;
        return inRange(k) ? map.get(k) : null;
    }

    @Override
    public boolean containsKey(final Object key) {
        final long k = (Long) key;
        return inRange(k) && map.containsKey(k);
    }

    /**
     * @throws IllegalArgumentException if the key is out of the range of the view
     * @throws NullPointerException     if the key or the value is null
     */
    @Override
    public V put(final Long key, final V value) {
        final long k = key;
        if (!inRange(k)) {
            throw new IllegalArgumentException("key out of range: " + k);
        }
        return map.put(k, value);
    }

    @Override
    public V remove(final Object key) {
        final long k = (Long) key;
        return inRange(k) ? map.remove(k) : null;
    }

    /** the whole map: the counter; a range: its keys counted, as TreeMap's sub-maps do - O(keys in it) */
    @Override
    public int size() {
        if (whole()) {
            return map.size();
        }
        final Cursor<V> c = map.cursor();
        int n = 0;
        for (boolean on = absCeiling(c, min); on; on = c.next() && c.key() <= max) {
            n++;
        }
        return n;
    }

    @Override
    public boolean isEmpty() {
        return whole() ? map.isEmpty() : !absCeiling(map.cursor(), min);
    }

    @Override
    public void clear() {
        if (whole()) {
            map.clear();
            return;
        }
        final Cursor<V> c = map.cursor();
        for (boolean on = absCeiling(c, min); on; on = c.isValid() && c.key() <= max) {
            c.remove();
        }
    }

    // ---- navigation, in the order of the view ----

    @Override
    public Map.Entry<Long, V> firstEntry() {
        final Cursor<V> c = map.cursor();
        return entry(c, seekFirst(c));
    }

    @Override
    public Map.Entry<Long, V> lastEntry() {
        final Cursor<V> c = map.cursor();
        return entry(c, seekLast(c));
    }

    @Override
    public Map.Entry<Long, V> pollFirstEntry() {
        final Cursor<V> c = map.cursor();
        return poll(c, seekFirst(c));
    }

    @Override
    public Map.Entry<Long, V> pollLastEntry() {
        final Cursor<V> c = map.cursor();
        return poll(c, seekLast(c));
    }

    @Override
    public Long firstKey() {
        final Cursor<V> c = map.cursor();
        return keyOrThrow(c, seekFirst(c));
    }

    @Override
    public Long lastKey() {
        final Cursor<V> c = map.cursor();
        return keyOrThrow(c, seekLast(c));
    }

    @Override
    public Map.Entry<Long, V> ceilingEntry(final Long key) {
        final Cursor<V> c = map.cursor();
        return entry(c, seekCeiling(c, key));
    }

    @Override
    public Long ceilingKey(final Long key) {
        final Cursor<V> c = map.cursor();
        return key(c, seekCeiling(c, key));
    }

    @Override
    public Map.Entry<Long, V> floorEntry(final Long key) {
        final Cursor<V> c = map.cursor();
        return entry(c, seekFloor(c, key));
    }

    @Override
    public Long floorKey(final Long key) {
        final Cursor<V> c = map.cursor();
        return key(c, seekFloor(c, key));
    }

    @Override
    public Map.Entry<Long, V> higherEntry(final Long key) {
        final Cursor<V> c = map.cursor();
        return entry(c, seekHigher(c, key));
    }

    @Override
    public Long higherKey(final Long key) {
        final Cursor<V> c = map.cursor();
        return key(c, seekHigher(c, key));
    }

    @Override
    public Map.Entry<Long, V> lowerEntry(final Long key) {
        final Cursor<V> c = map.cursor();
        return entry(c, seekLower(c, key));
    }

    @Override
    public Long lowerKey(final Long key) {
        final Cursor<V> c = map.cursor();
        return key(c, seekLower(c, key));
    }

    /** a snapshot, as TreeMap's - setValue is not supported */
    private Map.Entry<Long, V> entry(final Cursor<V> c, final boolean found) {
        return found ? new SimpleImmutableEntry<>(c.key(), c.value()) : null;
    }

    private Map.Entry<Long, V> poll(final Cursor<V> c, final boolean found) {
        if (!found) {
            return null;
        }
        final long key = c.key();
        final V value = c.value();
        map.remove(key);
        return new SimpleImmutableEntry<>(key, value);
    }

    private static Long key(final Cursor<?> c, final boolean found) {
        return found ? c.key() : null;
    }

    private static Long keyOrThrow(final Cursor<?> c, final boolean found) {
        if (!found) {
            throw new NoSuchElementException();
        }
        return c.key();
    }

    private boolean seekFirst(final Cursor<V> c) {
        return descending ? absFloor(c, max) : absCeiling(c, min);
    }

    private boolean seekLast(final Cursor<V> c) {
        return descending ? absCeiling(c, min) : absFloor(c, max);
    }

    private boolean seekCeiling(final Cursor<V> c, final long key) {
        return descending ? absFloor(c, key) : absCeiling(c, key);
    }

    private boolean seekFloor(final Cursor<V> c, final long key) {
        return descending ? absCeiling(c, key) : absFloor(c, key);
    }

    private boolean seekHigher(final Cursor<V> c, final long key) {
        return descending ? absLower(c, key) : absHigher(c, key);
    }

    private boolean seekLower(final Cursor<V> c, final long key) {
        return descending ? absHigher(c, key) : absLower(c, key);
    }

    // ascending, within the range: false if there is no such key in it - the cursor is then not to be read

    private boolean absCeiling(final Cursor<V> c, final long key) {
        return key <= max && c.seekCeiling(Math.max(key, min)) && c.key() <= max;
    }

    private boolean absFloor(final Cursor<V> c, final long key) {
        return key >= min && c.seekFloor(Math.min(key, max)) && c.key() >= min;
    }

    private boolean absHigher(final Cursor<V> c, final long key) {
        // key < max, so key + 1 does not overflow
        return key < max && absCeiling(c, key + 1);
    }

    private boolean absLower(final Cursor<V> c, final long key) {
        return key > min && absFloor(c, key - 1);
    }

    // ---- views ----

    @Override
    public Comparator<? super Long> comparator() {
        return descending ? Collections.reverseOrder() : null;
    }

    @Override
    public ArtNavigableMap<V> descendingMap() {
        return new ArtNavigableMap<>(map, lo, loInclusive, hi, hiInclusive, !descending);
    }

    @Override
    public ArtNavigableMap<V> subMap(final Long fromKey,
                                     final boolean fromInclusive,
                                     final Long toKey,
                                     final boolean toInclusive) {
        final long from = fromKey;
        final long to = toKey;
        checkBound(from, fromInclusive, "fromKey");
        checkBound(to, toInclusive, "toKey");
        return descending
                ? view(to, toInclusive, from, fromInclusive)
                : view(from, fromInclusive, to, toInclusive);
    }

    @Override
    public ArtNavigableMap<V> headMap(final Long toKey, final boolean inclusive) {
        final long to = toKey;
        checkBound(to, inclusive, "toKey");
        return descending
                ? view(to, inclusive, hi, hiInclusive)
                : view(lo, loInclusive, to, inclusive);
    }

    @Override
    public ArtNavigableMap<V> tailMap(final Long fromKey, final boolean inclusive) {
        final long from = fromKey;
        checkBound(from, inclusive, "fromKey");
        return descending
                ? view(lo, loInclusive, from, inclusive)
                : view(from, inclusive, hi, hiInclusive);
    }

    @Override
    public ArtNavigableMap<V> subMap(final Long fromKey, final Long toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ArtNavigableMap<V> headMap(final Long toKey) {
        return headMap(toKey, false);
    }

    @Override
    public ArtNavigableMap<V> tailMap(final Long fromKey) {
        return tailMap(fromKey, true);
    }

    /** TreeMap's rule: an inclusive bound of a nested view in the range, an exclusive one at most at its bounds */
    private void checkBound(final long key, final boolean inclusive, final String name) {
        if (inclusive ? !inRange(key) : key < lo || key > hi) {
            throw new IllegalArgumentException(name + " out of range: " + key);
        }
    }

    private ArtNavigableMap<V> view(final long lo, final boolean loInclusive, final long hi, final boolean hiInclusive) {
        if (lo > hi) {
            throw new IllegalArgumentException("fromKey > toKey");
        }
        return new ArtNavigableMap<>(map, lo, loInclusive, hi, hiInclusive, descending);
    }

    @Override
    public NavigableSet<Long> navigableKeySet() {
        return new KeySet<>(this);
    }

    @Override
    public NavigableSet<Long> keySet() {
        return navigableKeySet();
    }

    @Override
    public NavigableSet<Long> descendingKeySet() {
        return new KeySet<>(descendingMap());
    }

    @Override
    public Set<Map.Entry<Long, V>> entrySet() {
        return new EntrySet();
    }

    Iterator<Long> keyIterator() {
        return new Walk<>() {
            @Override
            Long item(final long key, final V value) {
                return key;
            }
        };
    }

    private Iterator<Map.Entry<Long, V>> entryIterator() {
        return new Walk<>() {
            @Override
            Map.Entry<Long, V> item(final long key, final V value) {
                return new WalkEntry(key, value);
            }
        };
    }

    /**
     * A walk over the view in its order, on a cursor of its own: it fails fast as the cursor does, and removes
     * through the map.
     */
    private abstract class Walk<T> implements Iterator<T> {

        private final Cursor<V> cursor = map.cursor();
        private int expectedModCount = map.modCount;
        // the cursor is at the next key
        private boolean on = seekFirst(cursor);
        private boolean removable;
        private long lastKey;

        abstract T item(long key, V value);

        @Override
        public boolean hasNext() {
            return on;
        }

        @Override
        public T next() {
            if (!on) {
                throw new NoSuchElementException();
            }
            // ConcurrentModificationException after a change of the key set made not through this walk
            final long key = cursor.key();
            final V value = cursor.value();
            on = descending
                    ? cursor.prev() && cursor.key() >= min
                    : cursor.next() && cursor.key() <= max;
            lastKey = key;
            removable = true;
            return item(key, value);
        }

        @Override
        public void remove() {
            if (!removable) {
                throw new IllegalStateException();
            }
            if (map.modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            removable = false;
            // the cursor is already at the next key - seek it again: the removal may change the nodes on the way to it
            final long next = on ? cursor.key() : 0;
            map.remove(lastKey);
            expectedModCount = map.modCount;
            if (on) {
                cursor.seekCeiling(next);
            }
        }
    }

    /** an entry of an entry set walk: setValue writes to the map, as TreeMap's entries do */
    private final class WalkEntry extends SimpleEntry<Long, V> {

        WalkEntry(final long key, final V value) {
            super(key, value);
        }

        /**
         * @throws IllegalStateException if the key is no longer in the map
         */
        @Override
        public V setValue(final V value) {
            Objects.requireNonNull(value, "value");
            final long key = getKey();
            if (!map.containsKey(key)) {
                throw new IllegalStateException("the entry was removed from the map");
            }
            super.setValue(value);
            // a present key: a value update, not a change of the key set - the walks go on
            return map.put(key, value);
        }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<Long, V>> {

        @Override
        public Iterator<Map.Entry<Long, V>> iterator() {
            return entryIterator();
        }

        @Override
        public int size() {
            return ArtNavigableMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return ArtNavigableMap.this.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return keyOf(o) != null;
        }

        @Override
        public boolean remove(final Object o) {
            final Long key = keyOf(o);
            if (key == null) {
                return false;
            }
            map.remove(key);
            return true;
        }

        /** the key of the entry if the view has it with that value, else null - any other object is not in the set */
        private Long keyOf(final Object o) {
            if (!(o instanceof Map.Entry<?, ?> e) || !(e.getKey() instanceof Long key) || !inRange(key)) {
                return null;
            }
            final V value = map.get(key);
            return value != null && value.equals(e.getValue()) ? key : null;
        }

        @Override
        public void clear() {
            ArtNavigableMap.this.clear();
        }
    }

    /** the keys of a view, a view too: every call goes to the map view */
    private static final class KeySet<V> extends AbstractSet<Long> implements NavigableSet<Long> {

        private final ArtNavigableMap<V> m;

        KeySet(final ArtNavigableMap<V> m) {
            this.m = m;
        }

        @Override
        public Iterator<Long> iterator() {
            return m.keyIterator();
        }

        @Override
        public Iterator<Long> descendingIterator() {
            return m.descendingMap().keyIterator();
        }

        @Override
        public int size() {
            return m.size();
        }

        @Override
        public boolean isEmpty() {
            return m.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return m.containsKey(o);
        }

        @Override
        public boolean remove(final Object o) {
            // a value is never null
            return m.remove(o) != null;
        }

        @Override
        public void clear() {
            m.clear();
        }

        @Override
        public Comparator<? super Long> comparator() {
            return m.comparator();
        }

        @Override
        public Long first() {
            return m.firstKey();
        }

        @Override
        public Long last() {
            return m.lastKey();
        }

        @Override
        public Long lower(final Long key) {
            return m.lowerKey(key);
        }

        @Override
        public Long floor(final Long key) {
            return m.floorKey(key);
        }

        @Override
        public Long ceiling(final Long key) {
            return m.ceilingKey(key);
        }

        @Override
        public Long higher(final Long key) {
            return m.higherKey(key);
        }

        @Override
        public Long pollFirst() {
            final Map.Entry<Long, V> e = m.pollFirstEntry();
            return e != null ? e.getKey() : null;
        }

        @Override
        public Long pollLast() {
            final Map.Entry<Long, V> e = m.pollLastEntry();
            return e != null ? e.getKey() : null;
        }

        @Override
        public NavigableSet<Long> descendingSet() {
            return new KeySet<>(m.descendingMap());
        }

        @Override
        public NavigableSet<Long> subSet(final Long fromKey,
                                         final boolean fromInclusive,
                                         final Long toKey,
                                         final boolean toInclusive) {
            return new KeySet<>(m.subMap(fromKey, fromInclusive, toKey, toInclusive));
        }

        @Override
        public NavigableSet<Long> headSet(final Long toKey, final boolean inclusive) {
            return new KeySet<>(m.headMap(toKey, inclusive));
        }

        @Override
        public NavigableSet<Long> tailSet(final Long fromKey, final boolean inclusive) {
            return new KeySet<>(m.tailMap(fromKey, inclusive));
        }

        @Override
        public NavigableSet<Long> subSet(final Long fromKey, final Long toKey) {
            return subSet(fromKey, true, toKey, false);
        }

        @Override
        public NavigableSet<Long> headSet(final Long toKey) {
            return headSet(toKey, false);
        }

        @Override
        public NavigableSet<Long> tailSet(final Long fromKey) {
            return tailSet(fromKey, true);
        }
    }
}
