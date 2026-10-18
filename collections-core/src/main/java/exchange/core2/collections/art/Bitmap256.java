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

/**
 * Occupancy of the 256 slots of an {@link ArtNode48} / {@link ArtNode256}: one bit per key byte that has a child,
 * in a {@code long[4]} of the node. Ordered walks and the ceiling / floor searches jump to the next occupied slot
 * instead of reading every empty one (up to 255 of them).
 * <p>
 * An array, not four long fields: a put or removal then updates one word, {@code occupied[idx >>> 6]}. With fields
 * the word had to be picked by a switch (mispredicted when the keys spread over the node) or by masking all four
 * words - in ArtOpsJmh removeInsert both cost 2-3 times what the array does (+15-28% against +4-10%), while the
 * extra load of the array did not show in the walks.
 */
final class Bitmap256 {

    private Bitmap256() {
    }

    /**
     * @param from 0..256
     * @return the lowest occupied slot at or above from, or -1 if there is none
     */
    static int next(final long bits0, final long bits1, final long bits2, final long bits3, int from) {
        // -1L << from shifts by from % 64; past a word, from is the start of the next one - a full mask
        if (from < 64) {
            final long word = bits0 & (-1L << from);
            if (word != 0) {
                return Long.numberOfTrailingZeros(word);
            }
            from = 64;
        }
        if (from < 128) {
            final long word = bits1 & (-1L << from);
            if (word != 0) {
                return 64 + Long.numberOfTrailingZeros(word);
            }
            from = 128;
        }
        if (from < 192) {
            final long word = bits2 & (-1L << from);
            if (word != 0) {
                return 128 + Long.numberOfTrailingZeros(word);
            }
            from = 192;
        }
        if (from < 256) {
            final long word = bits3 & (-1L << from);
            if (word != 0) {
                return 192 + Long.numberOfTrailingZeros(word);
            }
        }
        return -1;
    }

    /**
     * @param from -1..255
     * @return the highest occupied slot at or below from, or -1 if there is none
     */
    static int prev(final long bits0, final long bits1, final long bits2, final long bits3, int from) {
        // -1L >>> (63 - from % 64) keeps the bits up to from; past a word, from is the end of the previous one
        if (from >= 192) {
            final long word = bits3 & (-1L >>> (63 - (from & 63)));
            if (word != 0) {
                return 255 - Long.numberOfLeadingZeros(word);
            }
            from = 191;
        }
        if (from >= 128) {
            final long word = bits2 & (-1L >>> (63 - (from & 63)));
            if (word != 0) {
                return 191 - Long.numberOfLeadingZeros(word);
            }
            from = 127;
        }
        if (from >= 64) {
            final long word = bits1 & (-1L >>> (63 - (from & 63)));
            if (word != 0) {
                return 127 - Long.numberOfLeadingZeros(word);
            }
            from = 63;
        }
        if (from >= 0) {
            final long word = bits0 & (-1L >>> (63 - from));
            if (word != 0) {
                return 63 - Long.numberOfLeadingZeros(word);
            }
        }
        return -1;
    }
}
