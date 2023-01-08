/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.core.utils.paged;

import java.util.function.LongConsumer;

public final class HugeAtomicBitSetOps {

    private static final int NUM_BITS = Long.SIZE;

    /**
     * Returns the state of the bit at the given index.
     */
    static boolean get(HugeAtomicLongArray bits, long numBits, long index) {
        assert (index < numBits);
        long wordIndex = index / NUM_BITS;
        int bitIndex = (int) (index % NUM_BITS);
        long bitmask = 1L << bitIndex;
        return (bits.get(wordIndex) & bitmask) != 0;
    }

    /**
     * Sets the bit at the given index to true.
     */
    static void set(HugeAtomicLongArray bits, long numBits, long index) {
        assert (index < numBits);

        long wordIndex = index / NUM_BITS;
        int bitIndex = (int) (index % NUM_BITS);
        long bitmask = 1L << bitIndex;

        long oldWord = bits.get(wordIndex);
        while (true) {
            long newWord = oldWord | bitmask;
            if (newWord == oldWord) {
                // nothing to set
                return;
            }
            long currentWord = bits.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    /**
     * Sets the bits from the startIndex (inclusive) to the endIndex (exclusive).
     */
    static void setRange(HugeAtomicLongArray bits, long numBits, long startIndex, long endIndex) {
        assert (startIndex <= endIndex);
        assert (endIndex <= numBits);

        long startWordIndex = startIndex / NUM_BITS;
        // since endIndex is exclusive, we need the word before that index
        long endWordIndex = (endIndex - 1) / NUM_BITS;

        long startBitMask = -1L << startIndex;
        long endBitMask = -1L >>> -endIndex;

        if (startWordIndex == endWordIndex) {
            // set within single word
            setWord(bits, startWordIndex, startBitMask & endBitMask);
        } else {
            // set within range
            setWord(bits, startWordIndex, startBitMask);
            for (long wordIndex = startWordIndex + 1; wordIndex < endWordIndex; wordIndex++) {
                bits.set(wordIndex, -1L);
            }
            setWord(bits, endWordIndex, endBitMask);
        }
    }

    private static void setWord(HugeAtomicLongArray bits, long wordIndex, long bitMask) {
        var oldWord = bits.get(wordIndex);
        while (true) {
            var newWord = oldWord | bitMask;
            if (newWord == oldWord) {
                // already set
                return;
            }
            var currentWord = bits.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAX successful
                return;
            }
            oldWord = currentWord;
        }
    }

    /**
     * Sets a bit and returns the previous value.
     * The index should be less than the BitSet size.
     */
    static boolean getAndSet(HugeAtomicLongArray bits, long numBits, long index) {
        assert (index < numBits);

        long wordIndex = index / NUM_BITS;
        int bitIndex = (int) (index % NUM_BITS);
        long bitmask = 1L << bitIndex;

        long oldWord = bits.get(wordIndex);
        while (true) {
            long newWord = oldWord | bitmask;
            if (newWord == oldWord) {
                // already set
                return true;
            }
            long currentWord = bits.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return false;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    /**
     * Toggles the bit at the given index.
     */
    static void flip(HugeAtomicLongArray bits, long numBits, long index) {
        assert (index < numBits);

        long wordIndex = index / NUM_BITS;
        int bitIndex = (int) (index % NUM_BITS);
        long bitmask = 1L << bitIndex;

        long oldWord = bits.get(wordIndex);
        while (true) {
            long newWord = oldWord ^ bitmask;
            long currentWord = bits.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    /**
     * Iterates the bit set in increasing order and calls the given consumer for each set bit.
     *
     * This method is not thread-safe.
     */
    static void forEachSetBit(HugeAtomicLongArray bits, LongConsumer consumer) {
        var cursor = bits.initCursor(bits.newCursor());

        while (cursor.next()) {
            long[] block = cursor.array;
            int offset = cursor.offset;
            int limit = cursor.limit;
            long base = cursor.base;

            for (int i = offset; i < limit; i++) {
                long word = block[i];
                while (word != 0) {
                    long next = Long.numberOfTrailingZeros(word);
                    consumer.accept(Long.SIZE * (base + i) + next);
                    word = word ^ Long.lowestOneBit(word);
                }
            }
        }
    }

    /**
     * Returns the number of set bits in the bit set.
     * <p>
     * Note: this method is not thread-safe.
     */
    static long cardinality(HugeAtomicLongArray bits) {
        long setBitCount = 0;

        for (long wordIndex = 0; wordIndex < bits.size(); wordIndex++) {
            setBitCount += Long.bitCount(bits.get(wordIndex));
        }

        return setBitCount;
    }

    /**
     * Returns true iff no bit is set.
     * <p>
     * Note: this method is not thread-safe.
     */
    static boolean isEmpty(HugeAtomicLongArray bits) {
        for (long wordIndex = 0; wordIndex < bits.size(); wordIndex++) {
            if (Long.bitCount(bits.get(wordIndex)) > 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true iff all bits are set.
     * <p>
     * Note: this method is not thread-safe.
     */
    static boolean allSet(HugeAtomicLongArray bits, long remainder) {
        for (long wordIndex = 0; wordIndex < bits.size() - 1; wordIndex++) {
            if (Long.bitCount(bits.get(wordIndex)) < NUM_BITS) {
                return false;
            }
        }
        return Long.bitCount(bits.get(bits.size() - 1)) >= remainder;
    }

    /**
     * Resets all bits in the bit set.
     * <p>
     * Note: this method is not thread-safe.
     */
    static void clear(HugeAtomicLongArray bits) {
        bits.setAll(0);
    }

    /**
     * Resets the bit at the given index.
     */
    static void clear(HugeAtomicLongArray bits, long numBits, long index) {
        assert (index < numBits);

        long wordIndex = index / NUM_BITS;
        int bitIndex = (int) (index % NUM_BITS);
        long bitmask = ~(1L << bitIndex);

        long oldWord = bits.get(wordIndex);
        while (true) {
            long newWord = oldWord & bitmask;
            if (newWord == oldWord) {
                // already cleared
                return;
            }
            long currentWord = bits.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    private HugeAtomicBitSetOps() {}
}
