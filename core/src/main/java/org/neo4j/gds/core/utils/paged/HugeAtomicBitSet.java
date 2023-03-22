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

import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.function.LongConsumer;

public final class HugeAtomicBitSet {

    private static final int NUM_BITS = Long.SIZE;

    private final HugeAtomicLongArray bits;
    private final long numBits;
    private final int remainder;

    public static long memoryEstimation(long size) {
        var wordsSize = BitUtil.ceilDiv(size, NUM_BITS);
        return HugeAtomicLongArray.memoryEstimation(wordsSize) + MemoryUsage.sizeOfInstance(HugeAtomicBitSet.class);
    }

    public static HugeAtomicBitSet create(long size) {
        var wordsSize = BitUtil.ceilDiv(size, NUM_BITS);
        int remainder = (int) (size % NUM_BITS);
        return new HugeAtomicBitSet(HugeAtomicLongArray.of(wordsSize, ParalleLongPageCreator.passThrough(1)), size, remainder);
    }

    private HugeAtomicBitSet(HugeAtomicLongArray bits, long numBits, int remainder) {
        this.bits = bits;
        this.numBits = numBits;
        this.remainder = remainder;
    }

    /**
     * Returns the state of the bit at the given index.
     */
    public boolean get(long index) {
        assert (index < numBits);
        long wordIndex = index / NUM_BITS;
        int bitIndex = (int) (index % NUM_BITS);
        long bitmask = 1L << bitIndex;
        return (bits.get(wordIndex) & bitmask) != 0;
    }

    /**
     * Sets the bit at the given index to true.
     */
    public void set(long index) {
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
    public void set(long startIndex, long endIndex) {
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

    /**
     * Sets a bit and returns the previous value.
     * The index should be less than the BitSet size.
     */
    public boolean getAndSet(long index) {
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
    public void flip(long index) {
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
    public void forEachSetBit(LongConsumer consumer) {
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
    public long cardinality() {
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
    public boolean isEmpty() {
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
    public boolean allSet() {
        for (long wordIndex = 0; wordIndex < bits.size() - 1; wordIndex++) {
            if (Long.bitCount(bits.get(wordIndex)) < NUM_BITS) {
                return false;
            }
        }
        return Long.bitCount(bits.get(bits.size() - 1)) >= (long) remainder;
    }

    /**
     * Returns the number of bits in the bitset.
     */
    public long size() {
        return numBits;
    }

    /**
     * Resets all bits in the bit set.
     * <p>
     * Note: this method is not thread-safe.
     */
    public void clear() {
        bits.setAll(0);
    }

    /**
     * Resets the bit at the given index.
     */
    public void clear(long index) {
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
}
