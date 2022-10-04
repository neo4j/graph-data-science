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

import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.mem.BitUtil;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

public final class HugeAtomicGrowingBitSet {

    private static final int NUM_BITS = Long.SIZE;

    private HugeAtomicLongArray bits;
    private long numBits;
    private int remainder;

    private final Lock growLock = new ReentrantLock(true);

    public static HugeAtomicGrowingBitSet create(long size) {
        var wordsSize = BitUtil.ceilDiv(size, NUM_BITS);
        int remainder = (int) (size % NUM_BITS);
        return new HugeAtomicGrowingBitSet(
            HugeAtomicLongArray.newArray(wordsSize),
            size,
            remainder
        );
    }

    private HugeAtomicGrowingBitSet(
        HugeAtomicLongArray bits,
        long numBits,
        int remainder
    ) {
        this.bits = bits;
        this.numBits = numBits;
        this.remainder = remainder;
    }

    /**
     * Returns the state of the bit at the given index.
     */
    public boolean get(long index) {
        return HugeAtomicBitSetOps.get(bits, numBits, index);
    }

    /**
     * Sets the bit at the given index to true.
     */
    public void set(long index) {
        if (index >= numBits) {
            grow(index + 1);
        }
        HugeAtomicBitSetOps.set(bits, numBits, index);
    }

    /**
     * Sets the bits from the startIndex (inclusive) to the endIndex (exclusive).
     */
    public void set(long startIndex, long endIndex) {
        if (endIndex > numBits) {
            grow(endIndex);
        }
        HugeAtomicBitSetOps.setRange(bits, numBits, startIndex, endIndex);
    }

    /**
     * Sets a bit and returns the previous value.
     * The index should be less than the BitSet size.
     */
    public boolean getAndSet(long index) {
        if (index >= numBits) {
            grow(index + 1);
        }
        return HugeAtomicBitSetOps.getAndSet(bits, numBits, index);
    }

    /**
     * Toggles the bit at the given index.
     */
    public void flip(long index) {
        if (index >= numBits) {
            grow(index + 1);
        }
        HugeAtomicBitSetOps.flip(bits, numBits, index);
    }

    /**
     * Iterates the bit set in increasing order and calls the given consumer for each set bit.
     *
     * This method is not thread-safe.
     */
    public void forEachSetBit(LongConsumer consumer) {
        HugeAtomicBitSetOps.forEachSetBit(bits, consumer);
    }

    /**
     * Returns the number of set bits in the bit set.
     * <p>
     * Note: this method is not thread-safe.
     */
    public long cardinality() {
        return HugeAtomicBitSetOps.cardinality(bits);
    }

    /**
     * Returns true iff no bit is set.
     * <p>
     * Note: this method is not thread-safe.
     */
    public boolean isEmpty() {
        return HugeAtomicBitSetOps.isEmpty(bits);
    }

    /**
     * Returns true iff all bits are set.
     * <p>
     * Note: this method is not thread-safe.
     */
    public boolean allSet() {
        return HugeAtomicBitSetOps.allSet(bits, remainder);
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
        HugeAtomicBitSetOps.clear(bits);
    }

    /**
     * Resets the bit at the given index.
     */
    public void clear(long index) {
        HugeAtomicBitSetOps.clear(bits, numBits, index);
    }

    private void grow(long minNumBits) {
        growLock.lock();
        try {
            if (minNumBits < numBits) {
                return;
            }

            var newNumBits = ArrayUtil.oversizeHuge(minNumBits, Long.BYTES);
            var wordsSize = BitUtil.ceilDiv(newNumBits, NUM_BITS);
            int remainder = (int) (newNumBits % NUM_BITS);
            var newBits = HugeAtomicLongArray.newArray(wordsSize);

            this.bits.copyTo(newBits, this.bits.size());
            this.bits = newBits;
            this.numBits = newNumBits;
            this.remainder = remainder;
        } finally {
            growLock.unlock();
        }
    }
}
