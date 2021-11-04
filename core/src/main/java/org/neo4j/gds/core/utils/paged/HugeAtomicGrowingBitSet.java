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

import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.HugeArrays;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class HugeAtomicGrowingBitSet extends HugeAtomicBitSet {

    private final Lock growLock = new ReentrantLock(true);
    private final AllocationTracker allocationTracker;

    static HugeAtomicGrowingBitSet create(long size, AllocationTracker allocationTracker) {
        var wordsSize = BitUtil.ceilDiv(size, NUM_BITS);
        int remainder = (int) (size % NUM_BITS);
        return new HugeAtomicGrowingBitSet(
            HugeAtomicLongArray.newArray(wordsSize, allocationTracker),
            size,
            remainder,
            allocationTracker
        );
    }

    private HugeAtomicGrowingBitSet(
        HugeAtomicLongArray bits,
        long numBits,
        int remainder,
        AllocationTracker allocationTracker
    ) {
        super(bits, numBits, remainder);
        this.allocationTracker = allocationTracker;
    }

    @Override
    public void set(long index) {
        if (index >= numBits) {
            grow(index + 1);
        }
        super.set(index);
    }

    @Override
    public void set(long startIndex, long endIndex) {
        if (endIndex > numBits) {
            grow(endIndex);
        }
        super.set(startIndex, endIndex);
    }

    @Override
    public boolean getAndSet(long index) {
        if (index >= numBits) {
            grow(index + 1);
        }
        return super.getAndSet(index);
    }

    @Override
    public void flip(long index) {
        if (index >= numBits) {
            grow(index + 1);
        }
        super.flip(index);
    }

    private void grow(long minNumBits) {
        growLock.lock();
        try {
            if (minNumBits < numBits) {
                return;
            }

            var newNumBits = HugeArrays.oversize(minNumBits, Long.BYTES);
            var wordsSize = BitUtil.ceilDiv(newNumBits, NUM_BITS);
            int remainder = (int) (newNumBits % NUM_BITS);
            var newBits = HugeAtomicLongArray.newArray(wordsSize, allocationTracker);

            this.bits.copyTo(newBits, this.bits.size());
            this.bits = newBits;
            this.numBits = newNumBits;
            this.remainder = remainder;
        } finally {
            growLock.unlock();
        }
    }
}
