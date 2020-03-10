/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.atomic.AtomicLong;

public class BitSetBuilder {

    private final BitSet bitSet;
    private final long length;
    private final AtomicLong allocationIndex;

    BitSetBuilder(BitSet bitSet, final long length) {
        this.bitSet = bitSet;
        this.length = length;
        this.allocationIndex = new AtomicLong();
    }

    public static BitSetBuilder of(long length, AllocationTracker tracker) {
        return new BitSetBuilder(new BitSet(length), length);
    }

    // naive implementation, probably not threadsafe! Use direct array assignment instead
    public final boolean bulkAdd(BitSet other) {
        long startIndex = allocationIndex.getAndAccumulate(other.size(), this::upperAllocation);

        for (int i = 0; i < other.size(); i++) {
            if (other.get(i)) {
                bitSet.set(startIndex + i);
            }
        }

        return true;
    }

    public final BitSet build() {
        return this.bitSet;
    }

    private long upperAllocation(long lower, long size) {
        return Math.min(length, lower + size);
    }
}
