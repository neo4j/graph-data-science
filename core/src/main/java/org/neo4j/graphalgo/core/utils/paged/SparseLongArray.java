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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;

public class SparseLongArray {

    public static final long NOT_FOUND = -1;
    private static final int BLOCK_SIZE = 64;

    private final int size;

    private final long[] array;
    private final long[] blockCounts;

    public SparseLongArray(long capacity) {
        this.size = (int) BitUtil.ceilDiv(capacity, Long.SIZE);
        this.array = new long[size];
        int blocks = size / BLOCK_SIZE;
        // blockCounts[0] is always 0
        this.blockCounts = new long[blocks + 1];
    }

    public long toMappedNodeId(long originalId) {
        var page = (int) (originalId / Long.SIZE);
        var indexInPage = originalId % Long.SIZE;

        // Check if original id is contained
        long mask = 1L << indexInPage;
        if ((mask & array[page]) != mask) {
            return NOT_FOUND;
        }

        var block = page / BLOCK_SIZE;
        // Get the count from all previous blocks
        var mappedId = blockCounts[block];
        // Count set bits up to original id
        var a = array;
        // Get count within current block
        for (int pageIdx = page & ~ 63; pageIdx < page; pageIdx++) {
            mappedId += Long.bitCount(a[pageIdx]);
        }
        // tail (long at offset)
        var shift = Long.SIZE - indexInPage - 1;
        mappedId += Long.bitCount(a[page] << shift);

        return mappedId - 1;
    }

    public long toOriginalNodeId(long mappedId) {
        var startBlockIndex = ArrayUtil.binaryLookup(mappedId, blockCounts);
        var array = this.array;
        var blockStart = startBlockIndex * BLOCK_SIZE;
        var blockEnd = Math.min((startBlockIndex + 1) * BLOCK_SIZE, array.length);
        var idSoFar = blockCounts[startBlockIndex];
        for (int blockIndex = blockStart; blockIndex < blockEnd; blockIndex++) {
            var page = array[blockIndex];
            var pos = 0;
            var idsInPage = Long.bitCount(page);
            if (idSoFar + idsInPage > mappedId) {
                while (idSoFar <= mappedId) {
                    if ((page & 1) == 1) {
                        ++idSoFar;
                    }
                    page >>>= 1;
                    ++pos;
                }
                return blockIndex * Long.SIZE + (pos - 1);
            }
            idSoFar += idsInPage;
        }

        return 0;
    }

    public synchronized void set(long originalId) {
        var page = (int) (originalId / Long.SIZE);
        var indexInPage = originalId % Long.SIZE;
        var mask = 1L << indexInPage;
        array[page] |= mask;
    }

    public void computeCounts() {
        int size = this.size - BLOCK_SIZE;
        long[] array = this.array;
        long[] blockCounts = this.blockCounts;

        long count = 0;
        for (int block = 0; block < size; block += BLOCK_SIZE) {
            for (int page = block; page < block + BLOCK_SIZE; page++) {
                count += Long.bitCount(array[page]);
            }
            blockCounts[block / BLOCK_SIZE + 1] = count;
        }
    }

}
