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

public class SparseLongArray {

    public static final long NOT_FOUND = -1;

    private long[] array;

    SparseLongArray(long capacity) {
        var size = (int) (capacity / Long.SIZE);
        this.array = new long[size + 1];
    }

    public long toMappedNodeId(long originalId) {
        var offset = (int) (originalId / Long.SIZE);
        var posInLong = originalId % Long.SIZE;

        // Check if original id is contained
        long mask = 1 << posInLong;
        if ((mask & array[offset]) != mask) {
            return NOT_FOUND;
        }

        // Count set bits up to original id
        var a = array;
        var result = 0L;
        // head
        for (int i = 0; i < offset; i++) {
            result += Long.bitCount(a[i]);
        }
        // tail (long at offset)
        var shift = Long.SIZE - posInLong - 1;
        result += Long.bitCount(a[offset] << shift);

        return result - 1;
    }

    public long toOriginalNodeId(long mappedId) {
        return 0;
    }

    public void set(long originalId) {
        var offset = (int) (originalId / Long.SIZE);
        var posInLong = originalId % Long.SIZE;

        var mask = 1L << posInLong;
        array[offset] |= mask;
    }

}
