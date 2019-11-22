/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package com.carrotsearch.hppc;

import static com.carrotsearch.hppc.HashContainers.MIN_HASH_ARRAY_LENGTH;

public final class OpenHashContainers {

    public static int emptyBufferSize() {
        return expectedBufferSize(Containers.DEFAULT_EXPECTED_ELEMENTS);
    }

    public static int expectedBufferSize(final int elements) {
        return minBufferSize(elements, HashContainers.DEFAULT_LOAD_FACTOR) + 1;
    }

    // com.carrotsearch.hppc.HashContainers.minBufferSize() without range check.
    static int minBufferSize(int elements, double loadFactor) {
        if (elements < 0) {
            throw new IllegalArgumentException(
                "Number of elements must be >= 0: " + elements);
        }

        long length = (long) Math.ceil(elements / loadFactor);
        if (length == elements) {
            length++;
        }
        length = Math.max(MIN_HASH_ARRAY_LENGTH, BitUtil.nextHighestPowerOfTwo(length));

        return (int) length;
    }

    private OpenHashContainers() {
        throw new UnsupportedOperationException("No instances");
    }
}
