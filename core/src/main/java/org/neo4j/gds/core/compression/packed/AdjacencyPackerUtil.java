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
package org.neo4j.gds.core.compression.packed;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;

final class AdjacencyPackerUtil {

    static final int BYTE_ARRAY_BASE_OFFSET = UnsafeUtil.arrayBaseOffset(byte[].class);

    static int bitsNeeded(long[] values, int offset, int length) {
        long bits = 0L;
        for (int i = offset; i < offset + length; i++) {
            bits |= values[i];
        }
        return Long.SIZE - Long.numberOfLeadingZeros(bits);
    }

    static int bytesNeeded(int bits) {
        return BitUtil.ceilDiv(AdjacencyPacking.BLOCK_SIZE * bits, Byte.SIZE);
    }

    static int bytesNeeded(int bits, int length) {
        return BitUtil.ceilDiv(length * bits, Byte.SIZE);
    }

    @TestOnly
    public static int align(int length) {
        return (int) BitUtil.align(length, AdjacencyPacking.BLOCK_SIZE);
    }

    private AdjacencyPackerUtil() {}
}
