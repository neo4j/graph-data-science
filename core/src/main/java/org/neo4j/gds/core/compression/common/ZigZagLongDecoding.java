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
package org.neo4j.gds.core.compression.common;

import org.neo4j.gds.api.compress.AdjacencyCompressor;

public final class ZigZagLongDecoding {

    public enum Identity implements AdjacencyCompressor.ValueMapper {
        INSTANCE {
            @Override
            public long map(long value) {
                return value;
            }
        }
    }

    public static int zigZagUncompress(byte[] compressedData, int length, long[] uncompressedData) {
        return zigZagUncompress(compressedData, length, uncompressedData, Identity.INSTANCE);
    }

    static int zigZagUncompress(
        byte[] compressedData,
        int length,
        long[] uncompressedData,
        AdjacencyCompressor.ValueMapper mapper
    ) {
        long input, startValue = 0L, value = 0L;
        int into = 0, shift = 0, offset = 0;
        while (offset < length) {
            input = compressedData[offset++];
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                startValue += ((value >>> 1L) ^ -(value & 1L));
                uncompressedData[into++] = mapper.map(startValue);
                value = 0L;
                shift = 0;
            } else {
                shift += 7;
            }
        }
        return into;
    }

    private ZigZagLongDecoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
