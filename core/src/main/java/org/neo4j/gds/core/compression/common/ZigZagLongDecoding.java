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

    public static int zigZagUncompress(byte[] chunk, int numberOfBytes, long[] out) {
        long input, startValue = 0L, value = 0L;
        int into = 0, shift = 0, offset = 0;

        while (offset < numberOfBytes) {
            input = chunk[offset++];
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                startValue += ((value >>> 1L) ^ -(value & 1L));
                out[into++] = startValue;
                value = 0L;
                shift = 0;
            } else {
                shift += 7;
            }
        }
        return into;
    }

    public static int zigZagUncompress(byte[][] chunks, int numberOfBytes, long[] out) {
        return zigZagUncompress(chunks, numberOfBytes, out, Identity.INSTANCE);
    }

    public static int zigZagUncompress(
        byte[][] chunks,
        int numberOfBytes,
        long[] out,
        AdjacencyCompressor.ValueMapper mapper
    ) {
        int currentChunk = 0;
        long input, startValue = 0L, value = 0L;
        int into = 0, shift = 0;

        while (numberOfBytes > 0) {
            var chunk = chunks[currentChunk++];
            var bytesToConsumeForChunk = Math.min(numberOfBytes, chunk.length);
            for (int offset = 0; offset < bytesToConsumeForChunk; offset++) {
                input = chunk[offset];
                value += (input & 127L) << shift;
                if ((input & 128L) == 128L) {
                    startValue += ((value >>> 1L) ^ -(value & 1L));
                    out[into++] = mapper.map(startValue);
                    value = 0L;
                    shift = 0;
                } else {
                    shift += 7;
                }
            }
            numberOfBytes -= bytesToConsumeForChunk;
        }
        return into;
    }

    private ZigZagLongDecoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
