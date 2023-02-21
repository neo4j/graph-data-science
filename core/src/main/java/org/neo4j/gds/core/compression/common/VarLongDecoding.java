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

import org.neo4j.internal.unsafe.UnsafeUtil;

public final class VarLongDecoding {

    public static int decodeDeltaVLongs(
        long startValue,
        byte[] adjacencyPage,
        int offset,
        int limit,
        long[] out
    ) {
        long input, value = 0L;
        int into = 0, shift = 0;
        while (into < limit) {
            input = adjacencyPage[offset++];
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                startValue += value;
                out[into++] = startValue;
                value = 0L;
                shift = 0;
            } else {
                shift += 7;
            }
        }

        return offset;
    }

    public static long unsafeDecodeDeltaVLongs(
        int length,
        long previousValue,
        long ptr,
        long[] out,
        int offset
    ) {
        long input, value = 0L;
        int shift = 0;
        while (length > 0) {
            input = UnsafeUtil.getByte(ptr);
            ptr++;
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                previousValue += value;
                out[offset++] = previousValue;
                value = 0L;
                shift = 0;
                length--;
            } else {
                shift += 7;
            }
        }

        return ptr;
    }

    public static long unsafeDecodeVLongs(
        int length,
        long ptr,
        long[] out,
        int offset
    ) {
        long input, value = 0L;
        int shift = 0;
        while (length > 0) {
            input = UnsafeUtil.getByte(ptr);
            ptr++;
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                out[offset++] = value;
                value = 0L;
                shift = 0;
                length--;
            } else {
                shift += 7;
            }
        }

        return ptr;
    }

    private VarLongDecoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
