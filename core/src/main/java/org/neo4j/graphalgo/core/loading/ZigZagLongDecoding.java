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
package org.neo4j.graphalgo.core.loading;

final class ZigZagLongDecoding {

    static int zigZagUncompress(byte[] array, int limit, long[] out) {
        return zigZagUncompress(array, 0, limit, out);
    }

    static int zigZagUncompress(byte[] array, int offset, int length, long[] out) {
        long input, startValue = 0L, value = 0L;
        int into = 0, shift = 0, limit = offset + length;
        while (offset < limit) {
            input = (long) array[offset++];
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

    private ZigZagLongDecoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
