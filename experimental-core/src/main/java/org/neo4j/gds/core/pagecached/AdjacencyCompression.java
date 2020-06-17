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
package org.neo4j.gds.core.pagecached;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.CompressedLongArray;

import java.util.Arrays;

final class AdjacencyCompression {

    private static long[] growWithDestroy(long[] values, int newLength) {
        if (values.length < newLength) {
            // give leeway in case of nodes with a reference to themselves
            // due to automatic skipping of identical targets, just adding one is enough to cover the
            // self-reference case, as it is handled as two relationships that aren't counted by BOTH
            // avoid repeated re-allocation for smaller degrees
            // avoid generous over-allocation for larger degrees
            int newSize = Math.max(32, 1 + newLength);
            return new long[newSize];
        }
        return values;
    }

    static void copyFrom(LongsRef into, CompressedLongArray array) {
        into.longs = growWithDestroy(into.longs, array.length());
        into.length = array.uncompress(into.longs);
    }

    static int applyDeltaEncoding(LongsRef data, Aggregation aggregation) {
        Arrays.sort(data.longs, 0, data.length);
        return data.length = applyDelta(data.longs, data.length, aggregation);
    }

    //@formatter:off
    static int writeBigEndianInt(byte[] out, int offset, int value) {
        out[    offset] = (byte) (value >>> 24);
        out[1 + offset] = (byte) (value >>> 16);
        out[2 + offset] = (byte) (value >>> 8);
        out[3 + offset] = (byte) (value);
        return 4 + offset;
    }
    //@formatter:on

    private static int applyDelta(long[] values, int length, Aggregation aggregation) {
        long value = values[0], delta;
        int in = 1, out = 1;
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L || aggregation == Aggregation.NONE) {
                values[out++] = delta;
            }
        }
        return out;
    }

    private AdjacencyCompression() {
    }
}
