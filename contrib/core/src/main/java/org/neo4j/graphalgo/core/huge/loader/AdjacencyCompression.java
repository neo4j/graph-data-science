/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.huge.loader;

import org.apache.lucene.util.LongsRef;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.huge.loader.VarLongEncoding.encodeVLongs;

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

    static int applyDeltaEncoding(LongsRef data) {
        Arrays.sort(data.longs, 0, data.length);
        return data.length = applyDelta(data.longs, data.length);
    }

    static int compress(LongsRef data, byte[] out) {
        return encodeVLongs(data.longs, data.length, out, 0);
    }

    //@formatter:off
    static int writeDegree(byte[] out, int offset, int degree) {
        out[    offset] = (byte) (degree);
        out[1 + offset] = (byte) (degree >>> 8);
        out[2 + offset] = (byte) (degree >>> 16);
        out[3 + offset] = (byte) (degree >>> 24);
        return 4 + offset;
    }
    //@formatter:on

    private static int applyDelta(long[] values, int length) {
        long value = values[0], delta;
        int in = 1, out = 1;
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L) {
                values[out++] = delta;
            }
        }
        return out;
    }

    private AdjacencyCompression() {
    }
}
