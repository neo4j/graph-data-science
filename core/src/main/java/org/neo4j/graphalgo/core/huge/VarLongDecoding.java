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
package org.neo4j.graphalgo.core.huge;

final class VarLongDecoding {

    static int decodeDeltaVLongs(
            long startValue,
            byte[] adjacencyPage,
            int offset,
            int limit,
            long[] out) {
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

    private VarLongDecoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
