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
package org.neo4j.graphalgo.core.loading;

import org.apache.lucene.util.LongsRef;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.Aggregation;

import static java.lang.Double.doubleToLongBits;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class AdjacencyCompressionTest {

    @Test
    void shouldCountRelationships() {
        long[] targetNodeIds = {1, 1, 3, 3, 1, 1};
        LongsRef data = new LongsRef(targetNodeIds, 0, 6);
        long[][] weights = new long[][]{
            {doubleToLongBits(2), doubleToLongBits(4), doubleToLongBits(3), doubleToLongBits(5), doubleToLongBits(5), doubleToLongBits(5)},
            {doubleToLongBits(4), doubleToLongBits(7), doubleToLongBits(6), doubleToLongBits(8), doubleToLongBits(8), doubleToLongBits(8)}
        };
        Aggregation[] aggregations = new Aggregation[]{
            Aggregation.COUNT,
            Aggregation.COUNT
        };
        AdjacencyCompression.applyDeltaEncoding(data, weights, aggregations, false);

        assertEquals(4d, Double.longBitsToDouble(weights[0][0]));
        assertEquals(2d, Double.longBitsToDouble(weights[0][1]));

        assertEquals(2, data.length);
        assertEquals(0, data.offset);
        assertSame(targetNodeIds, data.longs);
        assertEquals(1L, data.longs[0]);
        assertEquals(2L, data.longs[1]);
    }
}