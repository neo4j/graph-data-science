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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.Aggregation;

import java.util.Arrays;
import java.util.stream.Stream;

import static java.lang.Double.doubleToLongBits;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class AdjacencyCompressionTest {

    @ParameterizedTest(name = "{4}")
    @MethodSource("aggregationsWithResults")
    void shouldCountRelationships(long[] targetNodeIds, long[][] weights, Aggregation[] aggregations, double[][] expected, String aggregationType) {
        LongsRef data = new LongsRef(targetNodeIds, 0, targetNodeIds.length);

        // Calculate this before applying the delta because the target node ids array is updated in place
        long expectedDataLength = Arrays.stream(targetNodeIds).distinct().count();

        AdjacencyCompression.applyDeltaEncoding(data, weights, aggregations, false);

        for (int i = 0; i < expected.length; i++) {
            for (int j = 0; j < expected[i].length; j++) {
                assertEquals(expected[i][j], Double.longBitsToDouble(weights[i][j]));
            }
        }

        // The length of the data should be the count of the distinct elements in the target node ids array
        assertEquals(expectedDataLength, data.length);
        // The offset should be unchanged
        assertEquals(0, data.offset);

        // The target data.longs should be the same instance as the one it was created
        assertSame(targetNodeIds, data.longs);

        // These contain the `deltas` computed during the compression
        assertEquals(1L, data.longs[0]);
        assertEquals(4L, data.longs[1]);
    }

    static Stream<Arguments> aggregationsWithResults() {
        return Stream.of(
            Arguments.of(
                values(),
                weights(),
                new Aggregation[]{
                    Aggregation.COUNT,
                    Aggregation.COUNT
                },
                new double[][]{{4d, 2d}, {4d, 2d}},
                "COUNT"
            ),
            Arguments.of(
                values(),
                weights(),
                new Aggregation[]{
                    Aggregation.SUM,
                    Aggregation.SUM
                },
                new double[][]{{16d, 8d}, {27d, 14d}},
                "SUM"
            ),
            Arguments.of(
                values(),
                weights(),
                new Aggregation[]{
                    Aggregation.MIN,
                    Aggregation.MIN
                },
                new double[][]{{2d, 3d}, {4d, 6d}},
                "MIN"
            ),
            Arguments.of(
                values(),
                weights(),
                new Aggregation[]{
                    Aggregation.MAX,
                    Aggregation.MAX
                },
                new double[][]{{5d, 5d}, {8d, 8d}},
                "MAX"
            )
        );
    }

    private static long[][] weights() {
        return new long[][]{{
            doubleToLongBits(2),
            doubleToLongBits(4),
            doubleToLongBits(3),
            doubleToLongBits(5),
            doubleToLongBits(5),
            doubleToLongBits(5)
        }, {
            doubleToLongBits(4),
            doubleToLongBits(7),
            doubleToLongBits(6),
            doubleToLongBits(8),
            doubleToLongBits(8),
            doubleToLongBits(8)
        }};
    }

    private static long[] values() {
        return new long[]{1, 1, 5, 5, 1, 1};
    }

}
