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
package org.neo4j.gds.louvain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryTree;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class LouvainMemoryEstimateDefinitionTest {

    static Stream<Arguments> memoryEstimationTuples() {
        return Stream.of(

            arguments(1, 1, true, 6414145, 23057704),
            arguments(1, 1, false, 6414145, 23057704),
            arguments(1, 10, true, 6414145, 30258064),
            arguments(1, 10, false, 6414145, 23857744),

            arguments(4, 1, true, 6417433, 29057968),
            arguments(4, 1, false, 6417433, 29057968),
            arguments(4, 10, true, 6417433, 36258328),
            arguments(4, 10, false, 6417433, 29858008),

            arguments(42, 1, true, 6459081, 105061312),
            arguments(42, 1, false, 6459081, 105061312),
            arguments(42, 10, true, 6459081, 112261672),
            arguments(42, 10, false, 6459081, 105861352)

        );
    }


    @ParameterizedTest
    @MethodSource("memoryEstimationTuples")
    void testMemoryEstimation(
        int concurrencyValue,
        int levels,
        boolean includeIntermediateCommunities,
        long expectedMinBytes,
        long expectedMaxBytes
    ) {
        var concurrency = new Concurrency(concurrencyValue);
        var nodeCount = 100_000L;
        var relCount = 500_000L;

        var estimationParameters = new LouvainMemoryEstimationParameters(
            levels,
            includeIntermediateCommunities
        );

        var memoryEstimation = new LouvainMemoryEstimateDefinition(estimationParameters).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(nodeCount,relCount, concurrency)
            .hasMin(expectedMinBytes)
            .hasMax(expectedMaxBytes);
    }

    @Test
    void testMemoryEstimationUsesOnlyOnePropertyForEachEntity() {
        ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions.builder()
            .nodeCount(100_000L)
            .relCountUpperBound(500_000L);

        GraphDimensions dimensionsWithoutProperties = dimensionsBuilder.build();
        GraphDimensions dimensionsWithOneProperty = dimensionsBuilder
            .putRelationshipPropertyToken("foo", 1)
            .build();
        GraphDimensions dimensionsWithTwoProperties = dimensionsBuilder
            .putRelationshipPropertyToken("foo", 1)
            .putRelationshipPropertyToken("bar", 1)
            .build();


        var estimationParameters = new LouvainMemoryEstimationParameters(
            1,
            false
        );

        var memoryEstimation = new LouvainMemoryEstimateDefinition(estimationParameters).memoryEstimation();

        var concurrency = new Concurrency(1);
        MemoryTree memoryTree = memoryEstimation.estimate(dimensionsWithoutProperties, concurrency);
        MemoryTree memoryTreeOneProperty = memoryEstimation.estimate(dimensionsWithOneProperty, concurrency);
        MemoryTree memoryTreeTwoProperties =  memoryEstimation.estimate(dimensionsWithTwoProperties, concurrency);

        assertEquals(memoryTree.memoryUsage(), memoryTreeOneProperty.memoryUsage());
        assertEquals(memoryTreeOneProperty.memoryUsage(), memoryTreeTwoProperties.memoryUsage());
    }

}
