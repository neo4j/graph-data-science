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
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LouvainMemoryEstimateDefinitionTest {

    static Stream<Arguments> memoryEstimationTuples() {
        return Stream.of(

            arguments(1, 1, true, 6414153, 23057712),
            arguments(1, 1, false, 6414153, 23057712),
            arguments(1, 10, true, 6414153, 30258072),
            arguments(1, 10, false, 6414153, 23857752),

            arguments(4, 1, true, 6417441, 29057976),
            arguments(4, 1, false, 6417441, 29057976),
            arguments(4, 10, true, 6417441, 36258336),
            arguments(4, 10, false, 6417441, 29858016),

            arguments(42, 1, true, 6459089, 105061320),
            arguments(42, 1, false, 6459089, 105061320),
            arguments(42, 10, true, 6459089, 112261680),
            arguments(42, 10, false, 6459089, 105861360)

        );
    }


    @ParameterizedTest
    @MethodSource("memoryEstimationTuples")
    void testMemoryEstimation(
        int concurrency,
        int levels,
        boolean includeIntermediateCommunities,
        long expectedMinBytes,
        long expectedMaxBytes
    ) {
        var nodeCount = 100_000L;
        var relCount = 500_000L;

        var config = mock(LouvainBaseConfig.class);
        when(config.includeIntermediateCommunities()).thenReturn(includeIntermediateCommunities);
        when(config.maxLevels()).thenReturn(levels);

        var memoryEstimation = new LouvainMemoryEstimateDefinition().memoryEstimation(config);

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


        var config = mock(LouvainBaseConfig.class);
        when(config.includeIntermediateCommunities()).thenReturn(false);
        when(config.maxLevels()).thenReturn(1);

        var memoryEstimation = new LouvainMemoryEstimateDefinition().memoryEstimation(config);

        MemoryTree memoryTree = memoryEstimation
            .estimate(dimensionsWithoutProperties, 1);

        MemoryTree memoryTreeOneProperty = memoryEstimation
            .estimate(dimensionsWithOneProperty, 1);

        MemoryTree memoryTreeTwoProperties =  memoryEstimation
            .estimate(dimensionsWithTwoProperties, 1);

        assertEquals(memoryTree.memoryUsage(), memoryTreeOneProperty.memoryUsage());
        assertEquals(memoryTreeOneProperty.memoryUsage(), memoryTreeTwoProperties.memoryUsage());
    }

}
