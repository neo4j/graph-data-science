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
package org.neo4j.gds.core.loading;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.AdjacencyListBuilderFactory;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.compression.common.MemoryTracker;
import org.neo4j.gds.core.compression.varlong.CompressedAdjacencyListBuilder;
import org.neo4j.gds.core.compression.varlong.CompressedAdjacencyListBuilderFactory;
import org.neo4j.gds.core.compression.varlong.DeltaVarLongCompressor;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryTree;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AdjacencyBufferTest {

    @Test
    void test() {
        var nodeCount = 99L;
        var adjacencyBuffer = AdjacencyBuffer.of(SingleTypeRelationshipImporter.ImportMetaData.of(RelationshipProjection.of(
            "T",
            Orientation.UNDIRECTED
        ), 1, Map.of(), false),
            DeltaVarLongCompressor.factory(
                () -> nodeCount,
                CompressedAdjacencyListBuilderFactory.of(),
                PropertyMappings.builder().build(),
                new Aggregation[0],
                true,
                MemoryTracker.EMPTY
            ), ImportSizing.of(new Concurrency(4), nodeCount));

        var tasks = adjacencyBuffer.adjacencyListBuilderTasks(Optional.empty(), Optional.empty());

        tasks.forEach(Runnable::run);
    }

    @Test
    void memoryEstimationShouldGrowLinearlyWithNodeCount() {
        var estimationWith1Property = estimate(10_000_000, 10, 1, new Concurrency(4));
        var estimationWith2Property = estimate(100_000_000, 10, 1, new Concurrency(4));
        var estimationWith3Property = estimate(1_000_000_000, 10, 1, new Concurrency(4));

        var min1 = estimationWith1Property.memoryUsage().min;
        var min2 = estimationWith2Property.memoryUsage().min;
        var min3 = estimationWith3Property.memoryUsage().min;
        assertThat((double) min1 / min2).isCloseTo((double) min2 / min3, Percentage.withPercentage(20));

        var max1 = estimationWith1Property.memoryUsage().max;
        var max2 = estimationWith2Property.memoryUsage().max;
        var max3 = estimationWith3Property.memoryUsage().max;
        assertThat((double) max1 / max2).isCloseTo((double) max2 / max3, Percentage.withPercentage(20));
    }

    private MemoryTree estimate(long nodeCount, long avgDegree, int propertyCount, Concurrency concurrency) {
        var dimensions = ImmutableGraphDimensions
            .builder()
            .nodeCount(nodeCount)
            .build();

        return AdjacencyBuffer.memoryEstimation(avgDegree, nodeCount, propertyCount, concurrency).estimate(dimensions, concurrency);
    }
}
