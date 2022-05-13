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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GraphSageBatchSamplerTest {

    @GdlGraph
    public static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    Graph graph;

    @Test
    void seededNegativeBatch() {
        var batchSize = 5;
        var seed = 20L;

        var sampler = new GraphSageBatchSampler(seed);
        var otherSampler = new GraphSageBatchSampler(seed);

        var partitions = PartitionUtils.rangePartitionWithBatchSize(
            100,
            batchSize,
            Function.identity()
        );

        long[] neighborsSet = {0, 3, 5, 6, 10};

        for (int i = 0; i < partitions.size(); i++) {
            var localSeed = i + seed;
            var negativeBatch = sampler.negativeBatch(graph, Math.toIntExact(partitions.get(i).nodeCount()), neighborsSet, localSeed);
            var otherNegativeBatch = otherSampler.negativeBatch(graph, Math.toIntExact(partitions.get(i).nodeCount()), neighborsSet, localSeed);

            assertThat(negativeBatch).containsExactlyElementsOf(otherNegativeBatch.boxed().collect(Collectors.toList()));
        }
    }

    @Test
    void seededNeighborBatch() {
        var batchSize = 5;
        var seed = 20L;
        int searchDepth = 12;

        var sampler = new GraphSageBatchSampler(seed);
        var otherSampler = new GraphSageBatchSampler(seed);

        var partitions = PartitionUtils.rangePartitionWithBatchSize(
            graph.nodeCount(),
            batchSize,
            Function.identity()
        );

        for (int i = 0; i < partitions.size(); i++) {
            var localSeed = i + seed;
            var neighborBatch = sampler.neighborBatch(graph, partitions.get(i), localSeed, searchDepth);
            var otherNeighborBatch = otherSampler.neighborBatch(graph, partitions.get(i), localSeed, searchDepth);
            assertThat(neighborBatch).containsExactlyElementsOf(otherNeighborBatch.boxed().collect(Collectors.toList()));
        }
    }
}
