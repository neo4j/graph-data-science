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
package org.neo4j.gds.beta.indexInverse;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.CompressedAdjacencyList;
import org.neo4j.gds.core.huge.UncompressedAdjacencyList;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class IndexInverseAlgorithmFactory extends GraphStoreAlgorithmFactory<IndexInverse, IndexInverseConfig> {

    @Override
    public IndexInverse build(
        GraphStore graphStore,
        IndexInverseConfig configuration,
        ProgressTracker progressTracker
    ) {
        return new IndexInverse(graphStore, configuration, progressTracker, Pools.DEFAULT);
    }

    @Override
    public String taskName() {
        return "IndexInverse";
    }

    @Override
    public Task progressTask(GraphStore graphStore, IndexInverseConfig config) {
        long nodeCount = graphStore.nodeCount();
        return Tasks.task(
            taskName(),
            Tasks.leaf("Create inversely indexed relationships", nodeCount),
            Tasks.leaf("Build Adjacency list")
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(IndexInverseConfig configuration) {
        RelationshipType relationshipType = RelationshipType.of(configuration.relationshipType());

        var adjacencyListEstimations = CompressedAdjacencyList.adjacencyListEstimation(relationshipType, false);
        var builder = MemoryEstimations.builder(IndexInverse.class)
            .add("relationships", adjacencyListEstimations)
            .add("inverse relationships", adjacencyListEstimations);

        builder.perGraphDimension("properties", ((graphDimensions, concurrency) -> {
            long max = graphDimensions.relationshipPropertyTokens().keySet().stream().mapToLong(__ ->
                UncompressedAdjacencyList
                    .adjacencyPropertiesEstimation(relationshipType, false)
                    .estimate(graphDimensions, concurrency)
                    .memoryUsage().max
            ).sum();
            return MemoryRange.of(0, 2 * max);
        }));

        return builder.build();
    }
}
