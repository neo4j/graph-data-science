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
package org.neo4j.gds.beta.undirected;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class ToUndirectedAlgorithmFactory extends GraphStoreAlgorithmFactory<ToUndirected, ToUndirectedConfig> {

    @Override
    public ToUndirected build(
        GraphStore graphStore,
        ToUndirectedConfig configuration,
        ProgressTracker progressTracker
    ) {
        return new ToUndirected(graphStore, configuration, progressTracker, Pools.DEFAULT);
    }

    @Override
    public String taskName() {
        return "ToUndirected";
    }

    @Override
    public Task progressTask(GraphStore graphStore, ToUndirectedConfig config) {
        return Tasks.task(
            "ToUndirected",
            Tasks.leaf("Create Undirected Relationships", graphStore.nodeCount()),
            Tasks.leaf("Build undirected Adjacency list")
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(ToUndirectedConfig configuration) {
        RelationshipType relationshipType = configuration.internalRelationshipType();

        var builder = MemoryEstimations.builder(ToUndirected.class)
            .add("relationships", AdjacencyListBehavior.adjacencyListEstimation(relationshipType, true));

        builder.perGraphDimension("properties", ((graphDimensions, concurrency) -> {
            long max = graphDimensions.relationshipPropertyTokens().keySet().stream().mapToLong(__ ->
                AdjacencyListBehavior
                    .adjacencyPropertiesEstimation(relationshipType, true)
                    .estimate(graphDimensions, concurrency)
                    .memoryUsage().max
            ).sum();
            return MemoryRange.of(0, max);
        }));

        return builder.build();
    }
}
