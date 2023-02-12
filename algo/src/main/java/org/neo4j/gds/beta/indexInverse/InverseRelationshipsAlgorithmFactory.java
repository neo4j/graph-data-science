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

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InverseRelationshipsAlgorithmFactory extends GraphStoreAlgorithmFactory<InverseRelationships, InverseRelationshipsConfig> {

    @Override
    public InverseRelationships build(
        GraphStore graphStore,
        InverseRelationshipsConfig configuration,
        ProgressTracker progressTracker
    ) {
        return new InverseRelationships(graphStore, configuration, progressTracker, Pools.DEFAULT);
    }

    @Override
    public String taskName() {
        return "IndexInverse";
    }

    @Override
    public Task progressTask(GraphStore graphStore, InverseRelationshipsConfig config) {
        long nodeCount = graphStore.nodeCount();

        Collection<RelationshipType> relationshipTypes = config.internalRelationshipTypes(graphStore);

        List<Task> tasks = relationshipTypes.stream().flatMap(type -> Stream.of(
            Tasks.leaf(String.format(Locale.US, "Create inverse relationships of type '%s'", type.name), nodeCount),
            Tasks.leaf("Build Adjacency list")
        )).collect(Collectors.toList());

        return Tasks.task(taskName(), tasks);
    }

    @Override
    public MemoryEstimation memoryEstimation(InverseRelationshipsConfig configuration) {
        var relationshipTypes = configuration.relationshipTypes();

        var builder = MemoryEstimations.builder(InverseRelationships.class);

        for (String typeName : relationshipTypes) {
            var builderForType = MemoryEstimations.builder();
            if (typeName.equals(ElementProjection.PROJECT_ALL)) {
                builderForType.add(
                    "All relationships",
                    AdjacencyListBehavior.adjacencyListsFromStarEstimation(false)
                );

                builderForType.perGraphDimension("All properties", ((graphDimensions, concurrency) -> {
                    var singlePropertyEstimation = AdjacencyListBehavior
                        .adjacencyPropertiesFromStarEstimation( false)
                        .estimate(graphDimensions, concurrency)
                        .memoryUsage();

                    return singlePropertyEstimation.times(graphDimensions.relationshipPropertyTokens().size());
                }));

                builder.add(String.format(Locale.US, "Inverse '%s'", typeName), builderForType.build());
            } else {
                var relationshipType = RelationshipType.of(typeName);

                builderForType.add(
                    "relationships",
                    AdjacencyListBehavior.adjacencyListEstimation(relationshipType, false)
                );


                builderForType.perGraphDimension("properties", ((graphDimensions, concurrency) -> {
                    var singlePropertyEstimation = AdjacencyListBehavior
                        .adjacencyPropertiesEstimation(relationshipType, false)
                        .estimate(graphDimensions, concurrency)
                        .memoryUsage();

                    return singlePropertyEstimation.times(graphDimensions.relationshipPropertyTokens().size());
                }));

                builder.add(String.format(Locale.US, "Inverse '%s'", typeName), builderForType.build());
            }
        }

        return builder.build();
    }
}
