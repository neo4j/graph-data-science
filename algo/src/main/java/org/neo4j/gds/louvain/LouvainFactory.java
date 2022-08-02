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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.modularity.ModularityOptimizationFactory;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.NativeFactory;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public class LouvainFactory<CONFIG extends LouvainBaseConfig> extends GraphAlgorithmFactory<Louvain, CONFIG> {

    @Override
    public String taskName() {
        return "Louvain";
    }

    @Override
    public Louvain build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return new Louvain(
            graph,
            configuration,
            configuration.includeIntermediateCommunities(),
            configuration.maxLevels(),
            configuration.maxIterations(),
            configuration.tolerance(),
            configuration.concurrency(),
            progressTracker, Pools.DEFAULT

        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return MemoryEstimations.builder(Louvain.class)
            .add("modularityOptimization()", ModularityOptimizationFactory.MEMORY_ESTIMATION)
            .rangePerGraphDimension("subGraph", (graphDimensions, concurrency) -> {
                ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions
                    .builder()
                    .from(graphDimensions);

                GraphDimensions sparseDimensions = dimensionsBuilder.build();

                // Louvain creates a new graph every iteration, this graph has one relationship property
                RelationshipProjections relationshipProjections = RelationshipProjections.builder()
                    .putProjection(
                        RelationshipType.of("AGGREGATE"),
                        RelationshipProjection.builder()
                            .type("AGGREGATE")
                            .orientation(Orientation.UNDIRECTED)
                            .aggregation(Aggregation.SUM)
                            .addProperty("prop", "prop", DefaultValue.of(0.0))
                            .build()
                    )
                    .build();

                long maxGraphSize = NativeFactory
                    .getMemoryEstimation(NodeProjections.all(), relationshipProjections, false)
                    .estimate(sparseDimensions, concurrency)
                    .memoryUsage()
                    .max;

                return MemoryRange.of(1L, maxGraphSize); // rough estimate of graph size
            })
            .rangePerNode("dendrograms", (nodeCount) -> MemoryRange.of(
                HugeLongArray.memoryEstimation(nodeCount),
                HugeLongArray.memoryEstimation(nodeCount) * (config.includeIntermediateCommunities() ? config.maxLevels() : Math.min(
                    2,
                    config.maxLevels()))
            ))
            .build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.iterativeDynamic(
            taskName(),
            () -> List.of(ModularityOptimizationFactory.modularityOptimizationProgressTask(graph, config)),
            config.maxLevels()
        );
    }
}
