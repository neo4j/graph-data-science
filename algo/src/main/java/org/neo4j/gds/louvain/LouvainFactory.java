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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.modularity.ModularityOptimizationFactory;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.NativeFactory;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.logging.Log;

import java.util.List;

public class LouvainFactory<CONFIG extends LouvainBaseConfig> implements AlgorithmFactory<Louvain, CONFIG> {

    @Override
    public Louvain build(
        final Graph graph,
        final CONFIG configuration,
        final AllocationTracker tracker,
        final Log log,
        ProgressEventTracker eventTracker
    ) {
        var progressLogger = new BatchingProgressLogger(
            log,
            1,
            "Louvain",
            configuration.concurrency(),
            eventTracker
        );

        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger, eventTracker);

        return new Louvain(
            graph,
            configuration,
            Pools.DEFAULT,
            progressTracker,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return MemoryEstimations.builder(Louvain.class)
            .add("modularityOptimization()", ModularityOptimizationFactory.MEMORY_ESTIMATION)
            .rangePerGraphDimension("subGraph", (graphDimensions, concurrency) -> {
                ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions.builder().from(graphDimensions);

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
                    .getMemoryEstimation(NodeProjections.all(), relationshipProjections)
                    .estimate(sparseDimensions, concurrency)
                    .memoryUsage()
                    .max;

                return MemoryRange.of(1L, maxGraphSize); // rough estimate of graph size
            })
            .rangePerNode("dendrograms", (nodeCount) -> MemoryRange.of(
                HugeLongArray.memoryEstimation(nodeCount),
                HugeLongArray.memoryEstimation(nodeCount) * config.maxLevels()
            ))
            .build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.iterativeDynamic(
            "compute",
            () -> List.of(ModularityOptimizationFactory.modularityOptimizationProgressTask(graph, config)),
            config.maxLevels()
        );
    }
}
