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
package org.neo4j.gds.beta.modularity;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.beta.k1coloring.K1ColoringFactory;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public class ModularityOptimizationFactory<T extends ModularityOptimizationConfig> extends AlgorithmFactory<ModularityOptimization, T> {

    private static final String MODULARITY_OPTIMIZATION_TASK_NAME = "ModularityOptimization";

    public static final MemoryEstimation MEMORY_ESTIMATION =
        MemoryEstimations.builder(ModularityOptimization.class)
            .perNode("currentCommunities", HugeLongArray::memoryEstimation)
            .perNode("nextCommunities", HugeLongArray::memoryEstimation)
            .perNode("cumulativeNodeWeights", HugeDoubleArray::memoryEstimation)
            .perNode("nodeCommunityInfluences", HugeDoubleArray::memoryEstimation)
            .perNode("communityWeights", HugeAtomicDoubleArray::memoryEstimation)
            .perNode("colorsUsed", MemoryUsage::sizeOfBitset)
            .perNode("colors", HugeLongArray::memoryEstimation)
            .rangePerNode(
                "reversedSeedCommunityMapping", (nodeCount) ->
                    MemoryRange.of(0, HugeLongArray.memoryEstimation(nodeCount))
            )
            .perNode("communityWeightUpdates", HugeAtomicDoubleArray::memoryEstimation)
            .perThread("ModularityOptimizationTask", MemoryEstimations.builder()
                .rangePerNode(
                    "communityInfluences",
                    (nodeCount) -> MemoryRange.of(
                        MemoryUsage.sizeOfLongDoubleHashMap(50),
                        MemoryUsage.sizeOfLongDoubleHashMap(Math.max(50, nodeCount))
                    )
                )
                .build()
            )
            .build();

    @Override
    public MemoryEstimation memoryEstimation(T configuration) {
        return MEMORY_ESTIMATION;
    }

    @Override
    protected String taskName() {
        return MODULARITY_OPTIMIZATION_TASK_NAME;
    }

    @Override
    protected ModularityOptimization build(
        Graph graph, T configuration, AllocationTracker tracker, ProgressTracker progressTracker
    ) {
        var seedProperty = configuration.seedProperty() != null ? graph.nodeProperties(configuration.seedProperty()) : null;
        return build(graph, configuration, seedProperty, tracker, progressTracker);
    }

    public ModularityOptimization build(
        Graph graph,
        T configuration,
        NodeProperties seedProperty,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        return new ModularityOptimization(
            graph,
            configuration.maxIterations(),
            configuration.tolerance(),
            seedProperty,
            configuration.concurrency(),
            configuration.batchSize(),
            Pools.DEFAULT,
            progressTracker,
            tracker
        );
    }

    @Override
    public Task progressTask(Graph graph, T config) {
        return modularityOptimizationProgressTask(graph, config);
    }

    public static <T extends BaseConfig & IterationsConfig> Task modularityOptimizationProgressTask(Graph graph, T config) {
        return Tasks.task(
            MODULARITY_OPTIMIZATION_TASK_NAME,
            Tasks.task(
                "initialization",
                K1ColoringFactory.k1ColoringProgressTask(graph, config)
            ),
            Tasks.iterativeDynamic(
                "compute modularity",
                () -> List.of(Tasks.leaf("optimizeForColor", graph.relationshipCount())),
                config.maxIterations()
            )
        );
    }
}
