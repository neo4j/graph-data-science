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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.MemoryUsage;

public class CELFAlgorithmFactory<CONFIG extends InfluenceMaximizationBaseConfig> extends GraphAlgorithmFactory<CELF, CONFIG> {

    public static final int DEFAULT_BATCH_SIZE = 10;

    @Override
    public String taskName() {
        return "CELF";
    }

    @Override
    public CELF build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return new CELF(
            graph,
            configuration.seedSetSize(),
            configuration.propagationProbability(),
            configuration.monteCarloSimulations(),
            Pools.DEFAULT,
            configuration.concurrency(),
            configuration.randomSeed().orElse(0L),
            DEFAULT_BATCH_SIZE,
            progressTracker
        );
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            "CELF",
            Tasks.leaf("Greedy", graph.nodeCount()),
            Tasks.leaf("LazyForwarding", config.seedSetSize() - 1)
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(CELF.class);

        //CELF class
        builder.fixed(
                "seedSet",
                MemoryUsage.sizeOfLongDoubleScatterMap(configuration.seedSetSize())
            )
            .fixed("firstK", MemoryUsage.sizeOfLongArray(DEFAULT_BATCH_SIZE))
            .perNode("LazyForwarding: spread priority queue", HugeLongPriorityQueue.memoryEstimation())
            .perNode("greedy part: single spread array: ", HugeDoubleArray::memoryEstimation);

        //ICInitTask class

        builder
            .perGraphDimension(
                "active",
                (dimensions, concurrency) -> MemoryRange.of(MemoryUsage.sizeOfBitset(dimensions.nodeCount()))
            );
        builder.perNode("newActive", HugeLongArrayStack.memoryEstimation().times(configuration.concurrency()));


        //ICLazyMC
        builder.fixed("spread", MemoryUsage.sizeOfDoubleArray(DEFAULT_BATCH_SIZE));
        //ICLazyMCTask class

        builder
            .perGraphDimension(
                "seedActive",
                (dimensions, concurrency) -> MemoryRange.of(MemoryUsage.sizeOfBitset(dimensions.nodeCount()))
            ).perGraphDimension(
                "candidateActive",
                (dimensions, concurrency) -> MemoryRange.of(MemoryUsage.sizeOfBitset(dimensions.nodeCount()))
            ).perGraphDimension(
                "seedSetNodes",
                (dimensions, concurrency) -> MemoryRange.of(MemoryUsage.sizeOfLongArray(configuration.seedSetSize()))
            ).perGraphDimension(
                "candidateNodeIds",
                (dimensions, concurrency) -> MemoryRange.of(MemoryUsage.sizeOfLongArray(DEFAULT_BATCH_SIZE))
            ).perGraphDimension(
                "localSpread",
                (dimensions, concurrency) -> MemoryRange.of(MemoryUsage.sizeOfDoubleArray(DEFAULT_BATCH_SIZE))
            );
        builder.perNode("newActive", HugeLongArrayStack.memoryEstimation().times(configuration.concurrency()));
        return builder.build();
    }

}
