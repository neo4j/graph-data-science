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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;

public class BetweennessCentralityFactory<CONFIG extends BetweennessCentralityBaseConfig> extends GraphAlgorithmFactory<BetweennessCentrality, CONFIG> {

    @Override
    public String taskName() {
        return "BetweennessCentrality";
    }

    @Override
    public BetweennessCentrality build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var samplingSize = configuration.samplingSize();
        var samplingSeed = configuration.samplingSeed();

        var strategy = samplingSize.isPresent() && samplingSize.get() < graph.nodeCount()
            ? new SelectionStrategy.RandomDegree(samplingSize.get(), samplingSeed)
            : SelectionStrategy.ALL;

        return new BetweennessCentrality(
            graph,
            strategy,
            false,
            Pools.DEFAULT,
            configuration.concurrency(),
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return MemoryEstimations.builder(BetweennessCentrality.class)
            .perNode("centrality scores", HugeAtomicDoubleArray::memoryEstimation)
            .perThread("compute task", MemoryEstimations.builder(BetweennessCentrality.BCTask.class)
                .add("predecessors", MemoryEstimations.setup("", (dimensions, concurrency) -> {
                    // Predecessors are represented by LongArrayList which wrap a long[]
                    long averagePredecessorSize = sizeOfLongArray(dimensions.averageDegree());
                    return MemoryEstimations.builder(HugeObjectArray.class)
                        .perNode("array", nodeCount -> nodeCount * averagePredecessorSize)
                        .build();
                }))
                .perNode("forwardNodes", HugeLongArray::memoryEstimation)
                .perNode("backwardNodes", HugeLongArray::memoryEstimation)
                .perNode("deltas", HugeDoubleArray::memoryEstimation)
                .perNode("sigmas", HugeLongArray::memoryEstimation)
                .perNode("distances", HugeIntArray::memoryEstimation)
                .build())
            .build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.nodeCount());
    }
}
