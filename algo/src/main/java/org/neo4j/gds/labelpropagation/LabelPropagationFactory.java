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
package org.neo4j.gds.labelpropagation;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfLongArray;

public class LabelPropagationFactory<CONFIG extends LabelPropagationBaseConfig> extends AlgorithmFactory<LabelPropagation, CONFIG> {

    @Override
    protected String taskName() {
        return "LabelPropagation";
    }

    @Override
    protected LabelPropagation build(
        Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
    ) {
        return new LabelPropagation(
            graph,
            configuration,
            Pools.DEFAULT,
            progressTracker,
            allocationTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return MemoryEstimations.builder(LabelPropagation.class)
            .perNode("labels", HugeLongArray::memoryEstimation)
            .perThread("votes", MemoryEstimations.builder()
                .field("init step", InitStep.class)
                .field("compute step", ComputeStep.class)
                .field("step runner", StepRunner.class)
                .field("compute step consumer", ComputeStepConsumer.class)
                .field("votes container", LongDoubleScatterMap.class)
                .rangePerNode("votes", nodeCount -> {
                    long minBufferSize = MemoryUsage.sizeOfEmptyOpenHashContainer();
                    long maxBufferSize = MemoryUsage.sizeOfOpenHashContainer(nodeCount);
                    if (maxBufferSize < minBufferSize) {
                        maxBufferSize = minBufferSize;
                    }
                    long min = sizeOfLongArray(minBufferSize) + sizeOfDoubleArray(minBufferSize);
                    long max = sizeOfLongArray(maxBufferSize) + sizeOfDoubleArray(maxBufferSize);
                    return MemoryRange.of(min, max);
                }).build())
            .build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("Initialization", graph.relationshipCount()),
            Tasks.iterativeDynamic(
                "Assign labels",
                () -> List.of(Tasks.leaf("Iteration", graph.relationshipCount())),
                config.maxIterations()
            )
        );
    }
}
