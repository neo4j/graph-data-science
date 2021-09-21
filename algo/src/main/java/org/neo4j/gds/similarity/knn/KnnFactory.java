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
package org.neo4j.gds.similarity.knn;

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfOpenHashContainer;

public class KnnFactory<CONFIG extends KnnBaseConfig> extends AlgorithmFactory<Knn, CONFIG> {

    @Override
    protected String taskName() {
        return "Knn";
    }

    @Override
    protected Knn build(
        Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
    ) {
        return new Knn(
            graph,
            configuration,
            ImmutableKnnContext
                .builder()
                .progressTracker(progressTracker)
                .executor(Pools.DEFAULT)
                .allocationTracker(allocationTracker)
                .build()
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return MemoryEstimations.setup(
            taskName(),
            (dim, concurrency) -> {
                var boundedK = configuration.boundedK(dim.nodeCount());
                var sampledK = configuration.sampledK(dim.nodeCount());
                var tempListEstimation = HugeObjectArray.memoryEstimation(
                    MemoryEstimations.of("elements", MemoryRange.of(
                        0,
                        sizeOfInstance(LongArrayList.class) + sizeOfLongArray(sampledK)
                    ))
                );
                return MemoryEstimations
                    .builder(Knn.class)
                    .add(
                        "top-k-neighbors-list",
                        HugeObjectArray.memoryEstimation(NeighborList.memoryEstimation(boundedK))
                    )
                    .add("old-neighbors", tempListEstimation)
                    .add("new-neighbors", tempListEstimation)
                    .add("old-reverse-neighbors", tempListEstimation)
                    .add("new-reverse-neighbors", tempListEstimation)
                    .fixed(
                        "initial-random-neighbors (per thread)",
                        MemoryRange.of(
                            sizeOfLongArray(sizeOfOpenHashContainer(boundedK)) * concurrency
                        )
                    )
                    .fixed(
                        "sampled-random-neighbors (per thread)",
                        MemoryRange.of(
                            sizeOfIntArray(sizeOfOpenHashContainer(sampledK)) * concurrency
                        )
                    )
                    .build();
            }
        );
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("Initialize random neighbors", graph.nodeCount()),
            Tasks.iterativeDynamic(
                "Iteration",
                () -> List.of(
                    Tasks.leaf("Split old and new neighbors", graph.nodeCount()),
                    Tasks.leaf("Reverse old and new neighbors", graph.nodeCount()),
                    Tasks.leaf("Join neighbors", graph.nodeCount())
                ),
                config.maxIterations()
            )
        );
    }
}
