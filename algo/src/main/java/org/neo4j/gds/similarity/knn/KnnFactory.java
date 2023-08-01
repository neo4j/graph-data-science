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
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArrayEstimation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfOpenHashContainer;

public class KnnFactory<CONFIG extends KnnBaseConfig> extends GraphAlgorithmFactory<Knn, CONFIG> {

    private static final String KNN_BASE_TASK_NAME = "Knn";

    @Override
    public String taskName() {
        return KNN_BASE_TASK_NAME;
    }

    @Override
    public Knn build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return Knn.createWithDefaults(
            graph,
            configuration,
            ImmutableKnnContext
                .builder()
                .progressTracker(progressTracker)
                .executor(Pools.DEFAULT)
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
                var tempListEstimation = HugeObjectArrayEstimation.objectArray(
                    MemoryEstimations.of("elements", MemoryRange.of(
                        0,
                        sizeOfInstance(LongArrayList.class) + sizeOfLongArray(sampledK)
                    ))
                );
                return MemoryEstimations
                    .builder(Knn.class)
                    .add(
                        "top-k-neighbors-list",
                        HugeObjectArrayEstimation.objectArray(NeighborList.memoryEstimation(boundedK))
                    )
                    .add("old-neighbors", tempListEstimation)
                    .add("new-neighbors", tempListEstimation)
                    .add("old-reverse-neighbors", tempListEstimation)
                    .add("new-reverse-neighbors", tempListEstimation)
                    .fixed(
                        "initial-random-neighbors (per thread)",
                        initialSamplerMemoryEstimation(configuration.initialSampler(), boundedK).times(concurrency)
                    )
                    .fixed(
                        "sampled-random-neighbors (per thread)",
                        MemoryRange.of(
                            sizeOfIntArray(sizeOfOpenHashContainer(sampledK)) * concurrency
                        )
                    )
                    .add(MemoryEstimations.of("neighbour-consumers", MemoryRange.of(sizeOfInstance(NeighbourConsumers.class))))
                    .build();
            }
        );
    }

    public static MemoryRange initialSamplerMemoryEstimation(KnnSampler.SamplerType samplerType, long boundedK) {
        switch(samplerType) {
            case UNIFORM: {
                return UniformKnnSampler.memoryEstimation(boundedK);
            }
            case RANDOMWALK: {
                return RandomWalkKnnSampler.memoryEstimation(boundedK);
            }
            default:
                throw new IllegalStateException("Invalid KnnSampler");
        }
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return knnTaskTree(graph, config);
    }

    public static Task knnTaskTree(Graph graph, KnnBaseConfig config) {
        return Tasks.task(
            KNN_BASE_TASK_NAME,
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
