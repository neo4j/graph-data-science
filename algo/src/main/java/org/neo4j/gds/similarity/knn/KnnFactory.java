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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.List;
import java.util.function.LongFunction;

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
        var parameters = configuration.toParameters().finalize(graph.nodeCount());
        return Knn.create(
            graph,
            parameters,
            SimilarityComputer.ofProperties(graph, parameters.nodePropertySpecs()),
            new KnnNeighborFilterFactory(graph.nodeCount()),
            ImmutableKnnContext
                .builder()
                .progressTracker(progressTracker)
                .executor(DefaultPool.INSTANCE)
                .build()
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return KnnFactory.memoryEstimation(taskName(), Knn.class, configuration);
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
        return knnTaskTree(graph.nodeCount(), config.maxIterations());
    }

    public static Task knnTaskTree(long nodeCount, int maxIterations) {
        return Tasks.task(
            KNN_BASE_TASK_NAME,
            Tasks.leaf("Initialize random neighbors", nodeCount),
            Tasks.iterativeDynamic(
                "Iteration",
                () -> List.of(
                    Tasks.leaf("Split old and new neighbors", nodeCount),
                    Tasks.leaf("Reverse old and new neighbors", nodeCount),
                    Tasks.leaf("Join neighbors", nodeCount)
                ),
                maxIterations
            )
        );
    }

    public static <CONFIG extends KnnBaseConfig>  MemoryEstimation memoryEstimation(
        String taskName,
        Class<?> clazz,
        CONFIG configuration
    ) {
        return MemoryEstimations.setup(
            taskName,
            (dim, concurrency) -> {
                var k = configuration.k(dim.nodeCount());

                LongFunction<MemoryRange> tempListEstimation = nodeCount -> MemoryRange.of(
                    HugeObjectArray.memoryEstimation(nodeCount, 0),
                    HugeObjectArray.memoryEstimation(
                        nodeCount,
                        sizeOfInstance(LongArrayList.class) + sizeOfLongArray(k.sampledValue)
                    )
                );

                var neighborListEstimate = NeighborList.memoryEstimation(k.value)
                    .estimate(dim, concurrency)
                    .memoryUsage();

                LongFunction<MemoryRange> perNodeNeighborListEstimate = nodeCount -> MemoryRange.of(
                    HugeObjectArray.memoryEstimation(nodeCount, neighborListEstimate.min),
                    HugeObjectArray.memoryEstimation(nodeCount, neighborListEstimate.max)
                );

                return MemoryEstimations
                    .builder(clazz)
                    .rangePerNode("top-k-neighbors-list", perNodeNeighborListEstimate)
                    .rangePerNode("old-neighbors", tempListEstimation)
                    .rangePerNode("new-neighbors", tempListEstimation)
                    .rangePerNode("old-reverse-neighbors", tempListEstimation)
                    .rangePerNode("new-reverse-neighbors", tempListEstimation)
                    .fixed(
                        "initial-random-neighbors (per thread)",
                        KnnFactory
                            .initialSamplerMemoryEstimation(configuration.initialSampler(), k.value)
                            .times(concurrency)
                    )
                    .fixed(
                        "sampled-random-neighbors (per thread)",
                        MemoryRange.of(
                            sizeOfIntArray(sizeOfOpenHashContainer(k.sampledValue)) * concurrency
                        )
                    )
                    .build();
            }
        );
    }
}
