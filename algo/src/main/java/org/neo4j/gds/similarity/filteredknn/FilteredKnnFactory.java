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
package org.neo4j.gds.similarity.filteredknn;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.commons.lang3.function.TriFunction;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.similarity.knn.ImmutableKnnContext;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnFactory;
import org.neo4j.gds.similarity.knn.NeighborList;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfOpenHashContainer;

public class FilteredKnnFactory<CONFIG extends FilteredKnnBaseConfig> extends GraphAlgorithmFactory<FilteredKnn, CONFIG> {
    private static final String FILTERED_KNN_TASK_NAME = "Filtered KNN";

    private final TriFunction<Graph, CONFIG, KnnContext, FilteredKnn> unseededFilteredKnnSupplier;
    private final TriFunction<Graph, CONFIG, KnnContext, FilteredKnn> seededFilteredKnnSupplier;

    public FilteredKnnFactory() {
        this(FilteredKnn::createWithoutSeeding, FilteredKnn::createWithDefaultSeeding);
    }

    FilteredKnnFactory(
        TriFunction<Graph, CONFIG, KnnContext, FilteredKnn> unseededFilteredKnnSupplier,
        TriFunction<Graph, CONFIG, KnnContext, FilteredKnn> seededFilteredKnnSupplier
    ) {
        this.unseededFilteredKnnSupplier = unseededFilteredKnnSupplier;
        this.seededFilteredKnnSupplier = seededFilteredKnnSupplier;
    }

    @Override
    public String taskName() {
        return FILTERED_KNN_TASK_NAME;
    }

    @Override
    public FilteredKnn build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        KnnContext knnContext = ImmutableKnnContext
            .builder()
            .progressTracker(progressTracker)
            .executor(DefaultPool.INSTANCE)
            .build();

        if (configuration.seedTargetNodes()) {
            return seededFilteredKnnSupplier.apply(graph, configuration, knnContext);
        }

        return unseededFilteredKnnSupplier.apply(graph, configuration, knnContext);
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
                    .builder(FilteredKnn.class)
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
                        KnnFactory
                            .initialSamplerMemoryEstimation(configuration.initialSampler(), boundedK)
                            .times(concurrency)
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
        return KnnFactory.knnTaskTree(graph, config);
    }
}
