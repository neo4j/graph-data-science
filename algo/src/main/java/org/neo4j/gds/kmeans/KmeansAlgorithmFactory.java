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
package org.neo4j.gds.kmeans;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;

public final class KmeansAlgorithmFactory<CONFIG extends KmeansBaseConfig> extends GraphAlgorithmFactory<Kmeans, CONFIG> {

    public KmeansAlgorithmFactory() {
        super();
    }

    @Override
    public String taskName() {
        return "Kmeans";
    }

    @Override
    public Kmeans build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var seedCentroids = (List) configuration.seedCentroids();
        if (configuration.numberOfRestarts() > 1 && seedCentroids.size() > 0) {
            throw new IllegalArgumentException("K-Means cannot be run multiple time when seeded");
        }
        if (seedCentroids.size() > 0 && seedCentroids.size() != configuration.k()) {
            throw new IllegalArgumentException("Incorrect number of seeded centroids given for running K-Means");
        }
        return Kmeans.createKmeans(graph, configuration, ImmutableKmeansContext
            .builder()
            .progressTracker(progressTracker)
            .executor(Pools.DEFAULT)
            .build());
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {

        var iterations = config.numberOfRestarts();
        if (iterations == 1) {
            return kMeansTask(graph,taskName(), config);
        }

        return Tasks.iterativeFixed(
            taskName(),
            () -> List.of(kMeansTask(graph,"KMeans Iteration", config)),
            iterations
        );
    }

    @NotNull
    private Task kMeansTask(Graph graph, String description, CONFIG config) {
        if (config.computeSilhouette()) {
            return Tasks.task(description, List.of(
                Tasks.leaf("Initialization", config.k()),
                Tasks.iterativeDynamic("Main", () -> List.of(Tasks.leaf("Iteration")), config.maxIterations()),
                Tasks.leaf("Silhouette", graph.nodeCount())

                ));
        } else {
            return Tasks.task(description, List.of(
                Tasks.leaf("Initialization", config.k()),
                Tasks.iterativeDynamic("Main", () -> List.of(Tasks.leaf("Iteration")), config.maxIterations())
            ));
        }
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var fakeLength = 128;
        var builder = MemoryEstimations.builder(Kmeans.class)
            .perNode("bestCommunities", HugeIntArray::memoryEstimation)
            .fixed(
                "bestCentroids",
                MemoryUsage.sizeOfArray(configuration.k(), MemoryUsage.sizeOfDoubleArray(fakeLength))
            )
            .perNode("nodesInCluster", MemoryUsage::sizeOfLongArray)
            .perNode("distanceFromCentroid", HugeDoubleArray::memoryEstimation)
            .add(ClusterManager.memoryEstimation(
                configuration.k(),
                fakeLength
            ))
            .perThread("KMeansTask", KmeansTask.memoryEstimation(configuration.k(), fakeLength));

        if(configuration.computeSilhouette()) {
            builder.perNode("silhouette", HugeDoubleArray::memoryEstimation);
        }

        if(configuration.isSeeded()) {
            var centroids = configuration.seedCentroids();
            builder.fixed("seededCentroids", MemoryUsage.sizeOf(centroids));
        }

        return builder.build();
    }
}
