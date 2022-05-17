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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class FilteredKnn extends Algorithm<FilteredKnnResult> {
    private final ExecutorService executorService;
    private final int concurrency;
    private final Graph graph;
    private final SimilarityComputer similarityComputer;
    private final FilteredNeighborFilterFactory neighborFilterFactory;
    private final SplittableRandom splittableRandom;
    private final Function<SplittableRandom, FilteredKnnSampler> samplerSupplier;

    private final double deltaThreshold;
    private final int maxIterations;
    private final int minBatchSize;
    private final double perturbationRate;
    private final int randomJoins;
    private final int sampledK;
    private final double sampleRate;
    private final double similarityCutoff;
    private final NodeFilter sourceNodeFilter;
    private final NodeFilter targetNodeFilter;
    private final int topK;

    private long nodePairsConsidered;

    public static FilteredKnn createWithDefaults(Graph graph, FilteredKnnBaseConfig config, FilteredKnnContext context) {
        var similarityComputer = SimilarityComputer.ofProperties(graph, config.nodeProperties());
        var neighborFilterFactory = new FilteredKnnNeighborFilterFactory(graph.nodeCount());
        return create(graph, config, context, similarityComputer, neighborFilterFactory);
    }

    public static FilteredKnn create(
        Graph graph,
        FilteredKnnBaseConfig config,
        FilteredKnnContext context,
        SimilarityComputer similarityComputer,
        FilteredNeighborFilterFactory neighborFilterFactory
    ) {
        var splittableRandom = getSplittableRandom(config.randomSeed());
        var sourceNodeFilter = config.sourceNodeFilter().toNodeFilter(graph);
        var targetNodeFilter = config.targetNodeFilter().toNodeFilter(graph);
        var samplerSupplier = samplerSupplier(graph, config);
        return new FilteredKnn(
            context.progressTracker(),
            context.executor(),
            config.concurrency(),
            graph,
            similarityComputer,
            neighborFilterFactory,
            splittableRandom,
            samplerSupplier,
            config.deltaThreshold(),
            config.maxIterations(),
            config.minBatchSize(),
            config.perturbationRate(),
            config.randomJoins(),
            config.sampledK(graph.nodeCount()),
            config.sampleRate(),
            config.similarityCutoff(),
            sourceNodeFilter,
            targetNodeFilter,
            config.topK()
        );
    }

    @NotNull
    private static Function<SplittableRandom, FilteredKnnSampler> samplerSupplier(Graph graph, FilteredKnnBaseConfig config) {
        switch(config.initialSampler()) {
            case UNIFORM:
                return new UniformFilteredKnnSamplerSupplier(graph);
            case RANDOMWALK:
                return new RandomWalkFilteredKnnSamplerSupplier(
                    graph.concurrentCopy(),
                    config.randomSeed(),
                    config.boundedK(graph.nodeCount())
                );
            default:
                throw new IllegalStateException("Invalid FilteredKnnSampler");
        }
    }

    @NotNull
    private static SplittableRandom getSplittableRandom(Optional<Long> randomSeed) {
        return randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
    }

    private FilteredKnn(
        ProgressTracker progressTracker,
        ExecutorService executorService,
        int concurrency,
        Graph graph,
        SimilarityComputer similarityComputer,
        FilteredNeighborFilterFactory neighborFilterFactory,
        SplittableRandom splittableRandom,
        Function<SplittableRandom, FilteredKnnSampler> samplerSupplier,
        double deltaThreshold,
        int maxIterations,
        int minBatchSize,
        double perturbationRate,
        int randomJoins,
        int sampledK,
        double sampleRate,
        double similarityCutoff,
        NodeFilter sourceNodeFilter,
        NodeFilter targetNodeFilter,
        int topK

    ) {
        super(progressTracker);
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.graph = graph;
        this.similarityComputer = similarityComputer;
        this.neighborFilterFactory = neighborFilterFactory;
        this.splittableRandom = splittableRandom;
        this.samplerSupplier = samplerSupplier;
        this.deltaThreshold = deltaThreshold;
        this.maxIterations = maxIterations;
        this.minBatchSize = minBatchSize;
        this.perturbationRate = perturbationRate;
        this.randomJoins = randomJoins;
        this.sampledK = sampledK;
        this.sampleRate = sampleRate;
        this.similarityCutoff = similarityCutoff;
        this.sourceNodeFilter = sourceNodeFilter;
        this.targetNodeFilter = targetNodeFilter;
        this.topK = topK;
    }

    public long nodeCount() {
        return graph.nodeCount();
    }

    public ExecutorService executorService() {
        return this.executorService;
    }

    @Override
    public FilteredKnnResult compute() {
        this.progressTracker.beginSubTask();
        HugeObjectArray<FilteredNeighborList> neighbors;
        try (var ignored1 = ProgressTimer.start(this::logOverallTime)) {
            try (var ignored2 = ProgressTimer.start(this::logInitTime)) {
                this.progressTracker.beginSubTask();
                neighbors = this.initializeRandomNeighbors();
                this.progressTracker.endSubTask();
            }
            if (neighbors == null) {
                return FilteredKnnResult.empty();
            }

            var maxUpdates = (long) Math.ceil(this.sampleRate * this.topK * graph.nodeCount());
            var updateThreshold = (long) Math.floor(this.deltaThreshold * maxUpdates);

            long updateCount;
            int iteration = 0;
            boolean didConverge = false;

            this.progressTracker.beginSubTask();
            for (; iteration < this.maxIterations; iteration++) {
                int currentIteration = iteration;
                try (var ignored3 = ProgressTimer.start(took -> this.logIterationTime(currentIteration + 1, took))) {
                    updateCount = iteration(neighbors);
                }
                if (updateCount <= updateThreshold) {
                    iteration++;
                    didConverge = true;
                    break;
                }
            }
            if (this.similarityCutoff > 0) {
                var neighborFilterTasks = PartitionUtils.rangePartition(
                    this.concurrency,
                    neighbors.size(),
                    partition -> (Runnable) () -> partition.consume(
                        nodeId -> neighbors.get(nodeId).filterHighSimilarityResults(this.similarityCutoff)
                    ),
                    Optional.of(this.minBatchSize)
                );
                ParallelUtil.runWithConcurrency(this.concurrency, neighborFilterTasks, this.executorService);
            }
            this.progressTracker.endSubTask();

            this.progressTracker.endSubTask();
            return ImmutableFilteredKnnResult.of(neighbors, iteration, didConverge, this.nodePairsConsidered, this.sourceNodeFilter);
        }
    }

    @Override
    public void release() {

    }

    private @Nullable HugeObjectArray<FilteredNeighborList> initializeRandomNeighbors() {
        // (int) is safe since it is at most k, which is an int
        var boundedK = (int) Math.min(graph.nodeCount() - 1, this.topK);

        if (graph.nodeCount() < 2 || this.topK == 0) {
            return null;
        }

        var neighbors = HugeObjectArray.newArray(FilteredNeighborList.class, graph.nodeCount());

        var randomNeighborGenerators = PartitionUtils.rangePartition(
            this.concurrency,
            graph.nodeCount(),
            partition -> {
                var localRandom = splittableRandom.split();
                return new FilteredGenerateRandomNeighbors(
                    samplerSupplier.apply(localRandom),
                    localRandom,
                    this.similarityComputer,
                    this.neighborFilterFactory.create(),
                    neighbors,
                    this.topK,
                    boundedK,
                    partition,
                    progressTracker
                );
            },
            Optional.of(this.minBatchSize)
        );

        ParallelUtil.runWithConcurrency(this.concurrency, randomNeighborGenerators, this.executorService);

        this.nodePairsConsidered += randomNeighborGenerators.stream().mapToLong(FilteredGenerateRandomNeighbors::neighborsFound).sum();

        return neighbors;
    }

    private long iteration(HugeObjectArray<FilteredNeighborList> neighbors) {
        // this is a sanity check
        // we check for this before any iteration and return
        // and just make sure that this invariant holds on every iteration
        var nodeCount = graph.nodeCount();
        if (nodeCount < 2 || this.topK == 0) {
            return FilteredNeighborList.NOT_INSERTED;
        }

        // TODO: init in ctor and reuse - benchmark against new allocations
        var allOldNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var allNewNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        progressTracker.beginSubTask();
        ParallelUtil.readParallel(this.concurrency, nodeCount, this.executorService, new FilteredSplitOldAndNewNeighbors(
            this.splittableRandom,
            neighbors,
            allOldNeighbors,
            allNewNeighbors,
            this.sampledK,
            progressTracker
        ));
        progressTracker.endSubTask();

        // TODO: init in ctor and reuse - benchmark against new allocations
        var reverseOldNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var reverseNewNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        progressTracker.beginSubTask();
        reverseOldAndNewNeighbors(
            nodeCount,
            allOldNeighbors,
            allNewNeighbors,
            reverseOldNeighbors,
            reverseNewNeighbors,
            this.concurrency,
            this.minBatchSize,
            progressTracker
        );
        progressTracker.endSubTask();

        var neighborsJoiners = PartitionUtils.rangePartition(
            this.concurrency,
            nodeCount,
            partition -> new FilteredJoinNeighbors(
                this.splittableRandom.split(),
                this.similarityComputer,
                this.neighborFilterFactory.create(),
                neighbors,
                allOldNeighbors,
                allNewNeighbors,
                reverseOldNeighbors,
                reverseNewNeighbors,
                nodeCount,
                this.topK,
                this.sampledK,
                this.perturbationRate,
                this.randomJoins,
                partition,
                progressTracker
            ),
            Optional.of(this.minBatchSize)
        );

        progressTracker.beginSubTask();
        ParallelUtil.runWithConcurrency(this.concurrency, neighborsJoiners, executorService);
        progressTracker.endSubTask();

        this.nodePairsConsidered += neighborsJoiners.stream().mapToLong(FilteredJoinNeighbors::nodePairsConsidered).sum();

        return neighborsJoiners.stream().mapToLong(FilteredJoinNeighbors::updateCount).sum();
    }

    private static void reverseOldAndNewNeighbors(
        long nodeCount,
        HugeObjectArray<LongArrayList> allOldNeighbors,
        HugeObjectArray<LongArrayList> allNewNeighbors,
        HugeObjectArray<LongArrayList> reverseOldNeighbors,
        HugeObjectArray<LongArrayList> reverseNewNeighbors,
        int concurrency,
        int minBatchSize,
        ProgressTracker progressTracker
    ) {
        long logBatchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency, minBatchSize);

        // TODO: cursors
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            reverseNeighbors(nodeId, allOldNeighbors, reverseOldNeighbors);
            reverseNeighbors(nodeId, allNewNeighbors, reverseNewNeighbors);

            if ((nodeId + 1) % logBatchSize == 0) {
                progressTracker.logProgress(logBatchSize);
            }
        }
    }

    static void reverseNeighbors(
        long nodeId,
        HugeObjectArray<LongArrayList> allNeighbors,
        HugeObjectArray<LongArrayList> reverseNeighbors
    ) {
        var neighbors = allNeighbors.get(nodeId);
        if (neighbors != null) {
            for (var neighbor : neighbors) {
                assert neighbor.value != nodeId;
                var oldReverse = reverseNeighbors.get(neighbor.value);
                if (oldReverse == null) {
                    oldReverse = new LongArrayList();
                    reverseNeighbors.set(neighbor.value, oldReverse);
                }
                oldReverse.add(nodeId);
            }
        }
    }

    private void logInitTime(long ms) {
        progressTracker.logMessage(formatWithLocale("Graph init took %d ms", ms));
    }

    private void logIterationTime(int iteration, long ms) {
        progressTracker.logMessage(formatWithLocale("Graph iteration %d took %d ms", iteration, ms));
    }

    private void logOverallTime(long ms) {
        progressTracker.logMessage(formatWithLocale("Graph execution took %d ms", ms));
    }
}
