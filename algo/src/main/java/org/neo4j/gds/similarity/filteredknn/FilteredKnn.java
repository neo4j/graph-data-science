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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class FilteredKnn extends Algorithm<FilteredKnn.Result> {
    private final Graph graph;
    private final FilteredNeighborFilterFactory neighborFilterFactory;
    private final ExecutorService executorService;
    private final SplittableRandom splittableRandom;
    private final SimilarityComputer similarityComputer;
    private final List<Long> sourceNodes;
    private final int maxIterations;
    private final double sampleRate;
    private final int topK;
    private final double deltaThreshold;
    private final double similarityCutoff;
    private final int concurrency;
    private final int minBatchSize;
    private final double perturbationRate;
    private final int sampledK;
    private final int randomJoins;
    private final Function<SplittableRandom, FilteredKnnSampler> samplerSupplier;

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
        var sourceNodes = config.sourceNodeFilter().stream().map(graph::toMappedNodeId).collect(Collectors.toList());
        var samplerSupplier = samplerSupplier(graph, config);
        return new FilteredKnn(
            context.progressTracker(),
            graph,
            config.maxIterations(),
            sourceNodes,
            similarityComputer,
            neighborFilterFactory,
            context.executor(),
            splittableRandom,
            config.sampleRate(),
            config.deltaThreshold(),
            config.similarityCutoff(),
            config.topK(),
            config.concurrency(),
            config.minBatchSize(),
            config.perturbationRate(),
            config.sampledK(graph.nodeCount()),
            config.randomJoins(),
            samplerSupplier
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

    FilteredKnn(
        ProgressTracker progressTracker,
        Graph graph,
        int maxIterations,
        List<Long> sourceNodes,
        SimilarityComputer similarityComputer,
        FilteredNeighborFilterFactory neighborFilterFactory,
        ExecutorService executorService,
        SplittableRandom splittableRandom,
        double sampleRate,
        double deltaThreshold,
        double similarityCutoff,
        int topK,
        int concurrency,
        int minBatchSize,
        double perturbationRate,
        int sampledK,
        int randomJoins,
        Function<SplittableRandom, FilteredKnnSampler> samplerSupplier

    ) {
        super(progressTracker);
        this.graph = graph;
        this.sampleRate = sampleRate;
        this.deltaThreshold = deltaThreshold;
        this.similarityCutoff = similarityCutoff;
        this.topK = topK;
        this.concurrency = concurrency;
        this.minBatchSize = minBatchSize;
        this.perturbationRate = perturbationRate;
        this.sampledK = sampledK;
        this.randomJoins = randomJoins;
        this.maxIterations = maxIterations;
        this.similarityComputer = similarityComputer;
        this.neighborFilterFactory = neighborFilterFactory;
        this.executorService = executorService;
        this.splittableRandom = splittableRandom;
        this.sourceNodes = sourceNodes;
        this.samplerSupplier = samplerSupplier;
    }

    public long nodeCount() {
        return graph.nodeCount();
    }

    public ExecutorService executorService() {
        return this.executorService;
    }

    @Override
    public Result compute() {
        this.progressTracker.beginSubTask();
        HugeObjectArray<FilteredNeighborList> neighbors;
        try (var ignored1 = ProgressTimer.start(this::logOverallTime)) {
            try (var ignored2 = ProgressTimer.start(this::logInitTime)) {
                this.progressTracker.beginSubTask();
                neighbors = this.initializeRandomNeighbors();
                this.progressTracker.endSubTask();
            }
            if (neighbors == null) {
                return new EmptyResult();
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
            return ImmutableResult.of(neighbors, iteration, didConverge, this.nodePairsConsidered, this.sourceNodes);
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

    @ValueClass
    public abstract static class Result {
        abstract HugeObjectArray<FilteredNeighborList> neighborList();

        public abstract int ranIterations();

        public abstract boolean didConverge();

        public abstract long nodePairsConsidered();

        public abstract List<Long> sourceNodes();

        public LongStream neighborsOf(long nodeId) {
            return neighborList().get(nodeId).elements().map(FilteredNeighborList::clearCheckedFlag);
        }

        // http://www.flatmapthatshit.com/
        public Stream<SimilarityResult> streamSimilarityResult() {
            var neighborList = neighborList();
            return Stream.iterate(neighborList.initCursor(neighborList.newCursor()), HugeCursor::next, UnaryOperator.identity())
                .flatMap(cursor -> IntStream.range(cursor.offset, cursor.limit)
                    .filter(index -> sourceNodes().contains(index + cursor.base))
                    .mapToObj(index -> cursor.array[index].similarityStream(index + cursor.base))
                    .flatMap(Function.identity())
                );
        }

        public long totalSimilarityPairs() {
            var neighborList = neighborList();
            return Stream.iterate(neighborList.initCursor(neighborList.newCursor()), HugeCursor::next, UnaryOperator.identity())
                .flatMapToLong(cursor -> IntStream.range(cursor.offset, cursor.limit)
                    .filter(index -> sourceNodes().contains(index + cursor.base))
                    .mapToLong(index -> cursor.array[index].size()))
                .sum();
        }

        public long size() {
            return neighborList().size();
        }
    }

    private static final class EmptyResult extends Result {

        @Override
        HugeObjectArray<FilteredNeighborList> neighborList() {
            return HugeObjectArray.of();
        }

        @Override
        public int ranIterations() {
            return 0;
        }

        @Override
        public boolean didConverge() {
            return false;
        }

        @Override
        public long nodePairsConsidered() {
            return 0;
        }

        @Override
        public List<Long> sourceNodes() {
            return List.of();
        }

        @Override
        public LongStream neighborsOf(long nodeId) {
            return LongStream.empty();
        }

        @Override
        public long size() {
            return 0;
        }
    }
}
