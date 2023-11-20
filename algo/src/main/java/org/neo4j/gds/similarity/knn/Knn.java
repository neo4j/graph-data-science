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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public class Knn extends Algorithm<KnnResult> {
    private final Graph graph;
    private final int concurrency;
    private final int maxIterations;
    private final double sampleRate;
    private final int topK;
    private final int boundedK;
    private final int sampledK;
    private final double deltaThreshold;
    private final double similarityCutoff;
    private final int minBatchSize;
    private final double perturbationRate;
    private final int randomJoins;
    private final NeighborFilterFactory neighborFilterFactory;
    private final ExecutorService executorService;
    private final SimilarityFunction similarityFunction;
    private final NeighbourConsumers neighborConsumers;
    private final SplittableRandom splittableRandom;
    private final KnnSampler.Provider samplerProvider;

    private long nodePairsConsidered;

    public static Knn createWithDefaults(Graph graph, KnnBaseConfig config, KnnContext context) {
        return createWithDefaultsAndInstrumentation(graph, config, context, NeighbourConsumers.no_op, defaultSimilarityFunction(graph, config.nodeProperties()));
    }

    public static SimilarityFunction defaultSimilarityFunction(Graph graph, List<KnnNodePropertySpec> nodeProperties) {
        return defaultSimilarityFunction(SimilarityComputer.ofProperties(graph, nodeProperties));
    }

    private static SimilarityFunction defaultSimilarityFunction(SimilarityComputer similarityComputer) {
        return new SimilarityFunction(similarityComputer);
    }

    @NotNull
    public static Knn createWithDefaultsAndInstrumentation(
        Graph graph,
        KnnBaseConfig config,
        KnnContext context,
        NeighbourConsumers neighborConsumers,
        SimilarityFunction similarityFunction
    ) {
        return new Knn(
            context.progressTracker(),
            graph,
            config.concurrency(),
            config.maxIterations(),
            config.sampleRate(),
            config.topK(),
            config.boundedK(graph.nodeCount()),
            config.sampledK(graph.nodeCount()),
            config.deltaThreshold(),
            config.similarityCutoff(),
            config.minBatchSize(),
            config.initialSampler(),
            config.randomSeed(),
            config.randomJoins(),
            config.perturbationRate(),
            similarityFunction,
            new KnnNeighborFilterFactory(graph.nodeCount()),
            context.executor(),
            neighborConsumers
        );
    }

    public static Knn create(
        Graph graph,
        KnnBaseConfig config,
        SimilarityComputer similarityComputer,
        NeighborFilterFactory neighborFilterFactory,
        KnnContext context
    ) {
        SimilarityFunction similarityFunction = defaultSimilarityFunction(similarityComputer);
        return new Knn(
            context.progressTracker(),
            graph,
            config.concurrency(),
            config.maxIterations(),
            config.sampleRate(),
            config.topK(),
            config.boundedK(graph.nodeCount()),
            config.sampledK(graph.nodeCount()),
            config.deltaThreshold(),
            config.similarityCutoff(),
            config.minBatchSize(),
            config.initialSampler(),
            config.randomSeed(),
            config.randomJoins(),
            config.perturbationRate(),
            similarityFunction,
            neighborFilterFactory,
            context.executor(),
            NeighbourConsumers.no_op
        );
    }

    Knn(
        ProgressTracker progressTracker,
        Graph graph,
        int concurrency,
        int maxIterations,
        double sampleRate,
        int topK,
        int boundedK,
        int sampledK,
        double deltaThreshold,
        double similarityCutoff,
        int minBatchSize,
        KnnSampler.SamplerType initialSamplerType,
        Optional<Long> randomSeed,
        int randomJoins,
        double perturbationRate,
        SimilarityFunction similarityFunction,
        NeighborFilterFactory neighborFilterFactory,
        ExecutorService executorService,
        NeighbourConsumers neighborConsumers
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.sampleRate = sampleRate;
        this.topK = topK;
        this.boundedK = boundedK;
        this.sampledK = sampledK;
        this.deltaThreshold = deltaThreshold;
        this.similarityCutoff = similarityCutoff;
        this.minBatchSize = minBatchSize;
        this.randomJoins = randomJoins;
        this.perturbationRate = perturbationRate;
        this.similarityFunction = similarityFunction;
        this.neighborFilterFactory = neighborFilterFactory;
        this.executorService = executorService;
        this.neighborConsumers = neighborConsumers;

        this.splittableRandom = randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
        switch (initialSamplerType) {
            case UNIFORM:
                this.samplerProvider = new UniformKnnSampler.Provider(graph.nodeCount(), splittableRandom);
                break;
            case RANDOMWALK:
                this.samplerProvider = new RandomWalkKnnSampler.Provider(graph, randomSeed, boundedK, splittableRandom);
                break;
            default:
                throw new IllegalStateException("Invalid KnnSampler");
        }
    }

    public ExecutorService executorService() {
        return this.executorService;
    }

    @Override
    public KnnResult compute() {
        if (graph.nodeCount() < 2 || topK == 0) {
            return new EmptyResult();
        }
        this.progressTracker.beginSubTask();
        this.progressTracker.beginSubTask();
        HugeObjectArray<NeighborList> neighbors = initializeRandomNeighbors();
        this.progressTracker.endSubTask();

        var maxUpdates = (long) Math.ceil(sampleRate * topK * graph.nodeCount());
        var updateThreshold = (long) Math.floor(deltaThreshold * maxUpdates);

        long updateCount;
        int iteration = 0;
        boolean didConverge = false;

        this.progressTracker.beginSubTask();
        for (; iteration < maxIterations; iteration++) {
            updateCount = iteration(neighbors);
            if (updateCount <= updateThreshold) {
                iteration++;
                didConverge = true;
                break;
            }
        }
        if (similarityCutoff > 0) {
            var neighborFilterTasks = PartitionUtils.rangePartition(
                concurrency,
                neighbors.size(),
                partition -> (Runnable) () -> partition.consume(
                    nodeId -> neighbors.get(nodeId).filterHighSimilarityResults(similarityCutoff)
                ),
                Optional.of(minBatchSize)
            );
            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(neighborFilterTasks)
                .terminationFlag(terminationFlag)
                .executor(this.executorService)
                .run();
        }
        this.progressTracker.endSubTask();

        this.progressTracker.endSubTask();
        return ImmutableKnnResult.of(
            neighbors,
            iteration,
            didConverge,
            this.nodePairsConsidered,
            graph.nodeCount()
        );
    }

    private HugeObjectArray<NeighborList> initializeRandomNeighbors() {
        var neighbors = HugeObjectArray.newArray(NeighborList.class, graph.nodeCount());

        var randomNeighborGenerators = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> {
                var localRandom = splittableRandom.split();
                return new GenerateRandomNeighbors(
                    samplerProvider.get(),
                    localRandom,
                    similarityFunction,
                    neighborFilterFactory.create(),
                    neighbors,
                    boundedK,
                    partition,
                    progressTracker,
                    neighborConsumers
                );
            },
            Optional.of(minBatchSize)
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(randomNeighborGenerators)
            .terminationFlag(terminationFlag)
            .executor(this.executorService)
            .run();

        this.nodePairsConsidered += randomNeighborGenerators.stream().mapToLong(GenerateRandomNeighbors::neighborsFound).sum();

        return neighbors;
    }

    private long iteration(HugeObjectArray<NeighborList> neighbors) {
        var nodeCount = graph.nodeCount();

        // TODO: init in ctor and reuse - benchmark against new allocations
        var allOldNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var allNewNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        progressTracker.beginSubTask();
        ParallelUtil.readParallel(concurrency, nodeCount, this.executorService, new SplitOldAndNewNeighbors(
            this.splittableRandom,
            neighbors,
            allOldNeighbors,
            allNewNeighbors,
            sampledK,
            progressTracker
        ));
        progressTracker.endSubTask();

        // TODO: init in ctor and reuse - benchmark against new allocations
        var reverseOldNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var reverseNewNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        progressTracker.beginSubTask();
        reverseOldAndNewNeighbors(
            allOldNeighbors,
            allNewNeighbors,
            reverseOldNeighbors,
            reverseNewNeighbors,
            concurrency,
            minBatchSize,
            progressTracker
        );
        progressTracker.endSubTask();

        var neighborsJoiners = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new JoinNeighbors(
                this.splittableRandom.split(),
                this.similarityFunction,
                this.neighborFilterFactory.create(),
                neighbors,
                allOldNeighbors,
                allNewNeighbors,
                reverseOldNeighbors,
                reverseNewNeighbors,
                sampledK,
                perturbationRate,
                randomJoins,
                partition,
                progressTracker
            ),
            Optional.of(minBatchSize)
        );

        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(neighborsJoiners)
            .terminationFlag(terminationFlag)
            .executor(this.executorService)
            .run();
        progressTracker.endSubTask();

        this.nodePairsConsidered += neighborsJoiners.stream().mapToLong(JoinNeighbors::nodePairsConsidered).sum();

        return neighborsJoiners.stream().mapToLong(JoinNeighbors::updateCount).sum();
    }

    private static void reverseOldAndNewNeighbors(
        HugeObjectArray<LongArrayList> allOldNeighbors,
        HugeObjectArray<LongArrayList> allNewNeighbors,
        HugeObjectArray<LongArrayList> reverseOldNeighbors,
        HugeObjectArray<LongArrayList> reverseNewNeighbors,
        int concurrency,
        int minBatchSize,
        ProgressTracker progressTracker
    ) {
        long nodeCount = allNewNeighbors.size();
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
        // adding nodeId to the neighbors of its neighbors (reversing the neighbors direction)
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

    private static final class EmptyResult extends KnnResult {

        @Override
        HugeObjectArray<NeighborList> neighborList() {
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
        public LongStream neighborsOf(long nodeId) {
            return LongStream.empty();
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public long nodesCompared() {
            return 0;
        }
    }
}
