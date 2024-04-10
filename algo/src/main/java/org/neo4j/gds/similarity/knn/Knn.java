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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public class Knn extends Algorithm<KnnResult> {

    public static Knn create(
        Graph graph,
        KnnParameters parameters,
        SimilarityComputer similarityComputer,
        NeighborFilterFactory neighborFilterFactory,
        KnnContext context
    ) {
        var similarityFunction = new SimilarityFunction(similarityComputer);
        return new Knn(
            graph,
            context.progressTracker(),
            context.executor(),
            parameters.kHolder(),
            parameters.concurrency().value(),
            parameters.minBatchSize(),
            parameters.maxIterations(),
            parameters.similarityCutoff(),
            parameters.perturbationRate(),
            parameters.randomJoins(),
            parameters.randomSeed(),
            parameters.samplerType(),
            similarityFunction,
            neighborFilterFactory,
            NeighbourConsumers.no_op
        );
    }

    private final Graph graph;
    private final int concurrency;
    private final int maxIterations;
    private final double similarityCutoff;
    private final int minBatchSize;
    private final NeighborFilterFactory neighborFilterFactory;
    private final ExecutorService executorService;
    private final KnnSampler.Factory samplerFactory;
    private final JoinNeighbors.Factory joinNeighborsFactory;
    private final GenerateRandomNeighbors.Factory generateRandomNeighborsFactory;
    private final SplitOldAndNewNeighbors.Factory splitOldAndNewNeighborsFactory;
    private final long updateThreshold;

    public Knn(
        Graph graph,
        ProgressTracker progressTracker,
        ExecutorService executorService,
        K k,
        int concurrency,
        int minBatchSize,
        int maxIterations,
        double similarityCutoff,
        double perturbationRate,
        int randomJoins,
        Optional<Long> randomSeed,
        KnnSampler.SamplerType initialSamplerType,
        SimilarityFunction similarityFunction,
        NeighborFilterFactory neighborFilterFactory,
        NeighbourConsumers neighborConsumers
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.similarityCutoff = similarityCutoff;
        this.minBatchSize = minBatchSize;
        this.neighborFilterFactory = neighborFilterFactory;
        this.executorService = executorService;

        this.updateThreshold = k.updateThreshold;

        var splittableRandom = randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
        switch (initialSamplerType) {
            case UNIFORM:
                this.samplerFactory = new UniformKnnSampler.Factory(graph.nodeCount(), splittableRandom);
                break;
            case RANDOMWALK:
                this.samplerFactory = new RandomWalkKnnSampler.Factory(graph, randomSeed, k.value, splittableRandom);
                break;
            default:
                throw new IllegalStateException("Invalid KnnSampler");
        }
        this.generateRandomNeighborsFactory = new GenerateRandomNeighbors.Factory(
            similarityFunction,
            neighborConsumers,
            k.value,
            splittableRandom,
            progressTracker
        );
        this.splitOldAndNewNeighborsFactory = new SplitOldAndNewNeighbors.Factory(
            k.sampledValue,
            splittableRandom,
            progressTracker
        );
        this.joinNeighborsFactory = new JoinNeighbors.Factory(
            similarityFunction,
            k.sampledValue,
            perturbationRate,
            randomJoins,
            splittableRandom,
            progressTracker
        );
    }

    public ExecutorService executorService() {
        return executorService;
    }

    @Override
    public KnnResult compute() {
        if (graph.nodeCount() < 2) {
            return new EmptyResult();
        }
        progressTracker.beginSubTask();
        progressTracker.beginSubTask();
        var neighbors = initializeRandomNeighbors();
        progressTracker.endSubTask();

        long updateCount;
        int iteration = 0;
        boolean didConverge = false;

        progressTracker.beginSubTask();
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
                    nodeId -> neighbors.filterHighSimilarityResult(nodeId, similarityCutoff)
                ),
                Optional.of(minBatchSize)
            );
            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(neighborFilterTasks)
                .terminationFlag(terminationFlag)
                .executor(executorService)
                .run();
        }
        progressTracker.endSubTask();

        progressTracker.endSubTask();
        return ImmutableKnnResult.of(
            neighbors.data(),
            iteration,
            didConverge,
            neighbors.neighborsFound() + neighbors.joinCounter(),
            graph.nodeCount()
        );
    }

    private Neighbors initializeRandomNeighbors() {
        var neighbors = new Neighbors(graph.nodeCount());

        var randomNeighborGenerators = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> generateRandomNeighborsFactory.create(
                partition,
                neighbors,
                samplerFactory.create(),
                neighborFilterFactory.create()
            ),
            Optional.of(minBatchSize)
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(randomNeighborGenerators)
            .terminationFlag(terminationFlag)
            .executor(executorService)
            .run();

        return neighbors;
    }

    private long iteration(Neighbors neighbors) {
        var nodeCount = graph.nodeCount();

        // TODO: init in ctor and reuse - benchmark against new allocations
        var allOldNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var allNewNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        progressTracker.beginSubTask();
        ParallelUtil.readParallel(concurrency, nodeCount, executorService, splitOldAndNewNeighborsFactory.create(
            neighbors,
            allOldNeighbors,
            allNewNeighbors
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
            partition -> joinNeighborsFactory.create(
                partition,
                neighbors,
                allOldNeighbors,
                allNewNeighbors,
                reverseOldNeighbors,
                reverseNewNeighbors,
                neighborFilterFactory.create()
            ),
            Optional.of(minBatchSize)
        );

        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(neighborsJoiners)
            .terminationFlag(terminationFlag)
            .executor(executorService)
            .run();
        progressTracker.endSubTask();

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
