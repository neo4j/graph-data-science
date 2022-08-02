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
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
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
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Knn extends Algorithm<Knn.Result> {
    private final Graph graph;
    private final KnnBaseConfig config;
    private final NeighborFilterFactory neighborFilterFactory;
    private final ExecutorService executorService;
    private final SplittableRandom splittableRandom;
    private final SimilarityFunction similarityFunction;
    private final NeighbourConsumers neighborConsumers;

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
            config,
            similarityFunction,
            new KnnNeighborFilterFactory(graph.nodeCount()),
            context.executor(),
            getSplittableRandom(config.randomSeed()),
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
        SplittableRandom splittableRandom = getSplittableRandom(config.randomSeed());
        SimilarityFunction similarityFunction = defaultSimilarityFunction(similarityComputer);
        return new Knn(
            context.progressTracker(),
            graph,
            config,
            similarityFunction,
            neighborFilterFactory,
            context.executor(),
            splittableRandom,
            NeighbourConsumers.no_op
        );
    }

    @NotNull
    private static SplittableRandom getSplittableRandom(Optional<Long> randomSeed) {
        return randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
    }

    Knn(
        ProgressTracker progressTracker,
        Graph graph,
        KnnBaseConfig config,
        SimilarityFunction similarityFunction,
        NeighborFilterFactory neighborFilterFactory,
        ExecutorService executorService,
        SplittableRandom splittableRandom,
        NeighbourConsumers neighborConsumers
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.similarityFunction = similarityFunction;
        this.neighborFilterFactory = neighborFilterFactory;
        this.executorService = executorService;
        this.splittableRandom = splittableRandom;
        this.neighborConsumers = neighborConsumers;
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
        HugeObjectArray<NeighborList> neighbors;
        try (var ignored1 = ProgressTimer.start(this::logOverallTime)) {
            try (var ignored2 = ProgressTimer.start(this::logInitTime)) {
                this.progressTracker.beginSubTask();
                neighbors = this.initializeRandomNeighbors();
                this.progressTracker.endSubTask();
            }
            if (neighbors == null) {
                return new EmptyResult();
            }

            var maxIterations = this.config.maxIterations();
            var maxUpdates = (long) Math.ceil(this.config.sampleRate() * this.config.topK() * graph.nodeCount());
            var updateThreshold = (long) Math.floor(this.config.deltaThreshold() * maxUpdates);

            long updateCount;
            int iteration = 0;
            boolean didConverge = false;

            this.progressTracker.beginSubTask();
            for (; iteration < maxIterations; iteration++) {
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
            if (config.similarityCutoff() > 0) {
                var similarityCutoff = config.similarityCutoff();
                var neighborFilterTasks = PartitionUtils.rangePartition(
                    config.concurrency(),
                    neighbors.size(),
                    partition -> (Runnable) () -> partition.consume(
                        nodeId -> neighbors.get(nodeId).filterHighSimilarityResults(similarityCutoff)
                    ),
                    Optional.of(config.minBatchSize())
                );
                RunWithConcurrency.builder()
                    .concurrency(config.concurrency())
                    .tasks(neighborFilterTasks)
                    .executor(this.executorService)
                    .run();
            }
            this.progressTracker.endSubTask();

            this.progressTracker.endSubTask();
            return ImmutableResult.of(neighbors, iteration, didConverge, this.nodePairsConsidered);
        }
    }

    @Override
    public void release() {

    }
    private @Nullable HugeObjectArray<NeighborList> initializeRandomNeighbors() {
        var k = this.config.topK();
        // (int) is safe since it is at most k, which is an int
        var boundedK = (int) Math.min(graph.nodeCount() - 1, k);

        assert boundedK <= k && boundedK <= graph.nodeCount() - 1;

        if (graph.nodeCount() < 2 || k == 0) {
            return null;
        }

        var neighbors = HugeObjectArray.newArray(NeighborList.class, graph.nodeCount());

        var randomNeighborGenerators = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            partition -> {
                var localRandom = splittableRandom.split();
                return new GenerateRandomNeighbors(
                    initializeSampler(localRandom),
                    localRandom,
                    this.similarityFunction,
                    this.neighborFilterFactory.create(),
                    neighbors,
                    k,
                    boundedK,
                    partition,
                    progressTracker,
                    neighborConsumers
                );
            },
            Optional.of(config.minBatchSize())
        );

        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(randomNeighborGenerators)
            .executor(this.executorService)
            .run();

        this.nodePairsConsidered += randomNeighborGenerators.stream().mapToLong(GenerateRandomNeighbors::neighborsFound).sum();

        return neighbors;
    }

    private KnnSampler initializeSampler(SplittableRandom random) {
        switch(config.initialSampler()) {
            case UNIFORM: {
                return new UniformKnnSampler(random, graph.nodeCount());
            }
            case RANDOMWALK: {
                return new RandomWalkKnnSampler(
                    graph.concurrentCopy(),
                    random,
                    config.randomSeed(),
                    config.boundedK(graph.nodeCount())
                );
            }
            default:
                throw new IllegalStateException("Invalid KnnSampler");
        }
    }

    private long iteration(HugeObjectArray<NeighborList> neighbors) {
        // this is a sanity check
        // we check for this before any iteration and return
        // and just make sure that this invariant holds on every iteration
        var nodeCount = graph.nodeCount();
        if (nodeCount < 2 || this.config.topK() == 0) {
            return NeighborList.NOT_INSERTED;
        }

        var concurrency = this.config.concurrency();

        var sampledK = this.config.sampledK(nodeCount);

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
            config,
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
                this.config.perturbationRate(),
                this.config.randomJoins(),
                partition,
                progressTracker
            ),
            Optional.of(config.minBatchSize())
        );

        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(neighborsJoiners)
            .executor(this.executorService)
            .run();
        progressTracker.endSubTask();

        this.nodePairsConsidered += neighborsJoiners.stream().mapToLong(JoinNeighbors::nodePairsConsidered).sum();

        return neighborsJoiners.stream().mapToLong(joiner -> joiner.updateCount).sum();
    }

    private static void reverseOldAndNewNeighbors(
        HugeObjectArray<LongArrayList> allOldNeighbors,
        HugeObjectArray<LongArrayList> allNewNeighbors,
        HugeObjectArray<LongArrayList> reverseOldNeighbors,
        HugeObjectArray<LongArrayList> reverseNewNeighbors,
        KnnBaseConfig config,
        ProgressTracker progressTracker
    ) {
        long nodeCount = allNewNeighbors.size();
        long logBatchSize = ParallelUtil.adjustedBatchSize(nodeCount, config.concurrency(), config.minBatchSize());

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

    private static final class JoinNeighbors implements Runnable {
        private final SplittableRandom random;
        private final SimilarityFunction similarityFunction;
        private final NeighborFilter neighborFilter;
        private final HugeObjectArray<NeighborList> neighbors;
        private final HugeObjectArray<LongArrayList> allOldNeighbors;
        private final HugeObjectArray<LongArrayList> allNewNeighbors;
        private final HugeObjectArray<LongArrayList> allReverseOldNeighbors;
        private final HugeObjectArray<LongArrayList> allReverseNewNeighbors;
        private final int sampledK;
        private final int randomJoins;
        private final ProgressTracker progressTracker;
        private long updateCount;
        private final Partition partition;
        private long nodePairsConsidered;
        private final double perturbationRate;

        private JoinNeighbors(
            SplittableRandom random,
            SimilarityFunction similarityFunction,
            NeighborFilter neighborFilter,
            HugeObjectArray<NeighborList> neighbors,
            HugeObjectArray<LongArrayList> allOldNeighbors,
            HugeObjectArray<LongArrayList> allNewNeighbors,
            HugeObjectArray<LongArrayList> allReverseOldNeighbors,
            HugeObjectArray<LongArrayList> allReverseNewNeighbors,
            int sampledK,
            double perturbationRate,
            int randomJoins,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.random = random;
            this.similarityFunction = similarityFunction;
            this.neighborFilter = neighborFilter;
            this.neighbors = neighbors;
            this.allOldNeighbors = allOldNeighbors;
            this.allNewNeighbors = allNewNeighbors;
            this.allReverseOldNeighbors = allReverseOldNeighbors;
            this.allReverseNewNeighbors = allReverseNewNeighbors;
            this.sampledK = sampledK;
            this.randomJoins = randomJoins;
            this.partition = partition;
            this.progressTracker = progressTracker;
            this.perturbationRate = perturbationRate;
            this.updateCount = 0;
            this.nodePairsConsidered = 0;
        }

        @Override
        public void run() {
            var rng = random;
            var similarityFunction = this.similarityFunction;
            var nodeCount = this.allNewNeighbors.size();
            var sampledK = this.sampledK;
            var allNeighbors = this.neighbors;
            var allNewNeighbors = this.allNewNeighbors;
            var allOldNeighbors = this.allOldNeighbors;
            var allReverseNewNeighbors = this.allReverseNewNeighbors;
            var allReverseOldNeighbors = this.allReverseOldNeighbors;

            var startNode = partition.startNode();
            long endNode = startNode + partition.nodeCount();

            for (long nodeId = startNode; nodeId < endNode; nodeId++) {
                // old[v] ∪ Sample(old′[v], ρK)
                var oldNeighbors = allOldNeighbors.get(nodeId);
                if (oldNeighbors != null) {
                    joinOldNeighbors(rng, sampledK, allReverseOldNeighbors.get(nodeId), oldNeighbors);
                }


                // new[v] ∪ Sample(new′[v], ρK)
                var newNeighbors = allNewNeighbors.get(nodeId);
                if (newNeighbors != null) {
                    this.updateCount += joinNewNeighbors(
                        rng,
                        similarityFunction,
                        sampledK,
                        allNeighbors,
                        nodeId,
                        allReverseNewNeighbors.get(nodeId),
                        oldNeighbors,
                        newNeighbors
                    );
                }

                // this isn't in the paper
                randomJoins(rng, similarityFunction, nodeCount, allNeighbors, nodeId, this.randomJoins);
            }
            progressTracker.logProgress(partition.nodeCount());
        }

        private void joinOldNeighbors(
            SplittableRandom rng,
            int sampledK,
            @Nullable LongArrayList reverseOldNeighbors,
            LongArrayList oldNeighbors
        ) {
            if (reverseOldNeighbors != null) {
                var numberOfReverseOldNeighbors = reverseOldNeighbors.size();
                for (var elem : reverseOldNeighbors) {
                    if (rng.nextInt(numberOfReverseOldNeighbors) < sampledK) {
                        // TODO: this could add nodes twice, maybe? should this be a set?
                        oldNeighbors.add(elem.value);
                    }
                }
            }
        }

        private long joinNewNeighbors(
            SplittableRandom rng,
            SimilarityFunction similarityFunction,
            int sampledK,
            HugeObjectArray<NeighborList> allNeighbors,
            // what are reversed new neighbors?
            long nodeId,
            LongArrayList reverseNewNeighbors,
            LongArrayList oldNeighbors,
            LongArrayList newNeighbors
        ) {
            long updateCount = 0;

            joinOldNeighbors(rng, sampledK, reverseNewNeighbors, newNeighbors);

            var newNeighborElements = newNeighbors.buffer;
            var newNeighborsCount = newNeighbors.elementsCount;

            for (int i = 0; i < newNeighborsCount; i++) {
                var elem1 = newNeighborElements[i];
                assert elem1 != nodeId;

                // join(u1, v), this isn't in the paper
                updateCount += join(rng, similarityFunction, allNeighbors, elem1, nodeId);

                // join(new_nbd, new_ndb)
                for (int j = i + 1; j < newNeighborsCount; j++) {
                    var elem2 = newNeighborElements[i];
                    if (elem1 == elem2) {
                        continue;
                    }

                    // TODO the similarity is defined symmetric? (we could only try the second join if the first succeeded)
                    updateCount += join(rng, similarityFunction, allNeighbors, elem1, elem2);
                    updateCount += join(rng, similarityFunction, allNeighbors, elem2, elem1);
                }

                // join(new_nbd, old_ndb)
                if (oldNeighbors != null) {
                    for (var oldElemCursor : oldNeighbors) {
                        var elem2 = oldElemCursor.value;

                        if (elem1 == elem2) {
                            continue;
                        }

                        updateCount += join(rng, similarityFunction, allNeighbors, elem1, elem2);
                        updateCount += join(rng, similarityFunction, allNeighbors, elem2, elem1);
                    }
                }
            }
            return updateCount;
        }

        private void randomJoins(
            SplittableRandom rng,
            SimilarityFunction computer,
            long nodeCount,
            HugeObjectArray<NeighborList> allNeighbors,
            long nodeId,
            int randomJoins
        ) {
            for (int i = 0; i < randomJoins; i++) {
                var randomNodeId = rng.nextLong(nodeCount - 1);
                if (randomNodeId >= nodeId) {
                    ++randomNodeId;
                }
                // random joins are not counted towards the actual update counter
                join(rng, computer, allNeighbors, nodeId, randomNodeId);
            }
        }

        private long join(
            SplittableRandom splittableRandom,
            SimilarityFunction similarityFunction,
            HugeObjectArray<NeighborList> allNeighbors,
            long baseNode,
            long candidate
        ) {
            assert baseNode != candidate;

            if (neighborFilter.excludeNodePair(baseNode, candidate)) {
                return 0;
            }

            var similarity = similarityFunction.computeSimilarity(baseNode, candidate);
            nodePairsConsidered++;
            var neighbors = allNeighbors.get(baseNode);

            synchronized (neighbors) {
                return neighbors.add(candidate, similarity, splittableRandom, perturbationRate);
            }
        }

        long nodePairsConsidered() {
            return nodePairsConsidered;
        }
    }

    private void logInitTime(long ms) {
        progressTracker.logInfo(formatWithLocale("Graph init took %d ms", ms));
    }

    private void logIterationTime(int iteration, long ms) {
        progressTracker.logInfo(formatWithLocale("Graph iteration %d took %d ms", iteration, ms));
    }

    private void logOverallTime(long ms) {
        progressTracker.logInfo(formatWithLocale("Graph execution took %d ms", ms));
    }

    @ValueClass
    public abstract static class Result {
        abstract HugeObjectArray<NeighborList> neighborList();

        public abstract int ranIterations();

        public abstract boolean didConverge();

        public abstract long nodePairsConsidered();

        public LongStream neighborsOf(long nodeId) {
            return neighborList().get(nodeId).elements().map(NeighborList::clearCheckedFlag);
        }

        // http://www.flatmapthatshit.com/
        public Stream<SimilarityResult> streamSimilarityResult() {
            var neighborList = neighborList();
            return Stream.iterate(neighborList.initCursor(neighborList.newCursor()), HugeCursor::next, UnaryOperator.identity())
                .flatMap(cursor -> IntStream.range(cursor.offset, cursor.limit)
                    .mapToObj(index -> cursor.array[index].similarityStream(index + cursor.base))
                    .flatMap(Function.identity())
                );
        }

        public long totalSimilarityPairs() {
            var neighborList = neighborList();
            return Stream.iterate(neighborList.initCursor(neighborList.newCursor()), HugeCursor::next, UnaryOperator.identity())
                .flatMapToLong(cursor -> IntStream.range(cursor.offset, cursor.limit)
                    .mapToLong(index -> cursor.array[index].size()))
                .sum();
        }

        public long size() {
            return neighborList().size();
        }
    }

    private static final class EmptyResult extends Result {

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
    }
}
