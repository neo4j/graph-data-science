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
package org.neo4j.gds.traversal;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.SourceNodesConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.ImmutableDegreeCentralityConfig;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class RandomWalk extends Algorithm<Stream<long[]>> {

    private final Graph graph;
    private final RandomWalkBaseConfig config;
    private final ExecutorService executorService;

    private RandomWalk(
        Graph graph,
        RandomWalkBaseConfig config,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.executorService = executorService;
    }

    public static RandomWalk create(
        Graph graph,
        RandomWalkBaseConfig config,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                config.concurrency(),
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                executorService
            );
        }

        return new RandomWalk(graph, config, progressTracker, executorService);
    }

    @Override
    public Stream<long[]> compute() {
        progressTracker.beginSubTask("RandomWalk");

        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier = graph.hasRelationshipProperty()
            ? cumulativeWeights()::get
            : graph::degree;

        var randomSeed = config.randomSeed().orElseGet(() -> new Random().nextLong());

        NextNodeSupplier nextNodeSupplier = config.sourceNodes() == null || config.sourceNodes().isEmpty()
            ? new NextNodeSupplier.GraphNodeSupplier(graph.nodeCount())
            : NextNodeSupplier.ListNodeSupplier.of(config, graph);

        var terminationFlag = new ExternalTerminationFlag(this);

        BlockingQueue<long[]> walks = new ArrayBlockingQueue<>(config.walkBufferSize());
        long[] TOMB = new long[0];

        startWalkers(terminationFlag, cumulativeWeightSupplier, randomSeed, nextNodeSupplier, walks, TOMB);
        return walksQueueConsumer(terminationFlag, TOMB, walks);
    }

    private DegreeCentrality.DegreeFunction cumulativeWeights() {
        var degreeCentralityConfig = ImmutableDegreeCentralityConfig.builder()
            .concurrency(config.concurrency())
            // DegreeCentrality internally decides its computation on the config. The actual property key is not relevant
            .relationshipWeightProperty("DUMMY")
            .build();

        return new DegreeCentrality(
            graph,
            executorService,
            degreeCentralityConfig,
            progressTracker
        ).compute();
    }

    private void startWalkers(
        TerminationFlag terminationFlag,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        long randomSeed,
        NextNodeSupplier nextNodeSupplier,
        BlockingQueue<long[]> walks,
        long[] TOMB
    ) {
        var tasks = IntStream
            .range(0, this.config.concurrency())
            .mapToObj(i ->
                RandomWalkTask.of(
                    nextNodeSupplier,
                    cumulativeWeightSupplier,
                    this.graph.concurrentCopy(),
                    this.config,
                    walks,
                    randomSeed,
                    this.progressTracker,
                    terminationFlag
                )).collect(Collectors.toList());

        CompletableFuture.runAsync(
            () -> tasksRunner(
                tasks,
                walks,
                TOMB,
                terminationFlag
            ),
            Pools.DEFAULT_SINGLE_THREAD_POOL
        ).whenComplete((__, ___) -> {
            progressTracker.endSubTask("RandomWalk");
        });
    }

    private void tasksRunner(
        Iterable<? extends Runnable> tasks,
        BlockingQueue<long[]> walks,
        long[] tombstone,
        TerminationFlag terminationFlag
    ) {
        progressTracker.beginSubTask("create walks");

        RunWithConcurrency.builder()
            .executor(this.executorService)
            .concurrency(this.config.concurrency())
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .mayInterruptIfRunning(true)
            .run();

        progressTracker.endSubTask("create walks");

        try {
            boolean finished = false;
            while (!finished && terminationFlag.running()) {
                finished = walks.offer(tombstone, 100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private Stream<long[]> walksQueueConsumer(
        ExternalTerminationFlag terminationFlag,
        long[] tombstone,
        BlockingQueue<long[]> walks
    ) {
        int timeoutInSeconds = 100;
        var queueConsumer = new QueueBasedSpliterator<>(walks, tombstone, terminationFlag, timeoutInSeconds);
        return StreamSupport
            .stream(queueConsumer, false)
            .onClose(terminationFlag::stop);
    }

    private static final class ExternalTerminationFlag implements TerminationFlag {
        private volatile boolean running = true;
        private final Algorithm<?> algo;

        ExternalTerminationFlag(Algorithm<?> algo) {
            this.algo = algo;
        }

        @Override
        public boolean running() {
            return this.running && this.algo.getTerminationFlag().running();
        }

        void stop() {
            this.running = false;
        }
    }

    private static final class RandomWalkTask implements Runnable {

        private final Graph graph;
        private final BlockingQueue<long[]> walks;
        private final NextNodeSupplier nextNodeSupplier;
        private final long[][] buffer;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;
        private final RandomWalkBaseConfig config;
        private final RandomWalkSampler sampler;

        static RandomWalkTask of(
            NextNodeSupplier nextNodeSupplier,
            RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
            Graph graph,
            RandomWalkBaseConfig config,
            BlockingQueue<long[]> walks,
            long randomSeed,
            ProgressTracker progressTracker,
            TerminationFlag terminationFlag
        ) {
            var maxProbability = Math.max(Math.max(1 / config.returnFactor(), 1.0), 1 / config.inOutFactor());
            var normalizedReturnProbability = (1 / config.returnFactor()) / maxProbability;
            var normalizedSameDistanceProbability = 1 / maxProbability;
            var normalizedInOutProbability = (1 / config.inOutFactor()) / maxProbability;

            return new RandomWalkTask(
                nextNodeSupplier,
                cumulativeWeightSupplier,
                config,
                walks,
                normalizedReturnProbability,
                normalizedSameDistanceProbability,
                normalizedInOutProbability,
                graph,
                randomSeed,
                progressTracker,
                terminationFlag
            );
        }

        private RandomWalkTask(
            NextNodeSupplier nextNodeSupplier,
            RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
            RandomWalkBaseConfig config,
            BlockingQueue<long[]> walks,
            double normalizedReturnProbability,
            double normalizedSameDistanceProbability,
            double normalizedInOutProbability,
            Graph graph,
            long randomSeed,
            ProgressTracker progressTracker,
            TerminationFlag terminationFlag
        ) {
            this.nextNodeSupplier = nextNodeSupplier;
            this.graph = graph;
            this.config = config;
            this.walks = walks;
            this.progressTracker = progressTracker;
            this.terminationFlag = terminationFlag;
            this.sampler = new RandomWalkSampler(
                cumulativeWeightSupplier,
                config.walkLength(),
                normalizedReturnProbability,
                normalizedSameDistanceProbability,
                normalizedInOutProbability,
                graph,
                randomSeed
            );

            this.buffer = new long[1000][];
        }

        @Override
        public void run() {
            long nodeId;
            int bufferLength = 0;

            while (true) {
                nodeId = nextNodeSupplier.nextNode();

                if (nodeId == NextNodeSupplier.NO_MORE_NODES) break;

                if (graph.degree(nodeId) == 0) {
                    progressTracker.logProgress();
                    continue;
                }
                var walksPerNode = config.walksPerNode();

                sampler.prepareForNewNode(nodeId);

                for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                    buffer[bufferLength++] = sampler.walk(nodeId);
                    if (bufferLength == buffer.length) {
                        var shouldStop = flushBuffer(bufferLength);
                        bufferLength = 0;
                        if (!shouldStop) {
                            break;
                        }
                    }
                }

                progressTracker.logProgress();
            }

            flushBuffer(bufferLength);
        }

        // returns false if execution should be stopped, otherwise true
        private boolean flushBuffer(int bufferLength) {
            bufferLength = Math.min(bufferLength, this.buffer.length);

            int i = 0;
            while (i < bufferLength && terminationFlag.running()) {
                try {
                    // allow termination to occur if queue is full
                    if (walks.offer(this.buffer[i], 100, TimeUnit.MILLISECONDS)) {
                        i++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            return terminationFlag.running();
        }
    }

    @FunctionalInterface
    interface NextNodeSupplier {
        long NO_MORE_NODES = -1;

        long nextNode();

        class GraphNodeSupplier implements NextNodeSupplier {
            private final long numberOfNodes;
            private final AtomicLong nextNodeId;

            GraphNodeSupplier(long numberOfNodes) {
                this.numberOfNodes = numberOfNodes;
                this.nextNodeId = new AtomicLong(0);
            }

            @Override
            public long nextNode() {
                var nextNode = nextNodeId.getAndIncrement();
                return nextNode < numberOfNodes ? nextNode : NO_MORE_NODES;
            }
        }

        final class ListNodeSupplier implements NextNodeSupplier {
            private final List<Long> nodes;
            private final AtomicInteger nextIndex;

            static ListNodeSupplier of(SourceNodesConfig config, Graph graph) {
                var mappedIds = config.sourceNodes().stream().map(graph::toMappedNodeId).collect(Collectors.toList());
                return new ListNodeSupplier(mappedIds);
            }

            private ListNodeSupplier(List<Long> nodes) {
                this.nodes = nodes;
                this.nextIndex = new AtomicInteger(0);
            }

            @Override
            public long nextNode() {
                var index = nextIndex.getAndIncrement();
                return index < nodes.size() ? nodes.get(index) : NO_MORE_NODES;
            }
        }
    }
}
