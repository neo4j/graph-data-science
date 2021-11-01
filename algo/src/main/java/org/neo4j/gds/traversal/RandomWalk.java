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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class RandomWalk extends Algorithm<RandomWalk, Stream<long[]>> {

    private final Graph graph;
    private final RandomWalkBaseConfig config;
    private final AllocationTracker allocationTracker;

    private RandomWalk(
        Graph graph,
        RandomWalkBaseConfig config,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.allocationTracker = allocationTracker;
    }

    public static RandomWalk create(
        Graph graph,
        RandomWalkBaseConfig config,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                config.concurrency(),
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                Pools.DEFAULT
            );
        }

        return new RandomWalk(graph, config, allocationTracker, progressTracker);
    }

    @Override
    public Stream<long[]> compute() {
        progressTracker.beginSubTask("RandomWalk");
        int timeout = 100;
        BlockingQueue<long[]> walks = new ArrayBlockingQueue<>(config.walkBufferSize());
        long[] TOMB = new long[0];

        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier = graph.hasRelationshipProperty()
            ? cumulativeWeights()::get
            : graph::degree;

        var randomSeed = config.randomSeed().orElseGet(() -> new Random().nextLong());

        NextNodeSupplier nextNodeSupplier = config.sourceNodes() == null || config.sourceNodes().isEmpty()
            ? new NextNodeSupplier.GraphNodeSupplier(graph.nodeCount())
            : new NextNodeSupplier.ListNodeSupplier(config.sourceNodes());

        var tasks = IntStream
            .range(0, config.concurrency())
            .mapToObj(i ->
                RandomWalkTask.of(
                    nextNodeSupplier,
                    cumulativeWeightSupplier,
                    graph.concurrentCopy(),
                    config,
                    walks,
                    randomSeed,
                    progressTracker
                )).collect(Collectors.toList());

        progressTracker.beginSubTask("create walks");
        new Thread(() -> {
            ParallelUtil.runWithConcurrency(config.concurrency(), tasks, terminationFlag, Pools.DEFAULT);
            try {
                progressTracker.endSubTask("create walks");
                progressTracker.endSubTask("RandomWalk");
                walks.put(TOMB);
            } catch (InterruptedException e) {
            }
        }).start();
        return StreamSupport.stream(new QueueBasedSpliterator<>(walks, TOMB, terminationFlag, timeout), false);
    }

    private DegreeCentrality.DegreeFunction cumulativeWeights() {
        var degreeCentralityConfig = ImmutableDegreeCentralityConfig.builder()
            .concurrency(config.concurrency())
            // DegreeCentrality internally decides its computation on the config. The actual property key is not relevant
            .relationshipWeightProperty("DUMMY")
            .build();

        return new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            degreeCentralityConfig,
            progressTracker,
            allocationTracker
        ).compute();
    }

    @Override
    public RandomWalk me() {
        return this;
    }

    @Override
    public void release() { }

    private static final class RandomWalkTask implements Runnable {

        private final Graph graph;
        private final Random random = new Random();
        private final BlockingQueue<long[]> walks;
        private final NextNodeSupplier nextNodeSupplier;
        private final long[][] buffer;
        private final MutableInt bufferPosition;
        private final long randomSeed;
        private final ProgressTracker progressTracker;
        private final RandomWalkBaseConfig config;
        private final RandomWalkSampler sampler;

        static RandomWalkTask of(
            NextNodeSupplier nextNodeSupplier,
            RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
            Graph graph,
            RandomWalkBaseConfig config,
            BlockingQueue<long[]> walks,
            long randomSeed,
            ProgressTracker progressTracker
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
                progressTracker
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
            ProgressTracker progressTracker
        ) {
            this.nextNodeSupplier = nextNodeSupplier;
            this.graph = graph;
            this.config = config;
            this.walks = walks;
            this.randomSeed = randomSeed;
            this.progressTracker = progressTracker;
            this.sampler = new RandomWalkSampler(
                cumulativeWeightSupplier,
                config.walkLength(),
                normalizedReturnProbability,
                normalizedSameDistanceProbability,
                normalizedInOutProbability,
                graph,
                random
            );

            this.buffer = new long[1000][];
            this.bufferPosition = new MutableInt(0);
        }

        @Override
        public void run() {
            long nodeId;

            while (true) {
                nodeId = nextNodeSupplier.nextNode();

                if (nodeId == NextNodeSupplier.NO_MORE_NODES) break;

                if (graph.degree(nodeId) == 0) {
                    progressTracker.logProgress();
                    continue;
                }

                random.setSeed(randomSeed + nodeId);

                var walksPerNode = config.walksPerNode();

                for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                    buffer[bufferPosition.getAndIncrement()] = sampler.walk(nodeId);

                    if (bufferPosition.getValue() == buffer.length) {
                        flushBuffer();
                    }
                }

                progressTracker.logProgress();
            }

            flushBuffer();
        }

        private void flushBuffer() {
            for (int i = 0; i < bufferPosition.getValue(); i++) {
                try {
                    walks.put(buffer[i]);
                } catch (InterruptedException e) {

                }
            }
            bufferPosition.setValue(0);
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

        class ListNodeSupplier implements NextNodeSupplier {
            private final List<Long> nodes;
            private final AtomicInteger nextIndex;

            ListNodeSupplier(List<Long> nodes) {
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
