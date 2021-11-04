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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
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

import static org.neo4j.gds.traversal.RandomWalk.NextNodeSupplier.NO_MORE_NODES;

public final class RandomWalk extends Algorithm<RandomWalk, Stream<long[]>> {
    // The number of tries we will make to draw a random neighbour according to p and q
    private static final int MAX_TRIES = 100;

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


        CumulativeWeightSupplier cumulativeWeightSupplier = graph.hasRelationshipProperty()
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
        private final MutableDouble currentWeight;
        private final MutableLong randomNeighbour;
        private final long[][] buffer;
        private final MutableInt bufferPosition;
        private final double normalizedReturnProbability;
        private final double normalizedSameDistanceProbability;
        private final double normalizedInOutProbability;
        private final long randomSeed;
        private final ProgressTracker progressTracker;
        private final CumulativeWeightSupplier cumulativeWeightSupplier;
        private final RandomWalkBaseConfig config;

        static RandomWalkTask of(
            NextNodeSupplier nextNodeSupplier,
            CumulativeWeightSupplier cumulativeWeightSupplier,
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
            CumulativeWeightSupplier cumulativeWeightSupplier,
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
            this.cumulativeWeightSupplier = cumulativeWeightSupplier;
            this.graph = graph;
            this.config = config;
            this.walks = walks;
            this.normalizedReturnProbability = normalizedReturnProbability;
            this.normalizedSameDistanceProbability = normalizedSameDistanceProbability;
            this.normalizedInOutProbability = normalizedInOutProbability;
            this.randomSeed = randomSeed;
            this.progressTracker = progressTracker;


            this.currentWeight = new MutableDouble(0);
            this.randomNeighbour = new MutableLong(-1);
            this.buffer = new long[1000][];
            this.bufferPosition = new MutableInt(0);
        }

        @Override
        public void run() {
            long nodeId;

            while (true) {
                nodeId = nextNodeSupplier.nextNode();

                if (nodeId == NO_MORE_NODES) break;

                if (graph.degree(nodeId) == 0) {
                    progressTracker.logProgress();
                    continue;
                }

                random.setSeed(randomSeed + nodeId);

                var walksPerNode = config.walksPerNode();

                for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                    buffer[bufferPosition.getAndIncrement()] = walk(nodeId);

                    if (bufferPosition.getValue() == buffer.length) {
                        flushBuffer();
                    }
                }

                progressTracker.logProgress();
            }

            flushBuffer();
        }

        private long[] walk(long startNode) {
            var walkLength = config.walkLength();
            var walk = new long[walkLength];
            walk[0] = startNode;
            walk[1] = randomNeighbour(startNode);

            for (int i = 2; i < walkLength; i++) {
                var nextNode = walkOneStep(walk[i - 2], walk[i - 1]);
                if (nextNode == NO_MORE_NODES) {
                    var shortenedWalk = new long[i];
                    System.arraycopy(walk, 0, shortenedWalk, 0, shortenedWalk.length);
                    walk = shortenedWalk;
                    break;
                } else {
                    walk[i] = nextNode;
                }
            }
            return walk;
        }

        private long walkOneStep(long previousNode, long currentNode) {
            var currentNodeDegree = graph.degree(currentNode);

            if (currentNodeDegree == 0) {
                // We have arrived at a node with no outgoing neighbors, we can stop walking
                return NO_MORE_NODES;
            } else if (currentNodeDegree == 1) {
                // This node only has one neighbour, no need to test
                return randomNeighbour(currentNode);
            } else {
                var tries = 0;
                while (tries < MAX_TRIES) {
                    var newNode = randomNeighbour(currentNode);
                    var r = random.nextDouble();

                    if (newNode == previousNode) {
                        if (r < normalizedReturnProbability) {
                            return newNode;
                        }
                    } else if (isNeighbour(previousNode, newNode)) {
                        if (r < normalizedSameDistanceProbability) {
                            return newNode;
                        }
                    } else if (r < normalizedInOutProbability) {
                        return newNode;
                    }
                    tries++;
                }

                // We did not find a valid neighbour in `MAX_TRIES` tries, so we just pick a random one.
                return randomNeighbour(currentNode);
            }
        }

        private long randomNeighbour(long node) {
            var cumulativeWeight = cumulativeWeightSupplier.forNode(node);
            var randomWeight = cumulativeWeight * random.nextDouble();

            currentWeight.setValue(0.0);
            randomNeighbour.setValue(-1);

            graph.forEachRelationship(node, 1.0D, (source, target, weight) -> {
                if (randomWeight <= currentWeight.addAndGet(weight)) {
                    randomNeighbour.setValue(target);
                    return false;
                }
                return true;
            });

            return randomNeighbour.getValue();
        }

        private boolean isNeighbour(long source, long target) {
            return graph.exists(source, target);
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

    @FunctionalInterface
    interface CumulativeWeightSupplier {
        double forNode(long nodeId);
    }
}
