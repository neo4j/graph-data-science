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
package org.neo4j.gds.embeddings.node2vec;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.graphalgo.degree.DegreeCentrality;
import org.neo4j.graphalgo.degree.ImmutableDegreeCentralityConfig;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RandomWalk extends Algorithm<RandomWalk, Stream<long[]>> {
    // The number of tries we will make to draw a random neighbour according to p and q
    private static final int MAX_TRIES = 100;

    private final Graph graph;
    private final int steps;
    private final int concurrency;
    private final int walksPerNode;
    private final int queueSize;
    private final double returnParam;
    private final double inOutParam;
    private final AtomicLong nodeIndex;
    private final long randomSeed;
    private final AllocationTracker tracker;
    private final ProgressLogger progressLogger;

    private RandomWalk(
        Graph graph,
        int steps,
        int concurrency,
        int walksPerNode,
        int queueSize,
        double returnParam,
        double inOutParam,
        long randomSeed,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.steps = steps;
        this.concurrency = concurrency;
        this.walksPerNode = walksPerNode;
        this.queueSize = queueSize;
        this.returnParam = returnParam;
        this.inOutParam = inOutParam;
        this.randomSeed = randomSeed;
        this.tracker = tracker;
        this.progressLogger = progressLogger;
        nodeIndex = new AtomicLong(0);
    }

    public static RandomWalk create(
        Graph graph,
        int steps,
        int concurrency,
        int walksPerNode,
        int queueSize,
        double returnParam,
        double inOutParam,
        Optional<Long> randomSeed,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        var seed = randomSeed.orElseGet(() -> new Random().nextLong());

        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                concurrency,
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                Pools.DEFAULT
            );
        }

        return new RandomWalk(graph, steps, concurrency, walksPerNode, queueSize, returnParam, inOutParam, seed, tracker, progressLogger);
    }

    @Override
    public Stream<long[]> compute() {
        int timeout = 100;
        BlockingQueue<long[]> walks = new ArrayBlockingQueue<>(queueSize);
        long[] TOMB = new long[0];


        CumulativeWeightSupplier cumulativeWeightSupplier = graph.hasRelationshipProperty()
            ? cumulativeWeights()::get
            : graph::degree;

        var tasks = IntStream
            .range(0, concurrency)
            .mapToObj(i ->
                RandomWalkTask.of(
                    nodeIndex::getAndIncrement,
                    cumulativeWeightSupplier,
                    graph.concurrentCopy(),
                    walksPerNode,
                    steps,
                    returnParam,
                    inOutParam,
                    walks,
                    randomSeed
                )).collect(Collectors.toList());

        new Thread(() -> {
            ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            try {
                walks.put(TOMB);
            } catch (InterruptedException e) {
            }
        }).start();

        return StreamSupport.stream(new QueueBasedSpliterator<>(walks, TOMB, terminationFlag, timeout), false);
    }

    private DegreeCentrality.DegreeFunction cumulativeWeights() {
        var config = ImmutableDegreeCentralityConfig.builder()
            .concurrency(concurrency)
            // DegreeCentrality internally decides its computation on the config. The actual property value is not relevant
            .relationshipWeightProperty("DUMMY")
            .build();

        return new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            config,
            progressLogger,
            tracker
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
        private final int numWalks;
        private final int walkLength;
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
        private final CumulativeWeightSupplier cumulativeWeightSupplier;

        static RandomWalkTask of(
            NextNodeSupplier nextNodeSupplier,
            CumulativeWeightSupplier cumulativeWeightSupplier,
            Graph graph,
            int numWalks,
            int walkLength,
            double returnParam,
            double inOutParam,
            BlockingQueue<long[]> walks,
            long randomSeed
        ) {
            var maxProbability = Math.max(Math.max(1 / returnParam, 1.0), 1 / inOutParam);
            var normalizedReturnProbability = (1 / returnParam) / maxProbability;
            var normalizedSameDistanceProbability = 1 / maxProbability;
            var normalizedInOutProbability = (1 / inOutParam) / maxProbability;

            return new RandomWalkTask(
                nextNodeSupplier,
                cumulativeWeightSupplier,
                numWalks,
                walkLength,
                walks,
                normalizedReturnProbability,
                normalizedSameDistanceProbability,
                normalizedInOutProbability,
                graph,
                randomSeed
            );
        }

        private RandomWalkTask(
            NextNodeSupplier nextNodeSupplier,
            CumulativeWeightSupplier cumulativeWeightSupplier,
            int numWalks,
            int walkLength,
            BlockingQueue<long[]> walks,
            double normalizedReturnProbability,
            double normalizedSameDistanceProbability,
            double normalizedInOutProbability,
            Graph graph,
            long randomSeed
        ) {
            this.nextNodeSupplier = nextNodeSupplier;
            this.cumulativeWeightSupplier = cumulativeWeightSupplier;
            this.graph = graph;
            this.numWalks = numWalks;
            this.walkLength = walkLength;
            this.walks = walks;
            this.normalizedReturnProbability = normalizedReturnProbability;
            this.normalizedSameDistanceProbability = normalizedSameDistanceProbability;
            this.normalizedInOutProbability = normalizedInOutProbability;
            this.randomSeed = randomSeed;

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

                if (nodeId >= graph.nodeCount()) break;

                if (graph.degree(nodeId) == 0) {
                    continue;
                }

                random.setSeed(randomSeed + nodeId);

                for (int walkIndex = 0; walkIndex < numWalks; walkIndex++) {
                    buffer[bufferPosition.getAndIncrement()] = walk(nodeId);

                    if (bufferPosition.getValue() == buffer.length) {
                        flushBuffer();
                    }
                }
            }

            flushBuffer();
        }

        private long[] walk(long startNode) {
            var walk = new long[walkLength];
            walk[0] = startNode;
            walk[1] = randomNeighbour(startNode);

            for (int i = 2; i < walkLength; i++) {
                var nextNode = walkOneStep(walk[i - 2], walk[i - 1]);
                if (nextNode == -1) {
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
                return -1;
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
        long nextNode();
    }

    @FunctionalInterface
    interface CumulativeWeightSupplier {
        double forNode(long nodeId);
    }
}
