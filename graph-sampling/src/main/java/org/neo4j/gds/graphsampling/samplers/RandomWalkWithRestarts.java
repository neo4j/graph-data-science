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
package org.neo4j.gds.graphsampling.samplers;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongCollection;
import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.DoubleCursor;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomWalkWithRestarts implements NodesSampler {
    private static final double ALPHA = 0.9;
    private static final double QUALITY_THRESHOLD = 0.05;
    private final RandomWalkWithRestartsConfig config;
    private final SplittableRandom rng;

    public RandomWalkWithRestarts(RandomWalkWithRestartsConfig config) {
        this.config = config;
        this.rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));
    }

    @Override
    public HugeAtomicBitSet sampleNodes(Graph inputGraph) {
        long expectedNodes = Math.round(inputGraph.nodeCount() * config.samplingRatio());
        var walkQualityPerStartNode = initializeQualityMap(inputGraph);
        var startNodes = new ConcurrentIndexedLongList(walkQualityPerStartNode.keys());
        var numberOfStartNodes = new AtomicInteger(startNodes.size());
        var seenNodes = HugeAtomicBitSet.create(inputGraph.nodeCount());
        var tasks = ParallelUtil.tasks(config.concurrency(), () ->
            new Walker(
                startNodes,
                numberOfStartNodes,
                seenNodes,
                expectedNodes,
                new LongDoubleHashMap(walkQualityPerStartNode),
                rng.split(),
                inputGraph.concurrentCopy(),
                config
            )
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .run();

        return seenNodes;
    }

    private LongDoubleMap initializeQualityMap(Graph inputGraph) {
        var qualityMap = new LongDoubleHashMap();
        if (!config.startNodes().isEmpty()) {
            config.startNodes().forEach(nodeId -> {
                qualityMap.put(inputGraph.toMappedNodeId(nodeId), 1.0);
            });
        } else {
            qualityMap.put(rng.nextLong(inputGraph.nodeCount()), 1.0);
        }
        return qualityMap;
    }

    static class Walker implements Runnable {

        private final ConcurrentIndexedLongList startNodes;
        private final AtomicInteger numberOfStartNodes;
        private final HugeAtomicBitSet seenNodes;
        private final long expectedNodes;
        private final LongDoubleHashMap walkQualityPerStartNode;
        private final SplittableRandom rng;
        private final Graph inputGraph;
        private final RandomWalkWithRestartsConfig config;

        Walker(
            ConcurrentIndexedLongList startNodes,
            AtomicInteger numberOfStartNodes,
            HugeAtomicBitSet seenNodes,
            long expectedNodes,
            LongDoubleHashMap walkQualityPerStartNode,
            SplittableRandom rng,
            Graph inputGraph,
            RandomWalkWithRestartsConfig config
        ) {
            this.startNodes = startNodes;
            this.numberOfStartNodes = numberOfStartNodes;
            this.seenNodes = seenNodes;
            this.expectedNodes = expectedNodes;
            this.walkQualityPerStartNode = walkQualityPerStartNode;
            this.rng = rng;
            this.inputGraph = inputGraph;
            this.config = config;
        }

        @Override
        public void run() {
            long currentNode = nextStartNode();
            long currentStartNode = currentNode;
            int addedNodes = 0;
            int nodesConsidered = 1;

            while (seenNodes.cardinality() < expectedNodes) {
                if (!seenNodes.getAndSet(currentNode)) {
                    addedNodes++;
                }

                // walk a step
                int degree = inputGraph.degree(currentNode);
                if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
                    // walk ended, so check if we need to add a new startNode
                    double walkQuality = ((double) addedNodes) / nodesConsidered;
                    double oldQuality = walkQualityPerStartNode.get(currentStartNode);
                    walkQualityPerStartNode.put(
                        currentStartNode,
                        ALPHA * oldQuality + (1 - ALPHA) * walkQuality
                    );

                    updateLocalStartNodes();

                    double expectedQuality = expectedQuality(walkQualityPerStartNode);
                    if (expectedQuality < QUALITY_THRESHOLD) {
                        // If another thread added a new start node, then quality has probably increased enough for now.
                        int localNumberOfStartNodes = walkQualityPerStartNode.size();
                        if (numberOfStartNodes.compareAndSet(localNumberOfStartNodes, localNumberOfStartNodes + 1)) {
                            long newNode;
                            do {
                                newNode = rng.nextLong(inputGraph.nodeCount());
                            } while (!startNodes.add(newNode));
                        }
                    }

                    currentStartNode = nextStartNode();
                    currentNode = currentStartNode;
                    addedNodes = 0;
                    nodesConsidered = 1;
                } else {
                    int targetOffset = rng.nextInt(degree);
                    currentNode = inputGraph.getNeighbor(currentNode, targetOffset);
                    nodesConsidered++;
                }
            }
        }

        // Make sure using all possible start nodes added by all threads.
        private void updateLocalStartNodes() {
            if (walkQualityPerStartNode.size() >= startNodes.size()) {
                return;
            }
            for (int i = walkQualityPerStartNode.size(); i < startNodes.size(); i++) {
                walkQualityPerStartNode.put(startNodes.get(i), 1.0);
            }
        }

        private long nextStartNode() {
            double sum = valueSum(walkQualityPerStartNode);
            double sample = rng.nextDouble(sum);
            double traversedSum = 0.0;
            for (LongDoubleCursor cursor : walkQualityPerStartNode) {
                traversedSum += cursor.value;
                if (traversedSum >= sample) {
                    return cursor.key;
                }
            }
            throw new IllegalStateException("Something went wrong :(");
        }

        private double expectedQuality(LongDoubleMap walkQualityPerStartNode) {
            double sumOfQualities = valueSum(walkQualityPerStartNode);
            double sumOfSquaredQualities = 0.0;
            for (DoubleCursor quality : walkQualityPerStartNode.values()) {
                sumOfSquaredQualities += quality.value * quality.value;
            }
            return sumOfSquaredQualities / sumOfQualities;
        }

        private double valueSum(LongDoubleMap qualityMap) {
            double sum = 0.0;
            for (DoubleCursor quality : qualityMap.values()) {
                sum += quality.value;
            }
            return sum;
        }
    }

    static class ConcurrentIndexedLongList {
        private final LongArrayList list;
        private final LongHashSet set;


        ConcurrentIndexedLongList(LongCollection longCollection) {
            this.set = new LongHashSet(longCollection);
            this.list = new LongArrayList(longCollection);
        }

        synchronized boolean add(long x) {
            if (!set.add(x)) {
                return false;
            }
            list.add(x);
            return true;
        }

        synchronized int size() {
            return list.size();
        }

        synchronized long get(int i) {
            return list.get(i);
        }
    }
}
