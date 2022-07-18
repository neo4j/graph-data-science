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

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.DoubleCollection;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongCollection;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.SplittableRandom;

public class RandomWalkWithRestarts implements NodesSampler {
    private static final double QUALITY_MOMENTUM = 0.9;
    private static final double QUALITY_THRESHOLD_BASE = 0.05;
    private static final int MAX_WALKS_PER_START = 100;
    private final RandomWalkWithRestartsConfig config;

    public RandomWalkWithRestarts(RandomWalkWithRestartsConfig config) {
        this.config = config;
    }

    @Override
    public HugeAtomicBitSet sampleNodes(Graph inputGraph) {
        var rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));
        long expectedNodes = Math.round(inputGraph.nodeCount() * config.samplingRatio());
        var initialStartQualities = initializeQualities(inputGraph, rng);
        var seenNodes = HugeAtomicBitSet.create(inputGraph.nodeCount());
        var tasks = ParallelUtil.tasks(config.concurrency(), () ->
            new Walker(
                seenNodes,
                expectedNodes,
                QUALITY_THRESHOLD_BASE / (config.concurrency() * config.concurrency()),
                new WalkQualities(initialStartQualities),
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

    @ValueClass
    interface InitialStartQualities {
        LongCollection nodeIds();

        DoubleCollection qualities();
    }

    private InitialStartQualities initializeQualities(Graph inputGraph, SplittableRandom rng) {
        var nodeIds = new LongArrayList();
        var qualities = new DoubleArrayList();
        if (!config.startNodes().isEmpty()) {
            config.startNodes().forEach(nodeId -> {
                nodeIds.add(inputGraph.toMappedNodeId(nodeId));
                qualities.add(1.0);
            });
        } else {
            nodeIds.add(rng.nextLong(inputGraph.nodeCount()));
            qualities.add(1.0);
        }

        return ImmutableInitialStartQualities.of(nodeIds, qualities);
    }

    static class Walker implements Runnable {

        private final HugeAtomicBitSet seenNodes;
        private final long expectedNodes;
        private final double qualityThreshold;
        private final WalkQualities walkQualities;
        private final SplittableRandom rng;
        private final Graph inputGraph;
        private final RandomWalkWithRestartsConfig config;

        Walker(
            HugeAtomicBitSet seenNodes,
            long expectedNodes,
            double qualityThreshold,
            WalkQualities walkQualities,
            SplittableRandom rng,
            Graph inputGraph,
            RandomWalkWithRestartsConfig config
        ) {
            this.seenNodes = seenNodes;
            this.expectedNodes = expectedNodes;
            this.qualityThreshold = qualityThreshold;
            this.walkQualities = walkQualities;
            this.rng = rng;
            this.inputGraph = inputGraph;
            this.config = config;
        }

        @Override
        public void run() {
            int currentStartNodePosition = rng.nextInt(walkQualities.size());
            long currentNode = walkQualities.nodeId(currentStartNodePosition);
            int addedNodes = 0;
            int nodesConsidered = 1;
            int walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * MAX_WALKS_PER_START);

            while (seenNodes.cardinality() < expectedNodes) {
                if (!seenNodes.getAndSet(currentNode)) {
                    addedNodes++;
                }

                // walk a step
                int degree = inputGraph.degree(currentNode);
                if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
                    double walkQuality = ((double) addedNodes) / nodesConsidered;
                    walkQualities.updateNodeQuality(currentStartNodePosition, walkQuality);
                    addedNodes = 0;
                    nodesConsidered = 1;

                    if (walksLeft-- > 0 && walkQualities.nodeQuality(currentStartNodePosition) > qualityThreshold) {
                        currentNode = walkQualities.nodeId(currentStartNodePosition);
                        continue;
                    }

                    if (walkQualities.nodeQuality(currentStartNodePosition) < 1.0 / MAX_WALKS_PER_START) {
                        walkQualities.removeNode(currentStartNodePosition);
                    }

                    if (walkQualities.expectedQuality() < qualityThreshold) {
                        long newNode;
                        do {
                            newNode = rng.nextLong(inputGraph.nodeCount());
                        } while (!walkQualities.addNode(newNode));
                    }

                    currentStartNodePosition = rng.nextInt(walkQualities.size());
                    currentNode = walkQualities.nodeId(currentStartNodePosition);
                    walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * MAX_WALKS_PER_START);
                } else {
                    int targetOffset = rng.nextInt(degree);
                    currentNode = inputGraph.getNeighbor(currentNode, targetOffset);
                    nodesConsidered++;
                }
            }
        }
    }

    static class WalkQualities {
        private final LongSet nodeIdIndex;
        private final LongArrayList nodeIds;
        private final DoubleArrayList qualities;
        private int size;
        private double sum;
        private double sumOfSquares;

        WalkQualities(InitialStartQualities initialStartQualities) {
            this.nodeIdIndex = new LongHashSet(initialStartQualities.nodeIds());
            this.nodeIds = new LongArrayList(initialStartQualities.nodeIds());
            this.qualities = new DoubleArrayList(initialStartQualities.qualities());
            this.sum = qualities.size();
            this.sumOfSquares = qualities.size();
            this.size = qualities.size();
        }

        boolean addNode(long nodeId) {
            if (nodeIdIndex.contains(nodeId)) {
                return false;
            }

            if (size >= nodeIds.size()) {
                nodeIds.add(nodeId);
                qualities.add(1.0);
            } else {
                nodeIds.set(size, nodeId);
                qualities.set(size, 1.0);
            }
            nodeIdIndex.add(nodeId);
            size++;

            sum += 1.0;
            sumOfSquares += 1.0;

            return true;
        }

        void removeNode(int position) {
            double quality = qualities.get(position);
            sum -= quality;
            sumOfSquares -= quality * quality;

            nodeIds.set(position, nodeIds.get(size - 1));
            qualities.set(position, qualities.get(size - 1));
            size--;
        }

        long nodeId(int position) {
            return nodeIds.get(position);
        }

        double nodeQuality(int position) {
            return qualities.get(position);
        }

        void updateNodeQuality(int position, double walkQuality) {
            double previousQuality = qualities.get(position);
            double updatedQuality = QUALITY_MOMENTUM * previousQuality + (1 - QUALITY_MOMENTUM) * walkQuality;

            sum += updatedQuality - previousQuality;
            sumOfSquares += updatedQuality * updatedQuality - previousQuality * previousQuality;

            qualities.set(position, updatedQuality);
        }

        double expectedQuality() {
            if (size <= 0) {
                return 0;
            }
            return sumOfSquares / sum;
        }

        int size() {
            return size;
        }
    }
}
