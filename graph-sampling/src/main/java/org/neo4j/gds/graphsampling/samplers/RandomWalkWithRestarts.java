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
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.Optional;
import java.util.SplittableRandom;

public class RandomWalkWithRestarts implements NodesSampler {
    private static final double QUALITY_MOMENTUM = 0.9;
    private static final double QUALITY_THRESHOLD_BASE = 0.05;
    private static final int MAX_WALKS_PER_START = 100;
    private static final double TOTAL_WEIGHT_MISSING = -1.0;

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

        Optional<HugeAtomicDoubleArray> totalWeights = initializeTotalWeights(inputGraph.nodeCount());

        var tasks = ParallelUtil.tasks(config.concurrency(), () ->
            new Walker(
                seenNodes,
                expectedNodes,
                totalWeights,
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

    private Optional<HugeAtomicDoubleArray> initializeTotalWeights(long nodeCount) {
        if (config.hasRelationshipWeightProperty()) {
            var totalWeights = HugeAtomicDoubleArray.newArray(nodeCount);
            totalWeights.setAll(TOTAL_WEIGHT_MISSING);
            return Optional.of(totalWeights);
        }
        return Optional.empty();
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
        private final Optional<HugeAtomicDoubleArray> totalWeights;
        private final double qualityThreshold;
        private final WalkQualities walkQualities;
        private final SplittableRandom rng;
        private final Graph inputGraph;
        private final RandomWalkWithRestartsConfig config;

        Walker(
            HugeAtomicBitSet seenNodes,
            long expectedNodes,
            Optional<HugeAtomicDoubleArray> totalWeights,
            double qualityThreshold,
            WalkQualities walkQualities,
            SplittableRandom rng,
            Graph inputGraph,
            RandomWalkWithRestartsConfig config
        ) {
            this.seenNodes = seenNodes;
            this.expectedNodes = expectedNodes;
            this.totalWeights = totalWeights;
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
                double degree = computeDegree(currentNode);
                if (degree == 0.0 || rng.nextDouble() < config.restartProbability()) {
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
                    long nextNode;
                    if (inputGraph.hasRelationshipProperty()) {
                        nextNode = weightedWalkOffset(currentNode);
                    } else {
                        int targetOffset = rng.nextInt(inputGraph.degree(currentNode));
                        nextNode = inputGraph.nthTarget(currentNode, targetOffset);
                        assert nextNode != IdMap.NOT_FOUND : "The offset '" + targetOffset + "' is bound by the degree but no target could be found for nodeId " + currentNode;
                    }
                    currentNode = nextNode;
                    nodesConsidered++;
                }
            }
        }

        private double computeDegree(long currentNode) {
            if (!inputGraph.hasRelationshipProperty()) {
                return inputGraph.degree(currentNode);
            }

            var presentTotalWeights = totalWeights.get();
            if (presentTotalWeights.get(currentNode) == TOTAL_WEIGHT_MISSING) {
                var degree = new MutableDouble(0.0);
                inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
                    degree.add(weight);
                    return true;
                });
                presentTotalWeights.set(currentNode, degree.doubleValue());
            }
            return presentTotalWeights.get(currentNode);
        }

        private long weightedWalkOffset(long currentNode) {
            final var remainingMass = new MutableDouble(rng.nextDouble(0, computeDegree(currentNode)));
            var target = new MutableLong(-1);
            inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
                if (remainingMass.doubleValue() < weight) {
                    target.setValue(trg);
                    return false;
                }
                remainingMass.subtract(weight);
                return true;
            });
            assert target.getValue() != -1;
            return target.getValue();
        }
    }

    /**
     *  In order be able to sample start nodes uniformly at random (for performance reasons) we have a special data
     *  structure which is optimized for exactly this. In particular, we need to be able to do random access by index
     *  of the set of start nodes we are currently interested in. A simple hashmap for example does not work for this
     *  reason.
     */
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
