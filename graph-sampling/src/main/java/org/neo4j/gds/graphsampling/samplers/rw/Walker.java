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
package org.neo4j.gds.graphsampling.samplers.rw;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;

import java.util.Optional;
import java.util.SplittableRandom;

public class Walker implements Runnable {

    protected final SeenNodes seenNodes;
    private final Optional<HugeAtomicDoubleArray> totalWeights;
    protected final double qualityThreshold;
    protected final WalkQualities walkQualities;
    protected final SplittableRandom rng;
    protected final Graph inputGraph;
    protected final RandomWalkWithRestartsConfig config;
    protected final ProgressTracker progressTracker;

    protected final LongSet startNodesUsed;

    private NextNodeStrategy nextNodeStrategy;

    public Walker(
        SeenNodes seenNodes,
        Optional<HugeAtomicDoubleArray> totalWeights,
        double qualityThreshold,
        WalkQualities walkQualities,
        SplittableRandom rng,
        Graph inputGraph,
        RandomWalkWithRestartsConfig config,
        ProgressTracker progressTracker,
        NextNodeStrategy nextNodeStrategy
    ) {
        this.seenNodes = seenNodes;
        this.totalWeights = totalWeights;
        this.qualityThreshold = qualityThreshold;
        this.walkQualities = walkQualities;
        this.rng = rng;
        this.inputGraph = inputGraph;
        this.config = config;
        this.progressTracker = progressTracker;
        this.startNodesUsed = new LongHashSet();
        this.nextNodeStrategy = nextNodeStrategy;
    }

    @Override
    public void run() {
        int currentStartNodePosition = rng.nextInt(walkQualities.size());
        long currentNode = walkQualities.nodeId(currentStartNodePosition);
        startNodesUsed.add(inputGraph.toOriginalNodeId(currentNode));
        int addedNodes = 0;
        int nodesConsidered = 1;
        int walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * RandomWalkWithRestarts.MAX_WALKS_PER_START);

        while (!seenNodes.hasSeenEnough()) {
            if (seenNodes.addNode(currentNode)) {
                addedNodes++;
            }

            // walk a step
            double degree = computeDegree(currentNode);
            if (degree == 0.0 || rng.nextDouble() < config.restartProbability()) {
                progressTracker.logSteps(addedNodes);

                double walkQuality = ((double) addedNodes) / nodesConsidered;
                walkQualities.updateNodeQuality(currentStartNodePosition, walkQuality);
                addedNodes = 0;
                nodesConsidered = 1;

                if (walksLeft-- > 0 && walkQualities.nodeQuality(currentStartNodePosition) > qualityThreshold) {
                    currentNode = walkQualities.nodeId(currentStartNodePosition);
                    continue;
                }

                if (walkQualities.nodeQuality(currentStartNodePosition) < 1.0 / RandomWalkWithRestarts.MAX_WALKS_PER_START) {
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
                startNodesUsed.add(inputGraph.toOriginalNodeId(currentNode));
                walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * RandomWalkWithRestarts.MAX_WALKS_PER_START);
            } else {
                currentNode = nextNodeStrategy.getNextNode(currentNode);
                nodesConsidered++;
            }
        }
    }

    protected double computeDegree(long currentNode) {
        if (totalWeights.isEmpty()) {
            return inputGraph.degree(currentNode);
        }

        var presentTotalWeights = totalWeights.get();
        if (presentTotalWeights.get(currentNode) == RandomWalkWithRestarts.TOTAL_WEIGHT_MISSING) {
            var degree = new MutableDouble(0.0);
            inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
                degree.add(weight);
                return true;
            });
            presentTotalWeights.set(currentNode, degree.doubleValue());
        }

        return presentTotalWeights.get(currentNode);
    }


    public LongSet startNodesUsed() {
        return startNodesUsed;
    }
}
