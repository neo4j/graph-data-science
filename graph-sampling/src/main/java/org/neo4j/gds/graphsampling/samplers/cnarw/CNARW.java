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
package org.neo4j.gds.graphsampling.samplers.cnarw;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.graphsampling.config.CNARWConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.rwr.RandomWalkWithRestarts;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.similarity.nodesim.OverlapSimilarityComputer;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CNARW extends RandomWalkWithRestarts {

    private final CNARWConfig config;

    public CNARW(CNARWConfig config) {
        super(config);
        this.config = config;
    }

    @Override
    public Task progressTask(GraphStore graphStore) {
        if (config.nodeLabelStratification()) {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf("Count node labels", graphStore.nodeCount()),
                Tasks.leaf(
                    getSubTaskMessage(),
                    10 * Math.round(graphStore.nodeCount() * config.samplingRatio())
                )
            );
        } else {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf(
                    getSubTaskMessage(),
                    10 * Math.round(graphStore.nodeCount() * config.samplingRatio())
                )
            );
        }
    }

    @Override
    public String progressTaskName() {
        return "Common neighbour aware random walks sampling";
    }

    @Override
    protected String getSubTaskMessage() {return "Do common neighbour aware random walks";}

    @Override
    public Runnable getWalker(
        SeenNodes seenNodes,
        Optional<HugeAtomicDoubleArray> totalWeights,
        double v,
        WalkQualities walkQualities,
        SplittableRandom split,
        Graph concurrentCopy,
        RandomWalkWithRestartsConfig config,
        ProgressTracker progressTracker
    ) {
        return new Walker(seenNodes, totalWeights, v, walkQualities, split, concurrentCopy, config, progressTracker);
    }

    static class Walker extends RandomWalkWithRestarts.Walker {

        private final OverlapSimilarityComputer overlapSimilarity;

        Walker(
            SeenNodes seenNodes,
            Optional<HugeAtomicDoubleArray> totalWeights,
            double qualityThreshold,
            RandomWalkWithRestarts.WalkQualities walkQualities,
            SplittableRandom rng,
            Graph inputGraph,
            RandomWalkWithRestartsConfig config,
            ProgressTracker progressTracker
        ) {
            super(seenNodes, totalWeights, qualityThreshold, walkQualities, rng, inputGraph, config, progressTracker);
            this.overlapSimilarity = new OverlapSimilarityComputer(0.0);
        }

        @Override
        public void run() {
            int currentStartNodePosition = rng.nextInt(walkQualities.size());
            long currentNode = walkQualities.nodeId(currentStartNodePosition);
            startNodesUsed.add(inputGraph.toOriginalNodeId(currentNode));
            int addedNodes = 0;
            int nodesConsidered = 1;
            int walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * MAX_WALKS_PER_START);

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
                    startNodesUsed.add(inputGraph.toOriginalNodeId(currentNode));
                    walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * MAX_WALKS_PER_START);
                } else {
                    double q, chanceOutOfNeighbours;
                    long candidateNode;

                    long[] currentNodeNeighbours = getSortedNeighbours(inputGraph, currentNode);
                    var currentWeights = getWeights(inputGraph, currentNode);
                    do {
                        if (totalWeights.isPresent()) {
                            candidateNode = weightedNextNode(currentNode);
                        } else {
                            int targetOffsetCandidate = rng.nextInt(inputGraph.degree(currentNode));
                            candidateNode = inputGraph.nthTarget(currentNode, targetOffsetCandidate);
                            assert candidateNode != IdMap.NOT_FOUND : "The offset '" + targetOffsetCandidate +
                                                                      "' is bound by the degree but no target could be found for nodeId " + candidateNode;
                        }
                        long[] candidateNodeNeighbours = getSortedNeighbours(inputGraph, candidateNode);
                        var candidateWeights = getWeights(inputGraph, candidateNode);
                        var overlap = computeOverlapSimilarity(
                            currentNodeNeighbours, currentWeights,
                            candidateNodeNeighbours, candidateWeights
                        );

                        chanceOutOfNeighbours = 1.0D - overlap;
                        q = rng.nextDouble();
                    } while (q > chanceOutOfNeighbours);

                    currentNode = candidateNode;
                    nodesConsidered++;
                }
            }
        }

        private double computeOverlapSimilarity(
            long[] currentNodeNeighbours, Optional<double[]> currentWeights,
            long[] candidateNodeNeighbours, Optional<double[]> candidateWeights
        ) {
            double similarity;
            if (currentWeights.isPresent()) {
                assert candidateWeights.isPresent();
                similarity = overlapSimilarity.computeWeightedSimilarity(
                    currentNodeNeighbours, candidateNodeNeighbours,
                    currentWeights.get(), candidateWeights.get()
                );
            } else {
                similarity = overlapSimilarity.computeSimilarity(currentNodeNeighbours, candidateNodeNeighbours);
            }
            if (!Double.isNaN(similarity))
                return similarity;
            else
                return 0.0D;
        }

        private long[] getSortedNeighbours(Graph inputGraph, long nodeId) {
            long[] neighbours = new long[inputGraph.degree(nodeId)];
            var idx = new AtomicInteger(0);
            inputGraph.forEachRelationship(nodeId, (src, dst) -> {
                neighbours[idx.getAndIncrement()] = dst;
                return true;
            });
            return neighbours;
        }

        private Optional<double[]> getWeights(Graph inputGraph, long nodeId) {
            if (totalWeights.isEmpty()) return Optional.empty();
            double[] weights = new double[inputGraph.degree(nodeId)];
            var idx = new AtomicInteger(0);
            inputGraph.forEachRelationship(nodeId, 0.0, (src, dst, w) -> {
                weights[idx.getAndIncrement()] = w;
                return true;
            });
            return Optional.of(weights);
        }
    }
}
