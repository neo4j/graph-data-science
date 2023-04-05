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
import org.neo4j.gds.functions.similairty.OverlapSimilarity;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.rwr.RandomWalkWithRestarts;

import java.util.Arrays;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class CommonNeighbourAwareRandomWalk extends RandomWalkWithRestarts {

    private final CommonNeighbourAwareRandomWalkConfig config;

    public CommonNeighbourAwareRandomWalk(CommonNeighbourAwareRandomWalkConfig config) {
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

        final boolean isWeighted;

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
            this.isWeighted = totalWeights.isPresent();
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
                    var uSortedNeighs = new SortedNeighsWithWeights(inputGraph, currentNode, isWeighted);
                    do {
                        candidateNode = getCandidateNode(uSortedNeighs);
                        var vSortedNeighs = new SortedNeighsWithWeights(inputGraph, candidateNode, isWeighted);
                        var overlap = computeOverlap(uSortedNeighs, vSortedNeighs);

                        chanceOutOfNeighbours = 1.0D - overlap;
                        q = rng.nextDouble();
                    } while (q > chanceOutOfNeighbours);

                    currentNode = candidateNode;
                    nodesConsidered++;
                }
            }
        }

        private long getCandidateNode(SortedNeighsWithWeights neighsWithWeights) {
            long candidateNode;
            if (isWeighted) {
                assert neighsWithWeights.getWeights().isPresent();
                candidateNode = weightedNextNode(neighsWithWeights.getNeighs(), neighsWithWeights.getWeights().get());
            } else {
                int targetOffsetCandidate = rng.nextInt(neighsWithWeights.getNeighs().length);
                candidateNode = neighsWithWeights.getNeighs()[targetOffsetCandidate];
                assert candidateNode != IdMap.NOT_FOUND : "The offset '" + targetOffsetCandidate +
                                                          "' is bound by the degree but no target could be found for nodeId " + candidateNode;
            }
            return candidateNode;
        }

        private double computeOverlap(SortedNeighsWithWeights uNeighs, SortedNeighsWithWeights vNeighs) {
            return computeOverlapSimilarity(
                uNeighs.getNeighs(),
                uNeighs.getWeights(),
                vNeighs.getNeighs(),
                vNeighs.getWeights()
            );
        }

        private double computeOverlapSimilarity(
            long[] neighsU, Optional<double[]> weightsU, long[] neighsV, Optional<double[]> weightsV
        ) {
            double similarity;
            if (isWeighted) {
                assert weightsU.isPresent();
                assert weightsV.isPresent();
                similarity = OverlapSimilarity.computeWeightedSimilarity(neighsU, neighsV, weightsU.get(), weightsV.get());
            } else {
                similarity = OverlapSimilarity.computeSimilarity(neighsU, neighsV);
            }
            if (!Double.isNaN(similarity))
                return similarity;
            else
                return 0.0D;
        }

        private long weightedNextNode(long[] neighs, double[] weights) {
            var sumWeights = Arrays.stream(weights).sum();
            var remainingMass = rng.nextDouble(0, sumWeights);

            int i = 0;
            while (remainingMass > 0) {
                remainingMass -= weights[i++];
            }

            return neighs[i - 1];
        }
    }

    static class SortedNeighsWithWeights {
        final long[] neighs;
        Optional<double[]> weights;

        SortedNeighsWithWeights(Graph graph, long nodeId, boolean isWeighted) {
            this.neighs = new long[graph.degree(nodeId)];
            this.weights = Optional.empty();
            var idx = new AtomicInteger(0);
            if (!isWeighted) {
                graph.forEachRelationship(nodeId, (src, dst) -> {
                    neighs[idx.getAndIncrement()] = dst;
                    return true;
                });
            } else {
                double[] weightsArray = new double[graph.degree(nodeId)];
                graph.forEachRelationship(nodeId, 0.0, (src, dst, w) -> {
                    var localIdx = idx.getAndIncrement();
                    neighs[localIdx] = dst;
                    weightsArray[localIdx] = w;
                    return true;
                });

                weights = Optional.of(IntStream.range(0, weightsArray.length).boxed()
                    .sorted((i, j) -> Long.compare(neighs[i], neighs[j]))
                    .map(i -> weightsArray[i]).mapToDouble(x -> x).toArray());
            }
            Arrays.sort(neighs);
        }

        long[] getNeighs() {
            return neighs;
        }

        Optional<double[]> getWeights() {
            return weights;
        }
    }
}
