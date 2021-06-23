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
package org.neo4j.graphalgo.impl.approxmaxkcut;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * Implements a parallelized version of the GRASP (optionally with VNS) maximum k-cut approximation algorithm.
 *
 * A serial version of the algorithm is outlined in [1] as GRASP (GRASP+VNS with VNS) for k = 2 and is known as FES02G
 * (FES02GV with VNS) in [2] which benchmarks it against a lot of other algorithms, also for k = 2.
 *
 * TODO: Add the path-relinking heuristic for possibly slightly better results when running single-threaded (making the
 *  algorithm GRASP+VNS+PR in [1] and FES02GVP in [2]).
 *
 * [1]: Festa et al. Randomized Heuristics for the Max-Cut Problem, 2002.
 * [2]: Dunning et al. What Works Best When? A Systematic Evaluation of Heuristics for Max-Cut and QUBO, 2018.
 */
public class ApproxMaxKCut extends Algorithm<ApproxMaxKCut, ApproxMaxKCut.CutResult> {

    private static final double DEFAULT_WEIGHT = 0.0D;

    private Graph graph;
    private final ExecutorService executor;
    private final ApproxMaxKCutConfig config;
    private final AllocationTracker tracker;

    public ApproxMaxKCut(
        Graph graph,
        ExecutorService executor,
        ApproxMaxKCutConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.executor = executor;
        this.config = config;
        this.progressLogger = progressLogger;
        this.tracker = tracker;
    }

    @ValueClass
    public interface CutResult {
        // Value at index `i` is the idx of the community to which node with id `i` belongs.
        HugeIntArray candidateSolution();

        double cutCost();

        static CutResult of(
            HugeIntArray candidateSolution,
            double cutCost
        ) {
            return ImmutableCutResult
                .builder()
                .candidateSolution(candidateSolution)
                .cutCost(cutCost)
                .build();
        }

        default LongNodeProperties asNodeProperties() {
            return candidateSolution().asNodeProperties();
        }
    }

    /*
     * Used to improve readability of `localSearch()`.
     *
     * TODO: Swap `long` for `byte`.
     */
    private static final class NodeSwapStatus {
        // No thread has tried to swap the node yet, and no incoming neighbor has tried to mark it.
        static final long UNTOUCHED = 0;
        // The node has been swapped to another community, or a thread is currently attempting to swap it.
        static final long SWAPPING = 1;
        // The node has had one of its incoming neighbors swapped (or at least attempted) so the improvement costs may be invalid.
        static final long NEIGHBOR = 2;

        private NodeSwapStatus() {}
    }

    @FunctionalInterface
    private interface WeightTransformer {
        double accept(double weight);
    }

    @Override
    public CutResult compute() {
        // We allocate two arrays in order to be able to compare results between iterations "GRASP style".
        var candidateSolutions = new HugeIntArray[]{
            HugeIntArray.newArray(graph.nodeCount(), tracker),
            HugeIntArray.newArray(graph.nodeCount(), tracker)
        };

        // TODO: Should we create a dedicated AtomicDouble class using `AtomicReference` or `VarHandle` to avoid the
        //  extra indirection of the array?
        var costs = new AtomicDoubleArray[]{
            new AtomicDoubleArray(1),
            new AtomicDoubleArray(1)
        };

        // Keep track of which candidate solution is currently being used and which is best.
        byte currIdx = 0, bestIdx = 1;

        // Used by `localSearch()` to keep track of the costs for swapping a node to another community.
        // TODO: If we had pull-based traversal we could have a |V| sized int array here instead of the |V|*k sized
        //  double array.
        var nodeToCommunityWeights = HugeAtomicDoubleArray.newArray(graph.nodeCount() * config.k(), tracker);

        // Used by `localSearch()` to keep track of whether we can swap a node into another community or not.
        // TODO: Add HugeAtomicByteArray and use that instead to save memory.
        var swapStatus = HugeAtomicLongArray.newArray(graph.nodeCount(), tracker);

        // Used with VNS to define the neighboring candidate solution.
        HugeIntArray neighboringSolution = null;
        if (config.vnsMaxNeighborhoodOrder() > 0) {
            neighboringSolution = HugeIntArray.newArray(graph.nodeCount(), tracker);
        }

        progressLogger.logStart();

        for (int i = 0; (i < config.iterations()) && running(); i++) {
            var currCandidateSolution = candidateSolutions[currIdx];
            var currCost = costs[currIdx];

            progressLogger.startSubTask(String.format("Iteration %d", i));

            progressLogger.startSubTask("Randomly assigning nodes to communities");

            placeNodesRandomly(currCandidateSolution);

            if (!running()) break;

            if (config.vnsMaxNeighborhoodOrder() > 0) {
                progressLogger.startSubTask("Variable neighborhood search");

                var improvedCandidateSolution = variableNeighborhoodSearch(
                    currCandidateSolution,
                    currCost,
                    neighboringSolution,
                    nodeToCommunityWeights,
                    swapStatus
                );
                // If we obtained a better candidate solution from VNS, swap with that with the one we used as input.
                if (improvedCandidateSolution != currCandidateSolution) {
                    candidateSolutions[currIdx] = improvedCandidateSolution;
                    neighboringSolution = currCandidateSolution;
                }
            } else {
                progressLogger.startSubTask("Local search");

                localSearch(currCandidateSolution, nodeToCommunityWeights, swapStatus);

                computeCost(currCandidateSolution, currCost);
            }


            // Store the newly computed candidate solution if it was better than the previous. Then reuse the previous data
            // structures to make a new solution candidate if we are doing more iterations.
            if (currCost.get(0) > costs[bestIdx].get(0)) {
                var tmp = bestIdx;
                bestIdx = currIdx;
                currIdx = tmp;
            }
        }

        progressLogger.logFinish();

        return CutResult.of(candidateSolutions[bestIdx], costs[bestIdx].get(0));
    }

    private void placeNodesRandomly(HugeIntArray candidateSolution) {
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            partition -> new PlaceNodesRandomly(
                candidateSolution,
                config.k(),
                partition,
                progressLogger
            ),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
    }

    private void computeCost(HugeIntArray candidateSolution, AtomicDoubleArray cost) {
        cost.set(0, 0);
        var tasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> new ComputeCost(
                graph.concurrentCopy(),
                candidateSolution,
                cost,
                config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D,
                partition,
                progressLogger
            ),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
    }

    /*
     * This is a Local Search procedure modified to run more efficiently in parallel. Instead of restarting the while
     * loop whenever anything has changed in the candidate solution we try to continue as long as we can in order to avoid the
     * overhead of rescheduling our tasks on threads and possibly losing hot caches.
     */
    private void localSearch(
        HugeIntArray candidateSolution,
        HugeAtomicDoubleArray nodeToCommunityWeights,
        HugeAtomicLongArray swapStatus
    ) {
        var change = new AtomicBoolean(true);

        while (change.get() && running()) {
            nodeToCommunityWeights.setAll(0.0D);
            var computeTasks = PartitionUtils.degreePartition(
                graph,
                config.concurrency(),
                partition -> new ComputeImprovements(
                    graph.concurrentCopy(),
                    candidateSolution,
                    nodeToCommunityWeights,
                    config.k(),
                    config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D,
                    partition,
                    progressLogger
                ),
                Optional.of(config.minBatchSize())
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), computeTasks, executor);

            swapStatus.setAll(NodeSwapStatus.UNTOUCHED);
            change.set(false);
            var swapTasks = PartitionUtils.degreePartition(
                graph,
                config.concurrency(),
                partition -> new SwapForImprovements(
                    graph.concurrentCopy(),
                    candidateSolution,
                    nodeToCommunityWeights,
                    swapStatus,
                    change,
                    partition,
                    config.k(),
                    progressLogger
                ),
                Optional.of(config.minBatchSize())
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), swapTasks, executor);
        }
    }

    private HugeIntArray variableNeighborhoodSearch(
        HugeIntArray bestCandidateSolution,
        AtomicDoubleArray bestCost,
        HugeIntArray neighboringSolution,
        HugeAtomicDoubleArray nodeToCommunityWeights,
        HugeAtomicLongArray swapStatus
    ) {
        var random = new Random();
        var neighborCost = new AtomicDoubleArray(1);
        var order = 1;

        while ((order < config.vnsMaxNeighborhoodOrder()) && running()) {
            bestCandidateSolution.copyTo(neighboringSolution, graph.nodeCount());

            // Generate a neighboring candidate solution of the current order.
            for (int i = 0; i < order; i++) {
                long nodeToFlip = Math.abs(random.nextLong()) % graph.nodeCount();
                // For `nodeToFlip`, move to a new random community not equal to its current community in the this cut.
                int rndNewCommunity = (neighboringSolution.get(nodeToFlip) + (random.nextInt(config.k() - 1) + 1)) % config
                    .k();
                neighboringSolution.set(nodeToFlip, rndNewCommunity);
            }

            localSearch(neighboringSolution, nodeToCommunityWeights, swapStatus);

            computeCost(neighboringSolution, neighborCost);

            if (neighborCost.get(0) > bestCost.get(0)) {
                var tmpCandidateSolution = bestCandidateSolution;
                bestCandidateSolution = neighboringSolution;
                neighboringSolution = tmpCandidateSolution;

                bestCost.set(0, neighborCost.get(0));

                // Start from scratch with the new candidate.
                order = 1;
            } else {
                order += 1;
            }
        }

        return bestCandidateSolution;
    }

    private static class PlaceNodesRandomly implements Runnable {

        private final HugeIntArray candidateSolution;
        private final int k;
        private final Partition partition;
        private final ProgressLogger progressLogger;

        PlaceNodesRandomly(
            HugeIntArray candidateSolution,
            int k,
            Partition partition,
            ProgressLogger progressLogger
        ) {
            this.candidateSolution = candidateSolution;
            this.k = k;
            this.partition = partition;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            partition.consume(nodeId -> candidateSolution.set(nodeId, random.nextInt(k)));

            progressLogger.logProgress(partition.nodeCount());
        }
    }

    private static class ComputeImprovements implements Runnable {

        private final Graph graph;
        private final HugeIntArray candidateSolution;
        private final HugeAtomicDoubleArray nodeToCommunityWeights;
        private final int k;
        private final WeightTransformer getDelta;
        private final Partition partition;
        private final ProgressLogger progressLogger;

        ComputeImprovements(
            Graph graph,
            HugeIntArray candidateSolution,
            HugeAtomicDoubleArray nodeToCommunityWeights,
            int k,
            WeightTransformer getDelta,
            Partition partition,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.nodeToCommunityWeights = nodeToCommunityWeights;
            this.k = k;
            this.getDelta = getDelta;
            this.partition = partition;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            // We keep a local tab to minimize atomic accesses.
            var outgoingImprovementCosts = new double[k];

            partition.consume(nodeId -> {
                Arrays.fill(outgoingImprovementCosts, 0.0D);

                graph.forEachRelationship(
                    nodeId,
                    DEFAULT_WEIGHT,
                    (sourceNodeId, targetNodeId, weight) -> {
                        // Loops doesn't affect the cut cost.
                        if (sourceNodeId == targetNodeId) return true;

                        double delta = getDelta.accept(weight);

                        outgoingImprovementCosts[candidateSolution.get(targetNodeId)] += delta;

                        // This accounts for the `nodeToCommunityWeight` for the incoming relationship
                        // `sourceNodeId -> targetNodeId` from `targetNodeId`'s point of view.
                        // TODO: We could avoid these cache-unfriendly accesses of the outgoing relationships if we had
                        //  a way to traverse incoming relationships (pull-based traversal).
                        nodeToCommunityWeights.getAndAdd(targetNodeId * k + candidateSolution.get(sourceNodeId), delta);

                        return true;
                    }
                );

                for (int i = 0; i < k; i++) {
                    nodeToCommunityWeights.getAndAdd(nodeId * k + i, outgoingImprovementCosts[i]);
                }
            });

            progressLogger.logProgress(partition.nodeCount());
        }
    }

    private static class SwapForImprovements implements Runnable {

        private final Graph graph;
        private final HugeIntArray candidateSolution;
        private final HugeAtomicDoubleArray nodeToCommunityWeights;
        private final HugeAtomicLongArray swapStatus;
        private final AtomicBoolean change;
        private final int k;
        private final Partition partition;
        private final ProgressLogger progressLogger;

        SwapForImprovements(
            Graph graph,
            HugeIntArray candidateSolution,
            HugeAtomicDoubleArray nodeToCommunityWeights,
            HugeAtomicLongArray swapStatus,
            AtomicBoolean change,
            Partition partition,
            int k,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.nodeToCommunityWeights = nodeToCommunityWeights;
            this.swapStatus = swapStatus;
            this.change = change;
            this.partition = partition;
            this.k = k;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            var failedSwap = new MutableBoolean();

            partition.consume(nodeId -> {
                int currCommunity = candidateSolution.get(nodeId);
                int bestCommunity = bestCommunity(nodeId, currCommunity);

                if (bestCommunity == currCommunity) return;

                change.set(true);

                // If no incoming neighbors has marked us as NEIGHBOR, we can attempt to swap the current node.
                if (!swapStatus.compareAndSet(nodeId, NodeSwapStatus.UNTOUCHED, NodeSwapStatus.SWAPPING)) return;

                failedSwap.setFalse();

                // We check all outgoing neighbors to see that they are not SWAPPING. And if they aren't we mark them
                // NEIGHBOR so that they don't try to swap later.
                graph.forEachRelationship(
                    nodeId,
                    DEFAULT_WEIGHT,
                    (sourceNodeId, targetNodeId, weight) -> {
                        // Loops should not stop us.
                        if (targetNodeId == sourceNodeId) return true;

                        // We try to mark the outgoing neighbor as NEIGHBOR to make sure it doesn't swap as well.
                        if (!swapStatus.compareAndSet(
                            targetNodeId,
                            NodeSwapStatus.UNTOUCHED,
                            NodeSwapStatus.NEIGHBOR
                        )) {
                            if (swapStatus.get(targetNodeId) != NodeSwapStatus.NEIGHBOR) {
                                // This outgoing neighbor must be SWAPPING and so we can no longer rely on the computed
                                // improvement cost of our current node being correct and therefore we abort.
                                failedSwap.setTrue();
                                return false;
                            }
                        }
                        return true;
                    }
                );

                if (failedSwap.isTrue()) {
                    // Since we didn't complete the swap for this node we can unmark it as SWAPPING and thus not
                    // stopping incoming neighbor nodes of swapping because of this node's status.
                    swapStatus.set(nodeId, NodeSwapStatus.NEIGHBOR);
                    return;
                }

                candidateSolution.set(nodeId, bestCommunity);
            });

            progressLogger.logProgress(partition.nodeCount());
        }

        private int bestCommunity(long nodeId, int currCommunity) {
            final long NODE_OFFSET = nodeId * k;
            int bestCommunity = currCommunity;
            double smallestCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + currCommunity);

            for (int i = 0; i < k; i++) {
                var nodeToCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + i);
                if (nodeToCommunityWeight < smallestCommunityWeight) {
                    bestCommunity = i;
                    smallestCommunityWeight = nodeToCommunityWeight;
                }
            }

            return bestCommunity;
        }
    }

    private static class ComputeCost implements Runnable {

        private final Graph graph;
        private final HugeIntArray candidateSolution;
        private final AtomicDoubleArray cost;
        private final WeightTransformer getDelta;
        private final Partition partition;
        private final ProgressLogger progressLogger;

        ComputeCost(
            Graph graph,
            HugeIntArray candidateSolution,
            AtomicDoubleArray cost,
            WeightTransformer getDelta,
            Partition partition,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.cost = cost;
            this.getDelta = getDelta;
            this.partition = partition;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            // We keep a local tab to minimize atomic accesses.
            var localCost = new MutableDouble(0.0);

            partition.consume(nodeId -> {
                graph.forEachRelationship(
                    nodeId,
                    DEFAULT_WEIGHT,
                    (sourceNodeId, targetNodeId, weight) -> {
                        if (candidateSolution.get(sourceNodeId) != candidateSolution.get(targetNodeId)) {
                            localCost.add(getDelta.accept(weight));
                        }
                        return true;
                    }
                );
            });
            cost.add(0, localCost.doubleValue());

            progressLogger.logProgress(partition.nodeCount());
        }
    }

    @Override
    public ApproxMaxKCut me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }
}
