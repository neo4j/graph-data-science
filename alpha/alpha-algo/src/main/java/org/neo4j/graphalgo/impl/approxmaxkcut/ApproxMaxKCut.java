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
 * Implements a parallelized version of a GRASP (optionally with VNS) maximum k-cut approximation algorithm.
 *
 * A serial version of the algorithm with a slightly different construction phase is outlined in [1] as GRASP(+VNS) for
 * k = 2, and is known as FES02G(V) in [2] which benchmarks it against a lot of other algorithms, also for k = 2.
 *
 * TODO: Add the path-relinking heuristic for possibly slightly better results when running single-threaded (basically
 *  making the algorithm GRASP+VNS+PR in [1] and FES02GVP in [2]).
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
    private final WeightTransformer weightTransformer;
    private final HugeIntArray[] candidateSolutions;
    private final AtomicDoubleArray[] costs;
    private final HugeAtomicDoubleArray nodeToCommunityWeights;
    private final HugeAtomicLongArray swapStatus;
    private HugeIntArray neighboringSolution;

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
        this.weightTransformer = config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D;

        // We allocate two arrays in order to be able to compare results between iterations "GRASP style".
        candidateSolutions = new HugeIntArray[]{
            HugeIntArray.newArray(graph.nodeCount(), tracker),
            HugeIntArray.newArray(graph.nodeCount(), tracker)
        };

        // TODO: Should we create a dedicated AtomicDouble class using `AtomicReference` or `VarHandle` to avoid the
        //  extra indirection of the array?
        costs = new AtomicDoubleArray[]{
            new AtomicDoubleArray(1),
            new AtomicDoubleArray(1)
        };

        // Used by `localSearch()` to keep track of the costs for swapping a node to another community.
        // TODO: If we had pull-based traversal we could have a |V| sized int array here instead of the |V|*k sized
        //  double array.
        nodeToCommunityWeights = HugeAtomicDoubleArray.newArray(graph.nodeCount() * config.k(), tracker);

        // Used by `localSearch()` to keep track of whether we can swap a node into another community or not.
        // TODO: Add HugeAtomicByteArray and use that instead to save memory.
        swapStatus = HugeAtomicLongArray.newArray(graph.nodeCount(), tracker);
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
        // Keep track of which candidate solution is currently being used and which is best.
        byte currIdx = 0, bestIdx = 1;

        // Used with VNS to define the neighboring candidate solution.
        if (config.vnsMaxNeighborhoodOrder() > 0) {
            neighboringSolution = HugeIntArray.newArray(graph.nodeCount(), tracker);
        }

        progressLogger.logStart();

        for (int i = 1; (i <= config.iterations()) && running(); i++) {
            var currCandidateSolution = candidateSolutions[currIdx];
            var currCost = costs[currIdx];

            var assignmentTaskLog = "Iteration " + i + ": Randomly assign nodes to communities";
            progressLogger.startSubTask(assignmentTaskLog);

            placeNodesRandomly(currCandidateSolution);

            progressLogger.finishSubTask(assignmentTaskLog);

            if (!running()) break;

            if (config.vnsMaxNeighborhoodOrder() > 0) {
                var vnsTaskLog = "Iteration " + i + ": Variable neighborhood search";
                progressLogger.startSubTask(vnsTaskLog);

                variableNeighborhoodSearch(currIdx);

                progressLogger.finishSubTask(vnsTaskLog);
            } else {
                var localSearchTaskLog = "Iteration " + i + ": Local search";
                progressLogger.startSubTask(localSearchTaskLog);

                localSearch(currCandidateSolution, currCost);

                progressLogger.finishSubTask(localSearchTaskLog);
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
            partition -> new PlaceNodesRandomly(candidateSolution, partition),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
    }

    /*
     * This is a Local Search procedure modified to run more efficiently in parallel. Instead of restarting the while
     * loop whenever anything has changed in the candidate solution we try to continue as long as we can in order to
     * avoid the overhead of rescheduling our tasks on threads and possibly losing hot caches.
     */
    private void localSearch(HugeIntArray candidateSolution, AtomicDoubleArray cost) {
        var change = new AtomicBoolean(true);

        while (change.get() && running()) {
            nodeToCommunityWeights.setAll(0.0D);
            var nodeToCommunityWeightTasks = PartitionUtils.degreePartition(
                graph,
                config.concurrency(),
                partition -> new ComputeNodeToCommunityWeights(
                    graph.concurrentCopy(),
                    candidateSolution,
                    partition
                ),
                Optional.of(config.minBatchSize())
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), nodeToCommunityWeightTasks, executor);

            swapStatus.setAll(NodeSwapStatus.UNTOUCHED);
            change.set(false);
            var swapTasks = PartitionUtils.degreePartition(
                graph,
                config.concurrency(),
                partition -> new SwapForLocalImprovements(
                    graph.concurrentCopy(),
                    candidateSolution,
                    change,
                    partition
                ),
                Optional.of(config.minBatchSize())
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), swapTasks, executor);
        }

        cost.set(0, 0);
        var costTasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> new ComputeCost(
                graph.concurrentCopy(),
                candidateSolution,
                cost,
                partition
            ),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), costTasks, executor);
    }

    private void variableNeighborhoodSearch(int candidateIdx) {
        var random = new Random();
        var bestCandidateSolution = candidateSolutions[candidateIdx];
        var bestCost = costs[candidateIdx];
        var neighborCost = new AtomicDoubleArray(1);
        var order = 1;

        while ((order < config.vnsMaxNeighborhoodOrder()) && running()) {
            bestCandidateSolution.copyTo(neighboringSolution, graph.nodeCount());

            // Generate a neighboring candidate solution of the current order.
            for (int i = 0; i < order; i++) {
                long nodeToFlip = Math.abs(random.nextLong()) % graph.nodeCount();
                // For `nodeToFlip`, move to a new random community not equal to its current community in
                // `neighboringSolution`.
                int rndNewCommunity = (neighboringSolution.get(nodeToFlip) + (random.nextInt(config.k() - 1) + 1)) % config
                    .k();
                neighboringSolution.set(nodeToFlip, rndNewCommunity);
            }

            localSearch(neighboringSolution, neighborCost);

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

        // If we obtained a better candidate solution from VNS, swap with that with the one we started with.
        if (bestCandidateSolution != candidateSolutions[candidateIdx]) {
            neighboringSolution = candidateSolutions[candidateIdx];
            candidateSolutions[candidateIdx] = bestCandidateSolution;
        }
    }

    private final class PlaceNodesRandomly implements Runnable {

        private final HugeIntArray candidateSolution;
        private final Partition partition;

        PlaceNodesRandomly(
            HugeIntArray candidateSolution,
            Partition partition
        ) {
            this.candidateSolution = candidateSolution;
            this.partition = partition;
        }

        @Override
        public void run() {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            partition.consume(nodeId -> candidateSolution.set(nodeId, random.nextInt(config.k())));
        }
    }

    private final class ComputeNodeToCommunityWeights implements Runnable {

        private final Graph graph;
        private final HugeIntArray candidateSolution;
        private final Partition partition;

        ComputeNodeToCommunityWeights(
            Graph graph,
            HugeIntArray candidateSolution,
            Partition partition
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.partition = partition;
        }

        @Override
        public void run() {
            // We keep a local tab to minimize atomic accesses.
            var outgoingImprovementCosts = new double[config.k()];

            partition.consume(nodeId -> {
                Arrays.fill(outgoingImprovementCosts, 0.0D);

                graph.forEachRelationship(
                    nodeId,
                    DEFAULT_WEIGHT,
                    (sourceNodeId, targetNodeId, weight) -> {
                        // Loops doesn't affect the cut cost.
                        if (sourceNodeId == targetNodeId) return true;

                        double transformedWeight = weightTransformer.accept(weight);

                        outgoingImprovementCosts[candidateSolution.get(targetNodeId)] += transformedWeight;

                        // This accounts for the `nodeToCommunityWeight` for the incoming relationship
                        // `sourceNodeId -> targetNodeId` from `targetNodeId`'s point of view.
                        // TODO: We could avoid these cache-unfriendly accesses of the outgoing relationships if we had
                        //  a way to traverse incoming relationships (pull-based traversal).
                        nodeToCommunityWeights.getAndAdd(
                            targetNodeId * config.k() + candidateSolution.get(sourceNodeId),
                            transformedWeight
                        );

                        return true;
                    }
                );

                for (int i = 0; i < config.k(); i++) {
                    nodeToCommunityWeights.getAndAdd(nodeId * config.k() + i, outgoingImprovementCosts[i]);
                }
            });
        }
    }

    private final class SwapForLocalImprovements implements Runnable {

        private final Graph graph;
        private final HugeIntArray candidateSolution;
        private final AtomicBoolean change;
        private final Partition partition;

        SwapForLocalImprovements(
            Graph graph,
            HugeIntArray candidateSolution,
            AtomicBoolean change,
            Partition partition
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.change = change;
            this.partition = partition;
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
        }

        private int bestCommunity(long nodeId, int currCommunity) {
            final long NODE_OFFSET = nodeId * config.k();
            int bestCommunity = currCommunity;
            double smallestCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + currCommunity);

            for (int i = 0; i < config.k(); i++) {
                var nodeToCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + i);
                if (nodeToCommunityWeight < smallestCommunityWeight) {
                    bestCommunity = i;
                    smallestCommunityWeight = nodeToCommunityWeight;
                }
            }

            return bestCommunity;
        }
    }

    private final class ComputeCost implements Runnable {

        private final Graph graph;
        private final HugeIntArray candidateSolution;
        private final AtomicDoubleArray cost;
        private final Partition partition;

        ComputeCost(
            Graph graph,
            HugeIntArray candidateSolution,
            AtomicDoubleArray cost,
            Partition partition
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.cost = cost;
            this.partition = partition;
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
                            localCost.add(weightTransformer.accept(weight));
                        }
                        return true;
                    }
                );
            });
            cost.add(0, localCost.doubleValue());
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
