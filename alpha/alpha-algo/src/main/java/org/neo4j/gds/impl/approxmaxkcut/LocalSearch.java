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
package org.neo4j.gds.impl.approxmaxkcut;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.AtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicByteArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LocalSearch {

    private static final double DEFAULT_WEIGHT = 0.0D;

    private final Graph graph;
    private final ApproxMaxKCut.Comparator comparator;
    private final ApproxMaxKCutConfig config;
    private final ExecutorService executor;
    private final WeightTransformer weightTransformer;
    private final HugeAtomicDoubleArray nodeToCommunityWeights;
    private final HugeAtomicByteArray swapStatus;
    private final List<Partition> degreePartition;
    private final ProgressTracker progressTracker;

    public LocalSearch(
        Graph graph,
        ApproxMaxKCut.Comparator comparator,
        ApproxMaxKCutConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        this.graph = graph;
        this.comparator = comparator;
        this.config = config;
        this.executor = executor;
        this.progressTracker = progressTracker;

        this.degreePartition = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> partition,
            Optional.of(config.minBatchSize())
        );

        // Used to keep track of the costs for swapping a node to another community.
        // TODO: If we had pull-based traversal we could have a |V| sized int array here instead of the |V|*k sized
        //  double array.
        this.nodeToCommunityWeights = HugeAtomicDoubleArray.newArray(graph.nodeCount() * config.k(), allocationTracker);

        // Used to keep track of whether we can swap a node into another community or not.
        this.swapStatus = HugeAtomicByteArray.newArray(graph.nodeCount(), allocationTracker);

        this.weightTransformer = config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D;
    }


    @FunctionalInterface
    private interface WeightTransformer {
        double accept(double weight);
    }

    /*
     * Used to improve readability of swapping.
     */
    private static final class NodeSwapStatus {
        // No thread has tried to swap the node yet, and no incoming neighbor has tried to mark it.
        static final byte UNTOUCHED = 0;
        // The node has been swapped to another community, or a thread is currently attempting to swap it.
        static final byte SWAPPING = 1;
        // The node has had one of its incoming neighbors swapped (or at least attempted) so the improvement costs may be invalid.
        static final byte NEIGHBOR = 2;

        private NodeSwapStatus() {}
    }


    /*
     * This is a Local Search procedure modified to run more efficiently in parallel. Instead of restarting the while
     * loop whenever anything has changed in the candidate solution we try to continue as long as we can in order to
     * avoid the overhead of rescheduling our tasks on threads and possibly losing hot caches.
     */
    void compute(
        HugeByteArray candidateSolution,
        AtomicDoubleArray cost,
        AtomicLongArray cardinalities,
        Supplier<Boolean> running
    ) {
        var change = new AtomicBoolean(true);

        progressTracker.beginSubTask();

        progressTracker.beginSubTask();
        while (change.get() && running.get()) {
            nodeToCommunityWeights.setAll(0.0D);
            var nodeToCommunityWeightTasks = degreePartition.stream()
                .map(partition ->
                    new ComputeNodeToCommunityWeights(
                        graph.concurrentCopy(),
                        candidateSolution,
                        partition
                    )
                ).collect(Collectors.toList());
            progressTracker.beginSubTask();
            ParallelUtil.runWithConcurrency(config.concurrency(), nodeToCommunityWeightTasks, executor);
            progressTracker.endSubTask();

            swapStatus.setAll(NodeSwapStatus.UNTOUCHED);
            change.set(false);
            var swapTasks = degreePartition.stream()
                .map(partition ->
                    new SwapForLocalImprovements(
                        graph.concurrentCopy(),
                        candidateSolution,
                        cardinalities,
                        change,
                        partition
                    )
                ).collect(Collectors.toList());
            progressTracker.beginSubTask();
            ParallelUtil.runWithConcurrency(config.concurrency(), swapTasks, executor);
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();

        cost.set(0, 0);
        var costTasks = degreePartition.stream()
            .map(partition ->
                new ComputeCost(
                    graph.concurrentCopy(),
                    candidateSolution,
                    cost,
                    partition
                )
            ).collect(Collectors.toList());
        progressTracker.beginSubTask();
        ParallelUtil.runWithConcurrency(config.concurrency(), costTasks, executor);
        progressTracker.endSubTask();

        progressTracker.endSubTask();
    }

    private final class ComputeNodeToCommunityWeights implements Runnable {

        private final Graph graph;
        private final HugeByteArray candidateSolution;
        private final Partition partition;

        ComputeNodeToCommunityWeights(
            Graph graph,
            HugeByteArray candidateSolution,
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
                        // Loops don't affect the cut cost.
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

            progressTracker.logProgress(partition.nodeCount());
        }
    }

    private final class SwapForLocalImprovements implements Runnable {

        private final Graph graph;
        private final HugeByteArray candidateSolution;
        private final AtomicLongArray cardinalities;
        private final AtomicBoolean change;
        private final Partition partition;

        SwapForLocalImprovements(
            Graph graph,
            HugeByteArray candidateSolution,
            AtomicLongArray cardinalities,
            AtomicBoolean change,
            Partition partition
        ) {
            this.graph = graph;
            this.candidateSolution = candidateSolution;
            this.cardinalities = cardinalities;
            this.change = change;
            this.partition = partition;
        }

        @Override
        public void run() {
            var failedSwap = new MutableBoolean();
            var localChange = new MutableBoolean(false);

            partition.consume(nodeId -> {
                byte currCommunity = candidateSolution.get(nodeId);
                byte bestCommunity = bestCommunity(nodeId, currCommunity);

                if (bestCommunity == currCommunity) return;

                if (cardinalities.getAndUpdate(
                    currCommunity,
                    currCount -> {
                        if (currCount > config.minCommunitySizes().get(currCommunity)) {
                            return currCount - 1;
                        }
                        return currCount;
                    }
                ) == config.minCommunitySizes().get(currCommunity)) {
                    return;
                }

                localChange.setTrue();

                // If no incoming neighbors has marked us as NEIGHBOR, we can attempt to swap the current node.
                if (!swapStatus.compareAndSet(nodeId, NodeSwapStatus.UNTOUCHED, NodeSwapStatus.SWAPPING)) {
                    cardinalities.getAndIncrement(currCommunity);
                    return;
                }

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
                    cardinalities.getAndIncrement(currCommunity);
                    return;
                }

                candidateSolution.set(nodeId, bestCommunity);
                cardinalities.getAndIncrement(bestCommunity);
            });

            if (localChange.getValue()) change.set(true);

            progressTracker.logProgress(partition.nodeCount());
        }

        private byte bestCommunity(long nodeId, byte currCommunity) {
            final long NODE_OFFSET = nodeId * config.k();
            byte bestCommunity = currCommunity;
            double bestCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + currCommunity);

            for (byte i = 0; i < config.k(); i++) {
                var nodeToCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + i);
                if (comparator.accept(bestCommunityWeight, nodeToCommunityWeight)) {
                    bestCommunity = i;
                    bestCommunityWeight = nodeToCommunityWeight;
                }
            }

            return bestCommunity;
        }
    }

    private final class ComputeCost implements Runnable {

        private final Graph graph;
        private final HugeByteArray candidateSolution;
        private final AtomicDoubleArray cost;
        private final Partition partition;

        ComputeCost(
            Graph graph,
            HugeByteArray candidateSolution,
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

            progressTracker.logProgress(partition.nodeCount());
        }
    }

}
