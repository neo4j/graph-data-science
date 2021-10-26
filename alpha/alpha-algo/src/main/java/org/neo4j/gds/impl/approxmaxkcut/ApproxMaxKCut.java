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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.AtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicByteArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private final Random random;
    private final ExecutorService executor;
    private final ApproxMaxKCutConfig config;
    private final AllocationTracker allocationTracker;
    private final WeightTransformer weightTransformer;
    private final Comparator comparator;
    private final HugeByteArray[] candidateSolutions;
    private final AtomicDoubleArray[] costs;
    private final HugeAtomicDoubleArray nodeToCommunityWeights;
    private final HugeAtomicByteArray swapStatus;
    private AtomicLongArray currCardinalities;
    private final List<Long> rangePartitionActualBatchSizes;
    private final List<Partition> degreePartition;
    private HugeByteArray neighborSolution;
    private AtomicLongArray neighborCardinalities;

    public ApproxMaxKCut(
        Graph graph,
        ExecutorService executor,
        ApproxMaxKCutConfig config,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.random = new Random(config.randomSeed().orElseGet(() -> new Random().nextLong()));
        this.executor = executor;
        this.config = config;
        this.allocationTracker = allocationTracker;
        this.weightTransformer = config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D;
        this.comparator = config.minimize() ? (lhs, rhs) -> lhs < rhs : (lhs, rhs) -> lhs > rhs;

        // We allocate two arrays in order to be able to compare results between iterations "GRASP style".
        this.candidateSolutions = new HugeByteArray[]{
            HugeByteArray.newArray(graph.nodeCount(), allocationTracker),
            HugeByteArray.newArray(graph.nodeCount(), allocationTracker)
        };

        this.costs = new AtomicDoubleArray[]{
            new AtomicDoubleArray(1),
            new AtomicDoubleArray(1)
        };
        costs[0].set(0, config.minimize() ? Double.MAX_VALUE : Double.MIN_VALUE);
        costs[1].set(0, config.minimize() ? Double.MAX_VALUE : Double.MIN_VALUE);

        // Used by `localSearch()` to keep track of the costs for swapping a node to another community.
        // TODO: If we had pull-based traversal we could have a |V| sized int array here instead of the |V|*k sized
        //  double array.
        this.nodeToCommunityWeights = HugeAtomicDoubleArray.newArray(graph.nodeCount() * config.k(), allocationTracker);

        // Used by `localSearch()` to keep track of whether we can swap a node into another community or not.
        this.swapStatus = HugeAtomicByteArray.newArray(graph.nodeCount(), allocationTracker);

        this.currCardinalities = new AtomicLongArray(config.k());

        this.degreePartition = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> partition,
            Optional.of(config.minBatchSize())
        );

        this.rangePartitionActualBatchSizes = PartitionUtils.rangePartitionActualBatchSizes(
            config.concurrency(),
            graph.nodeCount(),
            Optional.of(config.minBatchSize())
        );
    }

    @ValueClass
    public interface CutResult {
        // Value at index `i` is the idx of the community to which node with id `i` belongs.
        HugeByteArray candidateSolution();

        double cutCost();

        static CutResult of(
            HugeByteArray candidateSolution,
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

    @FunctionalInterface
    private interface WeightTransformer {
        double accept(double weight);
    }

    @FunctionalInterface
    private interface Comparator {
        boolean accept(double lhs, double rhs);
    }

    @Override
    public CutResult compute() {
        // Keep track of which candidate solution is currently being used and which is best.
        byte currIdx = 0, bestIdx = 1;

        progressTracker.beginSubTask();

        if (config.vnsMaxNeighborhoodOrder() > 0) {
            neighborSolution = HugeByteArray.newArray(graph.nodeCount(), allocationTracker);
            neighborCardinalities = new AtomicLongArray(config.k());
        }

        for (int i = 1; (i <= config.iterations()) && running(); i++) {
            var currCandidateSolution = candidateSolutions[currIdx];
            var currCost = costs[currIdx];

            placeNodesRandomly(currCandidateSolution);

            if (!running()) break;

            if (config.vnsMaxNeighborhoodOrder() > 0) {
                variableNeighborhoodSearch(currIdx);
            } else {
                localSearch(currCandidateSolution, currCost, currCardinalities);
            }

            // Store the newly computed candidate solution if it was better than the previous. Then reuse the previous data
            // structures to make a new solution candidate if we are doing more iterations.
            if (comparator.accept(currCost.get(0), costs[bestIdx].get(0))) {
                var tmp = bestIdx;
                bestIdx = currIdx;
                currIdx = tmp;
            }
        }

        progressTracker.endSubTask();

        return CutResult.of(candidateSolutions[bestIdx], costs[bestIdx].get(0));
    }

    // Assign the duty of assigning nodes to fill up minimum community size requirements among partitions in a
    // sufficiently random way.
    private long[][] minCommunitySizesToPartitions(List<Long> batchSizes) {
        // Balance granularity of communities' min sizes over partition such that it's sufficiently random while not
        // requiring too many iterations.
        double SIZE_TO_CHUNK_FACTOR = batchSizes.size() * 8.0;
        var chunkSizes = config
            .minCommunitySizes()
            .stream()
            .mapToLong(minSz -> (long) Math.ceil(minSz / SIZE_TO_CHUNK_FACTOR))
            .toArray();

        var currPartitionCounts = new long[batchSizes.size()];
        var remainingMinCommunitySizeCounts = new ArrayList<>(config.minCommunitySizes());

        var minCommunitiesPerPartition = new long[config.concurrency()][];
        Arrays.setAll(minCommunitiesPerPartition, i -> new long[config.k()]);

        var activePartitions = IntStream
            .range(0, batchSizes.size())
            .filter(partition -> batchSizes.get(partition) > 0)
            .boxed()
            .collect(Collectors.toList());
        var activeCommunities = IntStream
            .range(0, config.k())
            .filter(community -> config.minCommunitySizes().get(community) > 0)
            .boxed()
            .collect(Collectors.toList());

        while (!activeCommunities.isEmpty()) {
            int partitionIdx = random.nextInt(activePartitions.size());
            int communityIdx = random.nextInt(activeCommunities.size());
            int partition = activePartitions.get(partitionIdx);
            int community = activeCommunities.get(communityIdx);
            long increment = Math.min(
                Math.min(chunkSizes[community], batchSizes.get(partition) - currPartitionCounts[partition]),
                remainingMinCommunitySizeCounts.get(community)
            );

            minCommunitiesPerPartition[partition][community] += increment;
            currPartitionCounts[partition] += increment;
            if (currPartitionCounts[partition] == batchSizes.get(partition)) {
                activePartitions.remove(partitionIdx);
            }

            remainingMinCommunitySizeCounts.set(community, remainingMinCommunitySizeCounts.get(community) - increment);
            if (remainingMinCommunitySizeCounts.get(community) == 0) {
                activeCommunities.remove(communityIdx);
            }
        }

        return minCommunitiesPerPartition;
    }

    private void placeNodesRandomly(HugeByteArray candidateSolution) {
        assert graph.nodeCount() >= config.k();

        var minCommunitiesPerPartition = minCommunitySizesToPartitions(rangePartitionActualBatchSizes);
        for (byte i = 0; i < config.k(); i++) {
            currCardinalities.set(i, config.minCommunitySizes().get(i));
        }

        var partitionIndex = new AtomicInteger(0);
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            partition -> new PlaceNodesRandomly(
                candidateSolution,
                minCommunitiesPerPartition[partitionIndex.getAndIncrement()],
                partition
            ),
            Optional.of(config.minBatchSize())
        );
        progressTracker.beginSubTask();
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
        progressTracker.endSubTask();
    }

    /*
     * This is a Local Search procedure modified to run more efficiently in parallel. Instead of restarting the while
     * loop whenever anything has changed in the candidate solution we try to continue as long as we can in order to
     * avoid the overhead of rescheduling our tasks on threads and possibly losing hot caches.
     */
    private void localSearch(
        HugeByteArray candidateSolution,
        AtomicDoubleArray cost,
        AtomicLongArray cardinalities
    ) {
        var change = new AtomicBoolean(true);

        progressTracker.beginSubTask();

        progressTracker.beginSubTask();
        while (change.get() && running()) {
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

    // Handle that `Math.abs(Long.MIN_VALUE) == Long.MIN_VALUE`.
    // `min` is inclusive, and `max` is exclusive.
    private long randomNonNegativeLong(Random rand, long min, long max) {
        assert min >= 0;
        assert max > min;

        long randomNum;
        do {
            randomNum = rand.nextLong();
        } while (randomNum == Long.MIN_VALUE);

        return (Math.abs(randomNum) % (max - min)) + min;
    }

    private boolean perturbSolution(
        HugeByteArray solution,
        AtomicLongArray cardinalities
    ) {
        final int MAX_RETRIES = 100;
        int retries = 0;

        while (retries < MAX_RETRIES) {
            long nodeToFlip = randomNonNegativeLong(random, 0, graph.nodeCount());
            byte currCommunity = solution.get(nodeToFlip);

            if (cardinalities.get(currCommunity) <= config.minCommunitySizes().get(currCommunity)) {
                // Flipping this node will invalidate the solution in terms on min community sizes.
                retries++;
                continue;
            }

            // For `nodeToFlip`, move to a new random community not equal to its current community in
            // `neighboringSolution`.
            byte rndNewCommunity = (byte) ((solution.get(nodeToFlip) + (random.nextInt(config.k() - 1) + 1))
                                           % config.k());

            solution.set(nodeToFlip, rndNewCommunity);
            cardinalities.decrementAndGet(currCommunity);
            cardinalities.incrementAndGet(rndNewCommunity);

            break;
        }

        return retries != MAX_RETRIES;
    }

    private void copyCardinalities(AtomicLongArray source, AtomicLongArray target) {
        assert target.length() >= source.length();

        for (int i = 0; i < source.length(); i++) {
            target.setPlain(i, source.getPlain(i));
        }
    }

    private void variableNeighborhoodSearch(int candidateIdx) {
        var bestCandidateSolution = candidateSolutions[candidateIdx];
        var bestCardinalities = currCardinalities;
        var bestCost = costs[candidateIdx];
        var neighborCost = new AtomicDoubleArray(1);
        boolean perturbSuccess = true;
        var order = 0;

        progressTracker.beginSubTask();

        while ((order < config.vnsMaxNeighborhoodOrder()) && running()) {
            bestCandidateSolution.copyTo(neighborSolution, graph.nodeCount());
            copyCardinalities(bestCardinalities, neighborCardinalities);

            // Generate a neighboring candidate solution of the current order.
            for (int i = 0; i < order; i++) {
                perturbSuccess = perturbSolution(neighborSolution, neighborCardinalities);
                if (!perturbSuccess) {
                    break;
                }
            }

            localSearch(neighborSolution, neighborCost, neighborCardinalities);

            if (comparator.accept(neighborCost.get(0), bestCost.get(0))) {
                var tmpCandidateSolution = bestCandidateSolution;
                bestCandidateSolution = neighborSolution;
                neighborSolution = tmpCandidateSolution;

                var tmpCardinalities = bestCardinalities;
                bestCardinalities = neighborCardinalities;
                neighborCardinalities = tmpCardinalities;

                bestCost.set(0, neighborCost.get(0));

                // Start from scratch with the new candidate.
                order = 0;
            } else {
                order += 1;
            }

            if (!perturbSuccess) {
                // We were not able to perturb this solution further, so let's stop.
                break;
            }
        }

        // If we obtained a better candidate solution from VNS, swap with that with the one we started with.
        if (bestCandidateSolution != candidateSolutions[candidateIdx]) {
            neighborSolution = candidateSolutions[candidateIdx];
            candidateSolutions[candidateIdx] = bestCandidateSolution;
            currCardinalities = bestCardinalities;
        }

        progressTracker.endSubTask();
    }

    private final class PlaceNodesRandomly implements Runnable {

        private final HugeByteArray candidateSolution;
        private final long[] minNodesPerCommunity;
        private final Partition partition;

        PlaceNodesRandomly(
            HugeByteArray candidateSolution,
            long[] minNodesPerCommunity,
            Partition partition
        ) {
            this.candidateSolution = candidateSolution;
            this.minNodesPerCommunity = minNodesPerCommunity;
            this.partition = partition;
        }

        @Override
        public void run() {
            Random rand;
            if (config.concurrency() > 1) {
                rand = ThreadLocalRandom.current();
            } else {
                // We want the ability to obtain a deterministic result for single-threaded computations.
                rand = random;
            }

            var nodes = shuffle(rand, partition.startNode(), partition.nodeCount());

            // Fill in the nodes that this partition is required to provide to each community.
            long offset = 0;
            for (byte i = 0; i < config.k(); i++) {
                for (long j = 0; j < minNodesPerCommunity[i]; j++) {
                    candidateSolution.set(nodes.get(offset++), i);
                }
            }

            // Assign the rest of the nodes of the partition to random communities.
            var localCardinalities = new long[config.k()];
            for (long i = offset; i < nodes.size(); i++) {
                byte randomCommunity = (byte) rand.nextInt(config.k());
                localCardinalities[randomCommunity]++;
                candidateSolution.set(nodes.get(i), randomCommunity);
            }

            for (int i = 0; i < config.k(); i++) {
                currCardinalities.addAndGet(i, localCardinalities[i]);
            }

            progressTracker.logProgress(partition.nodeCount());
        }

        private HugeLongArray shuffle(Random random, long minInclusive, long length) {
            HugeLongArray elements = HugeLongArray.newArray(length, AllocationTracker.empty());

            for (long i = 0; i < length; i++) {
                long nextToAdd = minInclusive + i;
                long j = randomNonNegativeLong(random, 0, i + 1);
                if (j == i) {
                    elements.set(i, nextToAdd);
                } else {
                    elements.set(i, elements.get(j));
                    elements.set(j, nextToAdd);
                }
            }

            return elements;
        }

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

    @Override
    public ApproxMaxKCut me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }
}
