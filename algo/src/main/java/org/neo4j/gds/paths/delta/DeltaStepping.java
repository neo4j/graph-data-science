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
package org.neo4j.gds.paths.delta;

import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.delta.TentativeDistances.NO_PREDECESSOR;

public final class DeltaStepping extends Algorithm<PathFindingResult> {
    public static final String DESCRIPTION = "The Delta Stepping shortest path algorithm computes the shortest (weighted) path between one node and any other node in the graph. " +
                                             "The computation is run multi-threaded";

    private static final int NO_BIN = Integer.MAX_VALUE;
    private static final int BIN_SIZE_THRESHOLD = 1000;
    private static final int BATCH_SIZE = 64;


    private final Graph graph;
    private final long startNode;
    private final double delta;
    private final int concurrency;

    private final HugeLongArray frontier;
    private final TentativeDistances distances;

    private final ExecutorService executorService;

    public static DeltaStepping of(
        Graph graph,
        AllShortestPathsDeltaBaseConfig config,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        return new DeltaStepping(
            graph,
            graph.toMappedNodeId(config.sourceNode()),
            config.delta(),
            config.concurrency(),
            true,
            executorService,
            progressTracker
        );
    }

    public static MemoryEstimation memoryEstimation(boolean storePredecessors) {
        var builder = MemoryEstimations.builder(DeltaStepping.class)
            .perNode("distance array", HugeAtomicDoubleArray::memoryEstimation)
            .rangePerGraphDimension("shared bin", (dimensions, concurrency) -> {
                // This is the average case since it is likely that we visit most nodes
                // in one of the iterations due to power-law distributions.
                var lowerBound = HugeLongArray.memoryEstimation(dimensions.nodeCount());
                // This is the worst-case, which we will most likely never hit since the
                // graph needs to be complete to reach all nodes from all threads.
                var upperBound = HugeLongArray.memoryEstimation(dimensions.relCountUpperBound());

                return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
            })
            .rangePerGraphDimension("local bins", (dimensions, concurrency) -> {
                // We don't know how many buckets we have per thread since it depends on the delta
                // and the average path length within the graph. We try some bounds instead ...

                // Assuming that each node is visited by at most one thread, it is stored in at most
                // one thread-local bucket, hence the best case is dividing all the nodes across
                // thread-local buckets.
                var lowerBound = HugeLongArray.memoryEstimation(dimensions.nodeCount() / concurrency);

                // The worst case is again the fully-connected graph where we would replicate all nodes in
                // thread-local buckets in a single iteration.
                var upperBound = HugeLongArray.memoryEstimation(concurrency * dimensions.nodeCount());

                return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
            });

        if (storePredecessors) {
            builder.perNode("predecessor array", HugeAtomicLongArray::memoryEstimation);
        }

        return builder.build();
    }

    private DeltaStepping(
        Graph graph,
        long startNode,
        double delta,
        int concurrency,
        boolean storePredecessors,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.startNode = startNode;
        this.delta = delta;
        this.concurrency = concurrency;
        this.executorService = executorService;

        this.frontier = HugeLongArray.newArray(graph.relationshipCount());
        if (storePredecessors) {
            this.distances = TentativeDistances.distanceAndPredecessors(
                graph.nodeCount(),
                concurrency
            );
        } else {
            this.distances = TentativeDistances.distanceOnly(
                graph.nodeCount(),
                concurrency
            );
        }
    }

    @Override
    public PathFindingResult compute() {
        progressTracker.beginSubTask();
        int currentBin = 0;

        var frontierIndex = new AtomicLong(0);
        var frontierSize = new AtomicLong(1);

        this.frontier.set(currentBin, startNode);
        this.distances.set(startNode, -1, 0);

        var relaxTasks = IntStream
            .range(0, concurrency)
            .mapToObj(i -> new DeltaSteppingTask(graph, frontier, distances, delta, frontierIndex))
            .collect(Collectors.toList());

        while (currentBin != NO_BIN) {
            // Phase 1
            progressTracker.beginSubTask();
            for (var task : relaxTasks) {
                task.setPhase(Phase.RELAX);
                task.setBinIndex(currentBin);
                task.setFrontierLength(frontierSize.longValue());
            }
            ParallelUtil.run(relaxTasks, executorService);
            progressTracker.endSubTask();

            // Sync barrier
            // Find smallest non-empty bin across all tasks
            currentBin = relaxTasks.stream().mapToInt(DeltaSteppingTask::minNonEmptyBin).min().orElseThrow();

            // Phase 2
            progressTracker.beginSubTask();
            frontierIndex.set(0);
            relaxTasks.forEach(task -> task.setPhase(Phase.SYNC));

            for (var task : relaxTasks) {
                task.setPhase(Phase.SYNC);
                task.setBinIndex(currentBin);
            }
            ParallelUtil.run(relaxTasks, executorService);
            progressTracker.endSubTask();

            frontierSize.set(frontierIndex.longValue());
            frontierIndex.set(0);
        }

        return new PathFindingResult(pathResults(distances, startNode, concurrency), progressTracker::endSubTask);
    }

    enum Phase {
        RELAX,
        SYNC
    }

    private static class DeltaSteppingTask implements Runnable {
        private final Graph graph;
        private final HugeLongArray frontier;
        private final TentativeDistances distances;
        private final double delta;
        private int binIndex;
        private final AtomicLong frontierIndex;
        private long frontierLength;

        // Although there is a probability that a local bin exceeds
        // 2^31 entries, it is very unlikely and if it happens, we
        // certainly have a problem managing the graph size anyway.
        // Overflowing can only happen in the global phase, as the
        // local phase is bounded by the BIN_SIZE_THRESHOLD.
        private LongArrayList[] localBins;
        private Phase phase = Phase.RELAX;

        DeltaSteppingTask(
            Graph graph,
            HugeLongArray frontier,
            TentativeDistances distances,
            double delta,
            AtomicLong frontierIndex
        ) {

            this.graph = graph.concurrentCopy();
            this.frontier = frontier;
            this.distances = distances;
            this.delta = delta;
            this.frontierIndex = frontierIndex;

            this.localBins = new LongArrayList[0];
        }

        @Override
        public void run() {
            if (phase == Phase.RELAX) {
                relaxGlobalBin();
                relaxLocalBin();
            } else if (phase == Phase.SYNC) {
                updateFrontier();
            }
        }

        void setPhase(Phase phase) {
            this.phase = phase;
        }

        void setBinIndex(int binIndex) {
            this.binIndex = binIndex;
        }

        void setFrontierLength(long frontierLength) {
            this.frontierLength = frontierLength;
        }

        int minNonEmptyBin() {
            for (int i = binIndex; i < localBins.length; i++) {
                if (localBins[i] != null && !localBins[i].isEmpty()) {
                    return i;
                }
            }
            return NO_BIN;
        }

        private void relaxGlobalBin() {
            long offset;
            while ((offset = frontierIndex.getAndAdd(BATCH_SIZE)) < frontierLength) {
                long limit = Math.min(offset + BATCH_SIZE, frontierLength);

                for (long idx = offset; idx < limit; idx++) {
                    var nodeId = frontier.get(idx);
                    if (distances.distance(nodeId) >= delta * binIndex) {
                        relaxNode(nodeId);
                    }
                }
            }
        }

        private void relaxLocalBin() {
            while (binIndex < localBins.length
                   && localBins[binIndex] != null
                   && !localBins[binIndex].isEmpty()
                   && localBins[binIndex].size() < BIN_SIZE_THRESHOLD) {
                var binCopy = localBins[binIndex].clone();
                localBins[binIndex].elementsCount = 0;
                binCopy.forEach((LongProcedure) this::relaxNode);
            }
        }

        private void relaxNode(long nodeId) {
            graph.forEachRelationship(nodeId, 1.0, (sourceNodeId, targetNodeId, weight) -> {
                var oldDist = distances.distance(targetNodeId);
                var newDist = distances.distance(sourceNodeId) + weight;

                while (Double.compare(newDist, oldDist) < 0) {
                    var witness = distances.compareAndExchange(targetNodeId, oldDist, newDist, sourceNodeId);

                    if (Double.compare(witness, oldDist) == 0) {
                        int destBin = (int) (newDist / delta);

                        if (destBin >= localBins.length) {
                            this.localBins = Arrays.copyOf(localBins, destBin + 1);
                        }
                        if (localBins[destBin] == null) {
                            this.localBins[destBin] = new LongArrayList();
                        }

                        this.localBins[destBin].add(targetNodeId);
                        break;
                    }
                    // CAX failed, retry
                    //we need to fetch the most recent value from distances
                    oldDist = distances.distance(targetNodeId);
                }

                return true;
            });
        }

        private void updateFrontier() {
            if (binIndex < localBins.length && localBins[binIndex] != null && !localBins[binIndex].isEmpty()) {
                var size = localBins[binIndex].size();
                var offset = frontierIndex.getAndAdd(size);

                for (LongCursor longCursor : localBins[binIndex]) {
                    long index = offset + longCursor.index;
                    frontier.set(index, longCursor.value);
                }

                localBins[binIndex].elementsCount = 0;
            }
        }
    }

    private static Stream<PathResult> pathResults(
        TentativeDistances tentativeDistances,
        long sourceNode,
        int concurrency
    ) {
        var distances = tentativeDistances.distances();
        var predecessors = tentativeDistances.predecessors().orElseThrow();

        var pathIndex = new AtomicLong(0L);

        var partitions = PartitionUtils.rangePartition(
            concurrency,
            predecessors.size(),
            partition -> partition,
            Optional.empty()
        );

        return ParallelUtil.parallelStream(
            partitions.stream(),
            concurrency,
            parallelStream -> parallelStream.flatMap(partition -> {
                var localPathIndex = new MutableLong(pathIndex.getAndAdd(partition.nodeCount()));
                var pathResultBuilder = ImmutablePathResult.builder().sourceNode(sourceNode);

                return LongStream
                    .range(partition.startNode(), partition.startNode() + partition.nodeCount())
                    .filter(target -> predecessors.get(target) != NO_PREDECESSOR)
                    .mapToObj(targetNode -> pathResult(
                        pathResultBuilder,
                        localPathIndex.getAndIncrement(),
                        sourceNode,
                        targetNode,
                        distances,
                        predecessors
                    ));
            })
        );
    }

    private static final long[] EMPTY_ARRAY = new long[0];

    private static PathResult pathResult(
        ImmutablePathResult.Builder pathResultBuilder,
        long pathIndex,
        long sourceNode,
        long targetNode,
        HugeAtomicDoubleArray distances,
        HugeAtomicLongArray predecessors
    ) {
        // TODO: use LongArrayList and then ArrayUtils.reverse
        var pathNodeIds = new LongArrayDeque();
        var costs = new DoubleArrayDeque();

        // We backtrack until we reach the source node.
        var lastNode = targetNode;

        while (true) {
            pathNodeIds.addFirst(lastNode);
            costs.addFirst(distances.get(lastNode));

            // Break if we reach the end by hitting the source node.
            if (lastNode == sourceNode) {
                break;
            }

            lastNode = predecessors.get(lastNode);
        }

        return pathResultBuilder
            .index(pathIndex)
            .targetNode(targetNode)
            .nodeIds(pathNodeIds.toArray())
            .relationshipIds(EMPTY_ARRAY)
            .costs(costs.toArray())
            .build();
    }
}
