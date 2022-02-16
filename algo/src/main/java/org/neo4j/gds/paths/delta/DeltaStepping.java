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

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.DoublePageCreator;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DeltaStepping extends Algorithm<DeltaStepping.DeltaSteppingResult> {

    private static final double DIST_INF = Double.MAX_VALUE;
    private static final int NO_BIN = Integer.MAX_VALUE;

    private static final int BIN_SIZE_THRESHOLD = 1000;

    private final Graph graph;
    private final long startNode;
    private final double delta;
    private final int concurrency;

    private final HugeLongArray frontier;
    private final HugeAtomicDoubleArray distances;

    private final ExecutorService executorService;

    public DeltaStepping(
        Graph graph,
        long startNode,
        double delta,
        int concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.startNode = startNode;
        this.delta = delta;
        this.concurrency = concurrency;
        this.executorService = executorService;

        this.frontier = HugeLongArray.newArray(graph.relationshipCount(), allocationTracker);
        this.distances = HugeAtomicDoubleArray.newArray(
            graph.nodeCount(),
            DoublePageCreator.of(concurrency, index -> DIST_INF),
            allocationTracker
        );
    }

    @Override
    public DeltaSteppingResult compute() {
        int iteration = 0;
        int currentBin = 0;

        var frontierIndex = new AtomicLong(0);
        var frontierSize = new AtomicLong(1);

        this.frontier.set(currentBin, startNode);
        this.distances.set(startNode, 0);

        var relaxTasks = IntStream
            .range(0, concurrency)
            .mapToObj(i -> new DeltaSteppingTask(graph, frontier, distances, delta, frontierIndex))
            .collect(Collectors.toList());

        while (currentBin != NO_BIN) {
            // Phase 1
            for (var task : relaxTasks) {
                task.setPhase(Phase.RELAX);
                task.setBinIndex(currentBin);
                task.setFrontierLength(frontierSize.longValue());
            }
            ParallelUtil.run(relaxTasks, executorService);

            // Sync barrier
            // Find smallest non-empty bin across all tasks
            currentBin = relaxTasks.stream().mapToInt(DeltaSteppingTask::minNonEmptyBin).min().orElseThrow();

            // Phase 2
            frontierIndex.set(0);
            relaxTasks.forEach(task -> task.setPhase(Phase.SYNC));

            for (var task : relaxTasks) {
                task.setPhase(Phase.SYNC);
                task.setBinIndex(currentBin);
            }
            ParallelUtil.run(relaxTasks, executorService);

            iteration += 1;
            frontierSize.set(frontierIndex.longValue());
            frontierIndex.set(0);
        }

        return ImmutableDeltaSteppingResult.of(iteration, distances);
    }

    @Override
    public void release() {

    }

    enum Phase {
        RELAX,
        SYNC
    }

    static class DeltaSteppingTask implements Runnable {
        private final Graph graph;
        private final HugeLongArray frontier;
        private final HugeAtomicDoubleArray distances;
        private final double delta;
        private int binIndex;
        private final AtomicLong frontierIndex;
        private long frontierLength;

        // TODO: hugify
        private LongArrayList[] localBins;
        private Phase phase = Phase.RELAX;

        DeltaSteppingTask(
            Graph graph,
            HugeLongArray frontier,
            HugeAtomicDoubleArray distances,
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
            while ((offset = frontierIndex.getAndAdd(64)) < frontierLength) {
                long limit = Math.min(offset + 64, frontierLength);

                for (long idx = offset; idx < limit; idx++) {
                    var nodeId = frontier.get(idx);
                    if (distances.get(nodeId) >= delta * binIndex) {
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
                var oldDist = distances.get(targetNodeId);
                var newDist = distances.get(sourceNodeId) + weight;

                while (Double.compare(newDist, oldDist) < 0) {
                    var witness = distances.compareAndExchange(targetNodeId, oldDist, newDist);
                    
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
                    oldDist = witness;
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

    @ValueClass
    public interface DeltaSteppingResult {

        int iterations();

        HugeAtomicDoubleArray distances();
    }
}
