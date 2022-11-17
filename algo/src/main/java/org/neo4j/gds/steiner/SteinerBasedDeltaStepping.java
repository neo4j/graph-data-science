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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.delta.TentativeDistances;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class SteinerBasedDeltaStepping extends Algorithm<DijkstraResult> {

    public static final int NO_BIN = Integer.MAX_VALUE;
    public static final int BIN_SIZE_THRESHOLD = 1000;
    private final Graph graph;
    private final long startNode;
    private final double delta;
    private final int concurrency;
    private final HugeLongArray frontier;
    private final TentativeDistances distances;
    private final ExecutorService executorService;
    private long pathIndex;

    private final long numOfTerminals;
    private  final BitSet unvisitedTerminal;
    private final BitSet mergedWithSource;

    private final LongAdder metTerminals;

     SteinerBasedDeltaStepping(
        Graph graph,
        long startNode,
        double delta,
        BitSet isTerminal,
        int concurrency,
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
        this.distances = TentativeDistances.distanceAndPredecessors(
            graph.nodeCount(),
            concurrency
        );
        this.mergedWithSource =new BitSet(graph.nodeCount());
        this.unvisitedTerminal=  new BitSet(isTerminal.size());
        unvisitedTerminal.or(isTerminal);
        this.pathIndex=0;
        this.metTerminals=new LongAdder();
        this.numOfTerminals=isTerminal.cardinality();

    }
    private void mergeNodesOnPathToSource(long nodeId,AtomicLong frontierIndex) {
        long currentId = nodeId;
        //while not meeting merged nodes, add the current path node to the merge set
        //if the parent i merged, then it's path to the source has already been zeroed,
        //hence we can stop
        while (!mergedWithSource.getAndSet(currentId)) {
            var predecessor=distances.predecessor(currentId);
            distances.set(currentId,predecessor,0);
            frontier.set(frontierIndex.getAndIncrement(),currentId);
            currentId = predecessor;
        }
    }

    private void relaxPhase(List<SteinerBasedDeltaTask> tasks,int currentBin,AtomicLong frontierSize){
        // Phase 1
        for (var task : tasks) {
            task.setPhase(Phase.RELAX);
            task.setBinIndex(currentBin);
            task.setFrontierLength(frontierSize.longValue());
        }
        ParallelUtil.run(tasks, executorService);
    }

    private void syncPhase(List<SteinerBasedDeltaTask> tasks,int currentBin, AtomicLong frontierIndex){
        frontierIndex.set(0);
        tasks.forEach(task -> task.setPhase(Phase.SYNC));

        for (var task : tasks) {
            task.setPhase(Phase.SYNC);
            task.setBinIndex(currentBin);
        }
        ParallelUtil.run(tasks, executorService);
        progressTracker.endSubTask();
    }

    private long nextTerminal(){
        long index=unvisitedTerminal.nextSetBit(0);
        long bestTerminal=index;
        double bestDistance=distances.distance(bestTerminal);
        index=unvisitedTerminal.nextSetBit(index+1);
        while (index!=-1){
            double currentDistance=distances.distance(index);
            if (currentDistance  < bestDistance){
                bestTerminal=index;
                bestDistance=currentDistance;
            }
            index=unvisitedTerminal.nextSetBit(index+1);
        }
        return bestTerminal;
    }
    private boolean updateSteinerTree(long terminalId,AtomicLong frontierIndex,List<PathResult> paths, ImmutablePathResult.Builder pathResultBuilder) {
        paths.add(pathResult(
            pathResultBuilder,
            pathIndex++,
            terminalId,
            distances.distances(),
            distances.predecessors().get(),
            mergedWithSource
        ));
        frontierIndex.set(0);
        metTerminals.increment();
        unvisitedTerminal.flip(terminalId);
        if (metTerminals.longValue() == numOfTerminals) {
            return true;
        }
        mergeNodesOnPathToSource(terminalId, frontierIndex);
        return false;

    }

    private long tryToUpdateSteinerTree(long oldBin, long currentBin) {
        boolean shouldComputeClosestTerminal = false;
        if (currentBin == -1 || oldBin < currentBin) {
            shouldComputeClosestTerminal = true;
        }
        if (shouldComputeClosestTerminal) {
            long terminalId = nextTerminal();
            if (distances.distance(terminalId) < currentBin * delta) {
                return terminalId;
            }
        }
        return -1;
    }

    @Override
    public DijkstraResult compute() {
        int iteration = 0;
        int currentBin = 0;

        var pathResultBuilder = ImmutablePathResult.builder()
            .sourceNode(startNode);

        var frontierIndex = new AtomicLong(0);
        var frontierSize = new AtomicLong(1);

        List<PathResult>  paths=new ArrayList<>();

        this.frontier.set(currentBin, startNode);
        mergedWithSource.set(startNode);
        this.distances.set(startNode, -1, 0);

        var tasks = IntStream
            .range(0, concurrency)
            .mapToObj(i -> new SteinerBasedDeltaTask(graph,
                frontier,
                distances,
                delta,
                frontierIndex,
                mergedWithSource,
                unvisitedTerminal))
            .collect(Collectors.toList());

        boolean shouldBreak=false;
            while (currentBin != NO_BIN && !shouldBreak) {
                relaxPhase(tasks, currentBin, frontierSize);
                long oldCurrentBin = currentBin;

                // Sync barrier
                // Find smallest non-empty bin across all tasks
                currentBin = tasks.stream().mapToInt(SteinerBasedDeltaTask::minNonEmptyBin).min().orElseThrow();

                long terminalId = tryToUpdateSteinerTree(oldCurrentBin, currentBin);

                if (terminalId != -1) { //we
                    shouldBreak = updateSteinerTree(terminalId, frontierIndex, paths, pathResultBuilder);
                    currentBin = 0;
                } else {
                    // Phase 2
                    syncPhase(tasks, currentBin, frontierIndex);
                }
                iteration += 1;
                frontierSize.set(frontierIndex.longValue());
                frontierIndex.set(0);
            }


        return new DijkstraResult(paths.stream());
    }

    @Override
    public void release() {

    }

    enum Phase {
        RELAX,
        SYNC
    }

    private static final long[] EMPTY_ARRAY = new long[0];

    private static PathResult pathResult(
        ImmutablePathResult.Builder pathResultBuilder,
        long pathIndex,
        long targetNode,
        HugeAtomicDoubleArray distances,
        HugeAtomicLongArray predecessors,
        BitSet mergedWithSource
    ) {
        // TODO: use LongArrayList and then ArrayUtils.reverse
        var pathNodeIds = new LongArrayDeque();
        var costs = new DoubleArrayDeque();

        // We backtrack until we reach the source node.
        var lastNode = targetNode;

        while (true) {
            pathNodeIds.addFirst(lastNode);
            if (mergedWithSource.get(lastNode)) {
                break;
            }
            costs.addFirst(distances.get(lastNode)); //cost is added except the very last one

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
