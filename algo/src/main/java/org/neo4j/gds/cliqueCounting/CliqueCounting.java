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
package org.neo4j.gds.cliqueCounting;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyCursorUtils;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.cliqueCounting.intersect.CliqueAdjacency;
import org.neo4j.gds.cliqueCounting.intersect.CliqueAdjacencyFactory;
import org.neo4j.gds.cliquecounting.CliqueCountingMode;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;

// either global
// or global + for all vertices
// or [ (1,2,5), (2,4), (6) ] #cliques where () is included.
// Experimentally setting a minimum value on k didn't speed it up much.

/**
 * CliqueCounting counts the number of cliques of every size in the graph.
 * <p>
 * The algorithm supports
 * 1. Globally counting the number of cliques in the graph.
 * 2. For all nodes, calculating the number of cliques they are part of.
 * 3. For some given subsets (subcliques), calculate the number of cliques they are part of.
 * <p>
 * This implementation is based on the Succinct Clique Tree structure from 'The Power of Pivoting for Exact Clique Counting'
 * <a href="https://dl.acm.org/doi/10.1145/3336191.3371839">...</a>
 */

public final class CliqueCounting extends Algorithm<CliqueCountingResult> {
    private final Graph graph;
    private final ExecutorService executorService;
    private final Concurrency concurrency;
    private final AtomicLong rootQueue;

    private final CliqueCountingMode countingMode;
    private final long[][] subcliques;
    private final CliqueAdjacency cliqueAdjacency;
    private final CliqueCountsHandler cliqueCountsHandler;

    public final int MAX_CLIQUE_SIZE = Integer.MAX_VALUE;

    private CliqueCounting(
        Graph graph,
        CliqueCountingParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        long[][] subcliques,
        CliqueAdjacency cliqueAdjacency,
        AtomicLong rootQueue,
        CliqueCountsHandler cliqueCountsHandler
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = parameters.concurrency();
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
        this.rootQueue = rootQueue;
        this.countingMode = parameters.countingMode();
        this.subcliques = subcliques;
        this.cliqueAdjacency = cliqueAdjacency;
        this.cliqueCountsHandler = cliqueCountsHandler;
    }

    public static CliqueCounting create(
        Graph graph,
        CliqueCountingParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        long[][] subcliques = new long[parameters.subcliques().size()][];
        for (int i = 0; i < parameters.subcliques().size(); i++) {
            subcliques[i] = parameters.subcliques().get(i);
        }
        var rootQueue = new AtomicLong(0L);
        CliqueAdjacency cliqueAdjacency = CliqueAdjacencyFactory.createCliqueAdjacency(graph);
        CliqueCountsHandler cliqueCountsHandler = new CliqueCountsHandler(graph.nodeCount());

        return new CliqueCounting(
            graph,
            parameters,
            executorService,
            progressTracker,
            terminationFlag,
            subcliques,
            cliqueAdjacency,
            rootQueue,
            cliqueCountsHandler
        );
    }

    //public compute method
    @Override
    public CliqueCountingResult compute() {
        progressTracker.beginSubTask();

        var tasks = createTasks();
        RunWithConcurrency
            .builder()
            .tasks(tasks)
            .concurrency(concurrency)
            .executor(executorService)
            .run();

        progressTracker.endSubTask();
      return createResult();
    }

    CliqueCountingResult createResult(){
        switch (countingMode) {
            case GloballyOnly -> {
                var globalCount = cliqueCountsHandler.merge().globalCount.toLongArray();
                return new CliqueCountingResult(
                    globalCount,
                    HugeObjectArray.of(),
                    new long[0][0]
                );
            }
            case ForEveryNode -> {
                var sizeFrequencies = cliqueCountsHandler.merge();
                HugeObjectArray<long[]> perNodeCount = HugeObjectArray.newArray(long[].class, graph.nodeCount());
                perNodeCount.setAll(node -> sizeFrequencies.perNodeCount.get(node).toLongArray());
                return new CliqueCountingResult(
                    sizeFrequencies.globalCount.toLongArray(),
                    perNodeCount,
                    new long[0][0]
                );
            }
            case ForGivenSubcliques -> {
                return new CliqueCountingResult(
                    new long[0],
                    HugeObjectArray.of(),
                    cliqueCountsHandler.free.stream().map(sizeFrequencies -> sizeFrequencies.globalCount.toLongArray()).toArray(long[][]::new)
                );
            }
            default -> throw new IllegalStateException("Unexpected value: " + countingMode);
        }
    }

    private  Collection<? extends Runnable> createTasks(){
        return countingMode == CliqueCountingMode.ForGivenSubcliques ?
            ParallelUtil.tasks(
                concurrency,
                () -> new SubcliqueCliqueCountingTask(graph.concurrentCopy())
            ) :
            ParallelUtil.tasks(
                concurrency,
                () -> new GlobalCliqueCountingTask(graph.concurrentCopy(), cliqueCountsHandler.takeOrCreate())
            );

    }

    private void recursiveSctCliqueCount(long[] subset, long[][] feasibleNeighborhoods, RecursiveCliqueNodes cliqueNodes, SizeFrequencies sizeFrequencies) {
        if (subset.length == 0) {
            var nodes = cliqueNodes.activeNodes();
            var requiredPointer = cliqueNodes.requiredNodes();
            sizeFrequencies.updateFrequencies(countingMode, nodes, requiredPointer);
        } else {
            int[][] intersectionIds = new int[subset.length][];
            for (int i = 0; i < subset.length; i++) {
                intersectionIds[i] = intersectionIds(subset, feasibleNeighborhoods[i]);
            }
            SubsetPartitionIds partitionIds = partitionSubsetIds(subset, intersectionIds);

            var nsp = new long[partitionIds.includedNodesIds().length];
            for (int i = 0; i < partitionIds.includedNodesIds().length; i++) {
                nsp[i] = subset[partitionIds.includedNodesIds()[i]];
            }
            var newFeasibleNeighborhoods = new long[nsp.length][];
            for (int j = 0; j < nsp.length; j++) {
                var subsetIdx = partitionIds.includedNodesIds()[j];
                newFeasibleNeighborhoods[j] = new long[intersectionIds[subsetIdx].length];
                for (int k = 0; k < intersectionIds[subsetIdx].length; k++) {
                    newFeasibleNeighborhoods[j][k] = subset[intersectionIds[subsetIdx][k]];
                }
            }

            var pivot = subset[partitionIds.pivotIndex()];
            cliqueNodes.add(pivot, false);

            recursiveSctCliqueCount(
                nsp,
                newFeasibleNeighborhoods,
                cliqueNodes,
                sizeFrequencies
            );

            var vsIds = partitionIds.excludedNodesIds();
            for (int i = 0; i < vsIds.length; i++) {
                var newSubsetAsSubsetIds = difference(intersectionIds[vsIds[i]], vsIds, i);
                var newSubset = new long[newSubsetAsSubsetIds.length];
                newFeasibleNeighborhoods = new long[newSubset.length][];
                for (int j = 0; j < newSubsetAsSubsetIds.length; j++) {
                    newSubset[j] = subset[newSubsetAsSubsetIds[j]];
                    newFeasibleNeighborhoods[j] = new long[intersectionIds[newSubsetAsSubsetIds[j]].length];
                    for (int k = 0; k < intersectionIds[newSubsetAsSubsetIds[j]].length; k++) {
                        newFeasibleNeighborhoods[j][k] = subset[intersectionIds[newSubsetAsSubsetIds[j]][k]];
                    }
                }
                var vi = subset[vsIds[i]];
                cliqueNodes.add(vi, true);
                recursiveSctCliqueCount(
                    newSubset,
                    newFeasibleNeighborhoods,
                    cliqueNodes,
                    sizeFrequencies
                );
            }
        }
        cliqueNodes.finishRecursionLevel();
    }

    long[][] computeIntersections(long[] subset){
        long[][] intersections = new long[subset.length][];
        for (int i = 0; i < subset.length; i++) {
            intersections[i] = neighborhoodIntersection(subset, subset[i]);
        }
        return intersections;
    }

    // S n N(v)
    private long[] neighborhoodIntersection(long[] subset, long node) {
        long[] intersection = new long[subset.length];
        int intersectionPointer = 0;

        AdjacencyCursor neighborCursor = cliqueAdjacency.createCursor(node);
        var current = AdjacencyCursorUtils.peek(neighborCursor);
        for (var subsetNode : subset) {
            current = AdjacencyCursorUtils.advance(neighborCursor, current, subsetNode);
            if (current == subsetNode) {
                intersection[intersectionPointer++] = current;
            } else if (current == NOT_FOUND) {
                break; //subset is exhausted
            }
        }//subset is exhausted
        return Arrays.copyOf(intersection, intersectionPointer);
    }

    // S \ (v_1,...,v_{i-1}) // here S \ (v_0,...,v_{i-1}) where i is one lower.
    static int[] difference(int[] subset, int[] vs, int i){
        var difference = new int[subset.length];
        int differencePointer = 0;
        int j = 0;
        outer:
        for (var subsetNode : subset) {
            for (;j < i && vs[j] <= subsetNode; j++) {
                if (subsetNode == vs[j]) {
                    continue outer;
                }
            }
            difference[differencePointer++] = subsetNode;
        }
        return Arrays.copyOf(difference, differencePointer);
    }

    // S n N, as ids of S
    private int[] intersectionIds(long[] subset, long[] neighborhood) {
        int[] intersectionIds = new int[subset.length]; //min of lengths
        int intersectionPointer = 0;
        int subsetPointer = 0;
        int neighborhoodPointer = 0;
        while (subsetPointer < subset.length && neighborhoodPointer < neighborhood.length) {
            switch (Long.compare(subset[subsetPointer], neighborhood[neighborhoodPointer])) {
                case -1 -> subsetPointer++;
                case 0 -> {
                    intersectionIds[intersectionPointer++] = subsetPointer++;
                    neighborhoodPointer++;
                }
                case 1 -> neighborhoodPointer++;
            }
        }
        return Arrays.copyOf(intersectionIds, intersectionPointer);
    }

    // Partition subset into Included, Excluded, pivot. Where entries are ids of subet.
    static SubsetPartitionIds partitionSubsetIds(long[] subset, int[][] intersectionIds) {
        //TODO: Assure self loops are handled correctly!
        int pivotIdx = -1;
        int maxSize = -1;
        for(int i = 0; i < subset.length; i++) {
            if (intersectionIds[i].length > maxSize) {
                maxSize = intersectionIds[i].length;
                pivotIdx = i;
            }
        }
        int[] differenceIds = new int[subset.length-maxSize-1];
        int differencePointer = 0;
        int intersectionPointer = 0;

        outer:
        for (int subsetIdx = 0; subsetIdx < subset.length; subsetIdx++) {
            while (intersectionPointer < intersectionIds[pivotIdx].length) {
                if (subsetIdx > intersectionIds[pivotIdx][intersectionPointer]) {
                    intersectionPointer++;
                } else if (subsetIdx == intersectionIds[pivotIdx][intersectionPointer]) {
                    intersectionPointer++;
                    continue outer;
                } else {
                    break;
                }
            }
            if (subsetIdx != pivotIdx) {
                differenceIds[differencePointer++] = subsetIdx;
            }
            //check if subsetIdx is in intersectionIds[i]
            //if not (and if not pivotIndex) add to diff
        }

        return new SubsetPartitionIds(intersectionIds[pivotIdx], differenceIds, pivotIdx);
    }

    long[] rootNodeNeighbors(long rootNode, boolean filter){
        ArrayList<Long> neighbors = new ArrayList<>();
        var lastEntered = -1L;
        var cursor  = cliqueAdjacency.createCursor(rootNode);
        while (cursor.hasNextVLong()){
            var  nodeId  =  cursor.nextVLong();
            if (nodeId != NOT_FOUND && nodeId!=lastEntered){
                boolean should = !filter || (nodeId > rootNode);
                if (should) {
                    neighbors.add(nodeId);
                    lastEntered = nodeId;
                }
            }
        }

        return neighbors.stream().mapToLong(Long::longValue).toArray();
    }

    private class GlobalCliqueCountingTask implements Runnable {
        Graph graph;
        SizeFrequencies sizeFrequencies;

        GlobalCliqueCountingTask(Graph graph, SizeFrequencies sizeFrequencies) {
            this.graph = graph;
            this.sizeFrequencies = sizeFrequencies;
        }

        public void run() {
            long rootNode;
            while ((rootNode = rootQueue.getAndIncrement()) < graph.nodeCount() && terminationFlag.running()) {

                var cliqueNodes = new RecursiveCliqueNodes(Math.min(MAX_CLIQUE_SIZE, graph.degree(rootNode)+1));
                cliqueNodes.add(rootNode, true);
                var positiveNeighborhood = rootNodeNeighbors(rootNode, true);
                long[][] feasibleNeighborhoods = computeIntersections(positiveNeighborhood);
                recursiveSctCliqueCount(positiveNeighborhood, feasibleNeighborhoods, cliqueNodes, sizeFrequencies);

            }
            cliqueCountsHandler.giveBack(sizeFrequencies);
        }
    }

    private class SubcliqueCliqueCountingTask implements Runnable {
        Graph graph;

        SubcliqueCliqueCountingTask(Graph graph) {
            this.graph = graph;
        }

        public void run() {
            int subcliqueIdx;
            while ((subcliqueIdx = (int)rootQueue.getAndIncrement()) < subcliques.length && terminationFlag.running()) {
                var subclique = subcliques[subcliqueIdx];
                var sizeFrequencies = cliqueCountsHandler.create();
                var upperBound = MAX_CLIQUE_SIZE;
                for (var node : subclique){
                    upperBound = Math.min(upperBound, graph.degree(node)+1);
                }
                var cliqueNodes  = new RecursiveCliqueNodes(upperBound);
                for (var node : subclique){
                    cliqueNodes.add(node,true);
                }

                var subset = rootNodeNeighbors(subclique[0],false);
                for (var node : subclique) { //first is unnecessary
                    subset = neighborhoodIntersection(subset, node); //S = N(v1) n N(v2) n ... n N(vk)
                }
                long[][] feasibleNeighborhoods = computeIntersections(subset);
                recursiveSctCliqueCount(subset, feasibleNeighborhoods, cliqueNodes, sizeFrequencies);
                cliqueCountsHandler.giveBack(sizeFrequencies); //Needs to be stored but not be used by another thread.
            }
        }
    }
}
