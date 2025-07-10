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
import org.neo4j.gds.api.properties.relationships.RelationshipCursor;
import org.neo4j.gds.cliqueCounting.intersect.CliqueAdjacency;
import org.neo4j.gds.cliquecounting.CliqueCountingMode;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

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
//        long[][] subcliques = (long[][]) parameters.subcliques().toArray();
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

        final Collection<? extends Runnable> tasks =
            countingMode == CliqueCountingMode.ForGivenSubcliques ?
                ParallelUtil.tasks(
                    concurrency,
                    () -> new SubcliqueCliqueCountingTask(graph.concurrentCopy())
                ) :
                ParallelUtil.tasks(
                    concurrency,
                    () -> new GlobalCliqueCountingTask(graph.concurrentCopy(), cliqueCountsHandler.takeOrCreate())
                );

        ParallelUtil.run(tasks, executorService);
        progressTracker.endSubTask();

        switch (countingMode) {
            case GloballyOnly -> {
                var globalCount = cliqueCountsHandler.merge().globalCount.toLongArray();
                System.out.println("Global clique count: " + Arrays.toString(globalCount));
                return new CliqueCountingResult(
//                    cliqueCountsHandler.merge().globalCount.toLongArray(),
                    globalCount,
                    HugeObjectArray.of(),
                    new long[0][0]
                );
            }
            case ForEveryNode -> {
                var sizeFrequencies = cliqueCountsHandler.merge();
                HugeObjectArray<long[]> perNodeCount = HugeObjectArray.newArray(long[].class, graph.nodeCount());
                perNodeCount.setAll(node -> sizeFrequencies.perNodeCount.get(node).toLongArray());
                System.out.println("Global clique count: " + Arrays.toString(sizeFrequencies.globalCount.toLongArray()));
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

    private void recursiveSctCliqueCount(long[] subset, NodeStatus[] cliqueNodes, SizeFrequencies sizeFrequencies) {
//        System.out.println("Working on subset: " + Arrays.toString(subset));
        if (subset.length == 0) {
            long[] requiredNodes = Arrays.stream(cliqueNodes)
                .filter(NodeStatus::required)
                .mapToLong(NodeStatus::nodeId)
                .toArray();
            long[] optionalNodes = Arrays.stream(cliqueNodes)
                .filter(v -> !v.required())
                .mapToLong(NodeStatus::nodeId)
                .toArray();
            sizeFrequencies.updateFrequencies(countingMode, requiredNodes, optionalNodes);
            return;
        }
    // node 0  intersetcts node 1:  there exists edge (0,1)
        //user symmetry: so for node j: you can only consider nodes after >j or (<j) and brek early
        //if j intersects i, do int[i]++ and int[j]++

        int[] intersectionSizes = Arrays.stream(subset).mapToInt(node -> neighborhoodIntersectionSize(subset, node)).toArray();
        SubsetPartition partition = partitionSubset(subset, intersectionSizes);

        recursiveSctCliqueCount(
            partition.nsp(),
            Stream.concat(Arrays.stream(cliqueNodes), Stream.of(new NodeStatus(partition.pivot(), false)))
                .toArray(NodeStatus[]::new),
            sizeFrequencies
        );

        var viSubsetSize = Arrays.stream(partition.vsIds()).map(viIdx -> intersectionSizes[viIdx]).toArray();
        var vs = Arrays.stream(partition.vsIds()).mapToLong(viIdx -> subset[viIdx]).toArray();
        for (int i = 0; i < vs.length; i++) {
            var NSvi = neighborhoodIntersection(subset, vs[i], viSubsetSize[i]);
//            NodeStatus[] newCliqueVertices = Arrays.copyOf(cliqueNodes, cliqueNodes.length+1);  //This is significantly slower 7.2s->8.5s
//            newCliqueVertices[cliqueNodes.length] = new NodeStatus(vs[i], true);
            recursiveSctCliqueCount(
                difference(NSvi, vs, i),
                 Stream.concat(Arrays.stream(cliqueNodes), Stream.of(new NodeStatus(vs[i], true))).toArray(NodeStatus[]::new),
                sizeFrequencies
            );
        }
    }

    public SubsetPartition partitionSubset(long[] subset, int[] intersectionSizes) {
        //TODO: Assure self loops are handled correctly!
        long pivot = -1;
        int maxSize = -1;
        for(int i = 0; i < subset.length; i++) {
            if (intersectionSizes[i] > maxSize) {
                maxSize = intersectionSizes[i];
                pivot = subset[i];
            }
        }
        long[] intersection = new long[maxSize];
        int[] differenceIds = new int[subset.length-maxSize-1];
        int intersectionPointer = 0;
        int differencePointer = 0;

        AdjacencyCursor neighborCursor = cliqueAdjacency.createCursor(pivot);
        var current = AdjacencyCursorUtils.peek(neighborCursor);

        for (int subsetIdx = 0; subsetIdx < subset.length; subsetIdx++) {
            var subsetNode = subset[subsetIdx];
            current = AdjacencyCursorUtils.advance(neighborCursor, current, subsetNode);

            if (subsetNode == current) {
                intersection[intersectionPointer++] = current;
            } else if (subsetNode != pivot) {
                differenceIds[differencePointer++] = subsetIdx;
            }
        }

        return new SubsetPartition(intersection, differenceIds, pivot); //fixme
    }

    //size of intersection S n N(vi)
    int neighborhoodIntersectionSize(long[] subset, long node) {
        var intersectionSizeConsumer = new IntersectionSizeConsumer();
        neighborhoodIntersection(subset, node, intersectionSizeConsumer);
        return intersectionSizeConsumer.size;
    }


//    sorted intersection  S n N(v_i)
    private long[] neighborhoodIntersection(long[] subset, long node, int intersectionSize) { //way slower than previous. 7.2s vs 3.4s
        var intersectionConsumer = new IntersectionConsumer(intersectionSize);
        neighborhoodIntersection(subset, node, intersectionConsumer);
        return intersectionConsumer.intersection;
    }

    private void neighborhoodIntersection(long[] subset, long node, Consumer<Long> consumer) {
        AdjacencyCursor neighborCursor = cliqueAdjacency.createCursor(node);

        var current = AdjacencyCursorUtils.peek(neighborCursor);
        for (var subsetNode : subset) {
            current = AdjacencyCursorUtils.advance(neighborCursor, current, subsetNode);
            if (current == subsetNode) {
                consumer.accept(current);
            } else if (current == NOT_FOUND) {
                return; //subset is exhausted
            }
        }//subset is exhausted
    }

    // S \ (v_1,...,v_{i-1}) // here S \ (v_0,...,v_{i-1}) where i is one lower.
    private long[] difference(long[] subset, long[] vs, int i){
        var difference = new long[subset.length];
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

    private record NodeStatus(long nodeId, boolean required) { }

    private record SubsetPartition(long[] nsp, int[] vsIds, long pivot) { }

    private class GlobalCliqueCountingTask implements Runnable {
        //Fixme: Doesn't work for subcliques since they don't use positive neighborhood
        Graph graph;
        SizeFrequencies sizeFrequencies;

        GlobalCliqueCountingTask(Graph graph, SizeFrequencies sizeFrequencies) {
            this.graph = graph;
            this.sizeFrequencies = sizeFrequencies;
        }

        public void run() {
            long nodeCount = graph.nodeCount();
            long rootNode;
            while ((rootNode = rootQueue.getAndIncrement()) < graph.nodeCount() && terminationFlag.running()) {
                NodeStatus[] cliqueNodes = {new NodeStatus(rootNode, true)};
                long finalRootNode = rootNode;
                long[] positiveNeighborhood = graph
                    .streamRelationships(rootNode, -1)
                    .mapToLong(RelationshipCursor::targetId)
                    .sorted()
                    .distinct()
                    .filter(target -> target > finalRootNode)
//                    .filter(r -> r.targetId() > r.sourceId())
//                    .mapToLong(RelationshipCursor::targetId)
                    .toArray();
                if (rootNode % 100_000 == 99_999) {
                    System.out.println("Processing root node: " + rootNode + "/" + nodeCount + ". With positive neighborhood: " + Arrays.toString(
                        positiveNeighborhood));
                }
                recursiveSctCliqueCount(positiveNeighborhood, cliqueNodes, sizeFrequencies);
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
                NodeStatus[] cliqueNodes = Arrays.stream(subclique).mapToObj(node -> new NodeStatus(node, true)).toArray(NodeStatus[]::new);
                long[] subset = graph
                    .streamRelationships(subclique[0], -1)
                    .mapToLong(RelationshipCursor::targetId)
                    .sorted()
                    .distinct()
                    .toArray();
                for (var node : subclique) { //first is unnecessary
                    var newSubsetSize = neighborhoodIntersectionSize(subset, node);
                    subset = neighborhoodIntersection(subset, node, newSubsetSize); //S = N(v1) n N(v2) n ... n N(vk)
                }
                recursiveSctCliqueCount(subset, cliqueNodes, sizeFrequencies);
                cliqueCountsHandler.giveBack(sizeFrequencies); //Needs to be stored but not be used by another thread.
            }
        }
    }

    public class IntersectionSizeConsumer implements Consumer<Long> {
        int size;
        public IntersectionSizeConsumer() {
            this.size = 0;
        }
        public void accept(Long node){
            size++;
        }
    }

    public class IntersectionConsumer implements Consumer<Long>  {
        long[] intersection;
        int intersectionPointer;

        public IntersectionConsumer(int intersectionSize) {
            this.intersection = new long[intersectionSize];
            this.intersectionPointer = 0;
        }

        public void accept(Long node) {
            intersection[intersectionPointer++] = node;
        }
    }
}
