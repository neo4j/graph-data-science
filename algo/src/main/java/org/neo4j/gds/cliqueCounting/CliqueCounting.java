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

import java.math.BigInteger;
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
    private final HugeObjectArray<long[]> subcliques;

    //results
//    private final CliqueCount globalCliqueCount; //integer is big enough since degree is int
//    private final HugeObjectArray<CliqueCount> nodeCliqueCount;
    private final HugeObjectArray<CliqueCount> subcliqueCliqueCount;

    private final CliqueAdjacency cliqueAdjacency;

    private final ListCliqueCount globalCliqueCount;
    private final HugeObjectArray<ListCliqueCount> nodeCliqueCount;


//    private final NewCliqueCount newGlobalCliqueCount;
//    private final HugeObjectArray<NewCliqueCount> newNodeCliqueCount;
//    private final HugeObjectArray<NewCliqueCount> newSubcliqueCliqueCount;

    private CliqueCounting(
        Graph graph,
        CliqueCountingParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
//        CliqueCount globalCliqueCount,
//        HugeObjectArray<CliqueCount> nodeCliqueCount,
        ListCliqueCount globalCliqueCount,//new
        HugeObjectArray<ListCliqueCount> nodeCliqueCount, //new

        HugeObjectArray<long[]> subcliques,
        HugeObjectArray<CliqueCount> subcliqueCliqueCount,
        CliqueAdjacency cliqueAdjacency
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = parameters.concurrency();
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
        this.rootQueue = new AtomicLong(0L);
        this.countingMode = parameters.countingMode();
        this.globalCliqueCount = globalCliqueCount;
        this.nodeCliqueCount = nodeCliqueCount;
        this.subcliques = subcliques;
        this.subcliqueCliqueCount = subcliqueCliqueCount;
        this.cliqueAdjacency = cliqueAdjacency;
    }

    public static CliqueCounting create(
        Graph graph,
        CliqueCountingParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
//        CliqueCount globalCliqueCount;
//        HugeObjectArray<CliqueCount> nodeCliqueCount;

//        ListCliqueCount altGlobalCliqueCount = new ListCliqueCount(); //new
        ListCliqueCount globalCliqueCount = new ListCliqueCount(); //new
        HugeObjectArray<ListCliqueCount> nodeCliqueCount; //new
        HugeObjectArray<long[]> subcliques;
        HugeObjectArray<CliqueCount> subcliqueCliqueCount;
        switch (parameters.countingMode()) {
            case GloballyOnly -> {
//                globalCliqueCount = new CliqueCount();
                nodeCliqueCount = HugeObjectArray.of();
                subcliques = HugeObjectArray.of();
                subcliqueCliqueCount = HugeObjectArray.of();
            }
            case ForEveryNode -> {
//                globalCliqueCount = new CliqueCount();
//                nodeCliqueCount = HugeObjectArray.newArray(CliqueCount.class, graph.nodeCount());
//                nodeCliqueCount.setAll(_x -> new CliqueCount());
                nodeCliqueCount = HugeObjectArray.newArray(ListCliqueCount.class, graph.nodeCount());
                nodeCliqueCount.setAll(_x -> new ListCliqueCount());
                subcliques = HugeObjectArray.of();
                subcliqueCliqueCount = HugeObjectArray.of();
            }
            case ForEveryRelationship -> {
                throw new IllegalArgumentException("For every relationship is not supported yet");
            }
            case ForGivenSubcliques -> {
//                globalCliqueCount = new CliqueCount();
                nodeCliqueCount = HugeObjectArray.of();
                subcliqueCliqueCount = HugeObjectArray.newArray(CliqueCount.class, parameters.subcliques().size());
                subcliqueCliqueCount.setAll(_x -> new CliqueCount());
                subcliques = HugeObjectArray.newArray(long[].class, parameters.subcliques().size());
                subcliques.setAll(i -> parameters.subcliques().get((int) i));
            }
            default -> throw new IllegalStateException("Unexpected value: " + parameters.countingMode());
        }
        CliqueAdjacency cliqueAdjacency = CliqueAdjacencyFactory.createCliqueAdjacency(graph);

        return new CliqueCounting(
            graph,
            parameters,
            executorService,
            progressTracker,
            terminationFlag,
            globalCliqueCount,
            nodeCliqueCount,
            subcliques,
            subcliqueCliqueCount,
            cliqueAdjacency
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
                    () -> new GlobalCliqueCountingTask(graph.concurrentCopy())
                );
        ParallelUtil.run(tasks, executorService);
        progressTracker.endSubTask();

        var globalCliqueCountArray = globalCliqueCount.toLongArray();
//        var globalCliqueCountArray = altGlobalCliqueCount.toLongArray();

        var nodeCliqueCountArrays = HugeObjectArray.newArray(long[].class, nodeCliqueCount.size());
        nodeCliqueCountArrays.setAll(node -> nodeCliqueCount.get(node).toLongArray());

        var subcliqueCliqueCountArrays = HugeObjectArray.newArray(long[].class, subcliqueCliqueCount.size());
        subcliqueCliqueCountArrays.setAll(subclique_idx -> subcliqueCliqueCount.get(subclique_idx).toLongArray());

        return new CliqueCountingResult(globalCliqueCountArray, nodeCliqueCountArrays, subcliqueCliqueCountArrays);
    }



    private void recursiveSctCliqueCount(long[] subset, NodeStatus[] cliqueVertices, long rootSubcliqueIdx) {
        //TODO: Replace cliqueVertices with structure that's cheap to push to back on.
        if (subset.length == 0) {
//            updateCliqueCount(cliqueVertices, rootSubcliqueIdx);
            newUpdateCliqueCount(cliqueVertices, rootSubcliqueIdx);
            return;
        }

        int[] intersectionSizes = Arrays.stream(subset).mapToInt(node -> neighborhoodIntersectionSize(subset, node)).toArray();
        SubsetPartition partition = partitionSubset(subset, intersectionSizes);


        recursiveSctCliqueCount(
            partition.nsp(),
            Stream.concat(Arrays.stream(cliqueVertices), Stream.of(new NodeStatus(partition.pivot(), false)))
                .toArray(NodeStatus[]::new),
            rootSubcliqueIdx
        );

        var viSubsetSize = Arrays.stream(partition.vsIds()).map(viIdx -> intersectionSizes[viIdx]).toArray();
        var vs = Arrays.stream(partition.vsIds()).mapToLong(viIdx -> subset[viIdx]).toArray();
        for (int i = 0; i < vs.length; i++) {
            var NSvi = neighborhoodIntersection(subset, vs[i], viSubsetSize[i]);
//            NodeStatus[] newCliqueVertices = Arrays.copyOf(cliqueVertices, cliqueVertices.length+1);  //This is significantly slower 7.2s->8.5s
//            newCliqueVertices[cliqueVertices.length] = new NodeStatus(vs[i], true);
            recursiveSctCliqueCount(
                difference(NSvi, vs, i),
//                newCliqueVertices,
                 Stream.concat(Arrays.stream(cliqueVertices), Stream.of(new NodeStatus(vs[i], true))).toArray(NodeStatus[]::new),
                rootSubcliqueIdx
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

////    size of intersection S n N(vi)
//    public int neighborhoodIntersectionSize(long[] subset, long v) {
//        var intersectionSize = 0;
//        var subsetPointer = 0;
//
//        var neighborIterator = graph.streamRelationships(v, -1).iterator(); //concurrentCopy()?
//        Optional<Long> nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
//        while (subsetPointer < subset.length && nextNeighbor.isPresent()) {
//            switch (Long.compare(subset[subsetPointer], nextNeighbor.get())) {
//                case -1:
//                    subsetPointer++;
//                    break;
//                case 0:
//                    intersectionSize++;
//                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
//                    break;
//                case 1:
//                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
//                    break;
//            }
//        }
//        return intersectionSize;
//    }


//    sorted intersection  S n N(v_i)
    private long[] neighborhoodIntersection(long[] subset, long node, int intersectionSize) { //way slower than previous. 7.2s vs 3.4s
        var intersectionConsumer = new IntersectionConsumer(intersectionSize);
        neighborhoodIntersection(subset, node, intersectionConsumer);
        return intersectionConsumer.intersection;
    }

    //sorted intersection  S n N(v_i)
//    private long[] neighborhoodIntersection(long[] subset, long vi, int intersectionSize) {
//        var intersection = new long[Math.min(subset.length, graph.degree(vi))];
////        var intersection = new long[intersectionSize]; //no noticeable gain
//        var intersectionPointer = 0;
//        var subsetPointer = 0;
//        var neighborIterator = graph.streamRelationships(vi, -1).iterator();
//        Optional<Long> nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
//        while (subsetPointer < subset.length && nextNeighbor.isPresent()) {
//            switch (Long.compare(subset[subsetPointer], nextNeighbor.get())) {
//                case -1:
//                    subsetPointer++;
//                    break;
//                case 0:
//                    intersection[intersectionPointer++] = subset[subsetPointer++];
//                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
//                    break;
//                case 1:
//                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
//                    break;
//            }
//        }
//        return Arrays.copyOf(intersection, intersectionPointer);
////        return intersection;
//    }

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

    // S \ (v_1,...,v_{i-1})
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

    private void newUpdateCliqueCount(NodeStatus[] cliqueNodes, long rootSubcliqueIdx) {
        long[] requiredNodes = Arrays.stream(cliqueNodes)
            .filter(NodeStatus::required)
            .mapToLong(NodeStatus::nodeId)
            .toArray();
        long[] optionalNodes = Arrays.stream(cliqueNodes)
            .filter(v -> !v.required())
            .mapToLong(NodeStatus::nodeId)
            .toArray();

        switch (countingMode) {
            case GloballyOnly -> globalCliqueCount.add(requiredNodes.length, optionalNodes.length);
            case ForEveryNode -> {
                //fixme, reuse binomial computations even better.
                globalCliqueCount.add(requiredNodes.length, optionalNodes.length);

                var requiredNodesCliqueCount = Arrays.stream(requiredNodes).mapToObj(nodeCliqueCount::get).toList();
                ListCliqueCount.add(requiredNodes.length, optionalNodes.length, requiredNodesCliqueCount);
                if (optionalNodes.length > 0) {
                    var optionalNodesCliqueCount = Arrays.stream(optionalNodes).mapToObj(nodeCliqueCount::get).toList();
                    ListCliqueCount.add(requiredNodes.length+1, optionalNodes.length-1, optionalNodesCliqueCount);
                }
            }
            case ForGivenSubcliques -> {
                for (int numOptionalNodes = 0; numOptionalNodes <= optionalNodes.length; numOptionalNodes++) {
                    var cliqueSize = requiredNodes.length + numOptionalNodes;
                    var cliqueCount = binomialCoefficient(optionalNodes.length, numOptionalNodes);
                    subcliqueCliqueCount.get(rootSubcliqueIdx).merge(cliqueSize, cliqueCount, BigInteger::add);
                }
//                newSubcliqueCliqueCount.get(rootSubcliqueIdx).add(requiredNodes.length, optionalNodes.length);
            }
        }
    }

    private void updateCliqueCount(NodeStatus[] cliqueNodes, long rootSubcliqueIdx) {
        long[] requiredNodes = Arrays.stream(cliqueNodes)
            .filter(NodeStatus::required)
            .mapToLong(NodeStatus::nodeId)
            .toArray();
        long[] optionalNodes = Arrays.stream(cliqueNodes)
            .filter(v -> !v.required())
            .mapToLong(NodeStatus::nodeId)
            .toArray();
        var numRequiredNodes = requiredNodes.length;
        var maxNumOptionalNodes = optionalNodes.length;

        //Todo: Ignore size < 3
        switch (countingMode) {
            case GloballyOnly -> {
                globalCliqueCount.add(numRequiredNodes, maxNumOptionalNodes); //new
//                altGlobalCliqueCount.add(numRequiredNodes, maxNumOptionalNodes);
//                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes; numOptionalNodes++) {
//                    var cliqueSize = numRequiredNodes + numOptionalNodes;
//                    var cliqueCount = binomialCoefficient(maxNumOptionalNodes, numOptionalNodes);
//                    globalCliqueCount.merge(cliqueSize, cliqueCount, BigInteger::add);
//                }
            }
            case ForEveryNode -> {
                globalCliqueCount.add(numRequiredNodes, maxNumOptionalNodes); //new
//                altGlobalCliqueCount.add(numRequiredNodes, maxNumOptionalNodes);
//                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes; numOptionalNodes++) {
//                    var cliqueSize = numRequiredNodes + numOptionalNodes;
//                    var cliqueCount = binomialCoefficient(maxNumOptionalNodes, numOptionalNodes);
//                    globalCliqueCount.merge(cliqueSize, cliqueCount, BigInteger::add);
//                    for (long node : requiredNodes) {
//                        nodeCliqueCount.get(node).merge(cliqueSize, cliqueCount, BigInteger::add);
//                    }
//                }
                for (long node : requiredNodes) {
                    nodeCliqueCount.get(node).add(numRequiredNodes, maxNumOptionalNodes);
                }
//                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes - 1; numOptionalNodes++) {
//                    for (long node : optionalNodes) {
//                        var cliqueSize = numRequiredNodes + 1 + numOptionalNodes;
//                        var cliqueCount = binomialCoefficient(maxNumOptionalNodes - 1, numOptionalNodes);
//                        nodeCliqueCount.get(node).merge(cliqueSize, cliqueCount, BigInteger::add);
//                    }
//                }
                for (long node : optionalNodes) {
                    nodeCliqueCount.get(node).add(numRequiredNodes+1, maxNumOptionalNodes-1);
                }
            }
            case ForGivenSubcliques -> {
                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes; numOptionalNodes++) {
                    var cliqueSize = numRequiredNodes + numOptionalNodes;
                    var cliqueCount = binomialCoefficient(maxNumOptionalNodes, numOptionalNodes);
                    subcliqueCliqueCount.get(rootSubcliqueIdx).merge(cliqueSize, cliqueCount, BigInteger::add);
                }
            }
        }
    }

    private static BigInteger binomialCoefficient(int n, int k) {
        //TODO: Calculate multiple at the same time. Always need j for 3<=j<=k
        var k_ = Math.min(k, n - k);
        BigInteger numerator = BigInteger.ONE;
        BigInteger denominator = BigInteger.ONE;
        for (var i = 0; i < k_; i++) {
            numerator = numerator.multiply(BigInteger.valueOf(n - i));
            denominator = denominator.multiply(BigInteger.valueOf(i+1));
        }
        return numerator.divide(denominator);
    }

    private record NodeStatus(long nodeId, boolean required) { }

    private record SubsetPartition(long[] nsp, int[] vsIds, long pivot) { }

    private class GlobalCliqueCountingTask implements Runnable {
        Graph graph;
        GlobalCliqueCountingTask(Graph graph) {
            this.graph = graph;
        }

        @Override
        public void run() {
            long rootNode;
            while ((rootNode = rootQueue.getAndIncrement()) < graph.nodeCount() && terminationFlag.running()) {
                NodeStatus[] cliqueVertices = {new NodeStatus(rootNode, true)};
                long[] positiveNeighborhood = graph
                    .streamRelationships(rootNode, -1)
                    .filter(r -> r.targetId() > r.sourceId())
                    .mapToLong(RelationshipCursor::targetId)
                    .toArray();
                recursiveSctCliqueCount(positiveNeighborhood, cliqueVertices, -1);
                progressTracker.logProgress();
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

    private class SubcliqueCliqueCountingTask implements Runnable {
        Graph graph;
        SubcliqueCliqueCountingTask(Graph graph) {
            this.graph = graph;
        }

        @Override
        public void run() {
            long rootSubcliqueIdx;
            while ((rootSubcliqueIdx = rootQueue.getAndIncrement()) < subcliques.size() && terminationFlag.running()) {
                long[] subclique = subcliques.get(rootSubcliqueIdx);
                NodeStatus[] cliqueVertices = Arrays.stream(subclique).mapToObj(v -> new NodeStatus(v, true)).toArray(NodeStatus[]::new);
                long[] intersectedNeighborhoods = graph.streamRelationships(subclique[0], -1).mapToLong(RelationshipCursor::targetId).toArray();
                for (long node : subclique) {
                    var intersectionSize = neighborhoodIntersectionSize(intersectedNeighborhoods, node);
                    intersectedNeighborhoods = neighborhoodIntersection(intersectedNeighborhoods, node, intersectionSize);
                }
                recursiveSctCliqueCount(intersectedNeighborhoods, cliqueVertices, rootSubcliqueIdx);
                progressTracker.logProgress();
            }
        }
    }
}
