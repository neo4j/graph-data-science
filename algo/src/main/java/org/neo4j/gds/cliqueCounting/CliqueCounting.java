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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipCursor;
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
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.lang.Long.max;

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
 *
 */

class CliqueCount extends ConcurrentHashMap<Integer, BigInteger> {
    long[] toLongArray(){ //starting at 3 (triangles)
         var maxSize = this.size() == 0 ? 0 : Collections.max(this.keySet());
        int newSize = (int)max(maxSize-2, 0);
        long[] array = new long[newSize];
        for (int i = 0; i < array.length; i++) {
            array[i] = this.getOrDefault(i+3, BigInteger.ZERO).longValueExact(); //throws if to big
        }
        return array;
    }
}

// either global
// or global + for all vertices
// or [ (1,2,5), (2,4), (6) ] #cliques where () is included.
// Experimentally setting a minimum value on k didn't speed it up much.

public final class CliqueCounting extends Algorithm<CliqueCountingResult> {
    private final Graph graph;
    private final ExecutorService executorService;
    private final Concurrency concurrency;
    private final AtomicLong rootQueue;

    private final CliqueCountingMode countingMode;
    private final HugeObjectArray<long[]> subcliques;

    //results
    private final CliqueCount globalCliqueCount; //integer is big enough since degree is int
    private final HugeObjectArray<CliqueCount> nodeCliqueCount;
    private final HugeObjectArray<CliqueCount> subcliqueCliqueCount;

    public CliqueCounting(
        Graph graph,
        CliqueCountingParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = parameters.concurrency();
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
        this.rootQueue = new AtomicLong(0L);
        this.countingMode = parameters.countingMode();
        switch (countingMode) {
            case GloballyOnly -> {
                this.globalCliqueCount = new CliqueCount();
                this.nodeCliqueCount = HugeObjectArray.of();
                this.subcliques = HugeObjectArray.of();
                this.subcliqueCliqueCount = HugeObjectArray.of();
            }
            case ForEveryNode -> {
                this.globalCliqueCount = new CliqueCount();
                this.nodeCliqueCount = HugeObjectArray.newArray(CliqueCount.class, graph.nodeCount());
                this.nodeCliqueCount.setAll(_x -> new CliqueCount());
                this.subcliques = HugeObjectArray.of();
                this.subcliqueCliqueCount = HugeObjectArray.of();
            }
            case ForEveryRelationship -> {
                throw new IllegalArgumentException("For every relationship is not supported yet");
            }
            case ForGivenSubcliques -> {
                this.globalCliqueCount = new CliqueCount();
                this.nodeCliqueCount = HugeObjectArray.of();
                this.subcliqueCliqueCount = HugeObjectArray.newArray(CliqueCount.class, parameters.subcliques().size());
                this.subcliqueCliqueCount.setAll(_x -> new CliqueCount());
                this.subcliques = HugeObjectArray.newArray(long[].class, parameters.subcliques().size());
                this.subcliques.setAll(i -> parameters.subcliques().get((int)i));
            }
            default -> throw new IllegalStateException("Unexpected value: " + countingMode);
        }
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

        var nodeCliqueCountArrays = HugeObjectArray.newArray(long[].class, nodeCliqueCount.size());
        nodeCliqueCountArrays.setAll(node -> nodeCliqueCount.get(node).toLongArray());

        var subcliqueCliqueCountArrays = HugeObjectArray.newArray(long[].class, subcliqueCliqueCount.size());
        subcliqueCliqueCountArrays.setAll(subclique_idx -> subcliqueCliqueCount.get(subclique_idx).toLongArray());

        return new CliqueCountingResult(globalCliqueCountArray, nodeCliqueCountArrays, subcliqueCliqueCountArrays);
    }

    private void recursiveSctCliqueCount(Graph graph, long[] subset, NodeStatus[] cliqueVertices, long rootSubcliqueIdx) {
        if (subset.length == 0) {
            updateCliqueCount(cliqueVertices, rootSubcliqueIdx);
            return;
        }

        SubsetPartition partition = partitionSubset(graph, subset);

        recursiveSctCliqueCount(
            graph,
            partition.nsp(),
            Stream.concat(Arrays.stream(cliqueVertices), Stream.of(new NodeStatus(partition.pivot(), false)))
                .toArray(NodeStatus[]::new),
            rootSubcliqueIdx
        );

        for (int i = 0; i < partition.vs().length; i++){
            var vi = partition.vs()[i];
            var vi_subset = neighborhoodIntersection(graph, subset, vi);
            recursiveSctCliqueCount(
                graph,
                difference(vi_subset, partition.vs(), i),
                Stream.concat(Arrays.stream(cliqueVertices), Stream.of(new NodeStatus(vi, true))).toArray(NodeStatus[]::new),
                rootSubcliqueIdx
            );
        }
    }

    private SubsetPartition partitionSubset(Graph graph, long[] subset) {
        long pivot = -1;
        int max_size = -1;
        for(long node: subset){
            if (graph.degree(node) > max_size){
                var size = neighborhoodIntersectionSize(graph, subset, node); //improve more? //or reuse these intersections for the next call
                if (size > max_size) {
                    pivot = node;
                    max_size = size;
                }
            }
        }

        long[] intersection = new long[max_size];
        long[] difference = new long[subset.length-max_size-1];
        int intersectionPointer = 0;
        int differencePointer = 0;
        var neighborIterator = graph.concurrentCopy().streamRelationships(pivot, -1).iterator();
        Optional<Long> nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
        outer:
        for (var subsetNode : subset) {
            inner:
            while (nextNeighbor.isPresent()) {
                switch (Long.compare(subsetNode, nextNeighbor.get())) {
                    case -1: //not found in neighborSet
                        break inner;
                    case 0: //found! //progress both
                        intersection[intersectionPointer++] = subsetNode;
                        nextNeighbor = neighborIterator.hasNext()
                            ? Optional.of(neighborIterator.next().targetId())
                            : Optional.empty();
                        continue outer;
                    case 1:
                        //not found yet, check next neighbor (progress neighbor iterator)
                        nextNeighbor = neighborIterator.hasNext()
                            ? Optional.of(neighborIterator.next().targetId())
                            : Optional.empty();
                }
            }
            if (subsetNode != pivot) {
                difference[differencePointer++] = subsetNode;
            }
        }
        return new SubsetPartition(intersection, difference, pivot);
    }

    //sorted intersection  S n N(v_i)
    private long[] neighborhoodIntersection(Graph graph, long[] subset, long vi) {
        var intersection = new long[Math.min(subset.length, graph.degree(vi))];
        var intersectionPointer = 0;
        var subsetPointer = 0;
        var neighborIterator = graph.streamRelationships(vi, -1).iterator();
        Optional<Long> nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();

        while (subsetPointer < subset.length && nextNeighbor.isPresent()) {
            switch (Long.compare(subset[subsetPointer], nextNeighbor.get())) {
                case -1:
                    subsetPointer++;
                    break;
                case 0:
                    intersection[intersectionPointer++] = subset[subsetPointer++];
                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
                    break;
                case 1:
                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
                    break;
            }
        }
        return Arrays.copyOf(intersection, intersectionPointer);
    }

    //size of intersection S n N(vi)
    private int neighborhoodIntersectionSize(Graph graph, long[] subset, long v) {
        var intersectionSize = 0;
        var subsetPointer = 0;
        var neighborIterator = graph.streamRelationships(v, -1).iterator();
        Optional<Long> nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();

        while (subsetPointer < subset.length && nextNeighbor.isPresent()) {
            switch (Long.compare(subset[subsetPointer], nextNeighbor.get())) {
                case -1:
                    subsetPointer++;
                    break;
                case 0:
                    intersectionSize++;
                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
                    break;
                case 1:
                    nextNeighbor = neighborIterator.hasNext() ? Optional.of(neighborIterator.next().targetId()) : Optional.empty();
                    break;
            }
        }
        return intersectionSize;
    }

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
                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes; numOptionalNodes++) {
                    var cliqueSize = numRequiredNodes + numOptionalNodes;
                    var cliqueCount = binomialCoefficient(maxNumOptionalNodes, numOptionalNodes);
                    globalCliqueCount.merge(cliqueSize, cliqueCount, BigInteger::add);
                }
            }
            case ForEveryNode -> {
                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes; numOptionalNodes++) {
                    var cliqueSize = numRequiredNodes + numOptionalNodes;
                    var cliqueCount = binomialCoefficient(maxNumOptionalNodes, numOptionalNodes);
                    globalCliqueCount.merge(cliqueSize, cliqueCount, BigInteger::add);
                    for (long node : requiredNodes) {
                        nodeCliqueCount.get(node).merge(cliqueSize, cliqueCount, BigInteger::add);
                    }
                }
                for (int numOptionalNodes = 0; numOptionalNodes <= maxNumOptionalNodes - 1; numOptionalNodes++) {
                    for (long node : optionalNodes) {
                        var cliqueSize = numRequiredNodes + 1 + numOptionalNodes;
                        var cliqueCount = binomialCoefficient(maxNumOptionalNodes - 1, numOptionalNodes);
                        nodeCliqueCount.get(node).merge(cliqueSize, cliqueCount, BigInteger::add);
                    }
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

    //binomial coefficient
    private static BigInteger binomialCoefficient(int n, int k) {
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

    private record SubsetPartition(long[] nsp, long[] vs, long pivot) { }

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
                recursiveSctCliqueCount(graph, positiveNeighborhood, cliqueVertices, -1);
                progressTracker.logProgress();
            }
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
                    intersectedNeighborhoods = neighborhoodIntersection(graph, intersectedNeighborhoods, node);
                }
                recursiveSctCliqueCount(graph, intersectedNeighborhoods, cliqueVertices, rootSubcliqueIdx);
                progressTracker.logProgress();
            }
        }
    }
}
