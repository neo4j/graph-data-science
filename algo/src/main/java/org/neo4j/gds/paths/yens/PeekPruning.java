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
package org.neo4j.gds.paths.yens;

import com.carrotsearch.hppc.BitSet;
import org.eclipse.collections.api.block.function.primitive.LongToBooleanFunction;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.collections.haa.ValueTransformers;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.paged.HugeSerialObjectMergeSort;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DistanceAndPredecessors;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.stream.DoubleStream;

public final class PeekPruning {
    private PeekPruning() {}

    public record DoubleIndexPair(double value, long index) {}

    public record FoundPathCosts(long count, HugeObjectArray<DoubleIndexPair> paths) {}

    public record PathsAndReachability(
        BitSet reachable,
        DistanceAndPredecessors forward,
        DistanceAndPredecessors backward) {}

    public static FoundPathCosts sortedCombinedPathCosts(
        long nodeCount,
        long reachableCount,
        LongToBooleanFunction reachable,
        LongToDoubleFunction forwardCost,
        LongToDoubleFunction backwardCost
    ) {
        var result = HugeObjectArray.newArray(DoubleIndexPair.class, reachableCount);
        long next = 0;
        for (long n = 0; n < nodeCount; n++) {
            if (reachable.valueOf(n)) {
                double cost = forwardCost.applyAsDouble(n) + backwardCost.applyAsDouble(n);
                result.set(next, new DoubleIndexPair(cost, n));
                next++;
            }
        }
        sortPathCosts(result, reachableCount);
        return new FoundPathCosts(reachableCount, result);
    }

    private static void sortPathCosts(HugeObjectArray<DoubleIndexPair> pathCosts, long size) {
        HugeSerialObjectMergeSort.sort(DoubleIndexPair.class, pathCosts, size, DoubleIndexPair::value);
    }

    public static PathsAndReachability pathsAndReachability(
        Graph graph,
        long sourceNode,
        long targetNode,
        BiFunction<Graph, Long, DistanceAndPredecessors> deltaStep
    ) {
        long nodeCount = graph.nodeCount();
        Graph reverseGraph = graph.characteristics().isUndirected() ? graph : new YensReversedGraph(graph);
        var forwardPaths = deltaStep.apply(graph, sourceNode);
        var backwardPaths = deltaStep.apply(reverseGraph, targetNode);
        var reachable = new BitSet(nodeCount);
        reachable.set(0, nodeCount);
        clearUnreachable(nodeCount, reachable, forwardPaths::predecessor);
        clearUnreachable(nodeCount, reachable, backwardPaths::predecessor);
        return new PathsAndReachability(reachable, forwardPaths, backwardPaths);
    }

    private static void clearUnreachable(
        long nodeCount,
        BitSet reachable,
        ValueTransformers.LongToLongFunction predecessors
    ) {
        for (long n = 0; n < nodeCount; n++) {
            long predecessor = predecessors.apply(n);
            if (predecessor == DistanceAndPredecessors.NO_PREDECESSOR) {
                reachable.clear(n);
            }
        }
    }

    public static BitSet nodeFilter(ProgressTracker progressTracker, long nodeCount, FoundPathCosts found, double cutoff) {
        BitSet nodeIncluded = new BitSet(nodeCount);
        for (long n = 0; n < found.count; n++) {
            progressTracker.logProgress(1);
            var cd = found.paths.get(n);
            if (cd.value <= cutoff) {
                nodeIncluded.set(cd.index);
            }
        }
        return nodeIncluded;
    }

    private static void forEachNodeInPath(
        Consumer<Long> func,
        ValueTransformers.LongToLongFunction predecessor,
        long source,
        long target
    ) {
        long node = target;
        while (true) {
            func.accept(node);
            if (node == source) {
                break;
            }
            node = predecessor.apply(node);
        }
    }

    public static double[] validPathCosts(
        int k,
        long nodeCount,
        long source,
        long target,
        FoundPathCosts found,
        ValueTransformers.LongToLongFunction forwardPredecessor,
        ValueTransformers.LongToLongFunction backwardPredecessor
    ) {
        var result = DoubleStream.builder();

        BitSet partOfValidPath = new BitSet(nodeCount);
        BitSet visited = new BitSet(nodeCount);

        long added = 0;
        long j = 0;
        while (added < k && j < found.count) {
            var costIndex = found.paths.get(j);
            var node = costIndex.index;
            if (!partOfValidPath.get(node)) {
                visited.clear();
                AtomicBoolean falsePath = new AtomicBoolean(false);
                Consumer<Long> visitNodes = (id) -> {
                    if (visited.get(id)) {
                        falsePath.set(true);
                    }
                    visited.set(id);
                };
                forEachNodeInPath(visitNodes, forwardPredecessor, source, node);
                if (node != target) {
                    // we've already visited the first node, so skip it
                    forEachNodeInPath(visitNodes, backwardPredecessor, target, backwardPredecessor.apply(node));
                }
                if (!falsePath.get()) {
                    result.add(costIndex.value);
                    added++;
                    forEachNodeInPath(partOfValidPath::set, forwardPredecessor, source, node);
                    forEachNodeInPath(partOfValidPath::set, backwardPredecessor, target, node);
                }
            }
            j++;
        }
        return result.build().toArray();
    }

    public static Graph createPrunedGraph(
        Graph graph,
        long sourceNode,
        double cutoff,
        LongToDoubleFunction sourceCost,
        LongToDoubleFunction targetCost,
        LongPredicate nodeIncluded,
        Concurrency concurrency,
        ProgressTracker progressTracker
    ) {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(graph.nodeCount())
            .concurrency(concurrency)
            .build();

        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            nodeId -> {
                if (nodeIncluded.test(nodeId)) {
                    nodesBuilder.addNode(nodeId);
                }
            }
        );

        IdMap idMap = nodesBuilder.build().idMap();
        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(RelationshipType.of("_IGNORED_"))
            .addPropertyConfig(GraphFactory.PropertyConfig.builder()
                .propertyKey("property")
                .build())
            .executorService(DefaultPool.INSTANCE)
            .build();

        var relationshipCreators = PartitionUtils.degreePartition(
            graph,
            concurrency,
            partition ->
                new RelationshipPruningTask(
                    graph.concurrentCopy(),
                    sourceNode,
                    sourceCost,
                    targetCost,
                    nodeIncluded,
                    cutoff,
                    partition,
                    relationshipsBuilder,
                    progressTracker
                ),
            Optional.empty()
        );

        ParallelUtil.run(relationshipCreators, DefaultPool.INSTANCE);

        return GraphFactory.create(idMap, relationshipsBuilder.build());
    }

    public static PathFindingResult mapToOriginalGraph(Graph pruned, PathFindingResult paths) {
        return new PathFindingResult(paths.mapPaths(path ->
            ImmutablePathResult.of(
                path.index(),
                pruned.toOriginalNodeId(path.sourceNode()),
                pruned.toOriginalNodeId(path.targetNode()),
                Arrays.stream(path.nodeIds()).map(pruned::toOriginalNodeId).toArray(),
                new long[0],
                path.costs()
            )));
    }

    public static BiFunction<Graph, Long, DistanceAndPredecessors> deltaStep(
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        double delta = 2.0;
        return (graph, sourceNode) ->
            new DeltaStepping(
                graph,
                sourceNode,
                delta,
                concurrency,
                executorService,
                progressTracker,
                terminationFlag
            ).compute().tentativeDistances();
    }
}
