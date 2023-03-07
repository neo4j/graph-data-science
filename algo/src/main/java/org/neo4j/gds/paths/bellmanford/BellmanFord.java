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
package org.neo4j.gds.paths.bellmanford;

import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.delta.TentativeDistances.NO_PREDECESSOR;

public class BellmanFord extends Algorithm<BellmanFordResult> {
    public static final String DESCRIPTION =
        "The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node and any other node in the graph without negative cycles.";

    private final long sourceNode;
    private final Graph graph;
    private final int concurrency;

    public BellmanFord(Graph graph, ProgressTracker progressTracker, long sourceNode, int concurrency) {
        super(progressTracker);
        this.graph = graph;
        this.sourceNode = sourceNode;
        this.concurrency = concurrency;
    }

    @Override
    public BellmanFordResult compute() {
        progressTracker.beginSubTask();
        var frontier = HugeLongArray.newArray(graph.nodeCount());
        var frontierIndex = new AtomicLong();
        var frontierSize = new AtomicLong();

        var validBitset = HugeAtomicBitSet.create(graph.nodeCount());
        var distances = DistanceTracker.create(
            graph.nodeCount(),
            concurrency
        );
        var negativeCyclesVertices = HugeLongArray.newArray(graph.nodeCount());
        var negativeCyclesIndex = new AtomicLong();
        var tasks = new ArrayList<BellmanFordTask>();
        for (int i = 0; i < concurrency; ++i) {
            tasks.add(new BellmanFordTask(
                graph.concurrentCopy(),
                distances,
                frontier,
                frontierIndex,
                frontierSize,
                validBitset,
                negativeCyclesVertices,
                negativeCyclesIndex
            ));
        }

        frontier.set(0, sourceNode);
        frontierSize.incrementAndGet();
        distances.set(sourceNode, -1, 0, 1);
        validBitset.set(sourceNode);

        while (frontierSize.get() > 0) {
            progressTracker.beginSubTask();
            frontierIndex.set(0); // exhaust global queue
            RunWithConcurrency.builder()
                .tasks(tasks)
                .concurrency(concurrency)
                .run();
            progressTracker.endSubTask();
            progressTracker.beginSubTask();
            frontierSize.set(0); // fill global queue again
            RunWithConcurrency.builder()
                .tasks(tasks)
                .concurrency(concurrency)
                .run();
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();
        boolean containsNegativeCycle = negativeCyclesIndex.get() > 0;
        return produceResult(containsNegativeCycle, negativeCyclesVertices, negativeCyclesIndex, distances);
    }

    private BellmanFordResult produceResult(
        boolean containsNegativeCycle,
        HugeLongArray negativeCyclesVertices,
        AtomicLong negativeCyclesIndex,
        DistanceTracker distanceTracker
    ) {
        Stream<PathResult> paths = (containsNegativeCycle) ?
            Stream.empty()
            : pathResults(
                distanceTracker,
                sourceNode,
                concurrency
            );

        var negativeCycles =
            negativeCyclesResults(
                graph,
                distanceTracker,
                negativeCyclesIndex.longValue(),
                negativeCyclesVertices,
                graph.nodeCount(),
                concurrency
            );

        return BellmanFordResult.of(
            containsNegativeCycle,
            new DijkstraResult(paths),
            new DijkstraResult(negativeCycles)
        );
    }

    private static Stream<PathResult> negativeCyclesResults(
        Graph graph,
        DistanceTracker tentativeDistances,
        long numberOfNegativeCycles,
        HugeLongArray negativeCycleVertices,
        long nodeCount,
        int concurrency
    ) {

        AtomicLong cycleIndex = new AtomicLong();
        var predecessors = tentativeDistances.predecessors().orElseThrow();
        var distances = tentativeDistances.distances();

        var partitions = PartitionUtils.rangePartition(
            concurrency,
            numberOfNegativeCycles,
            partition -> partition,
            Optional.of(1 + (int) numberOfNegativeCycles / concurrency)
        );

        return ParallelUtil.parallelStream(
            partitions.stream(),
            concurrency,
            parallelStream -> parallelStream.flatMap(partition -> {
                var pathResultBuilder = ImmutablePathResult.builder();

                return LongStream
                    .range(partition.startNode(), partition.startNode() + partition.nodeCount())
                    .filter(target -> predecessors.get(target) != NO_PREDECESSOR)
                    .mapToObj(indexId -> negativeCycleResult(
                        pathResultBuilder,
                        cycleIndex,
                        negativeCycleVertices.get(indexId),
                        distances,
                        predecessors,
                        graph.concurrentCopy(),
                        nodeCount

                    )).filter(cycle -> cycle != PathResult.EMPTY);
            })
        );

    }


    private static Stream<PathResult> pathResults(
        DistanceTracker tentativeDistances,
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

    private static PathResult negativeCycleResult(
        ImmutablePathResult.Builder pathResultBuilder,
        AtomicLong cycleIndex,
        long startNode,
        HugeAtomicDoubleArray distances,
        HugeAtomicLongArray predecessors,
        Graph localGraph,
        long nodeCount
    ) {
        var pathNodeIds = new LongArrayDeque();
        double totalCost = 0;
        long nodeId = startNode;// we have a vertex within a cycle
        pathNodeIds.addFirst(nodeId);
        long curr = predecessors.get(nodeId);
        long length = 0;
        boolean shouldAdd = true;
        while (curr != nodeId) {
            pathNodeIds.addFirst(curr);
            long predecessor = predecessors.get(curr);
            length++;
            totalCost += distances.get(curr) - distances.get(predecessor); //this is not correct
            curr = predecessors.get(curr);

            if (length == nodeCount + 1) {
                shouldAdd = false;
                break;
            }
        }
        if (shouldAdd) {

            pathNodeIds.addFirst(startNode);
            var pathArray = pathNodeIds.toArray();
            int pathLength = pathArray.length;
            var costs = new double[pathLength];

            long endNode = pathArray[1];
            MutableDouble minimum = new MutableDouble(Double.MAX_VALUE);
            localGraph.forEachRelationship(startNode, 1.0, (s, t, w) -> {
                if (t == endNode) {
                    if (minimum.doubleValue() > w) {
                        minimum.setValue(w);
                    }
                }
                return true;
            });
            costs[1] = minimum.doubleValue();
            for (int j = 2; j < pathLength; ++j) {
                long node = pathArray[j];
                long previous = pathArray[j - 1];
                double currentDist = distances.get(node) - distances.get(previous);
                costs[j] = costs[j - 1] + currentDist;
            }

            return pathResultBuilder.
                index(cycleIndex.getAndIncrement())
                .sourceNode(-1)
                .targetNode(-1)
                .nodeIds(pathArray)
                .relationshipIds(EMPTY_ARRAY)
                .costs(costs).build();
        }

        return PathResult.EMPTY;

    }

}
