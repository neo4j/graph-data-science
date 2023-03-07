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
import java.util.concurrent.atomic.AtomicBoolean;
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

        AtomicBoolean negativeCycleFound = new AtomicBoolean();
        var tasks = new ArrayList<BellmanFordTask>();
        for (int i = 0; i < concurrency; ++i) {
            tasks.add(new BellmanFordTask(
                graph.concurrentCopy(),
                distances,
                frontier,
                frontierIndex,
                frontierSize,
                validBitset,
                negativeCycleFound
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
        return produceResult(negativeCycleFound.get(), distances);
    }

    private BellmanFordResult produceResult(boolean containsNegativeCycle, DistanceTracker distances) {
        Stream<PathResult> paths = (containsNegativeCycle) ? Stream.empty() : pathResults(
            distances,
            sourceNode,
            concurrency
        );
        return BellmanFordResult.of(containsNegativeCycle, new DijkstraResult(paths));
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
}
