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
package org.neo4j.gds.dag.longestPath;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.delta.TentativeDistances;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.delta.TentativeDistances.NO_PREDECESSOR;

/*
 * Longest Path algorithm implemented using topological sort
 */
public class DagLongestPath extends Algorithm<PathFindingResult> {
    private static final long[] EMPTY_ARRAY = new long[0];

    // The in degree for each node in the graph. Being updated (down) as we cross out visited nodes
    private final HugeAtomicLongArray inDegrees;
    private final Graph graph;
    private final long nodeCount;
    private final int concurrency;
    private final TentativeDistances parentsAndDistances;

    public DagLongestPath(
        Graph graph,
        ProgressTracker progressTracker,
        int concurrency
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.concurrency = concurrency;
        this.inDegrees = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(this.concurrency));
        this.parentsAndDistances = TentativeDistances.distanceAndPredecessors(nodeCount, concurrency, Double.MIN_VALUE, (a, b) -> Double.compare(a, b) < 0);
    }

    @Override
    public PathFindingResult compute() {
        this.progressTracker.beginSubTask("LongestPath");

        initializeInDegrees();
        traverse();

        return new PathFindingResult(pathResults(parentsAndDistances, concurrency), progressTracker::endSubTask);
    }

    private void initializeInDegrees() {
        this.progressTracker.beginSubTask("Initialization");
        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            terminationFlag,
            nodeId -> {
                graph.concurrentCopy().forEachRelationship(
                    nodeId,
                    (source, target) -> {
                        inDegrees.getAndAdd(target, 1L);
                        return true;
                    }
                );
                progressTracker.logProgress();
            }
        );
        this.progressTracker.endSubTask("Initialization");
    }

    private void traverse() {
        this.progressTracker.beginSubTask("Traversal");

        ForkJoinPool forkJoinPool = ExecutorServiceUtil.createForkJoinPool(concurrency);
        var tasks = ConcurrentHashMap.<ForkJoinTask<Void>>newKeySet();

        LongFunction<CountedCompleter<Void>> taskProducer =
            (nodeId) -> new LongestPathTask(
                null,
                nodeId,
                graph.concurrentCopy(),
                inDegrees,
                parentsAndDistances
            );

        ParallelUtil.parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, nodeId -> {
            if (inDegrees.get(nodeId) == 0L) {
                tasks.add(taskProducer.apply(nodeId));
                parentsAndDistances.set(nodeId, nodeId, 0);
            }
            // Might not reach 100% if there are cycles in the graph
            progressTracker.logProgress();
        });

        for (ForkJoinTask<Void> task : tasks) {
               forkJoinPool.submit(task);
        }

        // calling join makes sure the pool waits for all the tasks to complete before shutting down
        tasks.forEach(ForkJoinTask::join);
        forkJoinPool.shutdown();
        this.progressTracker.endSubTask("Traversal");
    }

    private static final class LongestPathTask extends CountedCompleter<Void> {
        private final long sourceId;
        private final Graph graph;
        private final HugeAtomicLongArray inDegrees;
        private final TentativeDistances parentsAndDistances;

        LongestPathTask(
            @Nullable DagLongestPath.LongestPathTask parent,
            long sourceId,
            Graph graph,
            HugeAtomicLongArray inDegrees,
            TentativeDistances parentsAndDistances
        ) {
            super(parent);
            this.sourceId = sourceId;
            this.graph = graph;
            this.inDegrees = inDegrees;
            this.parentsAndDistances = parentsAndDistances;
        }

        @Override
        public void compute() {
            graph.forEachRelationship(sourceId, 1.0, (source, target, weight) -> {

                longestPathTraverse(source, target, weight);

                long prevDegree = inDegrees.getAndAdd(target, -1);
                // if the previous degree was 1, this node is now a source
                if (prevDegree == 1) {
                    addToPendingCount(1);
                    LongestPathTask traversalTask = new LongestPathTask(
                        this,
                        target,
                        graph.concurrentCopy(),
                        inDegrees,
                        parentsAndDistances
                    );
                    traversalTask.fork();
                }
                return true;
            });

            propagateCompletion();
        }

        void longestPathTraverse(long source, long target, double weight) {
            // the source distance will never change anymore, but the target distance might
            var potentialDistance = parentsAndDistances.distance(source) + weight;
            var currentTargetDistance = parentsAndDistances.distance(target);
            while (Double.compare(potentialDistance, currentTargetDistance) > 0) {
                var witnessValue = parentsAndDistances.compareAndExchange(target, currentTargetDistance, potentialDistance, source);
                if (Double.compare(currentTargetDistance, witnessValue) == 0) {
                    break;
                }
                currentTargetDistance = parentsAndDistances.distance(target);
            }
        }
    }

    private static Stream<PathResult> pathResults(
        TentativeDistances tentativeDistances,
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

                var pathNodeIds = new LongArrayList();
                var costs = new DoubleArrayList();

                return LongStream
                    .range(partition.startNode(), partition.startNode() + partition.nodeCount())
                    .filter(target -> predecessors.get(target) != NO_PREDECESSOR)
                    .mapToObj(targetNode -> pathResult(
                        localPathIndex.getAndIncrement(),
                        targetNode,
                        distances,
                        predecessors,
                        pathNodeIds,
                        costs
                    ));
            })
        );
    }

    private static PathResult pathResult(
        long pathIndex,
        long targetNode,
        HugeAtomicDoubleArray distances,
        HugeAtomicLongArray predecessors,
        LongArrayList pathNodeIds,
        DoubleArrayList costs
    ) {
        // We backtrack until we reach the source node.
        var lastNode = targetNode;

        while (true) {
            pathNodeIds.add(lastNode);
            costs.add(distances.get(lastNode));

            // Break if we reach a source node
            if (lastNode == predecessors.get(lastNode)) {
                break;
            }

            lastNode = predecessors.get(lastNode);
        }

        var pathNodeIdsArray = pathNodeIds.toArray();
        ArrayUtils.reverse(pathNodeIdsArray);
        pathNodeIds.elementsCount = 0;
        var costsArray = costs.toArray();
        ArrayUtils.reverse(costsArray);
        costs.elementsCount = 0;

        return ImmutablePathResult.builder()
            .sourceNode(lastNode)
            .index(pathIndex)
            .targetNode(targetNode)
            .nodeIds(pathNodeIdsArray)
            .costs(costsArray)
            .relationshipIds(EMPTY_ARRAY)
            .build();
    }
}
