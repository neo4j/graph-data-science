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
package org.neo4j.gds.dag.topologicalsort;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.LongFunction;

/*
 * Topological sort algorithm.
 *
 * Topological sort is not defined for graphs with cycles. If the graph contains cycles this implementation will ignore
 * all the nodes that are part of the cycle, and also all the nodes that are reachable from a cycle.
 *
 * For example for this graph:
 *  (A)-->(B)<-->(C)-->(D)
 * Only A will be returned by the topological sort because it is the only node that is not part of a cycle or reachable
 * from a cycle.
 *
 * This algorithm is also capable of calculating the longest path for each node (unless it is ignored during sorting).
 */
public class TopologicalSort extends Algorithm<TopologicalSortResult> {
    // Contains the sorted nodes, which is the array we iterate on during the run
    private final TopologicalSortResult result;
    // The in degree for each node in the graph. Being updated (down) as we cross out visited nodes
    private final HugeAtomicLongArray inDegrees;
    private final Graph graph;
    private final long nodeCount;
    private final int concurrency;

    // Saves the maximal distance from a source node, which is the longest path in DAG
    private final Optional<HugeAtomicDoubleArray> longestPathDistances;

    protected TopologicalSort(
        Graph graph,
        TopologicalSortBaseConfig config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.concurrency = config.concurrency();
        this.inDegrees = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(this.concurrency));
        this.longestPathDistances = config.computeLongestPathDistances()
            ? Optional.of(HugeAtomicDoubleArray.of(nodeCount, ParallelDoublePageCreator.passThrough(this.concurrency)))
            : Optional.empty();
        this.result = new TopologicalSortResult(nodeCount, longestPathDistances);
    }

    @Override
    public TopologicalSortResult compute() {
        this.progressTracker.beginSubTask("TopologicalSort");

        initializeInDegrees();
        traverse();

        this.progressTracker.endSubTask("TopologicalSort");
        return result;
    }

    private void initializeInDegrees() {
        this.progressTracker.beginSubTask("Initialization");
        try (var concurrentCopy = CloseableThreadLocal.withInitial(graph::concurrentCopy)) {
            ParallelUtil.parallelForEachNode(
                graph.nodeCount(),
                concurrency,
                terminationFlag,
                nodeId -> {
                    concurrentCopy.get().forEachRelationship(
                            nodeId,
                            (source, target) -> {
                                inDegrees.getAndAdd(target, 1L);
                                return true;
                            }
                        );
                    progressTracker.logProgress();
                }
            );
        }
        this.progressTracker.endSubTask("Initialization");
    }

    private void traverse() {
        this.progressTracker.beginSubTask("Traversal");

        ForkJoinPool forkJoinPool = Pools.createForkJoinPool(concurrency);
        var tasks = ConcurrentHashMap.<ForkJoinTask<Void>>newKeySet();

        LongFunction<CountedCompleter> taskProducer = longestPathDistances.isPresent()
            ? (nodeId) -> new LongestPathTask(
            null,
            nodeId,
            graph.concurrentCopy(),
            result,
            inDegrees,
            longestPathDistances.get()
        )
            : (nodeId) -> new TraversalTask(
                null,
                nodeId,
                graph.concurrentCopy(),
                result,
                inDegrees
            );

        ParallelUtil.parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, nodeId -> {
            if (inDegrees.get(nodeId) == 0L) {
                result.addNode(nodeId);
                tasks.add(taskProducer.apply(nodeId));
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

    private static final class TraversalTask extends CountedCompleter<Void> {
        private final long sourceId;
        private final Graph graph;
        private final TopologicalSortResult result;
        private final HugeAtomicLongArray inDegrees;

        TraversalTask(
            @Nullable TraversalTask parent,
            long sourceId,
            Graph graph,
            TopologicalSortResult result,
            HugeAtomicLongArray inDegrees
        ) {
            super(parent);
            this.sourceId = sourceId;
            this.graph = graph;
            this.result = result;
            this.inDegrees = inDegrees;
        }

        @Override
        public void compute() {
            graph.forEachRelationship(sourceId, 1.0, (source, target, weight) -> {

                long prevDegree = inDegrees.getAndAdd(target, -1);
                // if the previous degree was 1, this node is now a source
                if (prevDegree == 1) {
                    result.addNode(target);
                    addToPendingCount(1);
                    TraversalTask traversalTask = new TraversalTask(
                        this,
                        target,
                        graph.concurrentCopy(),
                        result,
                        inDegrees
                    );
                    traversalTask.fork();
                }
                return true;
            });

            propagateCompletion();
        }
    }


    private static final class LongestPathTask extends CountedCompleter<Void> {
        private final long sourceId;
        private final Graph graph;
        private final TopologicalSortResult result;
        private final HugeAtomicLongArray inDegrees;
        private final HugeAtomicDoubleArray longestPathDistances;

        LongestPathTask(
            @Nullable TopologicalSort.LongestPathTask parent,
            long sourceId,
            Graph graph,
            TopologicalSortResult result,
            HugeAtomicLongArray inDegrees,
            HugeAtomicDoubleArray longestPathDistances
        ) {
            super(parent);
            this.sourceId = sourceId;
            this.graph = graph;
            this.result = result;
            this.inDegrees = inDegrees;
            this.longestPathDistances = longestPathDistances;
        }

        @Override
        public void compute() {
            graph.forEachRelationship(sourceId, 1.0, (source, target, weight) -> {

                longestPathTraverse(source, target, weight);

                long prevDegree = inDegrees.getAndAdd(target, -1);
                // if the previous degree was 1, this node is now a source
                if (prevDegree == 1) {
                    result.addNode(target);
                    addToPendingCount(1);
                    LongestPathTask traversalTask = new LongestPathTask(
                        this,
                        target,
                        graph.concurrentCopy(),
                        result,
                        inDegrees,
                        longestPathDistances
                    );
                    traversalTask.fork();
                }
                return true;
            });

            propagateCompletion();
        }

        void longestPathTraverse(long source, long target, double weight) {
            // the source distance will never change anymore, but the target distance might
            var potentialDistance = longestPathDistances.get(source) + weight;
            var currentTargetDistance = longestPathDistances.get(target);
            while(potentialDistance > currentTargetDistance) {
                var witnessValue = longestPathDistances.compareAndExchange(target, currentTargetDistance, potentialDistance);
                if(currentTargetDistance == witnessValue) {
                    break;
                }
                currentTargetDistance = witnessValue;
            }
        }
    }
}
