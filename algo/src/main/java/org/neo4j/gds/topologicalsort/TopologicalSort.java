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
package org.neo4j.gds.topologicalsort;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.ArrayList;
import java.util.Collection;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

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
 */
public class TopologicalSort extends Algorithm<TopologicalSortResult> {
    // Contains the sorted nodes, which is the array we iterate on during the run
    private TopologicalSortResult result;
    // The in degree for each node in the graph. Being updated (down) as we cross out visited nodes
    private HugeAtomicLongArray inDegrees;
    private Graph graph;
    private final long nodeCount;
    private final ExecutorService executor;
    private final int numThreads;
    private final TopologicalSortQueue queue;

    protected TopologicalSort(
        Graph graph,
        TopologicalSortConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executor = executor;
        int concurrency = config.concurrency();
        numThreads = (nodeCount < concurrency) ? 1 : concurrency;
        result = new TopologicalSortResult(nodeCount);
        queue = new TopologicalSortQueue(nodeCount, numThreads);
        inDegrees = HugeAtomicLongArray.newArray(nodeCount);
    }

    @Override
    public TopologicalSortResult compute() {
        this.progressTracker.beginSubTask("TopologicalSort");
        initializeInDegrees();
        addFirstSources();
        performParallelSourcesSteps();

        this.progressTracker.endSubTask("TopologicalSort");
        return result;
    }

    @Override
    public void release() {}

    private void initializeInDegrees() {
        try (var concurrentCopy = CloseableThreadLocal.withInitial(() -> graph.concurrentCopy())) {
            ParallelUtil.parallelForEachNode(graph,
                numThreads,
                nodeId -> concurrentCopy.get().forEachRelationship(
                    nodeId,
                    (source, target) -> {
                        inDegrees.getAndAdd(target, 1L);
                        return true;
                    }
                )
            );
        }
    }

    private void addFirstSources() {
        ParallelUtil.parallelForEachNode(nodeCount, numThreads,
            nodeId -> {
            if (inDegrees.get(nodeId) == 0L) {
                queue.add(nodeId);
                result.addNode(nodeId);
            }
        });
    }

    private void performParallelSourcesSteps() {
        Collection<WorkerThread> tasks = new ArrayList<>(numThreads);
        for(int i = 0; i < numThreads; ++i) {
            WorkerThread t = new WorkerThread(i);
            tasks.add(t);
        }

        RunWithConcurrency.builder()
            .concurrency(numThreads)
            .tasks(tasks)
            .waitTime(1, MICROSECONDS)
            .terminationFlag(terminationFlag)
            .executor(executor)
            .run();
    }

    class WorkerThread implements Runnable {
        // The thread should identify itself with this id to fetch nodes from the right queue
        private final int threadId;
        private final RelationshipIterator iter;

        WorkerThread(int threadId) {
            this.threadId = threadId;
            iter = graph.concurrentCopy();
        }

        @Override
        public void run() {
            long sourceId = queue.peekBy(threadId);
            while(sourceId > -1L) {
                // Because of how the queue works, it's important to first do the work, only then pop
                performStep(iter, sourceId);
                queue.popBy(threadId);
                sourceId = queue.peekBy(threadId);
            }
        }
    }
    private void performStep(RelationshipIterator iter, long sourceId) {
        iter.forEachRelationship(sourceId,
            (source, target) -> {
                long prevDegree = inDegrees.getAndAdd(target, -1L);
                // if the previous degree was 1, this node is now a source
                if(prevDegree == 1L) {
                    queue.add(target);
                    result.addNode((target));
                }
                return true;
            });
    }
}
