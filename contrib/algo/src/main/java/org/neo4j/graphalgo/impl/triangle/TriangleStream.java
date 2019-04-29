/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.triangle;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node. Instead of
 * emitting the nodeId and the number of triangles the node is part of,
 * this impl. streams the actual nodeIds of each triangle once.
 *
 * @author mknblch
 */
public class TriangleStream extends Algorithm<TriangleStream> {

    public static final Direction D = Direction.OUTGOING;
    private Graph graph;
    private ExecutorService executorService;
    private final AtomicInteger queue;
    private final int concurrency;
    private final int nodeCount;
    private AtomicInteger visitedNodes;
    private AtomicInteger runningThreads;
    private BlockingQueue<Result> resultQueue;

    public TriangleStream(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.resultQueue = new ArrayBlockingQueue<>(concurrency << 10);
        runningThreads = new AtomicInteger();
        visitedNodes = new AtomicInteger();
        queue = new AtomicInteger();
    }

    @Override
    public TriangleStream me() {
        return this;
    }

    /**
     * release inner data structures
     * @return
     */
    @Override
    public TriangleStream release() {
        visitedNodes = null;
        runningThreads = null;
        resultQueue = null;
        graph = null;
        executorService = null;
        return this;
    }

    /**
     * return result stream of triangle triples
     * consisting of its 3 nodeIds
     * @return
     */
    public Stream<Result> resultStream() {
        submitTasks();
        final TerminationFlag flag = getTerminationFlag();
        final Iterator<Result> it = new Iterator<Result>() {

            @Override
            public boolean hasNext() {
                return flag.running() && (runningThreads.get() > 0 || !resultQueue.isEmpty());
            }

            @Override
            public Result next() {
                Result result = null;
                while (hasNext() && result == null) {
                    result = resultQueue.poll();
                }
                return result;
            }
        };

        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(it, 0), false)
                .filter(Objects::nonNull);
    }

    private void submitTasks() {
        queue.set(0);
        runningThreads.set(0);
        final Collection<Runnable> tasks;
        if (graph instanceof HugeGraph || graph instanceof HeavyGraph) {
            tasks = ParallelUtil.tasks(concurrency, () -> new IntersectTask(graph));
        } else {
            tasks = ParallelUtil.tasks(concurrency, Task::new);
        }
        ParallelUtil.run(tasks, false, executorService, null);
    }

    private abstract class BaseTask implements Runnable {

        BaseTask() {
            runningThreads.incrementAndGet();
        }

        @Override
        public final void run() {
            try {
                ProgressLogger progressLogger = getProgressLogger();
                int node;
                while ((node = queue.getAndIncrement()) < nodeCount && running()) {
                    evaluateNode(node);
                    progressLogger.logProgress(visitedNodes.incrementAndGet(), nodeCount);
                }
            } finally {
                runningThreads.decrementAndGet();
            }
        }

        abstract void evaluateNode(int nodeId);

        void emit(int nodeA, int nodeB, int nodeC) {
            Result result = new Result(
                    graph.toOriginalNodeId(nodeA),
                    graph.toOriginalNodeId(nodeB),
                    graph.toOriginalNodeId(nodeC));
            resultQueue.offer(result);
        }
    }

    private final class Task extends BaseTask {

        private final Graph graph;
        private IntStack nodes;

        private Task() {
            this.graph = TriangleStream.this.graph;
            nodes = new IntStack();
        }

        @Override
        void evaluateNode(final int nodeId) {
            IntStack nodes = this.nodes;
            graph.forEachRelationship(nodeId, D, (s, t, r) -> {
                if (t > s) {
                    nodes.add(t);
                }
                return running();
            });
            while (!nodes.isEmpty()) {
                final int node = nodes.pop();
                graph.forEachRelationship(node, D, (s, t, r) -> {
                    if (t > s && graph.exists(t, nodeId, D)) {
                        emit(nodeId, s, t);
                    }
                    return running();
                });
            }
        }
    }

    private final class IntersectTask extends BaseTask implements IntersectionConsumer {

        private RelationshipIntersect intersect;

        IntersectTask(Graph graph) {
            intersect = graph.intersection();
        }

        @Override
        void evaluateNode(final int nodeId) {
            intersect.intersectAll(nodeId, this);
        }

        @Override
        public void accept(final long nodeA, final long nodeB, final long nodeC) {
            emit((int) nodeA, (int) nodeB, (int) nodeC);
        }
    }

    /**
     * result type
     */
    public static class Result {

        public final long nodeA;
        public final long nodeB;
        public final long nodeC;

        public Result(long nodeA, long nodeB, long nodeC) {
            this.nodeA = nodeA;
            this.nodeB = nodeB;
            this.nodeC = nodeC;
        }

        @Override
        public String toString() {
            return "Triangle{" +
                    nodeA +
                    ", " + nodeB +
                    ", " + nodeC +
                    '}';
        }
    }
}
