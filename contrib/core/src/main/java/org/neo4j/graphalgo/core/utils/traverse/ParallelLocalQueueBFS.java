/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.traverse;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * parallel breadth first search
 *
 * @author mknblch
 */
public class ParallelLocalQueueBFS implements BFS {

    // the graph
    private final Graph graph;
    // number of active threads
    private final AtomicInteger threads;
    // set of visited ID's
    private final AtomicBitSet visited;
    // the executor
    private final ExecutorService executorService;
    // intended number of concurrently active threads
    private final int concurrency;
    // result future queue
    private final ConcurrentLinkedQueue<Future<?>> futures;
    // count the number of created threads during evaluation
    private AtomicInteger threadsCreated = new AtomicInteger(0);

    // probability for a thread to get started
    private double concurrencyFactor = 0.5;

    public ParallelLocalQueueBFS(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        visited = new AtomicBitSet(Math.toIntExact(graph.nodeCount()));
        this.executorService = executorService;
        this.concurrency = concurrency;
        threads = new AtomicInteger(0);
        futures = new ConcurrentLinkedQueue<>();
    }

    /**
     * reset underlying container
     *
     * @return itself
     */
    public ParallelLocalQueueBFS reset() {
        visited.clear();
        futures.clear();
        threadsCreated.set(0);
        threads.set(0);
        return this;
    }

    /**
     * wait for all started futures to complete
     *
     * @return itself
     */
    public ParallelLocalQueueBFS awaitTermination() {
        ParallelUtil.awaitTerminations(futures);
        return this;
    }

    /**
     * start bfs at startNodeId using the supplied direction. On each relationship the targetNode is tested
     * using the predicate. If it succeeds the node is enqueued for the next iteration. Upon first arrival at a
     * node the visitor is called with its node Id.
     *
     * NOTE: predicate and visitor must be thread safe
     */
    public ParallelLocalQueueBFS bfs(
            int startNodeId,
            Direction direction,
            IntPredicate predicate,
            IntConsumer visitor) {
        if (!predicate.test(startNodeId)) {
            return this;
        }
        final IntPriorityQueue queue = IntPriorityQueue.max();
        queue.add(startNodeId, 0d);
        while (!queue.isEmpty()) {
            final int node = queue.pop();

            if (!visited.trySet(node)) {
                continue;
            }
            visitor.accept(node);

            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                // should we visit this node?
                if (!predicate.test(targetNodeId)) {
                    return true;
                }
                // don't visit id's twice
                if (visited.get(targetNodeId)) {
                    return true;
                }
                if (!addThread(() -> bfs(targetNodeId, direction, predicate, visitor))) {
                    queue.add(targetNodeId, (double) graph.degree(targetNodeId, direction));
                }
                return true;
            });
        }
        threads.decrementAndGet();
        return this;
    }

    public ParallelLocalQueueBFS withConcurrencyFactor(double concurrencyFactor) {
        this.concurrencyFactor = concurrencyFactor;
        return this;
    }

    /**
     * tell whether a new thread was added or not
     *
     * @return true if there is room for another thread, false otherwise
     */
    private boolean addThread(Runnable runnable) {
        if (Math.random() >= concurrencyFactor) {
            return false;
        }
        int current;
        current = threads.get();
        if (current >= concurrency) {
            return false;
        }
        if (threads.compareAndSet(current, current + 1)) {
            futures.add(executorService.submit(runnable));
            threadsCreated.incrementAndGet();
            return true;
        }
        return false;
    }

    public int getThreadsCreated() {
        return threadsCreated.get();
    }
}
