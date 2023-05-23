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
package org.neo4j.gds.impl.msbfs;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.queue.IntPriorityQueue;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.neo4j.gds.Converters.longToIntConsumer;

/**
 * WeightedAllShortestPaths:
 * <p>
 * multi-source parallel dijkstra algorithm for computing the shortest path between
 * each pair of nodes.
 * <p>
 * Since all nodeId's have already been ordered by the idMap we can use an integer
 * instead of a queue which just count's up for each startNodeId as long as it is
 * {@code < nodeCount}. Each thread tries to take one int from the counter at one time and
 * starts its computation on it.
 * <p>
 * The {@link WeightedAllShortestPaths#concurrency} value determines the count of workers
 * that should be spawned.
 * <p>
 * Due to the high memory footprint the result set would have we emit each result into
 * a blocking queue. The result stream takes elements from the queue while the workers
 * add elements to it. The result stream is limited by N^2. If the stream gets closed
 * prematurely the workers get closed too.
 */
public class WeightedAllShortestPaths extends MSBFSASPAlgorithm {
    private final BlockingQueue<AllShortestPathsStreamResult> resultQueue = new LinkedBlockingQueue<>();

    private final int nodeCount;
    private final int concurrency; // maximum number of workers
    private final ExecutorService executorService;
    private final Graph graph;
    private final AtomicInteger counter; // nodeId counter (init with nodeCount, counts down for each node)

    private volatile boolean outputStreamOpen;

    public WeightedAllShortestPaths(Graph graph, ExecutorService executorService, int concurrency) {
        super(ProgressTracker.NULL_TRACKER);
        if (!graph.hasRelationshipProperty()) {
            throw new UnsupportedOperationException("WeightedAllShortestPaths is not supported on graphs without a weight property");
        }

        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >0");
        }
        this.concurrency = concurrency;
        this.counter = new AtomicInteger();
    }

    /**
     * the compute(..) method starts the computation and
     * returns a Stream of SP-Tuples (source, target, minDist)
     *
     * @return the result stream
     */
    @Override
    public Stream<AllShortestPathsStreamResult> compute() {
        progressTracker.beginSubTask();

        counter.set(0);
        outputStreamOpen = true;

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(new ShortestPathTask());
        }

        return AllShortestPathsStream.stream(resultQueue, () -> {
            outputStreamOpen = false;
            progressTracker.endSubTask();
        })
            .limit((long) nodeCount * nodeCount)
            .filter(result -> result.distance != Double.POSITIVE_INFINITY);
    }

    /**
     * Dijkstra Task. Takes one element of the counter at a time
     * and starts dijkstra on it. It starts emitting results to the
     * queue once all reachable nodes have been visited.
     */
    private final class ShortestPathTask implements Runnable {

        private final IntPriorityQueue queue;
        private final double[] distance;
        private final RelationshipIterator threadLocalGraph;

        private ShortestPathTask() {
            distance = new double[nodeCount];
            queue = IntPriorityQueue.min();
            this.threadLocalGraph = graph.concurrentCopy();
        }

        @Override
        public void run() {
            int startNode;
            while (outputStreamOpen && terminationFlag.running() && (startNode = counter.getAndIncrement()) < nodeCount) {
                compute(startNode);
                for (int i = 0; i < nodeCount; i++) {
                    var result = AllShortestPathsStreamResult.result(
                        graph.toOriginalNodeId(startNode),
                        graph.toOriginalNodeId(i),
                        distance[i]
                    );
                    try {
                        resultQueue.put(result);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                progressTracker.logProgress();
            }
        }

        void compute(int startNode) {
            Arrays.fill(distance, Double.POSITIVE_INFINITY);
            distance[startNode] = 0D;
            queue.add(startNode, 0D);
            while (outputStreamOpen && !queue.isEmpty()) {
                final int node = queue.pop();
                final double sourceDistance = distance[node];
                threadLocalGraph.forEachRelationship(
                        node,
                        Double.NaN,
                        longToIntConsumer((source, target, weight) -> {
                            // relax
                            final double targetDistance = weight + sourceDistance;
                            if (targetDistance < distance[target]) {
                                distance[target] = targetDistance;
                                queue.set(target, targetDistance);
                            }
                            return true;
                        }));
            }
        }
    }
}
