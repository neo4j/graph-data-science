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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * WeightedAllShortestPaths:
 * <p>
 * multi-source parallel dijkstra algorithm for computing the shortest path between
 * each pair of nodes.
 * <p>
 * Since all nodeId's have already been ordered by the idMapping we can use an integer
 * instead of a queue which just count's up for each startNodeId as long as it is
 * < nodeCount. Each thread tries to take one int from the counter at one time and
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
public class WeightedAllShortestPaths extends MSBFSASPAlgorithm<WeightedAllShortestPaths> {

    private Graph graph;
    private final int nodeCount;

    /**
     * maximum number of workers
     */
    private final int concurrency;
    /**
     * nodeId counter (init with nodeCount,
     * counts down for each node)
     */
    private AtomicInteger counter;
    private ExecutorService executorService;
    private final Direction direction;
    private BlockingQueue<Result> resultQueue;

    private volatile boolean outputStreamOpen;

    public WeightedAllShortestPaths(Graph graph, ExecutorService executorService, int concurrency, Direction direction) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.direction = direction;
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >0");
        }
        this.concurrency = concurrency;
        this.counter = new AtomicInteger();
        this.resultQueue = new LinkedBlockingQueue<>(); // TODO limit size?
    }

    /**
     * the resultStream(..) method starts the computation and
     * returns a Stream of SP-Tuples (source, target, minDist)
     *
     * @return the result stream
     */
    @Override
    public Stream<Result> resultStream() {

        counter.set(0);
        outputStreamOpen = true;

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(new ShortestPathTask());
        }

        long end = (long) nodeCount * nodeCount;

        return LongStream.range(0, end)
                .onClose(() -> outputStreamOpen = false)
                .mapToObj(i -> {
                    try {
                        return resultQueue.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }).filter(result -> result.distance != Double.POSITIVE_INFINITY);
    }

    @Override
    public WeightedAllShortestPaths me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        counter = null;
        resultQueue = null;
    }

    /**
     * Dijkstra Task. Takes one element of the counter at a time
     * and starts dijkstra on it. It starts emitting results to the
     * queue once all reachable nodes have been visited.
     */
    private class ShortestPathTask implements Runnable {

        private final IntPriorityQueue queue;
        private final double[] distance;

        private ShortestPathTask() {
            distance = new double[nodeCount];
            queue = IntPriorityQueue.min();
        }

        @Override
        public void run() {
            final ProgressLogger progressLogger = getProgressLogger();
            int startNode;
            while (outputStreamOpen && running() && (startNode = counter.getAndIncrement()) < nodeCount) {
                compute(startNode);
                for (int i = 0; i < nodeCount; i++) {
                    final Result result = new Result(
                            graph.toOriginalNodeId(startNode),
                            graph.toOriginalNodeId(i),
                            distance[i]);

                    try {
                        resultQueue.put(result);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                progressLogger.logProgress((double) startNode / (nodeCount - 1));
            }
        }

        public void compute(int startNode) {
            Arrays.fill(distance, Double.POSITIVE_INFINITY);
            distance[startNode] = 0D;
            queue.add(startNode, 0D);
            while (outputStreamOpen && !queue.isEmpty()) {
                final int node = queue.pop();
                final double sourceDistance = distance[node];
                graph.forEachRelationship(
                        node,
                        direction,
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

    /**
     * Result DTO
     */
    public static class Result {

        /**
         * neo4j nodeId of the source node
         */
        public final long sourceNodeId;
        /**
         * neo4j nodeId of the target node
         */
        public final long targetNodeId;
        /**
         * minimum distance between source and target
         */
        public final double distance;

        public Result(long sourceNodeId, long targetNodeId, double distance) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "sourceNodeId=" + sourceNodeId +
                    ", targetNodeId=" + targetNodeId +
                    ", distance=" + distance +
                    '}';
        }
    }
}
