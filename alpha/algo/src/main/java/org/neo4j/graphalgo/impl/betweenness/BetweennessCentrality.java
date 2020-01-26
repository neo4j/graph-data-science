/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.betweenness;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.Paths;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs
 * as specified in <a href="https://kops.uni-konstanz.de/handle/123456789/5739">this paper</a>
 *
 * the algo additionally uses node partitioning to run multiple tasks concurrently. each
 * task takes a node from a shared counter and calculates its bc value. The counter increments
 * until nodeCount is reached (works because we have consecutive ids)
 *
 * Note:
 * The algo can be adapted to use the MSBFS but at the time of development some must have
 * features in the MSBFS were missing (like manually canceling evaluation if some conditions have been met).
 */
public class BetweennessCentrality extends Algorithm<BetweennessCentrality, BetweennessCentrality> {

    private Graph graph;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    private AtomicDoubleArray centrality;
    private final int nodeCount;
    private final ExecutorService executorService;
    private final int concurrency;
    private Direction direction = Direction.OUTGOING;
    private double divisor = 1.0;

    public BetweennessCentrality(Graph graph, ExecutorService executorService, int concurrency) {
        this(graph, executorService, concurrency, false);
    }

    public BetweennessCentrality(Graph graph, ExecutorService executorService, int concurrency, boolean undirected) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.centrality = new AtomicDoubleArray(nodeCount);
        this.divisor = undirected ? 2.0 : 1.0;
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    @Override
    public BetweennessCentrality compute() {
        nodeQueue.set(0);
        ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(executorService.submit(new BCTask()));
        }
        ParallelUtil.awaitTermination(futures);
        return this;
    }

    /**
     * get the centrality array
     *
     * @return array with centrality
     */
    public AtomicDoubleArray getCentrality() {
        return centrality;
    }

    /**
     * emit the result stream
     *
     * @return stream if Results
     */
    public Stream<Result> resultStream() {
        return IntStream
            .range(0, nodeCount)
            .mapToObj(nodeId ->
                new Result(
                    graph.toOriginalNodeId(nodeId),
                    centrality.get(nodeId)));
    }

    /**
     * @return
     */
    @Override
    public BetweennessCentrality me() {
        return this;
    }

    /**
     * release internal data structures
     */
    @Override
    public void release() {}

    /**
     * a BCTask takes one element from the nodeQueue and calculates it's centrality
     */
    class BCTask implements Runnable {

        private final RelationshipIterator localRelationshipIterator;
        // path map
        private final Paths paths;
        // stack to keep visited nodes
        private final IntStack stack;
        // bfs queue
        private final IntArrayDeque queue;
        // bc data structures
        private final double[] delta;
        private final int[] sigma;
        private final int[] distance;

        private BCTask() {
            this.localRelationshipIterator = graph.concurrentCopy();
            this.paths = new Paths();
            this.stack = new IntStack();
            this.queue = new IntArrayDeque();
            this.sigma = new int[nodeCount];
            this.distance = new int[nodeCount];
            this.delta = new double[nodeCount];
        }

        @Override
        public void run() {
            for (; ; ) {
                reset();
                int startNodeId = nodeQueue.getAndIncrement();
                if (startNodeId >= nodeCount || !running()) {
                    return;
                }
                if (calculateBetweenness(startNodeId)) {
                    return;
                }
            }
        }

        /**
         * calculate bc concurrently. a concurrent shared decimal array is used.
         *
         * @param startNodeId
         * @return
         */
        private boolean calculateBetweenness(int startNodeId) {
            getProgressLogger().logProgress((double) startNodeId / (nodeCount - 1));
            sigma[startNodeId] = 1;
            distance[startNodeId] = 0;
            queue.addLast(startNodeId);
            while (!queue.isEmpty()) {
                int node = queue.removeFirst();
                stack.push(node);
                localRelationshipIterator.forEachRelationship(node, direction, (source, targetId) -> {
                    // This will break for very large graphs
                    int target = (int) targetId;

                    if (distance[target] < 0) {
                        queue.addLast(target);
                        distance[target] = distance[node] + 1;
                    }
                    if (distance[target] == distance[node] + 1) {
                        sigma[target] += sigma[node];
                        paths.append(target, node);
                    }
                    return true;
                });
            }

            while (!stack.isEmpty()) {
                int node = stack.pop();
                paths.forEach(node, v -> {
                    delta[v] += (double) sigma[v] / (double) sigma[node] * (delta[node] + 1.0);
                    return true;
                });
                if (node != startNodeId) {
                    centrality.add(node, delta[node] / divisor);
                }
            }
            return false;
        }

        /**
         * reset local state
         */
        private void reset() {
            paths.clear();
            stack.clear();
            queue.clear();
            Arrays.fill(sigma, 0);
            Arrays.fill(delta, 0);
            Arrays.fill(distance, -1);
        }
    }

    /**
     * Consumer interface
     */
    public interface ResultConsumer {
        /**
         * consume nodeId and centrality value as long as the consumer returns true
         *
         * @param originalNodeId the neo4j node id
         * @param value          centrality value
         * @return a bool indicating if the loop should continue(true) or stop(false)
         */
        boolean consume(long originalNodeId, double value);
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        // original node id
        public final long nodeId;
        // centrality value
        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                   "nodeId=" + nodeId +
                   ", centrality=" + centrality +
                   '}';
        }
    }
}
