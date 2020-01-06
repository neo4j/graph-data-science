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
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Randomized Approximate Brandes. See https://arxiv.org/pdf/1702.06087.pdf.
 *
 * The implementation follows the same approach as {@link ParallelBetweennessCentrality}
 * with an additional node filter to select interesting nodes. the result is multiplied
 * with a factor which is based on the probability of which the filter accepts nodes.
 *
 * There is a significant performance drop if the direction is BOOTH. Its more efficient
 * to load the graph as undirected and do the
 *
 * @author mknblch
 */
public class RABrandesBetweennessCentrality extends LegacyAlgorithm<RABrandesBetweennessCentrality> {

    public interface SelectionStrategy {

        /**
         * node id filter
         * @return true if the nodes is accepted, false otherwise
         */
        boolean select(int nodeId);

        /**
         * count of selectable nodes
         */
        int size();
    }

    // the graph
    private Graph graph;
    // AI counts up for every node until nodeCount is reached
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    // atomic double array which supports only atomic-add
    private AtomicDoubleArray centrality;
    // the node count
    private final int nodeCount;
    private final int expectedNodeCount;
    // global executor service
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private SelectionStrategy selectionStrategy;
    private Direction direction = Direction.OUTGOING;
    // used to
    private double divisor = 1.0;

    private int maxDepth = Integer.MAX_VALUE;

    /**
     * @param graph the graph iface
     * @param executorService the executor service
     * @param concurrency desired number of threads to spawn
     */
    public RABrandesBetweennessCentrality(Graph graph, ExecutorService executorService, int concurrency, SelectionStrategy selectionStrategy) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.centrality = new AtomicDoubleArray(nodeCount);
        this.selectionStrategy = selectionStrategy;
        this.expectedNodeCount = selectionStrategy.size();
    }

    /**
     * set traversal direction. Use OUTGOING for undirected.
     * @param direction
     * @return
     */
    public RABrandesBetweennessCentrality withDirection(Direction direction) {
        this.direction = direction;
        // during evaluation booth counts each node twice
        this.divisor = direction == Direction.BOTH ? 2.0 : 1.0;
        return this;
    }

    /**
     * set max depth (maximum number of hops from the start node)
     * @param maxDepth
     * @return
     */
    public RABrandesBetweennessCentrality withMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    @Override
    public Void compute() {
        nodeQueue.set(0);
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(executorService.submit(new BCTask()));
        }
        ParallelUtil.awaitTermination(futures);
        return null;
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
    public Stream<BetweennessCentrality.Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new BetweennessCentrality.Result(
                                graph.toOriginalNodeId(nodeId),
                                centrality.get(nodeId)));
    }

    @Override
    public RABrandesBetweennessCentrality me() {
        return this;
    }

    /**
     * release inner data structures
     */
    @Override
    public void release() {
        graph = null;
        selectionStrategy = null;
    }

    /**
     * a BCTask takes one element from the nodeQueue as long as
     * it is lower then nodeCount and calculates it's centrality
     */
    private class BCTask implements Runnable {

        private final RelationshipIterator localRelationshipIterator;
        // we have to keep all paths during eval (memory intensive)
        private final IntObjectMap<IntArrayList> paths;
        /**
         * contains nodes which have been visited during the first round
         */
        private final IntStack pivots;
        /**
         * the queue contains 2 elements per node. the node itself
         * and its depth. Both values are pushed or taken from the
         * stack during the evaluation as pair.
         */
        private final IntArrayDeque queue;
        private final IntDoubleMap delta;
        private final IntIntMap sigma;
        private final int[] distance;

        private BCTask() {
            this.localRelationshipIterator = graph.concurrentCopy();
            this.paths = new IntObjectScatterMap<>(expectedNodeCount);
            this.pivots = new IntStack();
            this.queue = new IntArrayDeque();
            this.sigma = new IntIntScatterMap(expectedNodeCount);
            this.delta = new IntDoubleScatterMap(expectedNodeCount);

            this.distance = new int[nodeCount];
        }

        @Override
        public void run() {
            final double f = (nodeCount * divisor) / selectionStrategy.size();
            for (;;) {
                // take start node from the queue
                final int startNodeId = nodeQueue.getAndIncrement();
                if (startNodeId >= nodeCount || !running()) {
                    return;
                }
                // check whether the node is part of the subset
                if (!selectionStrategy.select(startNodeId)) {
                    continue;
                }
                // reset
                getProgressLogger().logProgress((double) startNodeId / (nodeCount - 1));
                // default value is -1 (checked during evaluation)
                Arrays.fill(distance, -1);
                sigma.clear();
                paths.clear();
                delta.clear();
                sigma.put(startNodeId, 1);
                distance[startNodeId] = 0;
                queue.addLast(startNodeId);
                queue.addLast(0);
                // as long as the inner queue has more nodes
                while (!queue.isEmpty()) {
                    int node = queue.removeFirst();
                    int nodeDepth = queue.removeFirst();
                    if (nodeDepth - 1 > maxDepth) {
                        continue;
                    }
                    pivots.push(node);
                    localRelationshipIterator.forEachRelationship(node, direction, (source, targetId) -> {
                        // This will break for very large graphs
                        int target = (int) targetId;

                        // check if distance has been set before
                        if (distance[target] < 0) {
                            queue.addLast(target);
                            queue.addLast(nodeDepth + 1);
                            // distance changes during exec and might trigger next condition
                            distance[target] = distance[node] + 1;
                        }
                        // no if-else here since distance could have been changed in the first condition
                        if (distance[target] == distance[node] + 1) {
                            sigma.addTo(target, sigma.getOrDefault(node, 0));
                            append(target, node);
                        }
                        return true;
                    });
                }

                while (!pivots.isEmpty()) {
                    int node = pivots.pop();
                    final IntArrayList intCursors = paths.get(node);
                    if (null != intCursors) {
                        intCursors.forEach((Consumer<? super IntCursor>) c -> {
                            delta.addTo(c.value,
                                    (double) sigma.getOrDefault(c.value, 0) /
                                            (double) sigma.getOrDefault(node, 0) *
                                            (delta.getOrDefault(node, 0) + 1.0));
                        });
                    }
                    if (node != startNodeId) {
                        centrality.add(node, f * (delta.getOrDefault(node, 0)));
                    }
                }
            }
        }

        // append node to the path at target
        private void append(int target, int node) {
            IntArrayList intCursors = paths.get(target);
            if (null == intCursors) {
                intCursors = new IntArrayList();
                paths.put(target, intCursors);
            }
            intCursors.add(node);
        }
    }
}
