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
package org.neo4j.graphalgo.impl.betweenness;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.Path;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Brandes Betweenness Centrality with
 * additional restriction on the maximum allowed
 * depth. Implementation behaves like {@link ParallelBetweennessCentrality}
 * with another depth queue which is handled
 * like the node-queue. It contains the number of steps
 * the process took to reach the node. if it exceeds
 * a given limit the dfs stops
 *
 * @author mknblch
 */
public class MaxDepthBetweennessCentrality extends Algorithm<MaxDepthBetweennessCentrality> {

    private Graph graph;

    private double[] centrality;
    private double[] delta; // auxiliary array
    private int[] sigma; // number of shortest paths
    private int[] distance; // distance to start node
    private IntStack stack;
    private IntArrayDeque depth;
    private IntArrayDeque queue;
    private Path[] paths;
    private int nodeCount;
    private final int maxDepth;
    private Direction direction = Direction.OUTGOING;
    private double divisor = 1.0;

    public MaxDepthBetweennessCentrality(Graph graph, int maxDepth) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.maxDepth = maxDepth;
        this.centrality = new double[nodeCount];
        this.stack = new IntStack();
        this.depth = new IntArrayDeque();
        this.sigma = new int[nodeCount];
        this.distance = new int[nodeCount];
        queue = new IntArrayDeque();
        paths = new Path[nodeCount];
        delta = new double[nodeCount];
    }

    /**
     * set traversal direction. If the graph is loaded as undirected
     * OUTGOING must be used.
     * @param direction
     * @return
     */
    public MaxDepthBetweennessCentrality withDirection(Direction direction) {
        this.direction = direction;
        this.divisor = direction == Direction.BOTH ? 2.0 : 1.0;
        return this;
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    public MaxDepthBetweennessCentrality compute() {
        Arrays.fill(centrality, 0);
        graph.forEachNode(this::compute);
        return this;
    }

    /**
     * return (inner)nodeId to bc value mapping
     * @return
     */
    public double[] getCentrality() {
        return centrality;
    }

    /**
     * iterate over each result until every node has
     * been visited or the consumer returns false
     *
     * @param consumer the result consumer
     */
    public void forEach(ResultConsumer consumer) {
        for (int i = nodeCount - 1; i >= 0; i--) {
            if (!consumer.consume(graph.toOriginalNodeId(i), centrality[i])) {
                return;
            }
        }
    }

    /**
     * result stream of pairs of original node-id to bc-value
     * @return
     */
    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new Result(
                                graph.toOriginalNodeId(nodeId),
                                centrality[nodeId]));
    }

    /**
     * start computation at startNode.
     *
     * This does not calculate the BC for startNode only but adds a little bit
     * to all nodes which are reachable from the startNode within maxDepth
     *
     * @param startNodeId
     * @return
     */
    private boolean compute(long startNodeId) {
        // This will break for very large graphs
        int startNode = (int) startNodeId;
        clearPaths();
        stack.clear();
        queue.clear();
        depth.clear();
        Arrays.fill(sigma, 0);
        Arrays.fill(delta, 0);
        Arrays.fill(distance, -1);
        sigma[startNode] = 1;
        distance[startNode] = 0;
        queue.addLast(startNode);
        depth.addLast(0);
        while (!queue.isEmpty() && running()) {
            int node = queue.removeFirst();
            int dp = depth.removeFirst();
            if (dp > maxDepth) {
                break;
            }
            stack.push(node);
            graph.forEachRelationship(node, direction, (source, targetId) -> {
                // This will break for very large graphs
                int target = (int) targetId;

                if (distance[target] < 0) {
                    queue.addLast(target);
                    depth.addLast(dp + 1);
                    distance[target] = distance[node] + 1;
                }
                if (distance[target] == distance[node] + 1) {
                    sigma[target] += sigma[node];
                    append(target, node);
                }
                return true;
            });
        }
        while (!stack.isEmpty() && running()) {
            final int node = stack.pop();
            if (null == paths[node]) {
                continue;
            }
            paths[node].forEach(v -> {
                delta[v] += (double) sigma[v] / (double) sigma[node] * (delta[node] + 1.0);
                return true;
            });
            if (node != startNode) {
                centrality[node] += (delta[node] / divisor);
            }
        }
        getProgressLogger().logProgress((double) startNode / (nodeCount - 1));
        return true;
    }

    /**
     * append nodeId to path
     *
     * @param path   the selected path
     * @param nodeId the node id
     */
    private void append(int path, int nodeId) {
        if (null == paths[path]) {
            paths[path] = new Path();
        }
        paths[path].append(nodeId);
    }

    private void clearPaths() {
        for (Path path : paths) {
            if (null == path) {
                continue;
            }
            path.clear();
        }
    }

    @Override
    public MaxDepthBetweennessCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        centrality = null;
        delta = null;
        sigma = null;
        distance = null;
        stack = null;
        queue = null;
        paths = null;
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

        public final long nodeId;
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
