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
import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.Path;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs
 * as specified in <a href="http://www.algo.uni-konstanz.de/publications/b-fabc-01.pdf">this paper</a>
 *
 * TODO: deprecated due to ParallelBC ?
 *
 * @author mknblch
 */
public class BetweennessCentrality extends LegacyAlgorithm<BetweennessCentrality> {

    private Graph graph;

    private double[] centrality;
    private double[] delta; // auxiliary array
    private int[] sigma; // number of shortest paths
    private int[] distance; // distance to start node
    private IntStack stack;
    private IntArrayDeque queue;
    private Path[] paths;
    private int nodeCount;
    private Direction direction = Direction.OUTGOING;
    private double divisor = 1.0;

    public BetweennessCentrality(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.centrality = new double[nodeCount];
        this.stack = new IntStack();
        this.sigma = new int[nodeCount];
        this.distance = new int[nodeCount];
        queue = new IntArrayDeque();
        paths = new Path[nodeCount];
        delta = new double[nodeCount];
    }

    public BetweennessCentrality withDirection(Direction direction) {
        this.direction = direction;
        this.divisor = direction == Direction.BOTH ? 2.0 : 1.0;
        return this;
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    @Override
    public Void compute() {
        Arrays.fill(centrality, 0);
        graph.forEachNode(this::compute);
        return null;
    }

    /**
     * get (inner)nodeId to centrality mapping.
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
     * returns a stream of bc-results
     * (nodeid to bc-value pairs)
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
     * start evaluation from startNode.
     *
     * This does not calculate the BC for startNode but adds a little bit
     * to all nodes which are reachable from the startNode
     *
     * @param start the node
     * @return
     */
    private boolean compute(long start) {
        // This will break for very large graphs
        int startNode = (int) start;
        clearPaths();
        stack.clear();
        queue.clear();
        Arrays.fill(sigma, 0);
        Arrays.fill(delta, 0);
        Arrays.fill(distance, -1);
        sigma[startNode] = 1;
        distance[startNode] = 0;
        queue.addLast(startNode);
        // bfs on the whole graph
        while (!queue.isEmpty() && running()) {
            int node = queue.removeFirst();
            stack.push(node);
            graph.forEachRelationship(node, direction, (source, targetId) -> {
                // This will break for very large graphs
                int target = (int) targetId;
                if (distance[target] < 0) {
                    queue.addLast(target);
                    distance[target] = distance[node] + 1;
                }
                if (distance[target] == distance[node] + 1) {
                    sigma[target] += sigma[node];
                    append(target, node);
                }
                return true;
            });
        }
        // for each pivot node
        while (!stack.isEmpty() && running()) {
            final int node = stack.pop();
            // check if a path exists
            if (null == paths[node]) {
                continue;
            }
            // update delta
            paths[node].forEach(v -> {
                delta[v] += (double) sigma[v] / (double) sigma[node] * (delta[node] + 1.0);
                return true;
            });
            // aggregate centrality
            if (node != startNode) {
                centrality[node] += (delta[node] / divisor);
            }
        }
        // log done for current node
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

    /**
     * clear all paths to reuse them in
     * the next round
     */
    private void clearPaths() {
        for (Path path : paths) {
            if (null == path) {
                continue;
            }
            path.clear();
        }
    }

    /**
     * self reference
     */
    @Override
    public BetweennessCentrality me() {
        return this;
    }

    /**
     * release internal structures
     */
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
