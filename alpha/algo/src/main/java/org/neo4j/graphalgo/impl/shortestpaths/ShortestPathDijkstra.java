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
package org.neo4j.graphalgo.impl.shortestpaths;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * Dijkstra single source - single target shortest path algorithm
 * <p>
 * The algorithm computes a (there might be more then one) shortest path
 * between a given start and target-NodeId. It returns result tuples of
 * [nodeId, distance] of each node in the path.
 */
public class ShortestPathDijkstra extends Algorithm<ShortestPathDijkstra, ShortestPathDijkstra> {

    private static final int PATH_END = -1;
    public static final double NO_PATH_FOUND = -1.0;
    public static final int UNUSED = 42;

    private Graph graph;

    // node to cost map
    private IntDoubleMap costs;
    // next node priority queue
    private IntPriorityQueue queue;
    // auxiliary path map
    private IntIntMap path;
    // path map (stores the resulting shortest path)
    private IntArrayDeque finalPath;
    private DoubleArrayDeque finalPathCosts;
    // visited set
    private BitSet visited;
    private final int nodeCount;
    private final DijkstraConfig config;
    // overall cost of the path
    private double totalCost;
    private ProgressLogger progressLogger;

    public ShortestPathDijkstra(Graph graph, DijkstraConfig config) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.config = config;
        this.costs = new IntDoubleScatterMap();
        this.queue = SharedIntPriorityQueue.min(
                IntPriorityQueue.DEFAULT_CAPACITY,
                costs,
                Double.MAX_VALUE);
        this.path = new IntIntScatterMap();
        this.visited = new BitSet();
        this.finalPath = new IntArrayDeque();
        this.finalPathCosts = new DoubleArrayDeque();
        this.progressLogger = getProgressLogger();
    }

    public ShortestPathDijkstra compute() {
        return compute(config.startNode(), config.endNode(), config.resolvedDirection());
    }

    public ShortestPathDijkstra compute(long startNode, long goalNode, Direction direction) {
        reset();

        int node = Math.toIntExact(graph.toMappedNodeId(startNode));
        int goal = Math.toIntExact(graph.toMappedNodeId(goalNode));
        costs.put(node, 0.0);
        queue.add(node, 0.0);
        run(goal, direction);
        if (!path.containsKey(goal)) {
            return this;
        }
        totalCost = costs.get(goal);
        int last = goal;
        while (last != PATH_END) {
            finalPath.addFirst(last);
            finalPathCosts.addFirst(costs.get(last));
            last = path.getOrDefault(last, PATH_END);
        }
        // destroy costs and path to remove the data for nodes that are not part of the graph
        // since clear never downsizes the buffer array
        costs.release();
        path.release();
        return this;
    }

    /**
     * return the result stream
     *
     * @return stream of result DTOs
     */
    public Stream<Result> resultStream() {
        double[] costs = finalPathCosts.buffer;
        return StreamSupport.stream(finalPath.spliterator(), false)
                .map(cursor -> new Result(graph.toOriginalNodeId(cursor.value), costs[cursor.index]));
    }

    public IntArrayDeque getFinalPath() {
        return finalPath;
    }

    public double[] getFinalPathCosts() {
        return finalPathCosts.toArray();
    }

    /**
     * get the distance sum of the path
     *
     * @return sum of distances between start and goal
     */
    public double getTotalCost() {
        return totalCost;
    }

    /**
     * return the number of nodes the path consists of
     *
     * @return number of nodes in the path
     */
    public int getPathLength() {
        return finalPath.size();
    }

    private void run(int goal, Direction direction) {
        while (!queue.isEmpty() && running()) {
            int node = queue.pop();
            if (node == goal) {
                return;
            }

            visited.set(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            graph.forEachRelationship(
                    node,
                    direction,
                    1.0D,
                    longToIntConsumer((source, target, weight) -> {
                        updateCosts(source, target, weight + costs);
                        return true;
                    }));
            progressLogger.logProgress((double) node / (nodeCount - 1));
        }
    }

    private void updateCosts(int source, int target, double newCosts) {

        if (costs.containsKey(target)) {
            if (newCosts < costs.getOrDefault(target, Double.MAX_VALUE)) {
                costs.put(target, newCosts);
                path.put(target, source);
                queue.update(target);
            }
        } else  {
            if (newCosts < costs.getOrDefault(target, Double.MAX_VALUE)) {
                costs.put(target, newCosts);
                path.put(target, source);
                queue.add(target, newCosts);
            }
        }
    }

    @Override
    public ShortestPathDijkstra me() {
        return this;
    }

    @Override
    public void release() {
        costs = null;
        queue = null;
        path = null;
        visited = null;
    }

    private void reset() {
        visited.clear();
        queue.clear();
        costs.clear();
        path.clear();
        finalPath.clear();
        totalCost = NO_PATH_FOUND;
    }

    /**
     * Result DTO
     */
    public static class Result {

        /**
         * the neo4j node id
         */
        public final Long nodeId;
        /**
         * cost to reach the node from startNode
         */
        public final Double cost;

        public Result(Long nodeId, Double cost) {
            this.nodeId = nodeId;
            this.cost = cost;
        }
    }
}
