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
package org.neo4j.graphalgo.beta.paths.dijkstra;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.queue.HugeLongPriorityQueue;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

public class Dijkstra extends Algorithm<Dijkstra, Dijkstra> {
    private static final long PATH_END = -1;
    public static final double NO_PATH_FOUND = -1.0;

    private final Graph graph;

    // next node priority queue
    private HugeLongPriorityQueue queue;
    // auxiliary path map
    private HugeLongLongMap path;
    // path map (stores the resulting shortest path)
    private LongArrayDeque finalPath;
    private DoubleArrayDeque finalPathCosts;
    // visited set
    private BitSet visited;
    private final DijkstraBaseConfig config;
    // overall cost of the path
    private double totalCost;
    private ProgressLogger progressLogger;

    public Dijkstra(Graph graph, DijkstraBaseConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.queue = HugeLongPriorityQueue.min(graph.nodeCount());
        this.path = new HugeLongLongMap(tracker);
        this.visited = new BitSet();
        this.finalPath = new LongArrayDeque();
        this.finalPathCosts = new DoubleArrayDeque();
        this.progressLogger = getProgressLogger();
    }

    public Dijkstra compute() {
        return compute(config.sourceNode(), config.targetNode());
    }

    public Dijkstra compute(long startNode, long goalNode) {
        reset();

        int node = Math.toIntExact(graph.toMappedNodeId(startNode));
        int goal = Math.toIntExact(graph.toMappedNodeId(goalNode));

        queue.add(node, 0.0);
        run(goal);
        if (!path.containsKey(goal)) {
            return this;
        }
        totalCost = queue.cost(goal);
        long last = goal;
        while (last != PATH_END) {
            finalPath.addFirst(last);
            finalPathCosts.addFirst(queue.cost(last));
            last = path.getOrDefault(last, PATH_END);
        }
        // destroy costs and path to remove the data for nodes that are not part of the graph
        // since clear never downsizes the buffer array
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

    public LongArrayDeque getFinalPath() {
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

    private void run(int goal) {
        while (!queue.isEmpty() && running()) {
            long node = queue.pop();
            if (node == goal) {
                return;
            }

            visited.set(node);
            double costs = queue.cost(node);
            graph.forEachRelationship(
                node,
                1.0D,
                longToIntConsumer((source, target, weight) -> {
                    updateCosts(source, target, weight + costs);
                    return true;
                }));
            progressLogger.logProgress((double) node / (graph.nodeCount() - 1));
        }
    }

    private void updateCosts(int source, int target, double newCosts) {
        // target has been visited, we already have a shortest path
        if (visited.get(target)) {
            return;
        }

        // we see target again
        if (queue.containsElement(target)) {
            // and found a shorter path to target
            if (newCosts < queue.cost(target)) {
                path.put(target, source);
                queue.updateCost(target, newCosts);
            }
        } else {
            // we see target for the first time
            path.put(target, source);
            queue.add(target, newCosts);
        }
    }

    @Override
    public Dijkstra me() {
        return this;
    }

    @Override
    public void release() {
        queue = null;
        path = null;
        visited = null;
    }

    private void reset() {
        visited.clear();
        queue.clear();
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
