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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * sequential single source Dijkstra implementation.
 * <p>
 * Calculates the minimum distance from a startNode to every other
 * node in the graph. {@link Double#POSITIVE_INFINITY} is returned
 * if no path exists between those nodes.
 *
 * @author mknblch
 */
public class ShortestPaths extends Algorithm<ShortestPaths> {

    private Graph graph;
    private IntDoubleMap costs;
    private IntPriorityQueue queue;
    private final int nodeCount;
    private ProgressLogger progressLogger;

    public ShortestPaths(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        costs = new IntDoubleScatterMap(nodeCount);
        queue = IntPriorityQueue.min();
        progressLogger = getProgressLogger();
    }

    /**
     * compute the shortest paths from startNode
     *
     * @param startNode the start node id (original neo4j id)
     * @return itself
     */
    public ShortestPaths compute(long startNode) {
        graph.forEachNode(node -> {
            costs.put(node, Double.POSITIVE_INFINITY);
            return true;
        });
        final int nodeId = graph.toMappedNodeId(startNode);
        costs.put(nodeId, 0d);
        queue.add(nodeId, 0d);
        run();
        return this;
    }

    /**
     * @return mapped-id to costSum array
     */
    public IntDoubleMap getShortestPaths() {
        return costs;
    }

    /**
     * @return a stream of [nodeId, min-distance]-pairs from
     * start node to each other node
     */
    public Stream<Result> resultStream() {
        return StreamSupport.stream(costs.spliterator(), false)
                .map(cursor -> new Result(graph.toOriginalNodeId(cursor.key), cursor.value));
    }

    private void run() {
        while (!queue.isEmpty() && running()) {
            final int node = queue.pop();
            double sourceCosts = this.costs.getOrDefault(node, Double.POSITIVE_INFINITY);
            // scan ALL relationships
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING,
                    (source, target, relId, weight) -> {
                        // relax
                        final double targetCosts = this.costs.getOrDefault(target, Double.POSITIVE_INFINITY);
                        if (weight + sourceCosts < targetCosts) {
                            costs.put(target, weight + sourceCosts);
                            queue.set(target, weight + sourceCosts);
                        }
                        return true;
                    });
            progressLogger.logProgress((double) node / (nodeCount - 1));
        }
    }

    @Override
    public ShortestPaths me() {
        return this;
    }

    @Override
    public ShortestPaths release() {
        graph = null;
        costs = null;
        queue = null;
        return this;
    }

    /**
     * The Result DTO
     */
    public static class Result {

        /**
         * the neo4j node id
         */
        public final long nodeId;
        /**
         * distance to nodeId from startNode
         */
        public final double distance;

        public Result(Long nodeId, Double distance) {
            this.nodeId = nodeId;
            this.distance = distance;
        }
    }
}
