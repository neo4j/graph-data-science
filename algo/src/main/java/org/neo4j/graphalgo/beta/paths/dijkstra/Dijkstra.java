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
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.beta.paths.PathResultBuilder;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.queue.HugeLongPriorityQueue;

import java.util.LinkedList;
import java.util.List;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

public class Dijkstra extends Algorithm<Dijkstra, DijkstraResult> {
    private static final long PATH_END = -1;

    private final Graph graph;
    private final DijkstraBaseConfig config;

    // priority queue
    private HugeLongPriorityQueue queue;
    // predecessor map
    private HugeLongLongMap path;
    // visited set
    private BitSet visited;

    private final ProgressLogger progressLogger;

    public Dijkstra(Graph graph, DijkstraBaseConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.queue = HugeLongPriorityQueue.min(graph.nodeCount());
        this.path = new HugeLongLongMap(tracker);
        this.visited = new BitSet();
        this.progressLogger = getProgressLogger();
    }

    public DijkstraResult compute() {
        var sourceNode = graph.toMappedNodeId(config.sourceNode());
        var targetNode = graph.toMappedNodeId(config.targetNode());

        queue.add(sourceNode, 0.0);

        run(targetNode);

        if (!path.containsKey(targetNode)) {
            return DijkstraResult.EMPTY;
        }

        var pathResultBuilder = new PathResultBuilder();

        pathResultBuilder
            .index(0)
            .sourceNode(sourceNode)
            .targetNode(targetNode)
            .totalCost(queue.cost(targetNode));

        var finalPath = new LinkedList<Long>();
        var finalPathCosts = new LinkedList<Double>();

        long lastNode = targetNode;
        while (lastNode != PATH_END) {
            finalPath.addFirst(lastNode);
            finalPathCosts.addFirst(queue.cost(lastNode));
            lastNode = path.getOrDefault(lastNode, PATH_END);
        }

        return ImmutableDijkstraResult
            .builder()
            .addPath(pathResultBuilder.nodeIds(finalPath).costs(finalPathCosts).build())
            .build();
    }

    private void run(long goal) {
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
                })
            );
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
                queue.set(target, newCosts);
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
        path.release();
        queue.release();
        queue = null;
        path = null;
        visited = null;
    }
}

@ValueClass
interface DijkstraResult {
    DijkstraResult EMPTY = ImmutableDijkstraResult.of(List.of());

    List<PathResult> paths();
}
