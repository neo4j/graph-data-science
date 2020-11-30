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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.ImmutablePathResult;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.queue.HugeLongPriorityQueue;

import java.util.LinkedList;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Dijkstra extends Algorithm<Dijkstra, DijkstraResult> {
    private static final long PATH_END = -1;

    private final Graph graph;
    private final DijkstraBaseConfig config;
    private final LongPredicate stopPredicate;

    // priority queue
    private final HugeLongPriorityQueue queue;
    // predecessor map
    private final HugeLongLongMap path;
    // visited set
    private final BitSet visited;
    // path id increasing in order of exploration
    private long pathIndex;

    /**
     * Configure Dijkstra to compute at most one source-target shortest path.
     */
    public static Dijkstra sourceTarget(
        Graph graph,
        DijkstraBaseConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        long targetNode = graph.toMappedNodeId(config.targetNode());
        return new Dijkstra(graph, config, node -> node == targetNode, progressLogger, tracker);
    }

    /**
     * Configure Dijkstra to compute all single-source shortest path.
     */
    public static Dijkstra singleSource(
        Graph graph,
        DijkstraBaseConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        return new Dijkstra(graph, config, node -> true, progressLogger, tracker);
    }

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(Dijkstra.class)
            .add("priority queue", HugeLongPriorityQueue.memoryEstimation())
            .add("reverse path", HugeLongLongMap.memoryEstimation())
            .perNode("visited set", MemoryUsage::sizeOfBitset)
            .build();
    }

    private Dijkstra(
        Graph graph,
        DijkstraBaseConfig config,
        LongPredicate stopPredicate,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.config = config;
        this.stopPredicate = stopPredicate;
        this.queue = HugeLongPriorityQueue.min(graph.nodeCount());
        this.path = new HugeLongLongMap(tracker);
        this.visited = new BitSet();
        this.pathIndex = 0L;
        this.progressLogger = progressLogger;
    }

    public DijkstraResult compute() {
        progressLogger.logStart();

        var sourceNode = graph.toMappedNodeId(config.sourceNode());

        queue.add(sourceNode, 0.0);

        var pathResultBuilder = ImmutablePathResult.builder()
            .sourceNode(sourceNode);

        var paths = Stream.generate(() -> next(stopPredicate, pathResultBuilder));

        return ImmutableDijkstraResult
            .builder()
            .paths(paths)
            .build();
    }

    private PathResult next(LongPredicate stopPredicate, ImmutablePathResult.Builder pathResultBuilder) {
        while (!queue.isEmpty() && running()) {
            var node = queue.pop();
            var cost = queue.cost(node);
            visited.set(node);

            // For disconnected graphs, this will not reach 100%.
            progressLogger.logProgress(graph.degree(node));

            graph.forEachRelationship(
                node,
                1.0D,
                (source, target, weight) -> {
                    updateCost(source, target, weight + cost);
                    return true;
                }
            );

            if (stopPredicate.test(node)) {
                return pathResult(node, cost, pathResultBuilder);
            }
        }
        progressLogger.logFinish();
        return PathResult.EMPTY;
    }

    private void updateCost(long source, long target, double newCost) {
        // target has been visited, we already have a shortest path
        if (visited.get(target)) {
            return;
        }

        // we see target again
        if (queue.containsElement(target)) {
            // and found a shorter path to target
            if (newCost < queue.cost(target)) {
                path.put(target, source);
                queue.set(target, newCost);
            }
        } else {
            // we see target for the first time
            path.put(target, source);
            queue.add(target, newCost);
        }
    }

    private PathResult pathResult(long target, double cost, ImmutablePathResult.Builder pathResultBuilder) {
        var pathNodeIds = new LinkedList<Long>();
        var costs = new LinkedList<Double>();

        var lastNode = target;

        while (lastNode != PATH_END) {
            pathNodeIds.addFirst(lastNode);
            costs.addFirst(queue.cost(lastNode));
            lastNode = this.path.getOrDefault(lastNode, PATH_END);
        }

        return pathResultBuilder
            .index(pathIndex++)
            .targetNode(target)
            .totalCost(cost)
            .nodeIds(pathNodeIds)
            .costs(costs)
            .build();
    }

    @Override
    public Dijkstra me() {
        return this;
    }

    @Override
    public void release() {
        // We do not release, since the result
        // is lazily computed when the consumer
        // iterates over the stream.
    }
}

@ValueClass
interface DijkstraResult {

    Stream<PathResult> paths();

    @TestOnly
    default Set<PathResult> pathSet() {
        return paths().takeWhile(p -> p != PathResult.EMPTY).collect(Collectors.toSet());
    }
}
