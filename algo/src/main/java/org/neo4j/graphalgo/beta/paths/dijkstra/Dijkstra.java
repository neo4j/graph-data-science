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
import com.carrotsearch.hppc.predicates.LongPredicate;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.AllShortestPathsBaseConfig;
import org.neo4j.graphalgo.beta.paths.ImmutablePathResult;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.beta.paths.ShortestPathBaseConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.queue.HugeLongPriorityQueue;

import java.util.stream.Stream;

public final class Dijkstra extends Algorithm<Dijkstra, DijkstraResult> {
    private static final long PATH_END = -1;

    private final Graph graph;
    private final LongPredicate stopPredicate;

    private long sourceNode;
    // priority queue
    private final HugeLongPriorityQueue queue;
    // predecessor map
    private final HugeLongLongMap predecessors;
    // True, iff the algo should track relationship ids.
    // A relationship id is the index of a relationship
    // in the adjacency list of a single node.
    private final boolean trackRelationships;
    // relationship ids (null, if trackRelationships is false)
    private final HugeLongLongMap relationships;
    // visited set
    private final BitSet visited;
    // path id increasing in order of exploration
    private long pathIndex;
    // returns true if the given relationship should be traversed
    private RelationshipFilter relationshipFilter = (sourceId, targetId, relationshipId) -> true;

    /**
     * Configure Dijkstra to compute at most one source-target shortest path.
     */
    public static Dijkstra sourceTarget(
        Graph graph,
        ShortestPathBaseConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        long sourceNode = graph.toMappedNodeId(config.sourceNode());
        long targetNode = graph.toMappedNodeId(config.targetNode());
        return new Dijkstra(
            graph,
            sourceNode,
            node -> node == targetNode,
            config.trackRelationships(),
            progressLogger,
            tracker
        );
    }

    /**
     * Configure Dijkstra to compute all single-source shortest path.
     */
    public static Dijkstra singleSource(
        Graph graph,
        AllShortestPathsBaseConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        return new Dijkstra(graph,
            graph.toMappedNodeId(config.sourceNode()),
            node -> true,
            config.trackRelationships(),
            progressLogger,
            tracker
        );
    }

    public static MemoryEstimation memoryEstimation() {
        return memoryEstimation(false);
    }

    public static MemoryEstimation memoryEstimation(boolean trackRelationships) {
        var builder = MemoryEstimations.builder(Dijkstra.class)
            .add("priority queue", HugeLongPriorityQueue.memoryEstimation())
            .add("reverse path", HugeLongLongMap.memoryEstimation());
        if (trackRelationships) {
            builder.add("relationship ids", HugeLongLongMap.memoryEstimation());
        }
        return builder
            .perNode("visited set", MemoryUsage::sizeOfBitset)
            .build();
    }

    private Dijkstra(
        Graph graph,
        long sourceNode,
        LongPredicate stopPredicate,
        boolean trackRelationships,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.sourceNode = sourceNode;
        this.stopPredicate = stopPredicate;
        this.trackRelationships = trackRelationships;
        this.queue = HugeLongPriorityQueue.min(graph.nodeCount());
        this.predecessors = new HugeLongLongMap(tracker);
        this.relationships = trackRelationships ? new HugeLongLongMap(tracker) : null;
        this.visited = new BitSet();
        this.pathIndex = 0L;
        this.progressLogger = progressLogger;
    }

    public Dijkstra withSourceNode(long sourceNode) {
        this.sourceNode = sourceNode;
        return this;
    }

    public Dijkstra withRelationshipFilter(RelationshipFilter relationshipFilter) {
        this.relationshipFilter = relationshipFilter;
        return this;
    }

    // Resets the internal state of the algorithm.
    public void clear() {
        queue.clear();
        visited.clear();
        predecessors.clear();
        if (trackRelationships) {
            relationships.clear();
        }
    }

    public DijkstraResult compute() {
        progressLogger.logStart();

        queue.add(sourceNode, 0.0);

        var pathResultBuilder = ImmutablePathResult.builder()
            .sourceNode(sourceNode);

        var paths = Stream
            .generate(() -> next(stopPredicate, pathResultBuilder))
            .takeWhile(pathResult -> pathResult != PathResult.EMPTY);

        return ImmutableDijkstraResult
            .builder()
            .paths(paths)
            .build();
    }

    private PathResult next(LongPredicate stopPredicate, ImmutablePathResult.Builder pathResultBuilder) {
        var relationshipId = new MutableInt();

        while (!queue.isEmpty() && running()) {
            var node = queue.pop();
            var cost = queue.cost(node);
            visited.set(node);

            // For disconnected graphs, this will not reach 100%.
            progressLogger.logProgress(graph.degree(node));

            relationshipId.setValue(0);
            graph.forEachRelationship(
                node,
                1.0D,
                (source, target, weight) -> {
                    if (relationshipFilter.test(source, target, relationshipId.longValue())) {
                        updateCost(source, target, relationshipId.intValue(), weight + cost);
                    }
                    relationshipId.increment();
                    return true;
                }
            );

            if (stopPredicate.apply(node)) {
                return pathResult(node, pathResultBuilder);
            }
        }
        progressLogger.logFinish();
        return PathResult.EMPTY;
    }

    private void updateCost(long source, long target, long relationshipId, double newCost) {
        // target has been visited, we already have a shortest path
        if (visited.get(target)) {
            return;
        }

        if (!queue.containsElement(target)) {
            // we see target for the first time
            queue.add(target, newCost);
            predecessors.put(target, source);
            if (trackRelationships) {
                relationships.put(target, relationshipId);
            }
        } else if (newCost < queue.cost(target)) {
            // we see target again and found a shorter path to target
            queue.set(target, newCost);
            predecessors.put(target, source);
            if (trackRelationships) {
                relationships.put(target, relationshipId);
            }
        }
    }

    private static final long[] EMPTY_ARRAY = new long[0];

    private PathResult pathResult(long target, ImmutablePathResult.Builder pathResultBuilder) {
        // TODO: use LongArrayList and then ArrayUtils.reverse
        var pathNodeIds = new LongArrayDeque();
        var relationshipIds = trackRelationships ? new LongArrayDeque() : null;
        var costs = new DoubleArrayDeque();

        var lastNode = target;
        var prevNode = lastNode;

        while (lastNode != PATH_END) {
            pathNodeIds.addFirst(lastNode);
            costs.addFirst(queue.cost(lastNode));
            prevNode = lastNode;
            lastNode = this.predecessors.getOrDefault(lastNode, PATH_END);
            if (trackRelationships && lastNode != PATH_END) {
                relationshipIds.addFirst(relationships.getOrDefault(prevNode, PATH_END));
            }
        }

        return pathResultBuilder
            .index(pathIndex++)
            .targetNode(target)
            .nodeIds(pathNodeIds.toArray())
            .relationshipIds(trackRelationships ? relationshipIds.toArray() : EMPTY_ARRAY)
            .costs(costs.toArray())
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

    @FunctionalInterface
    public interface RelationshipFilter {
        boolean test(long source, long target, long relationshipId);
    }
}
