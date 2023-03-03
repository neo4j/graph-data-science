/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.paths.yens;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.ToLongBiFunction;

public class YensTask implements Runnable {
    private final Graph localGraph;
    // Track nodes and relationships that are skipped in a single iteration.
    // The content of these data structures is reset after each of k iterations.
    private final long[] neighbors;
    private @Nullable Dijkstra localDijkstra;
    private final ArrayList<MutablePathResult> kShortestPaths;
    private MutablePathResult previousPath;
    private final ReentrantLock candidateLock;
    private final PriorityQueue<MutablePathResult> candidates;
    private AtomicInteger currentSpurIndexId;
    private int maxLength;
    private final boolean trackRelationships;
    private final ToLongBiFunction
        <MutablePathResult, Integer> relationshipAvoidMapper;
    private final BiConsumer<MutablePathResult, PathResult> pathAppender;
    private final long targetNode;

    private long filteringSpurNode;
    private int allNeighbors;
    private int neighborIndex;

    public static MemoryEstimation memoryEstimation(int k, boolean trackRelationships) {
        return MemoryEstimations.builder(YensTask.class)
            .fixed("neighbors", MemoryUsage.sizeOfLongArray(k))
            .add("Dijkstra", Dijkstra.memoryEstimation(trackRelationships)).build();
    }

    YensTask(
        Graph graph,
        long targetNode,
        ArrayList<MutablePathResult> kShortestPaths,
        ReentrantLock candidateLock,
        PriorityQueue<MutablePathResult> candidates,
        AtomicInteger currentSpurIndexId,
        boolean trackRelationships,
        int k
    ) {
        this.currentSpurIndexId = currentSpurIndexId;
        this.localGraph = graph;
        this.trackRelationships = trackRelationships;
        this.targetNode = targetNode;
        this.localDijkstra = null;


        this.kShortestPaths = kShortestPaths;
        this.candidates = candidates;
        this.candidateLock = candidateLock;

        if (trackRelationships) {
            // if we are in a multi-graph, we  must store the relationships ids as they are
            //since two nodes may be connected by multiple relationships and we must know which to avoid
            relationshipAvoidMapper = (path, position) -> path.relationship(position);
            pathAppender = (rootPath, spurPath) -> rootPath.append(MutablePathResult.of(spurPath));

        } else {
            //otherwise the graph has surely no parallel edges, we do not need to explicitly store relationship ids
            //we can just store endpoints, so that we know which nodes a node should avoid
            relationshipAvoidMapper = (path, position) -> path.node(position + 1);
            pathAppender = (rootPath, spurPath) -> rootPath.appendWithoutRelationshipIds(MutablePathResult.of(spurPath));
        }

        this.neighbors = new long[k];
    }

    void withPreviousPath(MutablePathResult previousPath) {
        this.previousPath = previousPath;
        this.maxLength = previousPath.nodeCount() - 1;
    }

    @Override
    public void run() {
        int indexId = currentSpurIndexId.getAndIncrement();
        while (indexId < maxLength) {
            if (localDijkstra == null) {
                setupDijkstra();
            }
            process(indexId);
            indexId = currentSpurIndexId.getAndIncrement();
        }
    }

    private void process(int indexId) {
        var spurNode = previousPath.node(indexId);
        var rootPath = previousPath.subPath(indexId + 1);

        createFilters(rootPath, spurNode, indexId);

        // Calculate the spur path from the spur node to the sink.
        var spurPath = computeDijkstra(spurNode);

        // No new candidate from this spur node, continue with next node.
        if (!spurPath.isEmpty()) {
            storePath(indexId, rootPath, spurPath);
        }

    }

    private void createFilters(MutablePathResult rootPath, long spurNode, int indexId) {
        //clean all filters
        localDijkstra.resetTraversalState();
        allNeighbors = 0;
        neighborIndex = 0;
        filteringSpurNode = spurNode;

        for (var path : kShortestPaths) {
            // Filter relationships that are part of the previous
            // shortest paths which share the same root path.
            System.out.println(path.toString() + " " + rootPath.toString());
            if (rootPath.matchesExactly(path, indexId + 1)) {
                var avoidId = relationshipAvoidMapper.applyAsLong(path, indexId);
                neighbors[allNeighbors++] = avoidId;
            }
        }
        Arrays.sort(neighbors, 0, allNeighbors);
        // Filter nodes from root path to avoid cyclic path searches.
        for (int j = 0; j < indexId; j++) {
            localDijkstra.withVisited(rootPath.node(j));
        }
    }

    private Optional<PathResult> computeDijkstra(long spurNode) {
        localDijkstra.withSourceNode(spurNode);
        var result = localDijkstra.compute().findFirst();
        return result;
    }

    private void storePath(int indexId, MutablePathResult rootPath, Optional<PathResult> spurPath) {

        // Entire path is made up of the root path and spur path.
        pathAppender.accept(rootPath, spurPath.get());
        rootPath.withIndex(indexId);
        // Add the potential k-shortest path to the heap.
        candidateLock.lock();
        if (!candidates.contains(rootPath)) {
            candidates.add(rootPath);
        }
        candidateLock.unlock();

    }

    private boolean validRelationship(long source, long target, long relationshipId) {
        if (source == filteringSpurNode) {

            long forbidden = trackRelationships
                ? relationshipId
                : target;

            if (neighborIndex == allNeighbors) return true;

            while (neighbors[neighborIndex] < forbidden) {
                if (++neighborIndex == allNeighbors) {
                    return true;
                }
            }

            return neighbors[neighborIndex] != forbidden;
        }

        return true;

    }

    private void setupDijkstra() {

        this.localDijkstra = Dijkstra.sourceTarget(
            localGraph,
            Yens.dijkstraConfig(targetNode, localGraph.isMultiGraph()),
            Optional.empty(),
            ProgressTracker.NULL_TRACKER
        );

        localDijkstra.withRelationshipFilter((source, target, relationshipId) ->
            validRelationship(source, target, relationshipId)
        );
    }

}
