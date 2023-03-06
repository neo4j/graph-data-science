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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class YensTask implements Runnable {
    private final Graph localGraph;
    // Track nodes and relationships that are skipped in a single iteration.
    // The content of these data structures is reset after each of k iterations.
    private @Nullable Dijkstra localDijkstra;
    private final boolean trackRelationships;
    private final long targetNode;
    //Dijkstra filter
    private final RelationshipFilterer relationshipFilterer;

    private MutablePathResult previousPath;
    private AtomicInteger currentSpurIndexId;

    private final ArrayList<MutablePathResult> kShortestPaths;
    private final CandidatePathsPriorityQueue candidatePathsQueue;
    private final BiConsumer<MutablePathResult, PathResult> pathAppender;

    public static MemoryEstimation memoryEstimation(int k, boolean trackRelationships) {
        return MemoryEstimations.builder(YensTask.class)
            .fixed("neighbors", MemoryUsage.sizeOfLongArray(k))
            .add("Dijkstra", Dijkstra.memoryEstimation(trackRelationships)).build();
    }

    YensTask(
        Graph graph,
        long targetNode,
        ArrayList<MutablePathResult> kShortestPaths,
        CandidatePathsPriorityQueue candidatePathsQueue,
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
        this.candidatePathsQueue = candidatePathsQueue;

        this.relationshipFilterer = new RelationshipFilterer(k, trackRelationships);
        if (trackRelationships) {
            pathAppender = (rootPath, spurPath) -> rootPath.append(MutablePathResult.of(spurPath));
        } else {
            pathAppender = (rootPath, spurPath) -> rootPath.appendWithoutRelationshipIds(MutablePathResult.of(spurPath));
        }
    }

    void withPreviousPath(MutablePathResult previousPath) {
        this.previousPath = previousPath;
    }

    @Override
    public void run() {
        int indexId = currentSpurIndexId.getAndIncrement();
        int maxLength = previousPath.nodeCount() - 1; //-1 is because in source-a1-a2-t path we ignore t
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
        relationshipFilterer.setFilter(spurNode);

        for (var path : kShortestPaths) {
            // Filter relationships that are part of the previous
            // shortest paths which share the same root path.
            if (rootPath.matchesExactly(path, indexId + 1)) {
                relationshipFilterer.addBlockingNeighbor(path, indexId);
            }
        }
        relationshipFilterer.prepare();
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

        //https://en.wikipedia.org/wiki/Yen%27s_algorithm#Lawler's_modification
        //We store the index of the spur node
        //so that if this path ever gets selected, we know where to start from
        rootPath.withIndex(indexId);
        // Add the potential k-shortest path to the heap.
        candidatePathsQueue.addPath(rootPath);

    }


    private void setupDijkstra() {

        this.localDijkstra = Dijkstra.sourceTarget(
            localGraph,
            Yens.dijkstraConfig(targetNode, trackRelationships),
            Optional.empty(),
            ProgressTracker.NULL_TRACKER
        );

        localDijkstra.withRelationshipFilter((source, target, relationshipId) ->
            relationshipFilterer.validRelationship(source, target, relationshipId)
        );
    }

}
