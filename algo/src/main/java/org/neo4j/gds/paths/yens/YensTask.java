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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectScatterMap;
import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.config.ImmutableShortestPathDijkstraStreamConfig;

import java.util.ArrayList;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.ToLongBiFunction;

import static org.neo4j.gds.paths.yens.Yens.EMPTY_SET;

public class YensTask implements Runnable {
    private final Graph localGraph;
    // Track nodes and relationships that are skipped in a single iteration.
    // The content of these data structures is reset after each of k iterations.
    private final LongScatterSet nodeAvoidList;
    private final LongObjectScatterMap<LongHashSet> relationshipAvoidList;
    private final Dijkstra localDijkstra;
    private final ArrayList<MutablePathResult> kShortestPaths;
    private MutablePathResult previousPath;
    private final ReentrantLock candidateLock;
    private final PriorityQueue<MutablePathResult> candidates;
    private AtomicInteger currentSpurIndexId;
    private int maxLength;
    private  final boolean trackRelationships;
    private final ToLongBiFunction
        <MutablePathResult, Integer> relationshipAvoidMapper;
    private final BiConsumer<MutablePathResult, PathResult> pathAppender;

    YensTask(
        Graph graph,
        long targetNode,
        ArrayList<MutablePathResult> kShortestPaths,
        ReentrantLock candidateLock,
        PriorityQueue<MutablePathResult> candidates,
        AtomicInteger currentSpurIndexId,
        boolean trackRelationships
    ) {
        this.currentSpurIndexId = currentSpurIndexId;
        this.localGraph = graph;
        this.nodeAvoidList = new LongScatterSet();
        this.relationshipAvoidList = new LongObjectScatterMap<>();
        this.trackRelationships=trackRelationships;
        var newConfig = ImmutableShortestPathDijkstraStreamConfig
            .builder()
            .sourceNode(targetNode)
            .targetNode(targetNode)
            .trackRelationships(trackRelationships)
            .build();
        this.localDijkstra = Dijkstra.sourceTarget(
            localGraph,
            newConfig,
            Optional.empty(),
            ProgressTracker.NULL_TRACKER
        );
        localDijkstra.withRelationshipFilter((source, target, relationshipId) ->
            !nodeAvoidList.contains(target)
            && !shouldAvoidRelationship(source,target,relationshipId)
        );

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
    }

    void withPreviousPath(MutablePathResult previousPath) {
        this.previousPath = previousPath;
        this.maxLength = previousPath.nodeCount() - 1;
    }

    @Override
    public void run() {
        int indexId = currentSpurIndexId.getAndIncrement();
        while (indexId < maxLength) {
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
            storePath(rootPath, spurPath);
        }

    }

    private void createFilters(MutablePathResult rootPath, long spurNode, int indexId) {
        for (var path : kShortestPaths) {
            // Filter relationships that are part of the previous
            // shortest paths which share the same root path.
            System.out.println(path.toString()+" "+rootPath.toString());
            if (rootPath.matchesExactly(path, indexId + 1)) {

                var avoidId = relationshipAvoidMapper.applyAsLong(path,indexId);

                var neighbors = relationshipAvoidList.get(spurNode);

                if (neighbors == null) {
                    neighbors = new LongHashSet();
                    relationshipAvoidList.put(spurNode, neighbors);
                }
                neighbors.add(avoidId);
            }
        }
        // Filter nodes from root path to avoid cyclic path searches.
        for (int j = 0; j < indexId; j++) {
            nodeAvoidList.add(rootPath.node(j));
        }
    }

    private Optional<PathResult> computeDijkstra(long spurNode) {
        localDijkstra.resetTraversalState();
        localDijkstra.withSourceNode(spurNode);
        var result = localDijkstra.compute().findFirst();
        // Clear filters for next spur node
        nodeAvoidList.clear();
        relationshipAvoidList.clear();
        return result;
    }

    private void storePath(MutablePathResult rootPath, Optional<PathResult> spurPath) {

        // Entire path is made up of the root path and spur path.
        pathAppender.accept(rootPath, spurPath.get());
        // Add the potential k-shortest path to the heap.
        candidateLock.lock();
        if (!candidates.contains(rootPath)) {
            candidates.add(rootPath);
        }
        candidateLock.unlock();

    }
    private boolean shouldAvoidRelationship(long source, long target, long relationshipId) {
        long forbidden = trackRelationships
            ? relationshipId
            : target;
        return relationshipAvoidList.getOrDefault(source, EMPTY_SET).contains(forbidden);

    }

}
