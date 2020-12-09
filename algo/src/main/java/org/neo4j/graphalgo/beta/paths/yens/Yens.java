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
package org.neo4j.graphalgo.beta.paths.yens;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectScatterMap;
import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.Dijkstra;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.ImmutableDijkstraResult;
import org.neo4j.graphalgo.beta.paths.yens.config.ImmutableShortestPathYensBaseConfig;
import org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class Yens extends Algorithm<Yens, DijkstraResult> {

    private static final LongHashSet EMPTY_SET = new LongHashSet(0);

    private final Graph graph;
    private final ShortestPathYensBaseConfig config;
    private final Dijkstra dijkstra;

    private final LongScatterSet nodeBlackList;
    private final LongObjectScatterMap<LongHashSet> relationshipBlackList;

    /**
     * Configure Yens to compute at most one source-target shortest path.
     */
    public static Yens sourceTarget(
        Graph graph,
        ShortestPathYensBaseConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        // If the input graph is a multi-graph, we need to track
        // parallel relationships. This is necessary since shortest
        // paths can visit the same nodes via different relationships.
        var newConfig = ImmutableShortestPathYensBaseConfig
            .builder()
            .from(config)
            .trackRelationships(graph.isMultiGraph())
            .build();
        // Init dijkstra algorithm for computing shortest paths
        var dijkstra = Dijkstra.sourceTarget(graph, newConfig, progressLogger, tracker);
        return new Yens(graph, dijkstra, newConfig, progressLogger);
    }

    // The blacklists contain nodes and relationships that are
    // "forbidden" to be traversed by Dijkstra. The size of that
    // blacklist is not known upfront and depends on the length
    // of the found paths.
    private static final long AVERAGE_BLACKLIST_SIZE = 10L;

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(Yens.class)
            .add("Dijkstra", Dijkstra.memoryEstimation())
            .fixed("nodeBlackList", MemoryUsage.sizeOfLongArray(AVERAGE_BLACKLIST_SIZE))
            .fixed("relationshipBlackList", MemoryUsage.sizeOfLongArray(AVERAGE_BLACKLIST_SIZE * 2))
            .build();
    }

    private Yens(Graph graph, Dijkstra dijkstra, ShortestPathYensBaseConfig config, ProgressLogger progressLogger) {
        this.graph = graph;
        this.config = config;
        // Track nodes and relationships that are skipped in a single iteration.
        // The content of these data structures is reset after each of k iterations.
        this.nodeBlackList = new LongScatterSet();
        this.relationshipBlackList = new LongObjectScatterMap<>();
        // set filter in Dijkstra to respect our blacklists
        this.dijkstra = dijkstra;
        dijkstra.withRelationshipFilter((source, target, relationshipId) ->
            !nodeBlackList.contains(target) &&
            !(relationshipBlackList.getOrDefault(source, EMPTY_SET).contains(relationshipId))
        );

        this.progressLogger = progressLogger;
    }

    @Override
    public DijkstraResult compute() {
        progressLogger.logStart();
        var kShortestPaths = new ArrayList<MutablePathResult>();
        // compute top 1 shortest path
        logStart(1);
        var shortestPath = computeDijkstra(config.sourceNode());
        logFinish(1);

        // no shortest path has been found
        if (shortestPath == PathResult.EMPTY) {
            return ImmutableDijkstraResult.of(Stream.of(shortestPath));
        }

        kShortestPaths.add(MutablePathResult.of(shortestPath));

        PriorityQueue<MutablePathResult> candidates = new PriorityQueue<>(Comparator.comparingDouble(MutablePathResult::totalCost));

        for (int i = 1; i < config.k(); i++) {
            logStart(i + 1);
            var prevPath = kShortestPaths.get(i - 1);

            for (int n = 0; n < prevPath.nodeCount() - 2; n++) {
                var spurNode = prevPath.node(n);
                var rootPath = prevPath.subPath(n + 1);

                for (var path : kShortestPaths) {
                    // Filter relationships that are part of the previous
                    // shortest paths which share the same root path.
                    if (rootPath.matches(path, n)) {
                        var relationshipId = path.relationship(n);

                        var neighbors = relationshipBlackList.get(spurNode);

                        if (neighbors == null) {
                            neighbors = new LongHashSet();
                            relationshipBlackList.put(spurNode, neighbors);
                        }
                        neighbors.add(relationshipId);
                    }
                }

                // Filter nodes from root path to avoid cyclic path searches.
                for (int j = 0; j < n; j++) {
                    nodeBlackList.add(rootPath.node(j));
                }

                // Calculate the spur path from the spur node to the sink.
                dijkstra.clear();
                dijkstra.withSourceNode(spurNode);
                var spurPath = computeDijkstra(graph.toOriginalNodeId(spurNode));
                // No new candidate from this spur node, continue with next node.
                if (spurPath == PathResult.EMPTY) {
                    continue;
                }

                // Entire path is made up of the root path and spur path.
                rootPath.append(MutablePathResult.of(spurPath));
                // Add the potential k-shortest path to the heap.
                if (!candidates.contains(rootPath)) {
                    candidates.add(rootPath);
                }

                // Clear filters for next spur node
                nodeBlackList.clear();
                relationshipBlackList.clear();
            }

            if (candidates.isEmpty()) {
                break;
            }

            kShortestPaths.add(candidates.poll().withIndex(i));
            logFinish(i + 1);
        }

        progressLogger.logFinish();

        return ImmutableDijkstraResult
            .builder()
            .paths(kShortestPaths.stream().map(MutablePathResult::toPathResult))
            .build();
    }

    @Override
    public Yens me() {
        return this;
    }

    @Override
    public void release() {
        dijkstra.release();
        nodeBlackList.release();
        relationshipBlackList.release();
    }

    private void logStart(int iteration) {
        progressLogger.logMessage(formatWithLocale(":: Start searching path %d of %d", iteration, config.k()));
    }

    private void logFinish(int iteration) {
        progressLogger.logMessage(formatWithLocale(":: Finished searching path %d of %d", iteration, config.k()));
    }

    private PathResult computeDijkstra(long sourceNode) {
        progressLogger.logMessage(formatWithLocale(":: Start Dijkstra for spur node %d", sourceNode));
        progressLogger.setTask("Dijkstra");
        progressLogger.reset(graph.relationshipCount());
        var pathResult = dijkstra.compute().pathSet().stream().findFirst().orElse(PathResult.EMPTY);
        progressLogger.setTask("Yens");
        return pathResult;
    }

}
