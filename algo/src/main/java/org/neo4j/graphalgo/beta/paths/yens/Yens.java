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
import org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public final class Yens extends Algorithm<Yens, DijkstraResult> {

    private static final LongHashSet EMPTY_SET = new LongHashSet(0);

    private final Dijkstra dijkstra;
    private final int k;

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
        // Init dijkstra algorithm for computing shortest paths
        var dijkstra = Dijkstra.sourceTarget(graph, config, progressLogger, tracker);

        return new Yens(
            dijkstra,
            config.k(),
            progressLogger
        );
    }

    private Yens(Dijkstra dijkstra, int k, ProgressLogger progressLogger) {
        this.k = k;
        // Track nodes and relationships that are skipped in a single iteration.
        // The content of these data structures is reset after each of k iterations.
        this.nodeBlackList = new LongScatterSet();
        this.relationshipBlackList = new LongObjectScatterMap<>();
        // set filter in Dijkstra to respect our blacklists
        this.dijkstra = dijkstra;
        dijkstra.withRelationshipFilter((source, target) ->
            !nodeBlackList.contains(target) &&
            !(relationshipBlackList.getOrDefault(source, EMPTY_SET).contains(target))
        );

        this.progressLogger = progressLogger;
    }

    @Override
    public DijkstraResult compute() {
        var kShortestPaths = new ArrayList<MutablePathResult>();
        // compute top 1 shortest path
        var shortestPath = dijkstra.compute().pathSet().stream().findFirst().orElse(PathResult.EMPTY);
        kShortestPaths.add(MutablePathResult.of(shortestPath));

        var candidates = new PriorityQueue<>(Comparator.comparingDouble(MutablePathResult::totalCost));

        for (int i = 1; i < k; i++) {
            var prevPath = kShortestPaths.get(i - 1);

            for (int n = 0; n < prevPath.nodeCount() - 2; n++) {
                var spurNode = prevPath.node(n);
                var rootPath = prevPath.subPath(n + 1);

                for (var path : kShortestPaths) {
                    // Filter relationships that are part of the previous
                    // shortest paths which share the same root path.
                    if (rootPath.matches(path, n)) {
                        var node2 = path.node(n + 1);

                        var neighbors = relationshipBlackList.get(spurNode);

                        if (neighbors == null) {
                            neighbors = new LongHashSet();
                            relationshipBlackList.put(spurNode, neighbors);
                        }
                        neighbors.add(node2);
                    }
                }

                // Filter nodes from root path to avoid cyclic path searches.
                for (int j = 0; j < n; j++) {
                    nodeBlackList.add(rootPath.node(j));
                }

                // Calculate the spur path from the spur node to the sink.
                dijkstra.clear();
                dijkstra.withSourceNode(spurNode);
                var spurPath = dijkstra.compute().paths().findFirst().orElse(PathResult.EMPTY);
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
        }

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
    }

}
