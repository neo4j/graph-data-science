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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.config.ImmutableShortestPathYensBaseConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class Yens extends Algorithm<DijkstraResult> {

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
        ProgressTracker progressTracker
    ) {
        // If the input graph is a multi-graph, we need to track
        // parallel relationships. This is necessary since shortest
        // paths can visit the same nodes via different relationships.

        System.out.println(graph.schema().relationshipSchema().toMap());
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                System.out.println(s + "-[" + w + "]->" + t);
                return true;
            });
            return true;
        });

        var newConfig = ImmutableShortestPathYensBaseConfig
            .builder()
            .from(config)
            .trackRelationships(graph.isMultiGraph())
            .build();
        // Init dijkstra algorithm for computing shortest paths
        var dijkstra = Dijkstra.sourceTarget(graph, newConfig, Optional.empty(), progressTracker);
        return new Yens(graph, dijkstra, newConfig, progressTracker);
    }

    // The blacklists contain nodes and relationships that are
    // "forbidden" to be traversed by Dijkstra. The size of that
    // blacklist is not known upfront and depends on the length
    // of the found paths.
    private static final long AVERAGE_BLACKLIST_SIZE = 10L;

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(Yens.class.getSimpleName())
            .add("Dijkstra", Dijkstra.memoryEstimation(false))
            .fixed("nodeBlackList", MemoryUsage.sizeOfLongArray(AVERAGE_BLACKLIST_SIZE))
            .fixed("relationshipBlackList", MemoryUsage.sizeOfLongArray(AVERAGE_BLACKLIST_SIZE * 2))
            .build();
    }

    private Yens(Graph graph, Dijkstra dijkstra, ShortestPathYensBaseConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
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
            !(relationshipBlackList.getOrDefault(source, EMPTY_SET).contains(relationshipId)) &&
            !(relationshipBlackList.getOrDefault(source, EMPTY_SET).contains(-target - 1)));
    }


    @Override
    public DijkstraResult compute() {
        progressTracker.beginSubTask();
        var kShortestPaths = new ArrayList<MutablePathResult>();
        // compute top 1 shortest path
        progressTracker.beginSubTask();
        progressTracker.beginSubTask();
        var shortestPath = computeDijkstra(config.sourceNode());

        // no shortest path has been found
        if (shortestPath.isEmpty()) {
            progressTracker.endSubTask();
            progressTracker.endSubTask();
            return new DijkstraResult(Stream.empty(), progressTracker::endSubTask);
        }

        progressTracker.endSubTask();

        kShortestPaths.add(MutablePathResult.of(shortestPath.get()));

        PriorityQueue<MutablePathResult> candidates = initCandidatesQueue();

        for (int i = 1; i < config.k(); i++) {
            progressTracker.beginSubTask();
            var prevPath = kShortestPaths.get(i - 1);

            for (int n = 0; n < prevPath.nodeCount() - 1; n++) {
                var spurNode = prevPath.node(n);
                var rootPath = prevPath.subPath(n + 1);

                for (var path : kShortestPaths) {
                    // Filter relationships that are part of the previous
                    // shortest paths which share the same root path.
                    if (rootPath.matchesExactly(path, n + 1)) {
                        System.out.println(i + ": " + rootPath + " |" + prevPath);
                        var relationshipId = graph.isMultiGraph() ? path.relationship(n) : -(1 + path.node(n + 1));

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
                dijkstra.resetTraversalState();
                dijkstra.withSourceNode(spurNode);
                var spurPath = computeDijkstra(graph.toOriginalNodeId(spurNode));

                // Clear filters for next spur node
                nodeBlackList.clear();
                relationshipBlackList.clear();

                // No new candidate from this spur node, continue with next node.
                if (spurPath.isEmpty()) {
                    continue;
                }

                // Entire path is made up of the root path and spur path.
                rootPath.append(MutablePathResult.of(spurPath.get()), graph.isMultiGraph());
                // Add the potential k-shortest path to the heap.
                if (!candidates.contains(rootPath)) {
                    candidates.add(rootPath);
                }
            }

            progressTracker.endSubTask();

            if (candidates.isEmpty()) {
                break;
            }

            kShortestPaths.add(candidates.poll().withIndex(i));
        }
        progressTracker.endSubTask();

        progressTracker.endSubTask();
        System.out.println("----");
        for (var path : kShortestPaths) {
            System.out.println(path);
        }
        return new DijkstraResult(kShortestPaths.stream().map(MutablePathResult::toPathResult));
    }

    @NotNull
    private PriorityQueue<MutablePathResult> initCandidatesQueue() {
        return new PriorityQueue<>(Comparator
            .comparingDouble(MutablePathResult::totalCost)
            .thenComparingInt(MutablePathResult::nodeCount));
    }

    @Override
    public void release() {
        dijkstra.release();
        nodeBlackList.release();
        relationshipBlackList.release();
    }

    private Optional<PathResult> computeDijkstra(long sourceNode) {
        progressTracker.logInfo(formatWithLocale("Dijkstra for spur node %d", sourceNode));
        return dijkstra.compute().findFirst();
    }

}
