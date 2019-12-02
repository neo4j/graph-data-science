/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.yens;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * Yen's k-shortest-paths Algorithm.
 *
 * The algorithm computes the k-shortest paths between start and goal node. due
 * to the workings of our weight logic we can either load the graph directed and
 * traverse incoming or outgoing directions or load it as undirected and traverse
 * outgoing relationships only. Direction.BOTH leads to incorrect results and is
 * therefore not supported.
 */
public class YensKShortestPaths extends LegacyAlgorithm<YensKShortestPaths> {

    private Dijkstra dijkstra;
    private Graph graph;
    private List<WeightedPath> shortestPaths;
    private PriorityQueue<WeightedPath> candidates;

    public YensKShortestPaths(Graph graph) {
        this.graph = graph;
        dijkstra = new Dijkstra(graph);
        shortestPaths = new ArrayList<>();
        candidates = new PriorityQueue<>(WeightedPath.comparator());
    }

    /**
     * retrieve the list of shortest paths
     * @return
     */
    public List<WeightedPath> getPaths() {
        return shortestPaths;
    }

    /**
     * compute at most k shortest paths between startNode and goalNode
     * while using only supplied traversal direction
     * @return itself
     */
    public YensKShortestPaths compute(long startNode, long goalNode, Direction direction, int k, int maxDepth) {
        yens(k,
                graph.toMappedNodeId(startNode),
                graph.toMappedNodeId(goalNode),
                direction,
                maxDepth);
        getProgressLogger().log(String.format("done.. found %d/%d paths", shortestPaths.size(), k));
        return this;
    }

    private void yens(int k, long start, long goal, Direction direction, int maxDepth) {
        final ProgressLogger progressLogger = getProgressLogger();
        // blacklist container for dijkstra
        final IntScatterSet nodeBlackList = new IntScatterSet();
        final LongScatterSet edgeBlackList = new LongScatterSet();
        // clear result of previous execution
        shortestPaths.clear();
        // equip dijkstra with a node and edge filter and set its traversal direction
        final Optional<WeightedPath> shortestPathOpt = dijkstra.withTerminationFlag(getTerminationFlag())
                .withDirection(direction)
                .withFilter(longToIntConsumer((s, t) ->
                        // set custom node filter
                        !nodeBlackList.contains(t) &&
                        // and edge filter by combining the nodeIds into a long
                        !edgeBlackList.contains(RawValues.combineIntInt(s, t))))
                .compute(start, goal, maxDepth);// compute the best shortest path first
        if (!shortestPathOpt.isPresent()) {
            // not a single path found
            return;
        }
        final WeightedPath shortestPath = shortestPathOpt.get();
        shortestPaths.add(shortestPath);
        progressLogger.log(String.format("found shortest path: %d nodes / %.2f weight",
                shortestPath.size(),
                shortestPath.getCost()));
        // keep running until k paths have been found or no further shortest path is possible
        for (int n = 1; n < k; n++) {
            // retrieve the last best shortest path
            final WeightedPath basePath = shortestPaths.get(shortestPaths.size() - 1);
            for (int i = basePath.size() - 2; i >= 0; i--) {
                // we don't alter the graph therefore we clear the filters at the beginning of each iteration.
                nodeBlackList.clear();
                edgeBlackList.clear();
                // Spur node is retrieved from the previous k-shortest path.
                final int spurNode = basePath.node(i);
                // The sequence of nodes from the source to the spur node of the previous k-shortest path.
                final WeightedPath rootPath = basePath
                        .pathTo(i)
                        .evaluateAndSetCost(graph);
                // check each of the known shortest paths
                for (Iterator<WeightedPath> iterator = shortestPaths.iterator(); iterator.hasNext(); ) {
                    final WeightedPath p = iterator.next();
                    if (rootPath.elementWiseEquals(p, i + 1)) {
                        // blacklist the rels that are part of the previous shortest paths with the same root path.
                        edgeBlackList.add(p.edge(i));
                    }
                }
                // blacklist nodes in rootPath if not spurNode to avoid cycles
                rootPath.forEachDo(rootPathNode -> {
                    if (rootPathNode != spurNode) {
                        nodeBlackList.add(rootPathNode);
                    }
                });
                // Calculate the spur path from the spur node to the goal node.
                int spurPathMaxDepth = maxDepth - rootPath.size() + 1; // + 1 is for dropped tail of root path
                final Optional<WeightedPath> spurPathOpt = dijkstra.compute(spurNode, goal, spurPathMaxDepth);
                // no path found, continue
                if (!spurPathOpt.isPresent()) {
                    continue;
                }
                // new candidate is the concatenation of rootPath and the spurPath.
                final WeightedPath concatenation = rootPath
                        .dropTail()
                        .concat(spurPathOpt.get());
                // already found?
                if (!candidates.contains(concatenation)) {
                    progressLogger.log(String.format("found candidate: %d nodes / %.2f weight",
                            concatenation.size(),
                            concatenation.getCost()));
                    candidates.add(concatenation);
                }
            }
            // no new candidates have been found.
            if (candidates.isEmpty()) {
                return;
            }
            // add the best candidate (with lowest weight) to the result set.
            final WeightedPath candidate = candidates.remove();
            progressLogger.log(String.format("found path: %d nodes / %.2f weight",
                    candidate.size(),
                    candidate.getCost()));
            shortestPaths.add(candidate);
        }
    }

    @Override
    public YensKShortestPaths me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        dijkstra = null;
        shortestPaths = null;
        candidates = null;
    }
}
