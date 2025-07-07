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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.steiner.SteinerTreeResult;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class PathFindingComputeFacade {

    CompletableFuture<Stream<AllShortestPathsStreamResult>> allShortestPaths() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<BellmanFordResult> bellmanFord() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<HugeLongArray> breadthFirstSearch() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> deltaStepping() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<HugeLongArray> depthFirstSearch() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<SpanningTree> kSpanningTree() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> longestPath() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<Stream<long[]>> randomWalk() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<HugeAtomicLongArray> randomWalkCountingNodeVisits() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PrizeSteinerTreeResult> pcst() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathAStar() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathDijkstra() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathYens() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singleSourceShortestPathDijkstra() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<SpanningTree> spanningTree() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<SteinerTreeResult> steinerTree() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<TopologicalSortResult> topologicalSort() {
        throw new RuntimeException("Not yet implemented");
    }
}
