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
package org.neo4j.gds.procedures.algorithms;

import java.util.Arrays;
import java.util.Optional;

/**
 * This is a fundamental need: translating user input text into reliable identifiers.
 * Second level is identifiers to behaviour.
 * So it is a ladder: "gds.dijkstra" -> Dijkstra -> {validation,estimation,execution}
 * Enums are great because we can switch on them and get completion - no forgetting to map one of them.
 * I have one central, authoritative mapping. But I need to apply the mapping in different contexts,
 * sometimes as part of request processing, sometimes outside request scope. So switch statements in different places,
 * and the safety that I won't forget to map a key.
 */
public enum Algorithm {
    AStar("gds.shortestpath.astar"),
    BellmanFord("gds.bellmanford"),
    BetaClosenessCentrality("gds.beta.closeness"),
    BetweennessCentrality("gds.betweenness"),
    BFS("gds.bfs"),
    ClosenessCentrality("gds.closeness"),
    DeltaStepping("gds.allshortestpaths.delta"),
    DFS("gds.dfs"),
    Dijkstra("gds.shortestpath.dijkstra"),
    FilteredKNN("gds.knn.filtered"),
    FilteredNodeSimilarity("gds.nodesimilarity.filtered"),
    KNN("gds.knn"),
    NodeSimilarity("gds.nodesimilarity"),
    SingleSourceDijkstra("gds.allshortestpaths.dijkstra"),
    SpanningTree("gds.spanningtree"),
    SteinerTree("gds.steinertree"),
    Yens("gds.shortestpath.yens");

    private final String identifier;

    Algorithm(String identifier) {
        this.identifier = identifier;
    }

    /**
     * @throws java.lang.IllegalArgumentException if there wasn't a match
     */
    public static Algorithm from(CanonicalProcedureName canonicalProcedureName) {
        Optional<Algorithm> algorithm = Arrays.stream(values())
            .filter(a -> canonicalProcedureName.matches(a.identifier))
            .findFirst();

        if (algorithm.isEmpty())
            throw new IllegalArgumentException(canonicalProcedureName + " did not match an algorithm");

        return algorithm.get();
    }
}
