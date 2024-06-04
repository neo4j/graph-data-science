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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.procedures.algorithms.CanonicalProcedureName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the one place where we map procedure names to algorithms
 * NB: only algorithms that have a mutate mode
 */
final class MutateModeAlgorithmLibrary {
    private final Map<CanonicalProcedureName, Algorithm> knownAlgorithms;

    private MutateModeAlgorithmLibrary(Map<CanonicalProcedureName, Algorithm> knownAlgorithms) {
        this.knownAlgorithms = knownAlgorithms;
    }

    static MutateModeAlgorithmLibrary create() {
        var knownAlgorithms = new HashMap<CanonicalProcedureName, Algorithm>();

        Arrays.stream(Algorithm.values())
            .forEach(algorithm -> {
                var procedureName = algorithmToName(algorithm);

                // skip the algorithms that do not have a mutate mode
                if (procedureName == null) return;

                knownAlgorithms.put(procedureName, algorithm);
            });

        return new MutateModeAlgorithmLibrary(knownAlgorithms);
    }

    /**
     * @return the canonical procedure name, or null if the algorithm does not have a mutate mode
     */
    static CanonicalProcedureName algorithmToName(Algorithm algorithm) {
        return switch (algorithm) {
            case AllShortestPaths -> null;
            case ArticleRank -> CanonicalProcedureName.parse("gds.articleRank");
            case AStar -> CanonicalProcedureName.parse("gds.shortestPath.astar");
            case BellmanFord -> CanonicalProcedureName.parse("gds.bellmanFord");
            case BetaClosenessCentrality -> CanonicalProcedureName.parse("gds.beta.closeness");
            case BetweennessCentrality -> CanonicalProcedureName.parse("gds.betweenness");
            case BFS -> CanonicalProcedureName.parse("gds.bfs");
            case CELF -> CanonicalProcedureName.parse("gds.influenceMaximization.celf");
            case ClosenessCentrality -> CanonicalProcedureName.parse("gds.closeness");
            case DegreeCentrality -> CanonicalProcedureName.parse("gds.degree");
            case DeltaStepping -> CanonicalProcedureName.parse("gds.allShortestPaths.delta");
            case DFS -> CanonicalProcedureName.parse("gds.dfs");
            case Dijkstra -> CanonicalProcedureName.parse("gds.shortestPath.dijkstra");
            case EigenVector -> CanonicalProcedureName.parse("gds.eigenvector");
            case FilteredKNN -> CanonicalProcedureName.parse("gds.knn.filtered");
            case FilteredNodeSimilarity -> CanonicalProcedureName.parse("gds.nodeSimilarity.filtered");
            case HarmonicCentrality -> CanonicalProcedureName.parse("gds.closeness.harmonic");
            case KNN -> CanonicalProcedureName.parse("gds.knn");
            case KSpanningTree -> null;
            case LongestPath -> null;
            case NodeSimilarity -> CanonicalProcedureName.parse("gds.nodeSimilarity");
            case PageRank -> CanonicalProcedureName.parse("gds.pageRank");
            case RandomWalk -> null;
            case SingleSourceDijkstra -> CanonicalProcedureName.parse("gds.allShortestPaths.dijkstra");
            case SpanningTree -> CanonicalProcedureName.parse("gds.spanningTree");
            case SteinerTree -> CanonicalProcedureName.parse("gds.steinerTree");
            case TopologicalSort -> null;
            case Yens -> CanonicalProcedureName.parse("gds.shortestPath.yens");
        };
    }

    boolean contains(CanonicalProcedureName canonicalProcedureName) {
        return knownAlgorithms.containsKey(canonicalProcedureName);
    }

    Algorithm lookup(CanonicalProcedureName canonicalProcedureName) {
        return knownAlgorithms.get(canonicalProcedureName);
    }
}
