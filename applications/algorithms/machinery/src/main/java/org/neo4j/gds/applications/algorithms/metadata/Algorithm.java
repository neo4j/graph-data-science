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
package org.neo4j.gds.applications.algorithms.metadata;

/**
 * This is a fundamental need: a stable, unique identifier to key things to an algorithm.
 * Enums are great because we can switch on them and get completion - no forgetting to map one of them.
 * I have one central authority. But I need to apply the mapping in different contexts,
 * sometimes as part of request processing, sometimes outside request scope. So switch statements in different places,
 * and the safety that I won't forget to map a key.
 */
public enum Algorithm {
    AllShortestPaths,
    ApproximateMaximumKCut,
    ArticleRank,
    AStar,
    BellmanFord,
    BetaClosenessCentrality,
    BetweennessCentrality,
    BFS,
    CELF,
    ClosenessCentrality,
    Conductance,
    DegreeCentrality,
    DeltaStepping,
    DFS,
    Dijkstra,
    EigenVector,
    FastRP,
    FilteredKNN,
    FilteredNodeSimilarity,
    GraphSage,
    GraphSageTrain,
    HarmonicCentrality,
    K1Coloring,
    KCore,
    KMeans,
    KNN,
    KSpanningTree,
    LabelPropagation,
    LCC,
    Leiden,
    Louvain,
    LongestPath,
    Modularity,
    ModularityOptimization,
    NodeSimilarity,
    PageRank,
    RandomWalk,
    SCC,
    SingleSourceDijkstra,
    SpanningTree,
    SteinerTree,
    TopologicalSort,
    TriangleCount,
    Triangles,
    WCC,
    Yens;
}
