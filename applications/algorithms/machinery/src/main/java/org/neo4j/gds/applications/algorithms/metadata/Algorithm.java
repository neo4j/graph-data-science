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
    AllShortestPaths("All Shortest Paths"),
    ApproximateMaximumKCut("ApproxMaxKCut"),
    ArticleRank("ArticleRank"),
    ArticulationPoints("Articulation Points"),
    AStar("AStar"),
    BellmanFord("Bellman-Ford"),
    BetaClosenessCentrality("Closeness Centrality (beta)"),
    BetweennessCentrality("Betweenness Centrality"),
    BFS("BFS"),
    Bridges("Bridges"),
    CELF("CELF"),
    ClosenessCentrality("Closeness Centrality"),
    CollapsePath("CollapsePath"),
    Conductance("Conductance"),
    DegreeCentrality("DegreeCentrality"),
    DeltaStepping("Delta Stepping"),
    DFS("DFS"),
    Dijkstra("Dijkstra"),
    EigenVector("EigenVector"),
    FastRP("FastRP"),
    FilteredKNN("Filtered K-Nearest Neighbours"),
    FilteredNodeSimilarity("Filtered Node Similarity"),
    GraphSage("GraphSage"),
    GraphSageTrain("GraphSageTrain"),
    HarmonicCentrality("HarmonicCentrality"),
    HashGNN("HashGNN"),
    IndexInverse("IndexInverse"),
    K1Coloring("K1Coloring"),
    KCore("KCoreDecomposition"),
    KGE("KGE"),
    KMeans("K-Means"),
    KNN("K-Nearest Neighbours"),
    KSpanningTree("K Spanning Tree"),
    LabelPropagation("Label Propagation"),
    LCC("LocalClusteringCoefficient"),
    Leiden("Leiden"),
    Louvain("Louvain"),
    LongestPath("LongestPath"),
    Modularity("Modularity"),
    ModularityOptimization("ModularityOptimization"),
    NodeSimilarity("Node Similarity"),
    Node2Vec("Node2Vec"),
    PageRank("PageRank"),
    RandomWalk("RandomWalk"),
    ScaleProperties("ScaleProperties"),
    SCC("SCC"),
    SingleSourceDijkstra("All Shortest Paths"),
    SpanningTree("SpanningTree"),
    SteinerTree("SteinerTree"),
    TopologicalSort("TopologicalSort"),
    ToUndirected("ToUndirected"),
    TriangleCount("TriangleCount"),
    Triangles("Triangles"),
    WCC("WCC"),
    Yens("Yens");

    public final String labelForProgressTracking;

    Algorithm(String labelForProgressTracking) {
        this.labelForProgressTracking = labelForProgressTracking;
    }
}
