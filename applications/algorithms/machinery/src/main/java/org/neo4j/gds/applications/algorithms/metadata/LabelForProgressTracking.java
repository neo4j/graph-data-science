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

public enum LabelForProgressTracking {
    AllShortestPaths("All Shortest Paths"),
    ApproximateMaximumKCut("ApproxMaxKCut"),
    ArticleRank("ArticleRank"),
    AStar("AStar"),
    BellmanFord("Bellman-Ford"),
    BetaClosenessCentrality("Closeness Centrality (beta)"),
    BetweennessCentrality("Betweenness Centrality"),
    BFS("BFS"),
    CELF("CELF"),
    ClosenessCentrality("Closeness Centrality"),
    Conductance("Conductance"),
    DegreeCentrality("DegreeCentrality"),
    DeltaStepping("Delta Stepping"),
    DFS("DFS"),
    Dijkstra("Dijkstra"),
    EigenVector("EigenVector"),
    FilteredKNN("Filtered K-Nearest Neighbours"),
    FilteredNodeSimilarity("Filtered Node Similarity"),
    HarmonicCentrality("HarmonicCentrality"),
    K1Coloring("K1Coloring"),
    KCore("KCoreDecomposition"),
    KNN("K-Nearest Neighbours"),
    KSpanningTree("K Spanning Tree"),
    LongestPath("LongestPath"),
    NodeSimilarity("Node Similarity"),
    PageRank("PageRank"),
    RandomWalk("RandomWalk"),
    SingleSourceDijkstra("All Shortest Paths"),
    SpanningTree("SpanningTree"),
    SteinerTree("SteinerTree"),
    TopologicalSort("TopologicalSort"),
    WCC("WCC"),
    Yens("Yens");

    public final String value;

    LabelForProgressTracking(String value) {this.value = value;}

    public static LabelForProgressTracking from(Algorithm algorithm) {
        return switch (algorithm) {
            case AllShortestPaths -> AllShortestPaths;
            case ApproximateMaximumKCut -> ApproximateMaximumKCut;
            case ArticleRank -> ArticleRank;
            case AStar -> AStar;
            case BellmanFord -> BellmanFord;
            case BetaClosenessCentrality -> BetaClosenessCentrality;
            case BetweennessCentrality -> BetweennessCentrality;
            case BFS -> BFS;
            case CELF -> CELF;
            case ClosenessCentrality -> ClosenessCentrality;
            case Conductance -> Conductance;
            case DegreeCentrality -> DegreeCentrality;
            case DeltaStepping -> DeltaStepping;
            case DFS -> DFS;
            case Dijkstra -> Dijkstra;
            case EigenVector -> EigenVector;
            case FilteredKNN -> FilteredKNN;
            case FilteredNodeSimilarity -> FilteredNodeSimilarity;
            case HarmonicCentrality -> HarmonicCentrality;
            case K1Coloring -> K1Coloring;
            case KCore -> KCore;
            case KNN -> KNN;
            case KSpanningTree -> KSpanningTree;
            case LongestPath -> LongestPath;
            case NodeSimilarity -> NodeSimilarity;
            case PageRank -> PageRank;
            case RandomWalk -> RandomWalk;
            case SingleSourceDijkstra -> SingleSourceDijkstra;
            case SpanningTree -> SpanningTree;
            case SteinerTree -> SteinerTree;
            case TopologicalSort -> TopologicalSort;
            case WCC -> WCC;
            case Yens -> Yens;
        };
    }

    @Override
    public String toString() {
        return value;
    }
}
