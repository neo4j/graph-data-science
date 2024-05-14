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
    AStar("AStar"),
    BellmanFord("Bellman-Ford"),
    BetaClosenessCentrality("Closeness Centrality (beta)"),
    BetweennessCentrality("Betweenness Centrality"),
    BFS("BFS"),
    ClosenessCentrality("Closeness Centrality"),
    DegreeCentrality("DegreeCentrality"),
    DeltaStepping("Delta Stepping"),
    DFS("DFS"),
    Dijkstra("Dijkstra"),
    FilteredKNN("Filtered K-Nearest Neighbours"),
    FilteredNodeSimilarity("Filtered Node Similarity"),
    KNN("K-Nearest Neighbours"),
    KSpanningTree("K Spanning Tree"),
    LongestPath("LongestPath"),
    NodeSimilarity("Node Similarity"),
    RandomWalk("RandomWalk"),
    SingleSourceDijkstra("All Shortest Paths"),
    SpanningTree("SpanningTree"),
    SteinerTree("SteinerTree"),
    TopologicalSort("TopologicalSort"),
    Yens("Yens");

    public final String value;

    LabelForProgressTracking(String value) {this.value = value;}

    public static LabelForProgressTracking from(Algorithm algorithm) {
        return switch (algorithm) {
            case AllShortestPaths -> AllShortestPaths;
            case AStar -> AStar;
            case BellmanFord -> BellmanFord;
            case BetaClosenessCentrality -> BetaClosenessCentrality;
            case BetweennessCentrality -> BetweennessCentrality;
            case BFS -> BFS;
            case ClosenessCentrality -> ClosenessCentrality;
            case DegreeCentrality -> DegreeCentrality;
            case DeltaStepping -> DeltaStepping;
            case DFS -> DFS;
            case Dijkstra -> Dijkstra;
            case FilteredKNN -> FilteredKNN;
            case FilteredNodeSimilarity -> FilteredNodeSimilarity;
            case KNN -> KNN;
            case KSpanningTree -> KSpanningTree;
            case LongestPath -> LongestPath;
            case NodeSimilarity -> NodeSimilarity;
            case RandomWalk -> RandomWalk;
            case SingleSourceDijkstra -> SingleSourceDijkstra;
            case SpanningTree -> SpanningTree;
            case SteinerTree -> SteinerTree;
            case TopologicalSort -> TopologicalSort;
            case Yens -> Yens;
        };
    }

    @Override
    public String toString() {
        return value;
    }
}
