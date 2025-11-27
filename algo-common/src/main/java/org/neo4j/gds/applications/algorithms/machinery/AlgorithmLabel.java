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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.applications.algorithms.metadata.Algorithm;

public enum AlgorithmLabel implements Label {
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
    CliqueCounting("Clique Counting"),
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
    HDBScan("HDBScan"),
    HITS("HITS"),
    IndexInverse("IndexInverse"),
    IndirectExposure("IndirectExposure"),
    K1Coloring("K1Coloring"),
    KCore("KCoreDecomposition"),
    KGE("KGE"),
    KMeans("K-Means"),
    KNN("Knn"),
    KSpanningTree("K Spanning Tree"),
    LabelPropagation("Label Propagation"),
    LCC("LocalClusteringCoefficient"),
    Leiden("Leiden"),
    Louvain("Louvain"),
    LongestPath("LongestPath"),
    MaxFlow("MaxFlow"),
    MCMF("MinCostMaxFlow"),
    Modularity("Modularity"),
    ModularityOptimization("ModularityOptimization"),
    NodeSimilarity("Node Similarity"),
    Node2Vec("Node2Vec"),
    PageRank("PageRank"),
    PCST("PrizeCollectingSteinerTree"),
    RandomWalk("RandomWalk"),
    ScaleProperties("ScaleProperties"),
    SCC("SCC"),
    SingleSourceDijkstra("All Shortest Paths"),
    SLLPA("SpeakerListenerLPA"),
    SpanningTree("SpanningTree"),
    SplitRelationships("SplitRelationships"),
    SteinerTree("SteinerTree"),
    TopologicalSort("TopologicalSort"),
    ToUndirected("ToUndirected"),
    TriangleCount("TriangleCount"),
    Triangles("Triangles"),
    WCC("WCC"),
    Yens("Yens");

    private final String value;

    AlgorithmLabel(String value) {this.value = value;}

    /**
     * Lookup powered by enum so compiler tells us what to remember
     */
    public static Label from(Algorithm algorithm) {
        return switch (algorithm) {
            case Algorithm.AllShortestPaths -> AllShortestPaths;
            case Algorithm.ApproximateMaximumKCut -> ApproximateMaximumKCut;
            case Algorithm.ArticleRank -> ArticleRank;
            case Algorithm.ArticulationPoints -> ArticulationPoints;
            case Algorithm.AStar -> AStar;
            case Algorithm.BellmanFord -> BellmanFord;
            case Algorithm.BetaClosenessCentrality -> BetaClosenessCentrality;
            case Algorithm.BetweennessCentrality -> BetweennessCentrality;
            case Algorithm.BFS -> BFS;
            case Algorithm.Bridges -> Bridges;
            case Algorithm.CELF -> CELF;
            case Algorithm.CliqueCounting -> CliqueCounting;
            case Algorithm.ClosenessCentrality -> ClosenessCentrality;
            case Algorithm.CollapsePath -> CollapsePath;
            case Algorithm.Conductance -> Conductance;
            case Algorithm.DegreeCentrality -> DegreeCentrality;
            case Algorithm.DeltaStepping -> DeltaStepping;
            case Algorithm.DFS -> DFS;
            case Algorithm.Dijkstra -> Dijkstra;
            case Algorithm.EigenVector -> EigenVector;
            case Algorithm.FastRP -> FastRP;
            case Algorithm.FilteredKNN -> FilteredKNN;
            case Algorithm.FilteredNodeSimilarity -> FilteredNodeSimilarity;
            case Algorithm.GraphSage -> GraphSage;
            case Algorithm.GraphSageTrain -> GraphSageTrain;
            case Algorithm.HITS -> HITS;
            case Algorithm.HarmonicCentrality -> HarmonicCentrality;
            case Algorithm.HashGNN -> HashGNN;
            case Algorithm.HDBScan -> HDBScan;
            case Algorithm.IndexInverse -> IndexInverse;
            case Algorithm.K1Coloring -> K1Coloring;
            case Algorithm.KCore -> KCore;
            case Algorithm.KGE -> KGE;
            case Algorithm.KMeans -> KMeans;
            case Algorithm.KNN -> KNN;
            case Algorithm.KSpanningTree -> KSpanningTree;
            case Algorithm.LabelPropagation -> LabelPropagation;
            case Algorithm.LCC -> LCC;
            case Algorithm.Leiden -> Leiden;
            case Algorithm.Louvain -> Louvain;
            case Algorithm.LongestPath -> LongestPath;
            case Algorithm.MaxFlow -> MaxFlow;
            case Algorithm.MCMF -> MCMF;
            case Algorithm.Modularity -> Modularity;
            case Algorithm.ModularityOptimization -> ModularityOptimization;
            case Algorithm.NodeSimilarity -> NodeSimilarity;
            case Algorithm.Node2Vec -> Node2Vec;
            case Algorithm.PageRank -> PageRank;
            case Algorithm.PCST -> PCST;
            case Algorithm.RandomWalk -> RandomWalk;
            case Algorithm.ScaleProperties -> ScaleProperties;
            case Algorithm.SCC -> SCC;
            case Algorithm.SingleSourceDijkstra -> SingleSourceDijkstra;
            case Algorithm.SLLPA -> SLLPA;
            case Algorithm.SpanningTree -> SpanningTree;
            case Algorithm.SplitRelationships -> SplitRelationships;
            case Algorithm.SteinerTree -> SteinerTree;
            case Algorithm.TopologicalSort -> TopologicalSort;
            case Algorithm.ToUndirected -> ToUndirected;
            case Algorithm.TriangleCount -> TriangleCount;
            case Algorithm.Triangles -> Triangles;
            case Algorithm.WCC -> WCC;
            case Algorithm.Yens -> Yens;
        };
    }

    @Override
    public String asString() {
        return value;
    }

    /**
     * Convenience.
     */
    @Override
    public String toString() {
        return asString();
    }
}
