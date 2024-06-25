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
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordMutateConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.traverse.BfsMutateConfig;
import org.neo4j.gds.paths.traverse.DfsMutateConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.knn.KnnMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;
import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.function.Function;

/**
 * One-stop shop for how to turn user input into configuration for a given algorithm in mutate mode.
 */
public class ConfigurationParsersForMutateMode {
    /**
     * @return the appropriate parser, or null if this algorithm doesn't have a mutate mode
     */
    public Function<CypherMapWrapper, AlgoBaseConfig> lookup(Algorithm algorithm) {
        return switch (algorithm) {
            case AllShortestPaths -> null;
            case ApproximateMaximumKCut -> ApproxMaxKCutMutateConfig::of;
            case ArticleRank -> PageRankMutateConfig::of;
            case AStar -> ShortestPathAStarMutateConfig::of;
            case BellmanFord -> BellmanFordMutateConfig::of;
            case BetaClosenessCentrality -> ClosenessCentralityMutateConfig::of;
            case BetweennessCentrality -> BetweennessCentralityMutateConfig::of;
            case BFS -> BfsMutateConfig::of;
            case CELF -> InfluenceMaximizationMutateConfig::of;
            case ClosenessCentrality -> ClosenessCentralityMutateConfig::of;
            case Conductance -> null;
            case DegreeCentrality -> DegreeCentralityMutateConfig::of;
            case DeltaStepping -> AllShortestPathsDeltaMutateConfig::of;
            case DFS -> DfsMutateConfig::of;
            case Dijkstra -> ShortestPathDijkstraMutateConfig::of;
            case EigenVector -> PageRankMutateConfig::of;
            case FastRP -> FastRPMutateConfig::of;
            case FilteredKNN -> FilteredKnnMutateConfig::of;
            case FilteredNodeSimilarity -> FilteredNodeSimilarityMutateConfig::of;
            case HarmonicCentrality -> HarmonicCentralityMutateConfig::of;
            case K1Coloring -> K1ColoringMutateConfig::of;
            case KCore -> KCoreDecompositionMutateConfig::of;
            case KMeans -> KmeansMutateConfig::of;
            case KNN -> KnnMutateConfig::of;
            case KSpanningTree -> null;
            case LabelPropagation -> LabelPropagationMutateConfig::of;
            case LCC -> LocalClusteringCoefficientMutateConfig::of;
            case Leiden -> LeidenMutateConfig::of;
            case Louvain -> LouvainMutateConfig::of;
            case LongestPath -> null;
            case Modularity -> null;
            case ModularityOptimization -> ModularityOptimizationMutateConfig::of;
            case NodeSimilarity -> NodeSimilarityMutateConfig::of;
            case PageRank -> PageRankMutateConfig::of;
            case RandomWalk -> null;
            case SCC -> SccMutateConfig::of;
            case SingleSourceDijkstra -> AllShortestPathsDijkstraMutateConfig::of;
            case SpanningTree -> SpanningTreeMutateConfig::of;
            case SteinerTree -> SteinerTreeMutateConfig::of;
            case TopologicalSort -> null;
            case TriangleCount -> TriangleCountMutateConfig::of;
            case Triangles -> null;
            case WCC -> WccMutateConfig::of;
            case Yens -> ShortestPathYensMutateConfig::of;
        };
    }
}
