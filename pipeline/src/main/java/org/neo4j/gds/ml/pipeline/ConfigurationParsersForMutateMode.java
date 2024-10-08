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

import org.neo4j.gds.algorithms.machinelearning.KGEPredictMutateConfig;
import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNMutateConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMutateConfig;
import org.neo4j.gds.pagerank.ArticleRankMutateConfig;
import org.neo4j.gds.pagerank.EigenvectorMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordMutateConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.traverse.BfsMutateConfig;
import org.neo4j.gds.paths.traverse.DfsMutateConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.knn.KnnMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;
import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.undirected.ToUndirectedConfig;
import org.neo4j.gds.walking.CollapsePathConfig;
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
            case ArticleRank -> ArticleRankMutateConfig::of;
            case ArticulationPoints -> ArticulationPointsMutateConfig::of;
            case AStar -> ShortestPathAStarMutateConfig::of;
            case BellmanFord -> AllShortestPathsBellmanFordMutateConfig::of;
            case BetaClosenessCentrality -> ClosenessCentralityMutateConfig::of;
            case BetweennessCentrality -> BetweennessCentralityMutateConfig::of;
            case BFS -> BfsMutateConfig::of;
            case Bridges -> null;
            case CELF -> InfluenceMaximizationMutateConfig::of;
            case ClosenessCentrality -> ClosenessCentralityMutateConfig::of;
            case CollapsePath -> CollapsePathConfig::of;
            case Conductance -> null;
            case DegreeCentrality -> DegreeCentralityMutateConfig::of;
            case DeltaStepping -> AllShortestPathsDeltaMutateConfig::of;
            case DFS -> DfsMutateConfig::of;
            case Dijkstra -> ShortestPathDijkstraMutateConfig::of;
            case EigenVector -> EigenvectorMutateConfig::of;
            case FastRP -> FastRPMutateConfig::of;
            case FilteredKNN -> FilteredKnnMutateConfig::of;
            case FilteredNodeSimilarity -> FilteredNodeSimilarityMutateConfig::of;
            case GraphSage -> graphSageParser();
            case GraphSageTrain -> null;
            case HarmonicCentrality -> HarmonicCentralityMutateConfig::of;
            case HashGNN -> HashGNNMutateConfig::of;
            case IndexInverse -> InverseRelationshipsConfig::of;
            case K1Coloring -> K1ColoringMutateConfig::of;
            case KCore -> KCoreDecompositionMutateConfig::of;
            case KGE -> KGEPredictMutateConfig::of;
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
            case Node2Vec -> Node2VecMutateConfig::of;
            case PageRank -> PageRankMutateConfig::of;
            case PCST -> null;
            case RandomWalk -> null;
            case ScaleProperties -> ScalePropertiesMutateConfig::of;
            case SCC -> SccMutateConfig::of;
            case SingleSourceDijkstra -> AllShortestPathsDijkstraMutateConfig::of;
            case SpanningTree -> SpanningTreeMutateConfig::of;
            case SplitRelationships -> SplitRelationshipsMutateConfig::of;
            case SteinerTree -> SteinerTreeMutateConfig::of;
            case TopologicalSort -> null;
            case ToUndirected -> ToUndirectedConfig::of;
            case TriangleCount -> TriangleCountMutateConfig::of;
            case Triangles -> null;
            case WCC -> WccMutateConfig::of;
            case Yens -> ShortestPathYensMutateConfig::of;
        };
    }

    /**
     * GraphSage is special. Did it need to be? Not sure.
     * But anyway, for this use case, we produce configurations for validating anonymously.
     * Hence, {@link org.neo4j.gds.core.Username#EMPTY_USERNAME} can be used here,
     * to fulfil the requirement of specifying _some_ user.
     */
    private static Function<CypherMapWrapper, AlgoBaseConfig> graphSageParser() {
        return cypherMapWrapper -> GraphSageMutateConfig.of(Username.EMPTY_USERNAME.username(), cypherMapWrapper);
    }
}
