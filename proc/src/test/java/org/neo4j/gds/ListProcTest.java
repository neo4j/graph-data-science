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
package org.neo4j.gds;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.fastrp.FastRPExtendedMutateProc;
import org.neo4j.gds.beta.fastrp.FastRPExtendedStatsProc;
import org.neo4j.gds.beta.fastrp.FastRPExtendedStreamProc;
import org.neo4j.gds.beta.fastrp.FastRPExtendedWriteProc;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringMutateProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringStatsProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringStreamProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringWriteProc;
import org.neo4j.gds.beta.modularity.ModularityOptimizationMutateProc;
import org.neo4j.gds.beta.modularity.ModularityOptimizationStreamProc;
import org.neo4j.gds.beta.modularity.ModularityOptimizationWriteProc;
import org.neo4j.gds.beta.node2vec.Node2VecMutateProc;
import org.neo4j.gds.beta.node2vec.Node2VecStreamProc;
import org.neo4j.gds.beta.node2vec.Node2VecWriteProc;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateProc;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsProc;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamProc;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteProc;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphDeleteRelationshipProc;
import org.neo4j.gds.catalog.GraphDropProc;
import org.neo4j.gds.catalog.GraphExistsFunc;
import org.neo4j.gds.catalog.GraphExistsProc;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.centrality.ClosenessCentralityProc;
import org.neo4j.gds.centrality.HarmonicCentralityProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageMutateProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageStreamProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageTrainProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageWriteProc;
import org.neo4j.gds.functions.IsFiniteFunc;
import org.neo4j.gds.functions.OneHotEncodingFunc;
import org.neo4j.gds.influenceΜaximization.CELFProc;
import org.neo4j.gds.influenceΜaximization.GreedyProc;
import org.neo4j.gds.linkprediction.LinkPredictionFunc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;
import org.neo4j.gds.model.catalog.ModelDeleteProc;
import org.neo4j.gds.model.catalog.ModelDropProc;
import org.neo4j.gds.model.catalog.ModelExistsProc;
import org.neo4j.gds.model.catalog.ModelListProc;
import org.neo4j.gds.model.catalog.ModelLoadProc;
import org.neo4j.gds.model.catalog.ModelStoreProc;
import org.neo4j.gds.pagerank.PageRankMutateProc;
import org.neo4j.gds.pagerank.PageRankStatsProc;
import org.neo4j.gds.pagerank.PageRankStreamProc;
import org.neo4j.gds.pagerank.PageRankWriteProc;
import org.neo4j.gds.paths.singlesource.AllShortestPathsDijkstraStreamProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathDijkstraStreamProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathYensStreamProc;
import org.neo4j.gds.scc.SccProc;
import org.neo4j.gds.shortestpath.ShortestPathDeltaSteppingProc;
import org.neo4j.gds.shortestpaths.AllShortestPathsProc;
import org.neo4j.gds.similarity.ApproxNearestNeighborsProc;
import org.neo4j.gds.similarity.CosineProc;
import org.neo4j.gds.similarity.EuclideanProc;
import org.neo4j.gds.similarity.OverlapProc;
import org.neo4j.gds.similarity.PearsonProc;
import org.neo4j.gds.similarity.SimilaritiesFunc;
import org.neo4j.gds.spanningtree.KSpanningTreeProc;
import org.neo4j.gds.spanningtree.SpanningTreeProc;
import org.neo4j.gds.traverse.TraverseProc;
import org.neo4j.gds.triangle.TriangleProc;
import org.neo4j.gds.walking.RandomWalkProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.functions.NodePropertyFunc;
import org.neo4j.gds.functions.VersionFunc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationMutateProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStatsProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationWriteProc;
import org.neo4j.graphalgo.louvain.LouvainMutateProc;
import org.neo4j.graphalgo.louvain.LouvainStatsProc;
import org.neo4j.graphalgo.louvain.LouvainStreamProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.similarity.knn.KnnMutateProc;
import org.neo4j.graphalgo.similarity.knn.KnnStatsProc;
import org.neo4j.graphalgo.similarity.knn.KnnStreamProc;
import org.neo4j.graphalgo.similarity.knn.KnnWriteProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityMutateProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityStatsProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityStreamProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityWriteProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientMutateProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientStatsProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientStreamProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientWriteProc;
import org.neo4j.graphalgo.triangle.TriangleCountMutateProc;
import org.neo4j.graphalgo.triangle.TriangleCountStatsProc;
import org.neo4j.graphalgo.triangle.TriangleCountStreamProc;
import org.neo4j.graphalgo.triangle.TriangleCountWriteProc;
import org.neo4j.graphalgo.wcc.WccMutateProc;
import org.neo4j.graphalgo.wcc.WccStatsProc;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ListProcTest extends BaseProcTest {

    private static final List<String> PROCEDURES = asList(
        "gds.alpha.model.delete",
        "gds.alpha.model.load",
        "gds.alpha.model.store",

        "gds.alpha.allShortestPaths.stream",
        "gds.alpha.bfs.stream",

        "gds.alpha.closeness.write",
        "gds.alpha.closeness.stream",
        "gds.alpha.closeness.harmonic.write",
        "gds.alpha.closeness.harmonic.stream",

        "gds.alpha.dfs.stream",
        "gds.alpha.scc.write",
        "gds.alpha.scc.stream",
        "gds.alpha.shortestPath.deltaStepping.write",
        "gds.alpha.shortestPath.deltaStepping.stream",
        "gds.alpha.randomWalk.stream",
        "gds.alpha.similarity.cosine.write",
        "gds.alpha.similarity.cosine.stream",
        "gds.alpha.similarity.cosine.stats",
        "gds.alpha.similarity.euclidean.write",
        "gds.alpha.similarity.euclidean.stream",
        "gds.alpha.similarity.euclidean.stats",
        "gds.alpha.similarity.overlap.write",
        "gds.alpha.similarity.overlap.stream",
        "gds.alpha.similarity.overlap.stats",
        "gds.alpha.similarity.pearson.write",
        "gds.alpha.similarity.pearson.stream",
        "gds.alpha.similarity.pearson.stats",
        "gds.alpha.spanningTree.write",
        "gds.alpha.spanningTree.kmax.write",
        "gds.alpha.spanningTree.kmin.write",
        "gds.alpha.spanningTree.maximum.write",
        "gds.alpha.spanningTree.minimum.write",
        "gds.alpha.triangles",
        "gds.alpha.ml.ann.write",
        "gds.alpha.ml.ann.stream",
        "gds.alpha.ml.nodeClassification.train",
        "gds.alpha.ml.nodeClassification.train.estimate",
        "gds.alpha.ml.splitRelationships.mutate",
        "gds.alpha.influenceMaximization.greedy.stream",
        "gds.alpha.influenceMaximization.celf.stream",

        "gds.beta.node2vec.mutate",
        "gds.beta.node2vec.mutate.estimate",
        "gds.beta.node2vec.stream",
        "gds.beta.node2vec.stream.estimate",
        "gds.beta.node2vec.write",
        "gds.beta.node2vec.write.estimate",

        "gds.allShortestPaths.dijkstra.stream",
        "gds.allShortestPaths.dijkstra.stream.estimate",

        "gds.beta.graphSage.mutate",
        "gds.beta.graphSage.mutate.estimate",
        "gds.beta.graphSage.stream",
        "gds.beta.graphSage.stream.estimate",
        "gds.beta.graphSage.train",
        "gds.beta.graphSage.train.estimate",
        "gds.beta.graphSage.write",
        "gds.beta.graphSage.write.estimate",

        "gds.beta.fastRPExtended.mutate",
        "gds.beta.fastRPExtended.mutate.estimate",
        "gds.beta.fastRPExtended.stats",
        "gds.beta.fastRPExtended.stats.estimate",
        "gds.beta.fastRPExtended.stream",
        "gds.beta.fastRPExtended.stream.estimate",
        "gds.beta.fastRPExtended.write",
        "gds.beta.fastRPExtended.write.estimate",

        "gds.beta.graph.generate",
        "gds.beta.graph.create.subgraph",

        "gds.beta.k1coloring.mutate",
        "gds.beta.k1coloring.mutate.estimate",
        "gds.beta.k1coloring.stats",
        "gds.beta.k1coloring.stats.estimate",
        "gds.beta.k1coloring.stream",
        "gds.beta.k1coloring.stream.estimate",
        "gds.beta.k1coloring.write",
        "gds.beta.k1coloring.write.estimate",

        "gds.beta.knn.mutate",
        "gds.beta.knn.mutate.estimate",
        "gds.beta.knn.stats",
        "gds.beta.knn.stats.estimate",
        "gds.beta.knn.stream",
        "gds.beta.knn.stream.estimate",
        "gds.beta.knn.write",
        "gds.beta.knn.write.estimate",

        "gds.beta.listProgress",

        "gds.beta.model.drop",
        "gds.beta.model.exists",
        "gds.beta.model.list",

        "gds.beta.modularityOptimization.mutate",
        "gds.beta.modularityOptimization.mutate.estimate",
        "gds.beta.modularityOptimization.stream",
        "gds.beta.modularityOptimization.stream.estimate",
        "gds.beta.modularityOptimization.write",
        "gds.beta.modularityOptimization.write.estimate",

        "gds.shortestPath.dijkstra.stream",
        "gds.shortestPath.dijkstra.stream.estimate",

        "gds.shortestPath.yens.stream",
        "gds.shortestPath.yens.stream.estimate",

        "gds.betweenness.mutate",
        "gds.betweenness.mutate.estimate",
        "gds.betweenness.stats",
        "gds.betweenness.stats.estimate",
        "gds.betweenness.stream",
        "gds.betweenness.stream.estimate",
        "gds.betweenness.write",
        "gds.betweenness.write.estimate",

        "gds.fastRP.mutate",
        "gds.fastRP.mutate.estimate",
        "gds.fastRP.stats",
        "gds.fastRP.stats.estimate",
        "gds.fastRP.stream",
        "gds.fastRP.stream.estimate",
        "gds.fastRP.write",
        "gds.fastRP.write.estimate",

        "gds.graph.create",
        "gds.graph.create.cypher",
        "gds.graph.create.cypher.estimate",
        "gds.graph.create.estimate",
        "gds.graph.deleteRelationships",
        "gds.graph.drop",
        "gds.graph.exists",
        "gds.graph.list",
        "gds.graph.streamNodeProperties",
        "gds.graph.streamNodeProperty",
        "gds.graph.streamRelationshipProperties",
        "gds.graph.streamRelationshipProperty",
        "gds.graph.writeNodeProperties",

        "gds.labelPropagation.mutate",
        "gds.labelPropagation.mutate.estimate",
        "gds.labelPropagation.stats",
        "gds.labelPropagation.stats.estimate",
        "gds.labelPropagation.stream",
        "gds.labelPropagation.stream.estimate",
        "gds.labelPropagation.write",
        "gds.labelPropagation.write.estimate",

        "gds.louvain.mutate",
        "gds.louvain.mutate.estimate",
        "gds.louvain.stats",
        "gds.louvain.stats.estimate",
        "gds.louvain.stream",
        "gds.louvain.stream.estimate",
        "gds.louvain.write",
        "gds.louvain.write.estimate",

        "gds.nodeSimilarity.mutate",
        "gds.nodeSimilarity.mutate.estimate",
        "gds.nodeSimilarity.stats",
        "gds.nodeSimilarity.stats.estimate",
        "gds.nodeSimilarity.stream",
        "gds.nodeSimilarity.stream.estimate",
        "gds.nodeSimilarity.write",
        "gds.nodeSimilarity.write.estimate",

        "gds.pageRank.mutate",
        "gds.pageRank.mutate.estimate",
        "gds.pageRank.stats",
        "gds.pageRank.stats.estimate",
        "gds.pageRank.stream",
        "gds.pageRank.stream.estimate",
        "gds.pageRank.write",
        "gds.pageRank.write.estimate",

        "gds.wcc.mutate",
        "gds.wcc.mutate.estimate",
        "gds.wcc.stats",
        "gds.wcc.stats.estimate",
        "gds.wcc.stream",
        "gds.wcc.stream.estimate",
        "gds.wcc.write",
        "gds.wcc.write.estimate",

        "gds.triangleCount.mutate",
        "gds.triangleCount.mutate.estimate",
        "gds.triangleCount.stats",
        "gds.triangleCount.stats.estimate",
        "gds.triangleCount.stream",
        "gds.triangleCount.stream.estimate",
        "gds.triangleCount.write",
        "gds.triangleCount.write.estimate",

        "gds.localClusteringCoefficient.mutate",
        "gds.localClusteringCoefficient.mutate.estimate",
        "gds.localClusteringCoefficient.stats",
        "gds.localClusteringCoefficient.stats.estimate",
        "gds.localClusteringCoefficient.stream",
        "gds.localClusteringCoefficient.stream.estimate",
        "gds.localClusteringCoefficient.write",
        "gds.localClusteringCoefficient.write.estimate"
    );

    private static final List<String> FUNCTIONS = asList(
        "gds.util.asNode",
        "gds.util.asNodes",
        "gds.util.NaN",
        "gds.util.infinity",
        "gds.util.isFinite",
        "gds.util.isInfinite",
        "gds.util.nodeProperty",

        "gds.version",

        "gds.alpha.linkprediction.adamicAdar",
        "gds.alpha.linkprediction.resourceAllocation",
        "gds.alpha.linkprediction.commonNeighbors",
        "gds.alpha.linkprediction.preferentialAttachment",
        "gds.alpha.linkprediction.totalNeighbors",
        "gds.alpha.linkprediction.sameCommunity",

        "gds.alpha.similarity.cosine",
        "gds.alpha.similarity.euclidean",
        "gds.alpha.similarity.euclideanDistance",
        "gds.alpha.similarity.jaccard",
        "gds.alpha.similarity.overlap",
        "gds.alpha.similarity.pearson",

        "gds.alpha.ml.oneHotEncoding",

        "gds.graph.exists"

    );

    private static final List<String> PAGE_RANK = asList(
        "gds.pageRank.mutate",
        "gds.pageRank.mutate.estimate",
        "gds.pageRank.stats",
        "gds.pageRank.stats.estimate",
        "gds.pageRank.stream",
        "gds.pageRank.stream.estimate",
        "gds.pageRank.write",
        "gds.pageRank.write.estimate"
    );

    private static final List<String> ALL = Stream
        .concat(PROCEDURES.stream(), FUNCTIONS.stream())
        .sorted()
        .collect(Collectors.toList());

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            AllShortestPathsDijkstraStreamProc.class,
            BetweennessCentralityStreamProc.class,
            BetweennessCentralityWriteProc.class,
            BetweennessCentralityMutateProc.class,
            BetweennessCentralityStatsProc.class,
            // We register it to check that all of those procedures will not be listed
            FeatureToggleProc.class,
            FastRPStreamProc.class,
            FastRPWriteProc.class,
            FastRPMutateProc.class,
            FastRPStatsProc.class,
            FastRPExtendedStreamProc.class,
            FastRPExtendedWriteProc.class,
            FastRPExtendedMutateProc.class,
            FastRPExtendedStatsProc.class,
            GraphCreateProc.class,
            GraphDropProc.class,
            GraphExistsProc.class,
            GraphListProc.class,
            GraphGenerateProc.class,
            GraphDeleteRelationshipProc.class,
            GraphSageMutateProc.class,
            GraphSageStreamProc.class,
            GraphSageTrainProc.class,
            GraphSageWriteProc.class,
            GraphStreamNodePropertiesProc.class,
            GraphStreamRelationshipPropertiesProc.class,
            GraphWriteNodePropertiesProc.class,
            K1ColoringMutateProc.class,
            K1ColoringStatsProc.class,
            K1ColoringWriteProc.class,
            K1ColoringStreamProc.class,
            KnnMutateProc.class,
            KnnStatsProc.class,
            KnnStreamProc.class,
            KnnWriteProc.class,
            LabelPropagationWriteProc.class,
            LabelPropagationStreamProc.class,
            LabelPropagationStatsProc.class,
            LabelPropagationMutateProc.class,
            ListProc.class,
            ListProgressProc.class,
            LouvainWriteProc.class,
            LouvainStreamProc.class,
            LouvainStatsProc.class,
            LouvainMutateProc.class,
            ModelDeleteProc.class,
            ModelDropProc.class,
            ModelExistsProc.class,
            ModelListProc.class,
            ModelLoadProc.class,
            ModelStoreProc.class,
            ModularityOptimizationMutateProc.class,
            ModularityOptimizationWriteProc.class,
            ModularityOptimizationStreamProc.class,
            NodeSimilarityWriteProc.class,
            NodeSimilarityStreamProc.class,
            NodeSimilarityMutateProc.class,
            NodeSimilarityStatsProc.class,
            Node2VecMutateProc.class,
            Node2VecStreamProc.class,
            Node2VecWriteProc.class,
            PageRankWriteProc.class,
            PageRankStreamProc.class,
            PageRankMutateProc.class,
            PageRankStatsProc.class,
            ShortestPathDijkstraStreamProc.class,
            ShortestPathYensStreamProc.class,
            TriangleCountStatsProc.class,
            TriangleCountWriteProc.class,
            TriangleCountStreamProc.class,
            TriangleCountMutateProc.class,
            WccWriteProc.class,
            WccStreamProc.class,
            WccMutateProc.class,
            WccStatsProc.class,
            LocalClusteringCoefficientStreamProc.class,
            LocalClusteringCoefficientStatsProc.class,
            LocalClusteringCoefficientWriteProc.class,
            LocalClusteringCoefficientMutateProc.class,

            // alpha
            AllShortestPathsProc.class,
            ApproxNearestNeighborsProc.class,
            ClosenessCentralityProc.class,
            HarmonicCentralityProc.class,
            IsFiniteFunc.class,
            KSpanningTreeProc.class,
            CosineProc.class,
            EuclideanProc.class,
            NodeClassificationTrainProc.class,
            OverlapProc.class,
            PearsonProc.class,
            RandomWalkProc.class,
            OneHotEncodingFunc.class,
            SpanningTreeProc.class,
            SplitRelationshipsMutateProc.class,
            ShortestPathDeltaSteppingProc.class,
            SimilaritiesFunc.class,
            SccProc.class,
            TraverseProc.class,
            TriangleProc.class,
            GreedyProc.class,
            CELFProc.class
        );

        registerFunctions(
            AsNodeFunc.class,
            GraphExistsFunc.class,
            NodePropertyFunc.class,
            IsFiniteFunc.class,
            OneHotEncodingFunc.class,
            VersionFunc.class,
            LinkPredictionFunc.class,
            SimilaritiesFunc.class
        );
    }

    @Test
    void shouldListAllThingsExceptTheListProcedure() {
        assertThat(listProcs(null)).containsExactlyInAnyOrderElementsOf(ALL);
    }

    @Test
    void listFilteredResult() {
        assertEquals(PAGE_RANK, listProcs("pageRank"));
        assertEquals(asList("gds.pageRank.stream", "gds.pageRank.stream.estimate"), listProcs("pageRank.stream"));
        assertEquals(emptyList(), listProcs("foo"));
    }

    @Test
    void allProcsHaveDescriptions() {
        SoftAssertions softly = new SoftAssertions();
        runQueryWithRowConsumer(
            "CALL gds.list()",
            resultRow -> softly
                    .assertThat(resultRow.getString("description"))
                    .withFailMessage(resultRow.get("name") + " has no description")
                    .isNotEmpty()
        );

        softly.assertAll();
    }

    @Test
    void listEmpty() {
        assertThat(
            runQuery("CALL gds.list()", (Function<Result, List<String>>) result -> result
                .<String>columnAs("name")
                .stream()
                .sorted()
                .collect(Collectors.toList())))
            .containsExactlyElementsOf(ALL);
    }

    private List<String> listProcs(Object name) {
        String query = "CALL gds.list($name)";
        return runQuery(
            query,
            MapUtil.map("name", name),
            result -> result.<String>columnAs("name")
                .stream()
                .sorted()
                .collect(Collectors.toList())
        );
    }
}
