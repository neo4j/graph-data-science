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
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.utils.TestProcedureAndFunctionScanner;
import org.neo4j.graphdb.Result;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SoftAssertionsExtension.class)
class OpenGdsProcedureSmokeTest extends BaseProcTest {

    private static final List<String> PROCEDURES = asList(
        "gds.graph.graphProperty.stream",
        "gds.graph.graphProperty.drop",

        "gds.graph.nodeLabel.mutate",
        "gds.graph.nodeLabel.write",

        "gds.graph.sample.rwr",
        "gds.graph.sample.cnarw",
        "gds.graph.sample.cnarw.estimate",

        "gds.ephemeral.database.create",
        "gds.ephemeral.database.drop",

        "gds.allShortestPaths.stream",

        "gds.bellmanFord.stats",
        "gds.bellmanFord.stats.estimate",
        "gds.bellmanFord.stream",
        "gds.bellmanFord.stream.estimate",
        "gds.bellmanFord.mutate",
        "gds.bellmanFord.mutate.estimate",
        "gds.bellmanFord.write",
        "gds.bellmanFord.write.estimate",

        "gds.collapsePath.mutate",

        "gds.conductance.stream",

        "gds.closeness.harmonic.mutate",
        "gds.closeness.harmonic.stats",
        "gds.closeness.harmonic.stream",
        "gds.closeness.harmonic.write",

        "gds.closeness.mutate",
        "gds.closeness.stats",
        "gds.closeness.stream",
        "gds.closeness.write",

        "gds.leiden.mutate",
        "gds.leiden.mutate.estimate",
        "gds.leiden.stats",
        "gds.leiden.stats.estimate",
        "gds.leiden.stream",
        "gds.leiden.stream.estimate",
        "gds.leiden.write",
        "gds.leiden.write.estimate",

        "gds.kmeans.mutate",
        "gds.kmeans.mutate.estimate",
        "gds.kmeans.stats",
        "gds.kmeans.stats.estimate",
        "gds.kmeans.stream",
        "gds.kmeans.stream.estimate",
        "gds.kmeans.write",
        "gds.kmeans.write.estimate",

        "gds.knn.filtered.mutate",
        "gds.knn.filtered.mutate.estimate",
        "gds.knn.filtered.stats",
        "gds.knn.filtered.stats.estimate",
        "gds.knn.filtered.stream",
        "gds.knn.filtered.stream.estimate",
        "gds.knn.filtered.write",
        "gds.knn.filtered.write.estimate",

        "gds.maxkcut.mutate",
        "gds.maxkcut.mutate.estimate",
        "gds.maxkcut.stream",
        "gds.maxkcut.stream.estimate",

        "gds.modularity.stats",
        "gds.modularity.stats.estimate",
        "gds.modularity.stream",
        "gds.modularity.stream.estimate",

        "gds.alpha.hits.mutate",
        "gds.alpha.hits.mutate.estimate",
        "gds.alpha.hits.stats",
        "gds.alpha.hits.stats.estimate",
        "gds.alpha.hits.stream",
        "gds.alpha.hits.stream.estimate",
        "gds.alpha.hits.write",
        "gds.alpha.hits.write.estimate",

        "gds.nodeSimilarity.filtered.mutate",
        "gds.nodeSimilarity.filtered.mutate.estimate",
        "gds.nodeSimilarity.filtered.stats",
        "gds.nodeSimilarity.filtered.stats.estimate",
        "gds.nodeSimilarity.filtered.stream",
        "gds.nodeSimilarity.filtered.stream.estimate",
        "gds.nodeSimilarity.filtered.write",
        "gds.nodeSimilarity.filtered.write.estimate",

        "gds.dag.topologicalSort.stream",
        "gds.dag.longestPath.stream",

        "gds.beta.hashgnn.mutate",
        "gds.beta.hashgnn.mutate.estimate",
        "gds.beta.hashgnn.stream",
        "gds.beta.hashgnn.stream.estimate",

        "gds.beta.pipeline.linkPrediction.addFeature",
        "gds.beta.pipeline.linkPrediction.addNodeProperty",
        "gds.beta.pipeline.linkPrediction.addLogisticRegression",
        "gds.alpha.pipeline.linkPrediction.addMLP",
        "gds.beta.pipeline.linkPrediction.addRandomForest",
        "gds.alpha.pipeline.linkPrediction.configureAutoTuning",
        "gds.beta.pipeline.linkPrediction.configureSplit",
        "gds.beta.pipeline.linkPrediction.create",
        "gds.beta.pipeline.linkPrediction.predict.mutate",
        "gds.beta.pipeline.linkPrediction.predict.mutate.estimate",
        "gds.beta.pipeline.linkPrediction.predict.stream",
        "gds.beta.pipeline.linkPrediction.predict.stream.estimate",
        "gds.beta.pipeline.linkPrediction.train",
        "gds.beta.pipeline.linkPrediction.train.estimate",

        "gds.alpha.pipeline.nodeRegression.addLinearRegression",
        "gds.alpha.pipeline.nodeRegression.addNodeProperty",
        "gds.alpha.pipeline.nodeRegression.addRandomForest",
        "gds.alpha.pipeline.nodeRegression.create",
        "gds.alpha.pipeline.nodeRegression.configureSplit",
        "gds.alpha.pipeline.nodeRegression.configureAutoTuning",
        "gds.alpha.pipeline.nodeRegression.predict.mutate",
        "gds.alpha.pipeline.nodeRegression.predict.stream",
        "gds.alpha.pipeline.nodeRegression.selectFeatures",
        "gds.alpha.pipeline.nodeRegression.train",

        "gds.beta.pipeline.nodeClassification.selectFeatures",
        "gds.beta.pipeline.nodeClassification.addNodeProperty",
        "gds.beta.pipeline.nodeClassification.addLogisticRegression",
        "gds.alpha.pipeline.nodeClassification.addMLP",
        "gds.beta.pipeline.nodeClassification.addRandomForest",
        "gds.alpha.pipeline.nodeClassification.configureAutoTuning",
        "gds.alpha.pipeline.nodeClassification.addRandomForest",  //Deprecated
        "gds.beta.pipeline.nodeClassification.configureSplit",
        "gds.beta.pipeline.nodeClassification.create",
        "gds.beta.pipeline.nodeClassification.predict.mutate",
        "gds.beta.pipeline.nodeClassification.predict.mutate.estimate",
        "gds.beta.pipeline.nodeClassification.predict.stream",
        "gds.beta.pipeline.nodeClassification.predict.stream.estimate",
        "gds.beta.pipeline.nodeClassification.predict.write",
        "gds.beta.pipeline.nodeClassification.predict.write.estimate",
        "gds.beta.pipeline.nodeClassification.train",
        "gds.beta.pipeline.nodeClassification.train.estimate",

        "gds.scc.stats",
        "gds.scc.mutate",
        "gds.scc.write",
        "gds.scc.stream",

        "gds.scaleProperties.mutate",
        "gds.scaleProperties.mutate.estimate",
        "gds.scaleProperties.stream",
        "gds.scaleProperties.stream.estimate",
        "gds.scaleProperties.stats",
        "gds.scaleProperties.stats.estimate",
        "gds.scaleProperties.write",
        "gds.scaleProperties.write.estimate",

        "gds.alpha.sllpa.mutate",
        "gds.alpha.sllpa.mutate.estimate",
        "gds.alpha.sllpa.stats",
        "gds.alpha.sllpa.stats.estimate",
        "gds.alpha.sllpa.stream",
        "gds.alpha.sllpa.stream.estimate",
        "gds.alpha.sllpa.write",
        "gds.alpha.sllpa.write.estimate",

        "gds.kSpanningTree.write",
        "gds.spanningTree.mutate",
        "gds.spanningTree.mutate.estimate",
        "gds.spanningTree.stats",
        "gds.spanningTree.stats.estimate",
        "gds.spanningTree.stream",
        "gds.spanningTree.stream.estimate",
        "gds.spanningTree.write",
        "gds.spanningTree.write.estimate",

        "gds.steinerTree.mutate",
        "gds.steinerTree.mutate.estimate",
        "gds.steinerTree.stats",
        "gds.steinerTree.stats.estimate",
        "gds.steinerTree.stream",
        "gds.steinerTree.stream.estimate",
        "gds.steinerTree.write",
        "gds.steinerTree.write.estimate",

        "gds.triangles",
        "gds.alpha.ml.splitRelationships.mutate",

        "gds.influenceMaximization.celf.mutate",
        "gds.influenceMaximization.celf.mutate.estimate",
        "gds.influenceMaximization.celf.stats",
        "gds.influenceMaximization.celf.stats.estimate",
        "gds.influenceMaximization.celf.stream",
        "gds.influenceMaximization.celf.stream.estimate",
        "gds.influenceMaximization.celf.write",
        "gds.influenceMaximization.celf.write.estimate",


        "gds.userLog",

        "gds.articleRank.mutate",
        "gds.articleRank.mutate.estimate",
        "gds.articleRank.stats",
        "gds.articleRank.stats.estimate",
        "gds.articleRank.stream",
        "gds.articleRank.stream.estimate",
        "gds.articleRank.write",
        "gds.articleRank.write.estimate",

        "gds.graph.export.csv",
        "gds.graph.export.csv.estimate",

        "gds.node2vec.mutate",
        "gds.node2vec.mutate.estimate",
        "gds.node2vec.stream",
        "gds.node2vec.stream.estimate",
        "gds.node2vec.write",
        "gds.node2vec.write.estimate",

        "gds.beta.graphSage.mutate",
        "gds.beta.graphSage.mutate.estimate",
        "gds.beta.graphSage.stream",
        "gds.beta.graphSage.stream.estimate",
        "gds.beta.graphSage.train",
        "gds.beta.graphSage.train.estimate",
        "gds.beta.graphSage.write",
        "gds.beta.graphSage.write.estimate",

        "gds.graph.generate",
        "gds.graph.filter",

        "gds.k1coloring.mutate",
        "gds.k1coloring.mutate.estimate",
        "gds.k1coloring.stats",
        "gds.k1coloring.stats.estimate",
        "gds.k1coloring.stream",
        "gds.k1coloring.stream.estimate",
        "gds.k1coloring.write",
        "gds.k1coloring.write.estimate",

        "gds.knn.mutate",
        "gds.knn.mutate.estimate",
        "gds.knn.stats",
        "gds.knn.stats.estimate",
        "gds.knn.stream",
        "gds.knn.stream.estimate",
        "gds.knn.write",
        "gds.knn.write.estimate",

        "gds.listProgress",

        "gds.model.drop",
        "gds.model.exists",
        "gds.model.list",

        "gds.modularityOptimization.mutate",
        "gds.modularityOptimization.mutate.estimate",
        "gds.modularityOptimization.stream",
        "gds.modularityOptimization.stream.estimate",
        "gds.modularityOptimization.write",
        "gds.modularityOptimization.write.estimate",

        "gds.pipeline.drop",
        "gds.pipeline.exists",
        "gds.pipeline.list",

        "gds.randomWalk.stats",
        "gds.randomWalk.stats.estimate",
        "gds.randomWalk.stream",
        "gds.randomWalk.stream.estimate",

        "gds.allShortestPaths.dijkstra.stream",
        "gds.allShortestPaths.dijkstra.stream.estimate",
        "gds.allShortestPaths.dijkstra.mutate",
        "gds.allShortestPaths.dijkstra.mutate.estimate",
        "gds.allShortestPaths.dijkstra.write",
        "gds.allShortestPaths.dijkstra.write.estimate",

        "gds.allShortestPaths.delta.stream",
        "gds.allShortestPaths.delta.stream.estimate",
        "gds.allShortestPaths.delta.mutate",
        "gds.allShortestPaths.delta.mutate.estimate",
        "gds.allShortestPaths.delta.write",
        "gds.allShortestPaths.delta.write.estimate",
        "gds.allShortestPaths.delta.stats",
        "gds.allShortestPaths.delta.stats.estimate",

        "gds.betweenness.mutate",
        "gds.betweenness.mutate.estimate",
        "gds.betweenness.stats",
        "gds.betweenness.stats.estimate",
        "gds.betweenness.stream",
        "gds.betweenness.stream.estimate",
        "gds.betweenness.write",
        "gds.betweenness.write.estimate",

        "gds.bfs.mutate",
        "gds.bfs.mutate.estimate",
        "gds.bfs.stream",
        "gds.bfs.stream.estimate",
        "gds.bfs.stats",
        "gds.bfs.stats.estimate",

        "gds.debug.sysInfo",

        "gds.degree.mutate",
        "gds.degree.mutate.estimate",
        "gds.degree.stats",
        "gds.degree.stats.estimate",
        "gds.degree.stream",
        "gds.degree.stream.estimate",
        "gds.degree.write",
        "gds.degree.write.estimate",

        "gds.dfs.mutate",
        "gds.dfs.mutate.estimate",
        "gds.dfs.stream",
        "gds.dfs.stream.estimate",

        "gds.eigenvector.mutate",
        "gds.eigenvector.mutate.estimate",
        "gds.eigenvector.stats",
        "gds.eigenvector.stats.estimate",
        "gds.eigenvector.stream",
        "gds.eigenvector.stream.estimate",
        "gds.eigenvector.write",
        "gds.eigenvector.write.estimate",

        "gds.graph.export",

        "gds.fastRP.mutate",
        "gds.fastRP.mutate.estimate",
        "gds.fastRP.stats",
        "gds.fastRP.stats.estimate",
        "gds.fastRP.stream",
        "gds.fastRP.stream.estimate",
        "gds.fastRP.write",
        "gds.fastRP.write.estimate",

        "gds.graph.relationships.stream",
        "gds.graph.relationships.toUndirected",
        "gds.graph.relationships.toUndirected.estimate",

        "gds.graph.relationships.drop",
        "gds.graph.drop",
        "gds.graph.exists",
        "gds.graph.list",
        "gds.graph.project",
        "gds.graph.project.cypher",
        "gds.graph.project.cypher.estimate",
        "gds.graph.project.estimate",
        "gds.graph.nodeProperties.stream",
        "gds.graph.nodeProperty.stream",
        "gds.graph.relationshipProperties.stream",
        "gds.graph.relationshipProperties.write",
        "gds.graph.relationshipProperty.stream",
        "gds.graph.nodeProperties.write",
        "gds.graph.relationship.write",

        "gds.labelPropagation.mutate",
        "gds.labelPropagation.mutate.estimate",
        "gds.labelPropagation.stats",
        "gds.labelPropagation.stats.estimate",
        "gds.labelPropagation.stream",
        "gds.labelPropagation.stream.estimate",
        "gds.labelPropagation.write",
        "gds.labelPropagation.write.estimate",

        "gds.license.state",

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

        "gds.shortestPath.astar.mutate",
        "gds.shortestPath.astar.mutate.estimate",
        "gds.shortestPath.astar.stream",
        "gds.shortestPath.astar.stream.estimate",
        "gds.shortestPath.astar.write",
        "gds.shortestPath.astar.write.estimate",

        "gds.shortestPath.dijkstra.stream",
        "gds.shortestPath.dijkstra.stream.estimate",
        "gds.shortestPath.dijkstra.mutate",
        "gds.shortestPath.dijkstra.mutate.estimate",
        "gds.shortestPath.dijkstra.write",
        "gds.shortestPath.dijkstra.write.estimate",

        "gds.shortestPath.yens.stream",
        "gds.shortestPath.yens.stream.estimate",
        "gds.shortestPath.yens.mutate",
        "gds.shortestPath.yens.mutate.estimate",
        "gds.shortestPath.yens.write",
        "gds.shortestPath.yens.write.estimate",

        "gds.graph.nodeProperties.drop",

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
        "gds.localClusteringCoefficient.write.estimate",

        "gds.kcore.stats",
        "gds.kcore.stats.estimate",
        "gds.kcore.stream",
        "gds.kcore.stream.estimate",
        "gds.kcore.mutate",
        "gds.kcore.mutate.estimate",
        "gds.kcore.write",
        "gds.kcore.write.estimate",

        "gds.version"
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
        "gds.isLicensed",

        "gds.alpha.linkprediction.adamicAdar",
        "gds.alpha.linkprediction.resourceAllocation",
        "gds.alpha.linkprediction.commonNeighbors",
        "gds.alpha.linkprediction.preferentialAttachment",
        "gds.alpha.linkprediction.totalNeighbors",
        "gds.alpha.linkprediction.sameCommunity",

        "gds.similarity.cosine",
        "gds.similarity.euclidean",
        "gds.similarity.euclideanDistance",
        "gds.similarity.jaccard",
        "gds.similarity.overlap",
        "gds.similarity.pearson",

        "gds.alpha.ml.oneHotEncoding",

        "gds.graph.exists"

    );

    private static final List<String> AGGREGATION_FUNCTIONS = List.of(
        "gds.graph.project"
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
        .of(PROCEDURES.stream(), FUNCTIONS.stream(), AGGREGATION_FUNCTIONS.stream())
        .flatMap(Function.identity())
        .sorted()
        .collect(Collectors.toList());

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(TestProcedureAndFunctionScanner.procedures());
        registerFunctions(TestProcedureAndFunctionScanner.functions());
        registerAggregationFunctions(TestProcedureAndFunctionScanner.aggregationFunctions());
    }

    @Test
    void countShouldMatch() {
        var returnedRows = runQueryWithRowConsumer(
            "CALL gds.list() YIELD name",
            ignored -> {}
        );

        // If you find yourself updating this count, please also update the count in SmokeTest.kt
        int expectedCount = 415;
        assertEquals(
            expectedCount,
            returnedRows,
            "The expected and registered procedures don't match. Please also update the SmokeTest counts."
        );
    }

    @Test
    void shouldListAllThingsExceptTheListProcedure() {
        assertThat(listProcs(null)).containsExactlyInAnyOrderElementsOf(ALL);
    }

    @Test
    void listFilteredResult() {
        assertEquals(PAGE_RANK, listProcs("pageRank"));
        assertEquals(asList("gds.pageRank.nm", "gds.pageRank.stream.estimate"), listProcs("pageRank.stream"));
        assertEquals(emptyList(), listProcs("foo"));
    }

    @Test
    void allProcsHaveDescriptions(SoftAssertions softly) {
        runQueryWithRowConsumer(
            "CALL gds.list()",
            resultRow -> softly
                .assertThat(resultRow.getString("description"))
                .withFailMessage(resultRow.get("name") + " has no description")
                .isNotEmpty()
        );
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
        var params = new HashMap<String, Object>();
        params.put("name", name);
        return runQuery(
            query,
            params,
            result -> result.<String>columnAs("name")
                .stream()
                .sorted()
                .collect(Collectors.toList())
        );
    }
}
