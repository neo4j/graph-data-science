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
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.utils.TestProcedureAndFunctionScanner;
import org.neo4j.graphdb.Result;

import java.util.ArrayList;
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
        "gds.alpha.graph.graphProperty.stream",
        "gds.alpha.graph.graphProperty.drop",

        "gds.alpha.graph.sample.rwr",

        "gds.alpha.create.cypherdb",

        "gds.alpha.allShortestPaths.stream",

        "gds.beta.collapsePath.mutate",

        "gds.alpha.conductance.stream",

        "gds.beta.closeness.mutate",
        "gds.beta.closeness.stats",
        "gds.beta.closeness.stream",
        "gds.beta.closeness.write",

        "gds.alpha.closeness.harmonic.write",
        "gds.alpha.closeness.harmonic.stream",

        "gds.beta.leiden.mutate",
        "gds.beta.leiden.mutate.estimate",
        "gds.beta.leiden.stats",
        "gds.beta.leiden.stats.estimate",
        "gds.beta.leiden.stream",
        "gds.beta.leiden.stream.estimate",
        "gds.beta.leiden.write",
        "gds.beta.leiden.write.estimate",

        "gds.beta.kmeans.mutate",
        "gds.beta.kmeans.mutate.estimate",
        "gds.beta.kmeans.stats",
        "gds.beta.kmeans.stats.estimate",
        "gds.beta.kmeans.stream",
        "gds.beta.kmeans.stream.estimate",
        "gds.beta.kmeans.write",
        "gds.beta.kmeans.write.estimate",

        "gds.alpha.knn.filtered.mutate",
        "gds.alpha.knn.filtered.stats",
        "gds.alpha.knn.filtered.stream",
        "gds.alpha.knn.filtered.write",

        "gds.alpha.maxkcut.mutate",
        "gds.alpha.maxkcut.mutate.estimate",
        "gds.alpha.maxkcut.stream",
        "gds.alpha.maxkcut.stream.estimate",

        "gds.alpha.modularity.stats",
        "gds.alpha.modularity.stream",

        "gds.alpha.hashgnn.mutate",
        "gds.alpha.hashgnn.mutate.estimate",
        "gds.alpha.hashgnn.stream",
        "gds.alpha.hashgnn.stream.estimate",

        "gds.alpha.hits.mutate",
        "gds.alpha.hits.mutate.estimate",
        "gds.alpha.hits.stats",
        "gds.alpha.hits.stats.estimate",
        "gds.alpha.hits.stream",
        "gds.alpha.hits.stream.estimate",
        "gds.alpha.hits.write",
        "gds.alpha.hits.write.estimate",

        "gds.alpha.nodeSimilarity.filtered.mutate",
        "gds.alpha.nodeSimilarity.filtered.mutate.estimate",
        "gds.alpha.nodeSimilarity.filtered.stats",
        "gds.alpha.nodeSimilarity.filtered.stats.estimate",
        "gds.alpha.nodeSimilarity.filtered.stream",
        "gds.alpha.nodeSimilarity.filtered.stream.estimate",
        "gds.alpha.nodeSimilarity.filtered.write",
        "gds.alpha.nodeSimilarity.filtered.write.estimate",

        "gds.beta.pipeline.linkPrediction.addFeature",
        "gds.beta.pipeline.linkPrediction.addNodeProperty",
        "gds.beta.pipeline.linkPrediction.addLogisticRegression",
        "gds.alpha.pipeline.linkPrediction.addMLP",
        "gds.alpha.pipeline.linkPrediction.addRandomForest",
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
        "gds.alpha.pipeline.nodeClassification.addRandomForest",
        "gds.alpha.pipeline.nodeClassification.configureAutoTuning",
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

        "gds.alpha.scc.write",
        "gds.alpha.scc.stream",

        "gds.alpha.scaleProperties.mutate",
        "gds.alpha.scaleProperties.stream",

        "gds.alpha.sllpa.mutate",
        "gds.alpha.sllpa.mutate.estimate",
        "gds.alpha.sllpa.stats",
        "gds.alpha.sllpa.stats.estimate",
        "gds.alpha.sllpa.stream",
        "gds.alpha.sllpa.stream.estimate",
        "gds.alpha.sllpa.write",
        "gds.alpha.sllpa.write.estimate",

        "gds.beta.spanningTree.mutate",
        "gds.beta.spanningTree.mutate.estimate",
        "gds.beta.spanningTree.stats",
        "gds.beta.spanningTree.stats.estimate",
        "gds.beta.spanningTree.stream",
        "gds.beta.spanningTree.stream.estimate",
        "gds.beta.spanningTree.write",
        "gds.beta.spanningTree.write.estimate",
        "gds.alpha.spanningTree.kmax.write",
        "gds.alpha.spanningTree.kmin.write",

        "gds.alpha.triangles",
        "gds.alpha.ml.splitRelationships.mutate",
        "gds.alpha.influenceMaximization.greedy.stream",

        "gds.beta.influenceMaximization.celf.mutate",
        "gds.beta.influenceMaximization.celf.mutate.estimate",
        "gds.beta.influenceMaximization.celf.stats",
        "gds.beta.influenceMaximization.celf.stats.estimate",
        "gds.beta.influenceMaximization.celf.stream",
        "gds.beta.influenceMaximization.celf.stream.estimate",
        "gds.beta.influenceMaximization.celf.write",
        "gds.beta.influenceMaximization.celf.write.estimate",


        "gds.alpha.userLog",

        "gds.articleRank.mutate",
        "gds.articleRank.mutate.estimate",
        "gds.articleRank.stats",
        "gds.articleRank.stats.estimate",
        "gds.articleRank.stream",
        "gds.articleRank.stream.estimate",
        "gds.articleRank.write",
        "gds.articleRank.write.estimate",

        "gds.beta.graph.export.csv",
        "gds.beta.graph.export.csv.estimate",

        "gds.beta.node2vec.mutate",
        "gds.beta.node2vec.mutate.estimate",
        "gds.beta.node2vec.stream",
        "gds.beta.node2vec.stream.estimate",
        "gds.beta.node2vec.write",
        "gds.beta.node2vec.write.estimate",

        "gds.beta.graphSage.mutate",
        "gds.beta.graphSage.mutate.estimate",
        "gds.beta.graphSage.stream",
        "gds.beta.graphSage.stream.estimate",
        "gds.beta.graphSage.train",
        "gds.beta.graphSage.train.estimate",
        "gds.beta.graphSage.write",
        "gds.beta.graphSage.write.estimate",

        "gds.beta.graph.generate",
        "gds.beta.graph.project.subgraph",

        "gds.beta.k1coloring.mutate",
        "gds.beta.k1coloring.mutate.estimate",
        "gds.beta.k1coloring.stats",
        "gds.beta.k1coloring.stats.estimate",
        "gds.beta.k1coloring.stream",
        "gds.beta.k1coloring.stream.estimate",
        "gds.beta.k1coloring.write",
        "gds.beta.k1coloring.write.estimate",

        "gds.knn.mutate",
        "gds.knn.mutate.estimate",
        "gds.knn.stats",
        "gds.knn.stats.estimate",
        "gds.knn.stream",
        "gds.knn.stream.estimate",
        "gds.knn.write",
        "gds.knn.write.estimate",

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

        "gds.beta.pipeline.drop",
        "gds.beta.pipeline.exists",
        "gds.beta.pipeline.list",

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

        "gds.beta.graph.relationships.stream",

        "gds.graph.deleteRelationships",
        "gds.graph.relationships.drop",
        "gds.graph.drop",
        "gds.graph.exists",
        "gds.graph.list",
        "gds.graph.project",
        "gds.graph.project.cypher",
        "gds.graph.project.cypher.estimate",
        "gds.graph.project.estimate",
        "gds.graph.streamNodeProperties",
        "gds.graph.nodeProperties.stream",
        "gds.graph.streamNodeProperty",
        "gds.graph.nodeProperty.stream",
        "gds.graph.streamRelationshipProperties",
        "gds.graph.relationshipProperties.stream",
        "gds.graph.streamRelationshipProperty",
        "gds.graph.relationshipProperty.stream",
        "gds.graph.writeNodeProperties",
        "gds.graph.nodeProperties.write",
        "gds.graph.writeRelationship",
        "gds.graph.relationship.write",

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

        "gds.graph.removeNodeProperties",
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
        "gds.alpha.graph.project"
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
        var registeredProcedures = new ArrayList<>();
        runQueryWithRowConsumer(
            "CALL gds.list() YIELD name",
            row -> registeredProcedures.add(row.getString("name"))
        );

        // If you find yourself updating this count, please also update the count in SmokeTest.kt
        int expectedCount = 371;
        assertEquals(
            expectedCount,
            registeredProcedures.size(),
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
        assertEquals(asList("gds.pageRank.stream", "gds.pageRank.stream.estimate"), listProcs("pageRank.stream"));
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
