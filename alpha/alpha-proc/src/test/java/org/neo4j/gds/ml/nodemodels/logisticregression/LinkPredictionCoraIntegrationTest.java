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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.ml.linkmodels.LinkPredictionPredictMutateProc;
import org.neo4j.gds.ml.linkmodels.LinkPredictionTrainProc;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.datasets.CommunityDbCreator;
import org.neo4j.graphalgo.datasets.Cora;
import org.neo4j.graphalgo.datasets.DatasetManager;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.graphalgo.model.catalog.ModelListProc;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.QueryRunner.runQueryWithResultConsumer;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerFunctions;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerProcedures;
import static org.neo4j.graphalgo.datasets.CoraSchema.CITES_TYPE;
import static org.neo4j.graphalgo.datasets.CoraSchema.PAPER_LABEL;
import static org.neo4j.graphalgo.datasets.CoraSchema.SUBJECT_NODE_PROPERTY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class LinkPredictionCoraIntegrationTest {

    private static final String GRAPH_CREATE_QUERY =
        "CALL gds.graph.create('g', 'Paper', {CITES: {orientation: 'UNDIRECTED'}})";
    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    // Minimum score ('test') for the AUCPR metric
    private static final double MIN_AUCPR = 0.8;
    private static final String TRAIN_GRAPH_REL_TYPE = "CITES_TRAINGRAPH";
    private static final String TEST_SET_REL_TYPE = "CITES_TESTSET";
    private static final String TRAIN_SET_REL_TYPE = "CITES_TRAINSET";
    private static final String EMBEDDING_GRAPH_REL_TYPE = "CITES_EMBEDDING";
    private static final String EMBEDDING_FEATURE = "frp";
    private static final String PREDICTED_REL_TYPE = "CITES_PREDICTED";

    private DatasetManager datasetManager;
    private GdsGraphDatabaseAPI cora;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        this.datasetManager = new DatasetManager(tempDir, CommunityDbCreator.getInstance());
        this.cora = datasetManager.openDb(Cora.ID);

        registerProcedures(
            cora,
            GraphCreateProc.class,
            FastRPMutateProc.class,
            GraphStreamRelationshipPropertiesProc.class,
            SplitRelationshipsMutateProc.class,
            LinkPredictionTrainProc.class,
            LinkPredictionPredictMutateProc.class,
            ModelListProc.class
        );

        registerFunctions(cora, AsNodeFunc.class);

        runQuery(cora, GRAPH_CREATE_QUERY, Map.of());
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
        datasetManager.closeDb(cora);
    }

    @Test
    void trainAndPredict() {
        testSplit();
        trainSplit();
        produceEmbeddings();
        trainModel();
        predict();
        assertModelScore();
        getResults();
    }

    private void testSplit() {
        var testSplitQuery = formatWithLocale(
            "CALL gds.alpha.ml.splitRelationships.mutate('%s', { " +
            " remainingRelationshipType: '%s', " +
            " holdoutRelationshipType: '%s', " +
            " holdoutFraction: .2" +
            "})",
            GRAPH_NAME,
            TRAIN_GRAPH_REL_TYPE,
            TEST_SET_REL_TYPE
        );
        runQuery(cora, testSplitQuery);
    }

    private void trainSplit() {
        var trainSplitQuery = formatWithLocale(
            "CALL gds.alpha.ml.splitRelationships.mutate('%s', { " +
            " relationshipTypes: ['%s'], " +
            " remainingRelationshipType: '%s'," +
            " holdoutRelationshipType: '%s', " +
            " holdoutFraction: .3" +
            "})",
            GRAPH_NAME,
            TRAIN_GRAPH_REL_TYPE,
            EMBEDDING_GRAPH_REL_TYPE,
            TRAIN_SET_REL_TYPE
        );
        runQuery(cora, trainSplitQuery);
    }

    private void produceEmbeddings() {
        var fastRpQuery = formatWithLocale(
            "CALL gds.fastRP.mutate('%s', { " +
            " relationshipTypes: ['%s'], " +
            " mutateProperty: '%s'," +
            " embeddingDimension: 512" +
            "})",
            GRAPH_NAME,
            EMBEDDING_GRAPH_REL_TYPE,
            EMBEDDING_FEATURE
        );
        runQuery(cora, fastRpQuery);
    }

    private void trainModel() {
        var trainQuery = formatWithLocale(
            "CALL gds.alpha.ml.linkPrediction.train('%s', { " +
            "  trainRelationshipType: '%s', " +
            "  testRelationshipType: '%s', " +
            "  modelName: '%s'," +
            "  featureProperties: ['%s'], " +
            "  validationFolds: 5, " +
            "  classRatio: 336.3," + // (2707 * 2706 / 2 - 10858) / 10858
            "  randomSeed: 2," +
            "  params: [" +
            "    {penalty: 9.999999999999991E-5, maxIterations: 1000}, " +
            "    {penalty: 7.742636826811261E-4, maxIterations: 1000}, " +
            "    {penalty: 0.005994842503189405, maxIterations: 1000}, " +
            "    {penalty: 0.046415888336127774, maxIterations: 1000}, " +
            "    {penalty: 0.3593813663804625, maxIterations: 1000}, " +
            "    {penalty: 2.7825594022071267, maxIterations: 1000}, " +
            "    {penalty: 21.544346900318843, maxIterations: 1000}," +
            "    {penalty: 166.81005372000587, maxIterations: 1000}," +
            "    {penalty: 1291.5496650148832, maxIterations: 1000}," +
            "    {penalty: 10000.00000000001, maxIterations: 1000}" +
            "  ]" +
            "})",
            GRAPH_NAME,
            TRAIN_SET_REL_TYPE,
            TEST_SET_REL_TYPE,
            MODEL_NAME,
            EMBEDDING_FEATURE,
            PAPER_LABEL.name(),
            SUBJECT_NODE_PROPERTY
        );
        runQuery(cora, trainQuery);
    }

    private void predict() {
        var predictQuery = formatWithLocale(
            "CALL gds.alpha.ml.linkPrediction.predict.mutate('%s', { " +
            "  relationshipTypes: ['%s'], " +
            "  modelName: '%s'," +
            "  mutateRelationshipType: '%s', " +
            "  topN: 100, " +
            "  threshold: 1e-2 " +
            "})",
            GRAPH_NAME,
            CITES_TYPE,
            MODEL_NAME,
            PREDICTED_REL_TYPE
        );
        runQuery(cora, predictQuery);
    }

    private void assertModelScore() {
//        var f1Score = formatWithLocale(
//            " CALL gds.beta.model.list('%s') YIELD modelInfo" +
//            " RETURN modelInfo.metrics.F1_WEIGHTED.test AS score ",
//            MODEL_NAME
//        );
//
//        assertThat(QueryRunner.<Double>runQuery(cora, f1Score, res -> (Double) res.next().get("score")))
//            .as("Metric score should not get worse over time.")
//            .isGreaterThan(MIN_AUCPR);
//
//        var accuracyScore = formatWithLocale(
//            " CALL gds.beta.model.list('%s') YIELD modelInfo" +
//            " RETURN modelInfo.metrics.ACCURACY.test AS score ",
//            MODEL_NAME
//        );

//        assertThat(QueryRunner.<Double>runQuery(cora, accuracyScore, res -> (Double) res.next().get("score")))
//            .as("Metric score should not get worse over time.")
//            .isGreaterThan(MIN_ACCURACY);
    }

    private void getResults() {
        var predictedLinks = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperty('%s', 'probability')" +
            " YIELD sourceNodeId, targetNodeId, relationshipType, propertyValue" +
            " WITH gds.util.asNode(sourceNodeId) AS sourceNode, gds.util.asNode(targetNodeId) AS targetNode, propertyValue" +
            " OPTIONAL MATCH (sourceNode)-[r]->(targetNode)" +
            " RETURN sourceNode, targetNode, propertyValue AS predictionStrength, r IS NOT NULL AS existedInGraph",
            GRAPH_NAME
        );

        var correctCounts = new HashMap<Integer, Integer>();
        var totalCounts = new HashMap<Integer, Integer>();

        runQueryWithResultConsumer(cora, predictedLinks, Map.of(), result -> System.out.println(result.resultAsString()));

//        runQueryWithRowConsumer(cora, predictedClasses, Map.of(), (transaction, row) -> {
//                var actual = SUBJECT_DICTIONARY.get(row.getString("actual"));
//                var predicted = row.getNumber("predicted").intValue();
//                if (actual == predicted) {
//                    correctCounts.compute(predicted, (key, value) -> value == null ? 1 : value + 1);
//                }
//                totalCounts.compute(predicted, (key, value) -> value == null ? 1 : value + 1);
//            }
//        );

//        correctCounts.forEach((predictedSubject, correctCount) -> {
//            var totalCount = totalCounts.get(predictedSubject);
//            assertThat((double) correctCount / totalCount)
//                .as("Check accuracy for subject: " + SUBJECT_DICTIONARY
//                    .entrySet()
//                    .stream()
//                    .filter(entry -> entry.getValue().equals(predictedSubject))
//                    .map(Map.Entry::getKey)
//                    .findFirst()
//                    .orElse("unknown subject"))
//                .isGreaterThan(MIN_ACCURACY);
//        });
    }
}
