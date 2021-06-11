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
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictMutateProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.datasets.CommunityDbCreator;
import org.neo4j.graphalgo.datasets.Cora;
import org.neo4j.graphalgo.datasets.DatasetManager;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.gds.model.catalog.ModelListProc;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.QueryRunner.runQueryWithRowConsumer;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerFunctions;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerProcedures;
import static org.neo4j.graphalgo.datasets.CoraSchema.CITES_TYPE;
import static org.neo4j.graphalgo.datasets.CoraSchema.EXT_ID_NODE_PROPERTY;
import static org.neo4j.graphalgo.datasets.CoraSchema.PAPER_LABEL;
import static org.neo4j.graphalgo.datasets.CoraSchema.SUBJECT_NODE_PROPERTY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class NodeClassificationCoraIntegrationTest {

    private static final String GRAPH_CREATE_QUERY;
    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    // Minimum score ('test') for the F1_WEIGHTED metric
    private static final double MIN_F1_SCORE = 0.82;
    // The ratio of nodes being classified with the correct subject
    private static final double MIN_ACCURACY = 0.82;

    private static final Map<String, Integer> SUBJECT_DICTIONARY = Map.of(
        "Neural_Networks", 0,
        "Rule_Learning", 1,
        "Reinforcement_Learning", 2,
        "Probabilistic_Methods", 3,
        "Theory", 4,
        "Genetic_Algorithms", 5,
        "Case_Based", 6
    );

    static {
        var caseWhenExpression = SUBJECT_DICTIONARY.entrySet()
            .stream()
            .map(entry -> formatWithLocale(
                "WHEN \\\"%s\\\" THEN %d",
                entry.getKey(),
                entry.getValue()
            ))
            .collect(Collectors.joining(" ", formatWithLocale("CASE n.%s ", SUBJECT_NODE_PROPERTY), " ELSE -1 END"));

        var nodeQuery = formatWithLocale(
            "MATCH (n:%s) RETURN id(n) AS id, labels(n) AS labels, %s AS %3$s, n.%4$s AS %4$s, 0 AS class",
            PAPER_LABEL.name(),
            caseWhenExpression,
            SUBJECT_NODE_PROPERTY,
            EXT_ID_NODE_PROPERTY
        );

        var relationshipQuery = formatWithLocale(
            "MATCH (n:%1$s)-[:%2$s]-(m:%1$s) RETURN id(n) AS source, id(m) AS target, \\\"%2$s\\\" AS type",
            PAPER_LABEL.name(),
            CITES_TYPE
        );

        GRAPH_CREATE_QUERY = formatWithLocale(
            "CALL gds.graph.create.cypher('%s', '%s', '%s')",
            GRAPH_NAME,
            nodeQuery,
            relationshipQuery
        );
    }

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
            GraphStreamNodePropertiesProc.class,
            NodeClassificationTrainProc.class,
            NodeClassificationPredictMutateProc.class,
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
        produceEmbeddings();
        trainModel();
        predict();
        assertModelScore();
        getResults();
    }

    private void produceEmbeddings() {
        var fastRp = formatWithLocale(
            "CALL gds.fastRP.mutate('%s', {" +
            " mutateProperty: 'frp'," +
            " embeddingDimension: 512" +
            "})",
            GRAPH_NAME
        );
        runQuery(cora, fastRp);
    }

    private void trainModel() {
        var trainQuery = formatWithLocale(
            "CALL gds.alpha.ml.nodeClassification.train('%s', {" +
            "  nodeLabels: ['%s']," +
            "  modelName: 'model'," +
            "  featureProperties: ['frp'], " +
            "  targetProperty: '%s', " +
            "  metrics: ['F1_WEIGHTED', 'ACCURACY'], " +
            "  holdoutFraction: 0.2, " +
            "  validationFolds: 5, " +
            "  randomSeed: 2," +
            "  concurrency: 1," +
            "  params: [" +
            "    {penalty: 9.999999999999991E-5, maxEpochs: 1000}, " +
            "    {penalty: 7.742636826811261E-4, maxEpochs: 1000}, " +
            "    {penalty: 0.005994842503189405, maxEpochs: 1000}, " +
            "    {penalty: 0.046415888336127774, maxEpochs: 1000}, " +
            "    {penalty: 0.3593813663804625, maxEpochs: 1000}, " +
            "    {penalty: 2.7825594022071267, maxEpochs: 1000}, " +
            "    {penalty: 21.544346900318843, maxEpochs: 1000}," +
            "    {penalty: 166.81005372000587, maxEpochs: 1000}," +
            "    {penalty: 1291.5496650148832, maxEpochs: 1000}," +
            "    {penalty: 10000.00000000001, maxEpochs: 1000}" +
            "  ]" +
            "})",
            GRAPH_NAME,
            PAPER_LABEL.name(),
            SUBJECT_NODE_PROPERTY
        );
        runQuery(cora, trainQuery);
    }

    private void predict() {
        var predictOnAll = formatWithLocale(
            "CALL gds.alpha.ml.nodeClassification.predict.mutate('%s', {" +
            "  nodeLabels: ['%s']," +
            "  modelName: '%s'," +
            "  mutateProperty: 'predicted_class', " +
            "  predictedProbabilityProperty: 'predicted_proba'" +
            "})",
            GRAPH_NAME,
            PAPER_LABEL.name(),
            MODEL_NAME
        );
        runQuery(cora, predictOnAll);
    }

    private void assertModelScore() {
        var f1Score = formatWithLocale(
            " CALL gds.beta.model.list('%s') YIELD modelInfo" +
            " RETURN modelInfo.metrics.F1_WEIGHTED.test AS score ",
            MODEL_NAME
        );

        assertThat(QueryRunner.<Double>runQuery(cora, f1Score, res -> (Double) res.next().get("score")))
            .as("Metric score should not get worse over time.")
            .isGreaterThan(MIN_F1_SCORE);

        var accuracyScore = formatWithLocale(
            " CALL gds.beta.model.list('%s') YIELD modelInfo" +
            " RETURN modelInfo.metrics.ACCURACY.test AS score ",
            MODEL_NAME
        );

        assertThat(QueryRunner.<Double>runQuery(cora, accuracyScore, res -> (Double) res.next().get("score")))
            .as("Metric score should not get worse over time.")
            .isGreaterThan(MIN_ACCURACY);
    }

    private void getResults() {
        var predictedClasses = formatWithLocale(
            "CALL gds.graph.streamNodeProperties('%s', ['predicted_class', 'predicted_proba'])" +
            " YIELD nodeId, propertyValue" +
            " WITH gds.util.asNode(nodeId) AS node, propertyValue" +
            " WITH node, collect(propertyValue) AS values" +
            " RETURN node.subject AS actual, values[0] AS predicted, values[1] AS probabilities",
            GRAPH_NAME
        );

        var correctCounts = new HashMap<Integer, Integer>();
        var totalCounts = new HashMap<Integer, Integer>();

        runQueryWithRowConsumer(cora, predictedClasses, Map.of(), (transaction, row) -> {
                var actual = SUBJECT_DICTIONARY.get(row.getString("actual"));
                var predicted = row.getNumber("predicted").intValue();
                if (actual == predicted) {
                    correctCounts.compute(predicted, (key, value) -> value == null ? 1 : value + 1);
                }
                totalCounts.compute(predicted, (key, value) -> value == null ? 1 : value + 1);
            }
        );

        correctCounts.forEach((predictedSubject, correctCount) -> {
            var totalCount = totalCounts.get(predictedSubject);
            assertThat((double) correctCount / totalCount)
                .as("Check accuracy for subject: " + SUBJECT_DICTIONARY
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().equals(predictedSubject))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse("unknown subject"))
                .isGreaterThan(MIN_ACCURACY);
        });
    }
}
