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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;
import org.neo4j.graphalgo.beta.fastrp.FastRPExtendedMutateProc;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.datasets.CommunityDbCreator;
import org.neo4j.graphalgo.datasets.Cora;
import org.neo4j.graphalgo.datasets.DatasetManager;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.gds.model.catalog.ModelListProc;
import org.neo4j.graphdb.Result;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.QueryRunner.runQueryWithResultConsumer;
import static org.neo4j.graphalgo.QueryRunner.runQueryWithRowConsumer;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerFunctions;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerProcedures;
import static org.neo4j.graphalgo.datasets.CoraSchema.CITES_TYPE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class LinkPredictionCoraIntegrationTest {

    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    // Minimum score ('test') for the AUCPR metric
    private static final String TRAIN_GRAPH_REL_TYPE = "CITES_TRAINGRAPH";
    private static final String TEST_SET_REL_TYPE = "CITES_TESTSET";
    private static final String TRAIN_SET_REL_TYPE = "CITES_TRAINSET";
    private static final String EMBEDDING_GRAPH_REL_TYPE = "CITES_EMBEDDING";
    private static final String EMBEDDING_FEATURE = "frp";
    private static final String PREDICTED_REL_TYPE = "CITES_PREDICTED";
    private static final int NUMBER_OF_FEATURES = 1433;
    private static final double MIN_AUCPR_TEST_SCORE = 0.25;

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
            FastRPExtendedMutateProc.class,
            GraphStreamRelationshipPropertiesProc.class,
            SplitRelationshipsMutateProc.class,
            LinkPredictionTrainProc.class,
            LinkPredictionPredictMutateProc.class,
            ModelListProc.class
        );

        registerFunctions(cora, AsNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
        datasetManager.closeDb(cora);
    }

    @Test
    void trainAndPredict() {
        createGraph();
        testSplit();
        trainSplit();
        fastRPExtendedEmbeddings();
        trainModel();
        predict();
        assertModelScore();
        //getResults();
    }

    private void createGraph() {
        IntFunction<String> nodePropertyTemplate = (i) -> formatWithLocale("w%d: {defaultValue: 0.0}", i);
        var nodeProperties = IntStream.range(0, NUMBER_OF_FEATURES)
            .mapToObj(nodePropertyTemplate)
            .collect(Collectors.joining(", "));

        var graphCreateQuery = "CALL gds.graph.create(" +
                  "  $graphName," +
                  "  {" +
                  "     Paper : {" +
                  "         properties: {"+ nodeProperties +"}" +
                  "     }" +
                  "  }," +
                  "  {CITES: {orientation: 'UNDIRECTED'}}" +
                  ");";

        runQuery(cora, graphCreateQuery, Map.of("graphName", GRAPH_NAME));
    }

    private void testSplit() {
        var testSplitQuery =
            "CALL gds.alpha.ml.splitRelationships.mutate($graphName, { " +
            " remainingRelationshipType: $trainGraphRelType, " +
            " holdoutRelationshipType: $testSetRelType, " +
            " holdoutFraction: 0.1," +
            " negativeSamplingRatio: 1.0," +
            " randomSeed: 42" +
            "})";

        runQuery(
            cora,
            testSplitQuery,
            Map.of(
                "graphName", GRAPH_NAME,
                "trainGraphRelType", TRAIN_GRAPH_REL_TYPE,
                "testSetRelType", TEST_SET_REL_TYPE
            )
        );
    }

    private void trainSplit() {
        var trainSplitQuery =
            "CALL gds.alpha.ml.splitRelationships.mutate($graphName, { " +
            " relationshipTypes: [$trainGraphRelType], " +
            " remainingRelationshipType: $embeddingGraphRelType, " +
            " holdoutRelationshipType: $trainSetRelType, " +
            " holdoutFraction: 0.1," +
            " negativeSamplingRatio: 1.0," +
            " randomSeed: 42" +
            "})";

        runQuery(
            cora,
            trainSplitQuery,
            Map.of(
                "graphName", GRAPH_NAME,
                "trainGraphRelType", TRAIN_GRAPH_REL_TYPE,
                "embeddingGraphRelType", EMBEDDING_GRAPH_REL_TYPE,
                "trainSetRelType", TRAIN_SET_REL_TYPE
            )
        );
    }

    private void fastRPExtendedEmbeddings() {
        IntFunction<String> featureProperty = (i) -> formatWithLocale("'w%d'", i);
        var featureProperties = IntStream.range(0, NUMBER_OF_FEATURES)
            .mapToObj(featureProperty)
            .collect(Collectors.joining(", "));

        var fastRpQuery = "CALL gds.beta.fastRPExtended.mutate($graphName, " +
                          "{" +
                          "  relationshipTypes: [$embeddingRelType]," +
                          "  mutateProperty: $mutateProperty, " +
                          "  embeddingDimension: 512, " +
                          "  propertyDimension: 256, " +
                          "  randomSeed: 42, " +
                          "  featureProperties: [" +featureProperties+ "]" +
                          "})";

        runQuery(cora, fastRpQuery, Map.of(
            "graphName", GRAPH_NAME,
            "embeddingRelType", EMBEDDING_GRAPH_REL_TYPE,
            "mutateProperty", EMBEDDING_FEATURE
        ));
    }

    private void trainModel() {
        var trainQuery =
            "CALL gds.alpha.ml.linkPrediction.train($graphName, { " +
            "  trainRelationshipType: $trainSetRelType, " +
            "  testRelationshipType: $testSetRelType, " +
            "  modelName: $modelName," +
            "  featureProperties: [$embeddingFeature], " +
            "  validationFolds: 5, " +
            "  negativeClassWeight: 673.6," + // (2707 * 2706 - 10858) / 10858
            "  randomSeed: 2," +
            "  concurrency: 1," +
            "  params: [" +
            "    {penalty: 9.999999999999991E-5, maxEpochs: 1000}, " +
            "    {penalty: 0.3593813663804625, maxEpochs: 1000}, " +
            "    {penalty: 2.7825594022071267, maxEpochs: 1000}, " +
            "    {penalty: 21.544346900318843, maxEpochs: 1000}," +
            "    {penalty: 10000.00000000001, maxEpochs: 1000}" +
            "  ]" +
            "}) " +
            "YIELD modelInfo " +
            "RETURN modelInfo.metrics.AUCPR.outerTrain, modelInfo.metrics.AUCPR.test, modelInfo.bestParameters";

        runQuery(cora, trainQuery, Map.of(
            "graphName", GRAPH_NAME,
            "trainSetRelType", TRAIN_SET_REL_TYPE,
            "testSetRelType", TEST_SET_REL_TYPE,
            "modelName", MODEL_NAME,
            "embeddingFeature", EMBEDDING_FEATURE
        ));
    }

    private void predict() {
        var predictQuery =
            "CALL gds.alpha.ml.linkPrediction.predict.mutate($graphName, { " +
            "  relationshipTypes: [$citesRelType], " +
            "  modelName: $modelName," +
            "  mutateRelationshipType: $predictRelType, " +
            "  topN: 10000, " +
            "  threshold: 0.4 " +
            "})";

        runQuery(cora, predictQuery, Map.of(
            "graphName", GRAPH_NAME,
            "citesRelType", CITES_TYPE,
            "modelName", MODEL_NAME,
            "predictRelType", PREDICTED_REL_TYPE
        ));
    }

    private void assertModelScore() {
        var scoreQuery =
            " CALL gds.beta.model.list($modelName) YIELD modelInfo" +
            " RETURN modelInfo.metrics.AUCPR.test AS score";

        runQueryWithRowConsumer(cora, scoreQuery, Map.of("modelName", MODEL_NAME), (tx, row) -> {
            assertThat(row.getNumber("score").doubleValue()).isGreaterThan(MIN_AUCPR_TEST_SCORE);
        });
    }

    private void getResults() {
        var predictedLinks =
            "CALL gds.graph.streamRelationshipProperty($graphName, 'probability')" +
            " YIELD sourceNodeId, targetNodeId, relationshipType, propertyValue" +
            " RETURN distinct(propertyValue) AS predictionStrength" +
            " ORDER BY predictionStrength DESC";

        var counter = new AtomicInteger(0);
        runQueryWithResultConsumer(
            cora,
            predictedLinks,
            Map.of("graphName", GRAPH_NAME),
            (result) -> {
                //noinspection CatchMayIgnoreException
                try {
                    result.accept((Result.ResultVisitor<Exception>) row -> {
                        counter.incrementAndGet();
                        return true;
                    });
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        );
        assertThat(counter.get()).isGreaterThan(1);
    }
}
