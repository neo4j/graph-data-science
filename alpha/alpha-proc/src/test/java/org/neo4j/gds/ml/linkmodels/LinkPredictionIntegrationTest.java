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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;
import org.neo4j.gds.model.catalog.ModelListProc;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LinkPredictionIntegrationTest extends BaseProcTest {

    // Five cliques of size 2, 3, or 4
    private static final String GRAPH =
        "CREATE " +
        "(a:N {z: 0}), " +
        "(b:N {z: 0}), " +
        "(c:N {z: 0}), " +
        "(d:N {z: 0}), " +
        "(e:N {z: 100}), " +
        "(f:N {z: 100}), " +
        "(g:N {z: 100}), " +
        "(h:N {z: 200}), " +
        "(i:N {z: 200}), " +
        "(j:N {z: 300}), " +
        "(k:N {z: 300}), " +
        "(l:N {z: 300}), " +
        "(m:N {z: 400}), " +
        "(n:N {z: 400}), " +
        "(o:N {z: 400}), " +

        "(a)-[:REL {label: 1}]->(b), " +
        "(a)-[:REL {label: 1}]->(c), " +       
        "(a)-[:REL {label: 1}]->(d), " +
        "(b)-[:REL {label: 1}]->(c), " +
        "(b)-[:REL {label: 1}]->(d), " +       
        "(c)-[:REL {label: 1}]->(d), " +

        "(e)-[:REL {label: 1}]->(f), " +       
        "(e)-[:REL {label: 1}]->(g), " +
        "(f)-[:REL {label: 1}]->(g), " +

        "(h)-[:REL {label: 1}]->(i), " +

        "(j)-[:REL {label: 1}]->(k), " +
        "(j)-[:REL {label: 1}]->(l), " +
        "(k)-[:REL {label: 1}]->(l), " +

        "(m)-[:REL {label: 1}]->(n), " +       
        "(m)-[:REL {label: 1}]->(o), " +       
        "(n)-[:REL {label: 1}]->(o), " +

        "(a)-[:REL {label: 0}]->(e), " +
        "(a)-[:REL {label: 0}]->(o), " +
        "(b)-[:REL {label: 0}]->(e), " +
        "(e)-[:REL {label: 0}]->(i), " +
        "(e)-[:REL {label: 0}]->(o), " +
        "(e)-[:REL {label: 0}]->(n), " +
        "(h)-[:REL {label: 0}]->(k), " +
        "(h)-[:REL {label: 0}]->(m), " +
        "(i)-[:REL {label: 0}]->(j), " +
        "(k)-[:REL {label: 0}]->(m), " +
        "(k)-[:REL {label: 0}]->(o), " +
        "(a)-[:REL {label: 0}]->(f), " +
        "(b)-[:REL {label: 0}]->(f), " +
        "(i)-[:REL {label: 0}]->(k), " +
        "(j)-[:REL {label: 0}]->(o), " +
        "(k)-[:REL {label: 0}]->(o)";

    private static final String GRAPH_NAME = "graph";
    private static final String MODEL_NAME = "model";
    private static final String TRAIN_GRAPH_REL_TYPE = "REL_TRAINGRAPH";
    private static final String TEST_SET_REL_TYPE = "REL_TESTSET";
    private static final String TRAIN_SET_REL_TYPE = "REL_TRAINSET";
    private static final String EMBEDDING_GRAPH_REL_TYPE = "REL_EMBEDDING";
    private static final String EMBEDDING_FEATURE = "embedding";
    private static final String PREDICTED_REL_TYPE = "REL_PREDICTED";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            FastRPMutateProc.class,
            GraphStreamRelationshipPropertiesProc.class,
            SplitRelationshipsMutateProc.class,
            LinkPredictionTrainProc.class,
            LinkPredictionPredictMutateProc.class,
            ModelListProc.class
        );

        registerFunctions(AsNodeFunc.class);

        runQuery(GRAPH);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void trainAndPredict() {
        createGraph();
        testSplit();
        trainSplit();
        fastRPEmbeddings();
        trainModel();
        predict();
        assertModelScore();
        //getResults();
    }

    private void createGraph() {
        var graphCreateQuery =
            "CALL gds.graph.create(" +
            "  $graphName," +
            "  {N: {properties: 'z'}}," +
            "  {REL: {orientation: 'UNDIRECTED', properties: 'label'}}" +
            ");";

        runQuery(graphCreateQuery, Map.of("graphName", GRAPH_NAME));
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
            trainSplitQuery,
            Map.of(
                "graphName", GRAPH_NAME,
                "trainGraphRelType", TRAIN_GRAPH_REL_TYPE,
                "embeddingGraphRelType", EMBEDDING_GRAPH_REL_TYPE,
                "trainSetRelType", TRAIN_SET_REL_TYPE
            )
        );
    }

    private void fastRPEmbeddings() {
        var fastRpQuery = "CALL gds.fastRP.mutate($graphName, " +
                          "{" +
                          "  relationshipTypes: [$embeddingRelType]," +
                          "  mutateProperty: $mutateProperty, " +
                          "  embeddingDimension: 512, " +
                          "  propertyRatio: 0.5, " +
                          "  randomSeed: 42, " +
                          "  featureProperties: ['z']" +
                          "})";

        runQuery(fastRpQuery, Map.of(
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
            "  negativeClassWeight: 0.01," +
            "  randomSeed: 2," +
            "  params: [{penalty: 0.3593813663804625, maxEpochs: 1000}]" +
            "})";

        runQuery(trainQuery, Map.of(
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
            "  relationshipTypes: [$relType], " +
            "  modelName: $modelName," +
            "  mutateRelationshipType: $predictRelType, " +
            "  topN: 1, " +
            "  threshold: 0.4 " +
            "})";

        runQuery(predictQuery, Map.of(
            "graphName", GRAPH_NAME,
            "relType", "REL",
            "modelName", MODEL_NAME,
            "predictRelType", PREDICTED_REL_TYPE
        ));
    }

    private void assertModelScore() {
        var scoreQuery =
            " CALL gds.beta.model.list($modelName) YIELD modelInfo" +
            " RETURN modelInfo.metrics.AUCPR.test AS score";

        runQueryWithRowConsumer(scoreQuery, Map.of("modelName", MODEL_NAME), (row) -> {
            assertThat(row.getNumber("score").doubleValue()).isCloseTo(0.82D, Offset.offset(1e-2));
        });
    }
}
