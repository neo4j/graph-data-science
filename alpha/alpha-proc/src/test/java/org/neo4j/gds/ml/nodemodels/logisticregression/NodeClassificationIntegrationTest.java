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
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictMutateProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.functions.AsNodeFunc;

import java.util.List;
import java.util.Map;

import static org.neo4j.graphalgo.assertj.ConditionFactory.hasSize;

class NodeClassificationIntegrationTest extends BaseProcTest {

    private static final String GRAPH =
        "CREATE " +
        "  (:N {name: '0_1', a: [1.2, 0.0], b: 1.2, class: 0})" +
        ", (:N {name: '0_2', a: [2.8, 0.0], b: 2.5, class: 0})" +
        ", (:N {name: '0_3', a: [3.3, 0.0], b: 0.5, class: 0})" +
        ", (:N {name: '0_4', a: [1.0, 0.0], b: 0.1, class: 0})" +
        ", (:N {name: '0_5', a: [1.32, 0.0], b: 0.0, class: 0})" +
        ", (:Hidden {name: '0_hidden', a: [2.32, 0.0], b: 3.2, class: 0})" +
        ", (:N {name: '1_1', a: [11.3, 0.0], b: 1.5, class: 1})" +
        ", (:N {name: '1_2', a: [34.3, 0.0], b: 10.5, class: 1})" +
        ", (:N {name: '1_3', a: [33.3, 0.0], b: 2.5, class: 1})" +
        ", (:N {name: '1_4', a: [93.0, 0.0], b: 66.8, class: 1})" +
        ", (:N {name: '1_5', a: [10.1, 0.0], b: 28.0, class: 1})" +
        ", (:N {name: '1_6', a: [11.66, 0.0], b: 2.8, class: 1})" +
        ", (:N {name: '1_7', a: [99.1, 0.0], b: 2.8, class: 1})" +
        ", (:N {name: '1_8', a: [19.66, 0.0], b: 0.8, class: 1})" +
        ", (:N {name: '1_9', a: [71.66, 0.0], b: 1.8, class: 1})" +
        ", (:N {name: '1_10', a: [11.1, 0.0], b: 2.2, class: 1})" +
        ", (:Hidden {name: '1_hidden', a: [22.32, 0.0], b: 3.2, class: 1})" +
        ", (:N {name: '2_1', a: [2.0, 0.0], b: -10.0, class: 2})" +
        ", (:N {name: '2_2', a: [2.0, 0.0], b: -1.6, class: 2})" +
        ", (:N {name: '2_3', a: [5.0, 0.0], b: -7.8, class: 2})" +
        ", (:N {name: '2_4', a: [5.0, 0.0], b: -73.8, class: 2})" +
        ", (:N {name: '2_5', a: [2.0, 0.0], b: -0.8, class: 2})" +
        ", (:N {name: '2_6', a: [5.0, 0.0], b: -7.8, class: 2})" +
        ", (:N {name: '2_7', a: [4.0, 0.0], b: -5.8, class: 2})" +
        ", (:N {name: '2_8', a: [1.0, 0.0], b: -0.9, class: 2})" +
        ", (:Hidden {name: '2_hidden', a: [3.0, 0.0], b: -10.9, class: 2})";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class,
            NodeClassificationTrainProc.class,
            NodeClassificationPredictMutateProc.class
        );
        registerFunctions(AsNodeFunc.class);
        runQuery(GRAPH);

        runQuery("CALL gds.graph.create('g', ['N', 'Hidden'], '*', {nodeProperties: ['a', 'b', 'class']})");
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void trainAndPredict() {
        var trainOnN = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                       "  nodeLabels: ['N']," +
                       "  modelName: 'model'," +
                       "  featureProperties: ['a', 'b'], " +
                       "  targetProperty: 'class', " +
                       "  metrics: ['F1_WEIGHTED'], " +
                       "  holdoutFraction: 0.2, " +
                       "  validationFolds: 5, " +
                       "  randomSeed: 2," +
                       "  concurrency: 1," +
                       "  params: [" +
                       "    {penalty: 0.0625, maxEpochs: 1000}, " +
                       "    {penalty: 0.125, maxEpochs: 1000}, " +
                       "    {penalty: 0.25, maxEpochs: 1000}, " +
                       "    {penalty: 0.5, maxEpochs: 1000}, " +
                       "    {penalty: 1.0, maxEpochs: 1000}, " +
                       "    {penalty: 2.0, maxEpochs: 1000}, " +
                       "    {penalty: 4.0, maxEpochs: 1000}" +
                       "  ]" +
                       "})";
        runQuery(trainOnN);

        var predictOnAll = "CALL gds.alpha.ml.nodeClassification.predict.mutate('g', {" +
                           "  nodeLabels: ['N', 'Hidden']," +
                           "  modelName: 'model'," +
                           "  mutateProperty: 'predicted_class', " +
                           "  predictedProbabilityProperty: 'predicted_proba'" +
                           "})";
        runQuery(predictOnAll);

        var predictedClasses = " CALL gds.graph.streamNodeProperties('g', ['predicted_class', 'predicted_proba'])" +
                               " YIELD nodeId, propertyValue" +
                               " WITH gds.util.asNode(nodeId).name AS name, propertyValue" +
                               " WITH name, collect(propertyValue) AS values" +
                               " RETURN name, values[0] AS predictedClass, values[1] AS probabilities" +
                               "   ORDER BY name ASC";

        assertCypherResult(predictedClasses, List.of(
            Map.of("name", "0_1", "predictedClass", 0L, "probabilities", hasSize(3)),
            Map.of("name", "0_2", "predictedClass", 0L, "probabilities", hasSize(3)),
            Map.of("name", "0_3", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "0_4", "predictedClass", 0L, "probabilities", hasSize(3)),
            Map.of("name", "0_5", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "0_hidden", "predictedClass", 0L, "probabilities", hasSize(3)),
            Map.of("name", "1_1", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_10", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_2", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_3", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_4", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_5", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_6", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_7", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_8", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_9", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "1_hidden", "predictedClass", 1L, "probabilities", hasSize(3)),
            Map.of("name", "2_1", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_2", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_3", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_4", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_5", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_6", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_7", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_8", "predictedClass", 2L, "probabilities", hasSize(3)),
            Map.of("name", "2_hidden", "predictedClass", 2L, "probabilities", hasSize(3))
        ));
    }
}
