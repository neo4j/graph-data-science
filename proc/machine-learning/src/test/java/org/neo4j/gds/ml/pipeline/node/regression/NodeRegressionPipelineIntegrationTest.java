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
package org.neo4j.gds.ml.pipeline.node.regression;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineAddStepProcs;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineAddTrainerMethodProcs;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineConfigureSplitProc;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineCreateProc;
import org.neo4j.gds.ml.pipeline.node.regression.predict.NodeRegressionPipelineStreamProc;
import org.neo4j.gds.model.catalog.ModelListProc;
import org.neo4j.gds.wcc.WccMutateProc;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;

@Neo4jModelCatalogExtension
final class NodeRegressionPipelineIntegrationTest extends BaseProcTest {

    private static final String GRAPH =
        "CREATE " +
        "  (a1:N {name: '0_1', a: [1.2, 0.0], b: 1.2, cost: 2.4})" +
        ", (a2:N {name: '0_2', a: [2.8, 0.0], b: 2.5, cost: 5.3})" +
        ", (a3:N {name: '0_3', a: [3.3, 0.0], b: 0.5, cost: 3.8})" +
        ", (a4:N {name: '0_4', a: [1.0, 0.0], b: 0.1, cost: 1.1})" +
        ", (a5:N {name: '0_5', a: [1.32, 0.0], b: 0.0, cost: 1.32})" +
        ", (a6h:Hidden {name: '0_hidden', a: [2.32, 0.0], b: 3.2, cost: 5.52})" +
        ", (b1:N {name: '1_1', a: [11.3, 0.0], b: 1.5, cost: 12.8})" +
        ", (b2:N {name: '1_2', a: [34.3, 0.0], b: 10.5, cost: 44.8})" +
        ", (b3:N {name: '1_3', a: [33.3, 0.0], b: 2.5, cost: 35.8})" +
        ", (b4:N {name: '1_4', a: [93.0, 0.0], b: 66.8, cost: 159.8})" +
        ", (b5:N {name: '1_5', a: [10.1, 0.0], b: 28.0, cost: 38.1})" +
        ", (b6:N {name: '1_6', a: [11.66, 0.0], b: 2.8, cost: 14.46})" +
        ", (b7:N {name: '1_7', a: [99.1, 0.0], b: 2.8, cost: 101.9})" +
        ", (b8:N {name: '1_8', a: [19.66, 0.0], b: 0.8, cost: 20.46})" +
        ", (b9:N {name: '1_9', a: [71.66, 0.0], b: 1.8, cost: 73.46})" +
        ", (b10:N {name: '1_10', a: [11.1, 0.0], b: 2.2, cost: 13.3})" +
        ", (b11h:Hidden {name: '1_hidden', a: [22.32, 0.0], b: 3.2, cost: 25.52})" +
        ", (c1:N {name: '2_1', a: [2.0, 0.0], b: -10.0, cost: -8.0})" +
        ", (c2:N {name: '2_2', a: [2.0, 0.0], b: -1.6, cost: 0.4})" +
        ", (c3:N {name: '2_3', a: [5.0, 0.0], b: -7.8, cost: -2.8})" +
        ", (c4:N {name: '2_4', a: [5.0, 0.0], b: -73.8, cost: -68.8})" +
        ", (c5:N {name: '2_5', a: [2.0, 0.0], b: -0.8, cost: 1.2})" +
        ", (c6:N {name: '2_6', a: [5.0, 0.0], b: -7.8, cost: -2.8})" +
        ", (c7:N {name: '2_7', a: [4.0, 0.0], b: -5.8, cost: -1.8})" +
        ", (c8:N {name: '2_8', a: [1.0, 0.0], b: -0.9, cost: 0.1})" +
        ", (c9h:Hidden {name: '2_hidden', a: [3.0, 0.0], b: -10.9, cost: -7.9})" +
        ", (a1)-[:T {w: 13}]->(a2)" +
        ", (a2)-[:T {w: 13}]->(c1)" +
        ", (a3)-[:T {w: 13}]->(b1)" +
        ", (a4)-[:T {w: 13}]->(b5)" +
        ", (a5)-[:T {w: 13}]->(b9)" +
        ", (a6h)-[:T {w: 13}]->(b11h)" +
        ", (b1)-[:T {w: 7}]->(a2)" +
        ", (b2)-[:T {w: 7}]->(c1)" +
        ", (b3)-[:T {w: 7}]->(b1)" +
        ", (b4)-[:T {w: 7}]->(b5)" +
        ", (b5)-[:T {w: 7}]->(b9)" +
        ", (b6)-[:T {w: 7}]->(a3)" +
        ", (b7)-[:T {w: 7}]->(a3)" +
        ", (b8)-[:T {w: 7}]->(a3)" +
        ", (b9)-[:T {w: 7}]->(a3)" +
        ", (b10)-[:T {w: 7}]->(a3)" +
        ", (b11h)-[:T {w: 7}]->(c9h)" +
        ", (c1)-[:T {w: 3}]->(a1)" +
        ", (c2)-[:T {w: 3}]->(b1)" +
        ", (c3)-[:T {w: 3}]->(b4)" +
        ", (c4)-[:T {w: 3}]->(b8)" +
        ", (c5)-[:T {w: 3}]->(c3)" +
        ", (c6)-[:T {w: 3}]->(c2)" +
        ", (c7)-[:T {w: 3}]->(b6)" +
        ", (c8)-[:T {w: 3}]->(b6)" +
        ", (c9h)-[:T {w: 3}]->(a6h)";

    @Inject
    ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            ModelListProc.class,
            NodeRegressionPipelineCreateProc.class,
            NodeRegressionPipelineAddStepProcs.class,
            NodeRegressionPipelineConfigureSplitProc.class,
            NodeRegressionPipelineAddTrainerMethodProcs.class,
            NodeRegressionPipelineTrainProc.class,
            NodeRegressionPipelineStreamProc.class,
            WccMutateProc.class
        );
        registerFunctions(AsNodeFunc.class);
        runQuery(GRAPH);
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
        modelCatalog.removeAllLoadedModels();
    }

    @Test
    void trainAndPredictWithLR() {
        runQuery("CALL gds.graph.project(" +
                 "'g', " +
                 "['N', 'Hidden'], " +
                 "{T: {properties: 'w'}}, " +
                 "{nodeProperties: ['a', 'b', 'cost']})"
        );

        runQuery("CALL gds.alpha.pipeline.nodeRegression.create('p')");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('p', 'wcc', {" +
                 "  mutateProperty: 'community', " +
                 "  relationshipWeightProperty: 'w'" +
                 "})");
        // let's try both list and single string syntaxes
        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('p', 'a')");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('p', ['b', 'community'])");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.configureSplit('p', {" +
                 "  testFraction: 0.2, " +
                 "  validationFolds: 5" +
                 "})");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addLinearRegression('p', {penalty: 0.0625, maxEpochs: 1000})");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addLinearRegression('p', {penalty: 4.0, maxEpochs: 100})");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.train('g', {" +
                 " nodeLabels: ['N']," +
                 " pipeline: 'p'," +
                 " modelName: 'model'," +
                 " targetProperty: 'cost'," +
                 " metrics: ['MEAN_SQUARED_ERROR']," +
                 " randomSeed: 2" +
                 "})");

        assertCypherResult(
            "CALL gds.beta.model.list() YIELD modelInfo RETURN count(*) AS modelCount",
            List.of(Map.of("modelCount", 1L))
        );

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.predict.stream('g', {" +
            "  nodeLabels: ['Hidden']," +
            "  modelName: 'model'" +
            "}) YIELD nodeId, predictedValue " +
            "RETURN gds.util.asNode(nodeId).name AS name, predictedValue AS predictedCost " +
            "  ORDER BY name ASC",
            List.of(
                Map.of("name", "0_hidden", "predictedCost", closeTo(6.3930, 1e-4)),
                Map.of("name", "1_hidden", "predictedCost", closeTo(26.2026, 1e-4)),
                Map.of("name", "2_hidden", "predictedCost", closeTo(-7.0334, 1e-4))
            )
        );
    }

    @Test
    void trainAndPredictWithRF() {
        runQuery("CALL gds.graph.project(" +
                 "'g', " +
                 "['N', 'Hidden'], " +
                 "{T: {properties: 'w'}}, " +
                 "{nodeProperties: ['a', 'b', 'cost']})"
        );

        runQuery("CALL gds.alpha.pipeline.nodeRegression.create('p')");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('p', 'wcc', {" +
                 "  mutateProperty: 'community', " +
                 "  relationshipWeightProperty: 'w'" +
                 "})");
        // let's try both list and single string syntaxes
        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('p', 'a')");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('p', ['b', 'community'])");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.configureSplit('p', {" +
                 "  testFraction: 0.2, " +
                 "  validationFolds: 5" +
                 "})");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addRandomForest('p', {numberOfDecisionTrees: 10})");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addRandomForest('p', {numberOfDecisionTrees: 25, minSplitSize: 5})");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.train('g', {" +
                 " nodeLabels: ['N']," +
                 " pipeline: 'p'," +
                 " modelName: 'model'," +
                 " targetProperty: 'cost'," +
                 " metrics: ['MEAN_SQUARED_ERROR']," +
                 " randomSeed: 2" +
                 "})");

        assertCypherResult(
            "CALL gds.beta.model.list() YIELD modelInfo RETURN count(*) AS modelCount",
            List.of(Map.of("modelCount", 1L))
        );

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.predict.stream('g', {" +
            "  nodeLabels: ['Hidden']," +
            "  modelName: 'model'" +
            "}) YIELD nodeId, predictedValue " +
            "RETURN gds.util.asNode(nodeId).name AS name, predictedValue AS predictedCost " +
            "  ORDER BY name ASC",
            List.of(
                Map.of("name", "0_hidden", "predictedCost", closeTo(13.829999999999998, 1e-4)),
                Map.of("name", "1_hidden", "predictedCost", closeTo(63.975999999999985, 1e-4)),
                Map.of("name", "2_hidden", "predictedCost", closeTo(7.44, 1e-4))
            )
        );
    }

}
