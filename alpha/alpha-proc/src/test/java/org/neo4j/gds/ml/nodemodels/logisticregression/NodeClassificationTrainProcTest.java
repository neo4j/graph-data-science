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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.assertj.ConditionFactory;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.InjectModelCatalog;
import org.neo4j.gds.core.ModelCatalogExtension;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ModelCatalogExtension
class NodeClassificationTrainProcTest extends BaseProcTest {

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeClassificationTrainProc.class, GraphCreateProc.class);
        runQuery(createQuery());

        runQuery("CALL gds.graph.create('g', 'N', '*', {nodeProperties: ['a', 'b', 't']})");
    }

    @Test
    void producesCorrectModel() {
        var query = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                    "modelName: 'model'," +
                    "targetProperty: 't', featureProperties: ['a', 'b'], " +
                    "metrics: ['F1_WEIGHTED', 'ACCURACY'], " +
                    "holdoutFraction: 0.2, " +
                    "validationFolds: 5, " +
                    "randomSeed: 2," +
                    "params: [{penalty: 1.0}, {penalty: 2.0}]})";

        var params1 = NodeLogisticRegressionTrainConfig.of(List.of("a", "b"), "t", Map.of("penalty", 1.0));
        var params2 = NodeLogisticRegressionTrainConfig.of(List.of("a", "b"), "t", Map.of("penalty", 2.0));

        var expectedModelInfo = Map.of(
            "bestParameters", params1.toMap(),
            "classes", List.of(0L, 1L),
            "metrics", Map.of(
                "ACCURACY", Map.of(
                    "outerTrain", 1.0,
                    "test", 0.0,
                    "train", List.of(
                        Map.of("avg", 1.0, "max", 1.0, "min", 1.0, "params", params1.toMap()),
                        Map.of("avg", 1.0, "max", 1.0, "min", 1.0, "params", params2.toMap())
                    ),
                    "validation", List.of(
                        Map.of("avg", 0.4, "max", 1.0, "min", 0.0, "params", params1.toMap()),
                        Map.of("avg", 0.4, "max", 1.0, "min", 0.0, "params", params2.toMap())
                    )
                ),
                "F1_WEIGHTED", Map.of(
                    "outerTrain", 0.9999999875000001,
                    "test", 0.0,
                    "train", List.of(
                        Map.of("avg", 0.899999988, "max", 0.9999999875000001, "min", 0.49999999500000003, "params", params1.toMap()),
                        Map.of("avg", 0.899999988, "max", 0.9999999875000001, "min", 0.49999999500000003, "params", params2.toMap())
                    ),
                    "validation", List.of(
                        Map.of("avg", 0.19999999700000004, "max", 0.4999999925000001, "min", 0.0, "params", params1.toMap()),
                        Map.of("avg", 0.19999999700000004, "max", 0.4999999925000001, "min", 0.0, "params", params2.toMap())
                    )
                )
            ),
            "modelName", "model",
            "modelType", "nodeLogisticRegression"
        );
        assertCypherResult(query, List.of(Map.of(
            "trainMillis", greaterThan(0L),
            "modelInfo", ConditionFactory.containsExactlyInAnyOrderEntriesOf(expectedModelInfo),
            "configuration", isA(Map.class)
        )));

        assertTrue(modelCatalog.exists("", "model"));
        var model = modelCatalog.list("", "model");
        assertThat(model.algoType()).isEqualTo("nodeLogisticRegression");
        assertThat(model.customInfo().toMap()).containsKeys("metrics", "classes", "bestParameters");
    }

    @Test
    void validateTargetPropertyExists() {
        String query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification")
            .trainMode()
            .addParameter("modelName", "model")
            .addParameter("targetProperty", "nope")
            .addParameter("metrics", List.of("F1_MACRO"))
            .addParameter("holdoutFraction", 0.2)
            .addParameter("validationFolds", 4)
            .addParameter("params", List.of(Map.of("penalty", 1)))
            .yields();

        assertError(query, "`targetProperty`: `nope` not found in graph with node properties: ['a', 'b', 't']");
    }

    @Test
    void shouldAcceptButNotPreserveCaseInsensitiveMetricNames() {
        String query = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                       "  modelName: 'model'," +
                       "  targetProperty: 't'," +
                       "  metrics: ['f1_weighted','aCcUrAcY', 'f1(  ClAss = 0  )']," +
                       "  holdoutFraction: 0.2," +
                       "  validationFolds: 5," +
                       "  params: [{penalty: 1.0}]" +
                       "})";

        String resultAsString = runQuery(query, Result::resultAsString);

        assertThat(resultAsString).doesNotContain("f1_weighted", "aCcUrAcY", "f1(  ClAss = 0  )", "f1");
        assertThat(resultAsString).contains("F1_WEIGHTED", "ACCURACY", "F1_class_0");
    }

    @Test
    void shouldFailWhenFirstMetricIsSyntacticSugar() {
        String query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification")
            .trainMode()
            .addParameter("modelName", "model")
            .addParameter("targetProperty", "t")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("validationFolds", 4)
            .addParameter("metrics", List.of("F1(class=*)", "invalid"))
            .addParameter("params", List.of(Map.of("penalty", 1)))
            .yields();

        assertError(query,
            "The primary (first) metric provided must be one of " +
            "F1_WEIGHTED, F1_MACRO, ACCURACY, ACCURACY(class=<class value>), " +
            "F1(class=<class value>), PRECISION(class=<class value>), RECALL(class=<class value>). " +
            "Invalid metric expression `invalid`.");
    }

    @Test
    void shouldFailWithInvalidMetrics() {

        String query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification")
            .trainMode()
            .addParameter("modelName", "model")
            .addParameter("targetProperty", "t")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("validationFolds", 4)
            .addParameter("metrics", List.of("foo", "F1(class=-42)", "F1(class=bar)"))
            .addParameter("params", List.of(Map.of("penalty", 1)))
            .yields();

        assertError(query, "Invalid metric expressions `foo`, `F1(class=bar)`.");
    }

    @Test
    void shouldNotAcceptEmptyMetrics() {
        var query = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                    "  modelName: 'model'," +
                    "  targetProperty: 't'," +
                    "  metrics: []," +
                    "  holdoutFraction: 0.2," +
                    "  validationFolds: 5," +
                    "  params: [{penalty: 1.0}]" +
                    "})";

        assertError(query, "No metrics specified, we require at least one");
    }

    @Test
    void shouldHandlePerClassMetrics() {
        var query = " CALL gds.alpha.ml.nodeClassification.train('g', {" +
                    "   modelName: 'model'," +
                    "   targetProperty: 't'," +
                    "   metrics: ['PRECISION(class=0)', 'RECALL(class=0)', 'ACCURACY(class=0)', 'F1(class=0)']," +
                    "   holdoutFraction: 0.2," +
                    "   validationFolds: 5," +
                    "   randomSeed: 42," +
                    "   params: [{penalty: 1.0}]" +
                    " }) YIELD modelInfo" +
                    " RETURN " +
                    "   modelInfo.metrics.F1_class_0.test AS f1_0," +
                    "   modelInfo.metrics.PRECISION_class_0.test AS precision_0," +
                    "   modelInfo.metrics.RECALL_class_0.test AS recall_0," +
                    "   modelInfo.metrics.ACCURACY_class_0.test AS accuracy_0";
        assertCypherResult(query, List.of(
            Map.of(
                "f1_0", 0.0,
                "precision_0", 0.0,
                "recall_0", 0.0,
                "accuracy_0", 0.0
            )
        ));
    }

    @Test
    void shouldHandleSyntacticSugarMetrics() {
        var query = " CALL gds.alpha.ml.nodeClassification.train('g', {" +
                    "   modelName: 'model'," +
                    "   targetProperty: 't'," +
                    "   metrics: ['F1_WEIGHTED', 'PRECISION(class=*)', 'RECALL(class=*)', 'ACCURACY(class=*)', 'F1(class=*)']," +
                    "   holdoutFraction: 0.2," +
                    "   validationFolds: 5," +
                    "   randomSeed: 42," +
                    "   params: [{penalty: 1.0}]" +
                    " }) YIELD modelInfo" +
                    " RETURN " +
                    "   modelInfo.metrics.F1_class_0.test AS f1_0," +
                    "   modelInfo.metrics.PRECISION_class_0.test AS precision_0," +
                    "   modelInfo.metrics.RECALL_class_0.test AS recall_0," +
                    "   modelInfo.metrics.ACCURACY_class_0.test AS accuracy_0," +
                    "   modelInfo.metrics.F1_class_1.test AS f1_1," +
                    "   modelInfo.metrics.PRECISION_class_1.test AS precision_1," +
                    "   modelInfo.metrics.RECALL_class_1.test AS recall_1," +
                    "   modelInfo.metrics.ACCURACY_class_1.test AS accuracy_1";
        assertCypherResult(query, List.of(
            Map.of(
                "f1_0", 0.0,
                "precision_0", 0.0,
                "recall_0", 0.0,
                "accuracy_0", 0.0,
                "f1_1", 0.0,
                "precision_1", 0.0,
                "recall_1", 0.0,
                "accuracy_1", 0.0
            )
        ));
    }

    @Test
    void shouldFailOnMisspelledOptionalParameters() {
        var query = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                    "modelName: 'model'," +
                    "targetProperty: 't', featureProperties: ['a', 'b'], " +
                    "metrics: ['F1_WEIGHTED', 'ACCURACY'], " +
                    "holdoutFraction: 0.2, " +
                    "validationFolds: 5, " +
                    "randomSeed: 2," +
                    "params: [{" +
                    "   penalty: 1.0," +
                    "   batchSizes: 1," +
                    "   miniepochs: 1," +
                    "   maxxepochs: 1," +
                    "   patiences: 1," +
                    "   tollerance: 1," +
                    "   shareUpdaters: true" +
                    "}]})";

        assertThatThrownBy(() -> runQuery(query))
            .hasMessageContaining("batchSizes (Did you mean [batchSize]?)")
            .hasMessageContaining("miniepochs (Did you mean one of [minEpochs, maxEpochs]?)")
            .hasMessageContaining("maxxepochs (Did you mean one of [maxEpochs, minEpochs]?)")
            .hasMessageContaining("patiences (Did you mean [patience]?)")
            .hasMessageContaining("tollerance (Did you mean [tolerance]?)");
    }

    @Test
    void shouldEstimateMemory() {
        var query = " CALL gds.alpha.ml.nodeClassification.train.estimate('g', {" +
                    "   modelName: 'model'," +
                    "   targetProperty: 't'," +
                    "   metrics: ['PRECISION(class=0)', 'RECALL(class=0)', 'ACCURACY(class=0)', 'F1(class=0)']," +
                    "   holdoutFraction: 0.2," +
                    "   validationFolds: 5," +
                    "   randomSeed: 42," +
                    "   params: [{penalty: 1.0}]" +
                    " })";
        assertDoesNotThrow(() -> runQuery(query));
    }

    public String createQuery() {
        return "CREATE " +
               "(n1:N {a: 2.0, b: 1.2, t: 1})," +
               "(n2:N {a: 1.3, b: 0.5, t: 0})," +
               "(n3:N {a: 0.0, b: 2.8, t: 0})," +
               "(n4:N {a: 1.0, b: 0.9, t: 1})";
    }

}
