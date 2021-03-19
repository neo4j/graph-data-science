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
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.assertj.ConditionFactory;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

class NodeClassificationTrainProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeClassificationTrainProc.class, GraphCreateProc.class);
        runQuery(createQuery());

        runQuery("CALL gds.graph.create('g', 'N', '*', {nodeProperties: ['a', 'b', 't']})");
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
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

        var expectedModelInfo = Map.of(
            "bestParameters", Map.of("penalty", 1.0),
            "classes", List.of(0, 1),
            "metrics", Map.of(
                "ACCURACY", Map.of(
                    "outerTrain", 1.0,
                    "test", 0.0,
                    "train", List.of(
                        Map.of("avg", 1.0, "max", 1.0, "min", 1.0, "params", Map.of("penalty", 1.0)),
                        Map.of("avg", 1.0, "max", 1.0, "min", 1.0, "params", Map.of("penalty", 2.0))
                    ),
                    "validation", List.of(
                        Map.of("avg", 0.4, "max", 1.0, "min", 0.0, "params", Map.of("penalty", 1.0)),
                        Map.of("avg", 0.4, "max", 1.0, "min", 0.0, "params", Map.of("penalty", 2.0))
                    )
                ),
                "F1_WEIGHTED", Map.of(
                    "outerTrain", 0.9999999875000001,
                    "test", 0.0,
                    "train", List.of(
                        Map.of("avg", 0.899999988, "max", 0.9999999875000001, "min", 0.49999999500000003, "params", Map.of("penalty", 1.0)),
                        Map.of("avg", 0.899999988, "max", 0.9999999875000001, "min", 0.49999999500000003, "params", Map.of("penalty", 2.0))
                    ),
                    "validation", List.of(
                        Map.of("avg", 0.19999999700000004, "max", 0.4999999925000001, "min", 0.0, "params", Map.of("penalty", 1.0)),
                        Map.of("avg", 0.19999999700000004, "max", 0.4999999925000001, "min", 0.0, "params", Map.of("penalty", 2.0))
                    )
                )
            ),
            "name", "model",
            "type", "multiClassNodeLogisticRegression"
        );
        assertCypherResult(query, List.of(Map.of(
            "trainMillis", greaterThan(0L),
            "modelInfo", ConditionFactory.containsExactlyInAnyOrderEntriesOf(expectedModelInfo),
            "configuration", isA(Map.class)
        )));

        assertTrue(ModelCatalog.exists("", "model"));
        var model = ModelCatalog.list("", "model");
        assertThat(model.algoType()).isEqualTo("multiClassNodeLogisticRegression");
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
                       "  metrics: ['f1_weighted','aCcUrAcY']," +
                       "  holdoutFraction: 0.2," +
                       "  validationFolds: 5," +
                       "  params: [{penalty: 1.0}]" +
                       "})";

        String resultAsString = runQuery(query, Result::resultAsString);

        assertThat(resultAsString).doesNotContain("f1_weighted", "aCcUrAcY");
        assertThat(resultAsString).contains("F1_WEIGHTED", "ACCURACY");
    }

    @Test
    void shouldFailWithInvalidMetric() {

        String query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification")
            .trainMode()
            .addParameter("modelName", "model")
            .addParameter("targetProperty", "t")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("validationFolds", 4)
            .addParameter("metrics", List.of("foo"))
            .addParameter("params", List.of(Map.of("penalty", 1)))
            .yields();

        assertError(query, "Invalid metric expression `foo`.");
    }

    @Test
    void shouldNotAcceptEmptyModelCandidates() {
        var query = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                    "  modelName: 'model'," +
                    "  targetProperty: 't'," +
                    "  metrics: ['F1_WEIGHTED']," +
                    "  holdoutFraction: 0.2," +
                    "  validationFolds: 5," +
                    "  params: []" +
                    "})";

        assertError(query, "No model candidates (params) specified, we require at least one");
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

    public String createQuery() {
        return "CREATE " +
               "(n1:N {a: 2.0, b: 1.2, t: 1})," +
               "(n2:N {a: 1.3, b: 0.5, t: 0})," +
               "(n3:N {a: 0.0, b: 2.8, t: 0})," +
               "(n4:N {a: 1.0, b: 0.9, t: 1})";
    }

}
