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
package org.neo4j.gds.model.catalog;

import org.hamcrest.Matchers;
import org.neo4j.gds.assertj.ConditionFactory;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.gds.compat.MapUtil.map;

@GdsEditionTest(Edition.EE)
class NodeClassificationModelStoreAndPublishIntegrationTest extends BaseModelStoreAndPublishIntegrationTest {

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

    @Override
    Class<?> trainProcClass() {
        return NodeClassificationTrainProc.class;
    }

    @Override
    String dbQuery() {
        return GRAPH;
    }

    @Override
    void createGraph() {
        runQuery("CALL gds.graph.create('g', ['N', 'Hidden'], '*', {nodeProperties: ['a', 'b', 'class']})");
    }

    @Override
    protected void modelToCatalog(String modelName) {
        var trainOnN = "CALL gds.alpha.ml.nodeClassification.train('g', {" +
                       "  nodeLabels: ['N']," +
                       "  modelName: $modelName," +
                       "  featureProperties: ['a', 'b'], " +
                       "  targetProperty: 'class', " +
                       "  metrics: ['F1_WEIGHTED'], " +
                       "  holdoutFraction: 0.2, " +
                       "  validationFolds: 5, " +
                       "  randomSeed: 2," +
                       "  concurrency: 1," +
                       "  params: [" +
                       "    {penalty: 0.0625, maxEpochs: 1000}" +
                       "  ]" +
                       "})";
        runQuery(trainOnN, Map.of("modelName", modelName));
    }

    @Override
    protected void publishModel(String modelName) {

        assertCypherResult(
            "CALL gds.alpha.model.publish($modelName)",
            map("modelName", modelName),
            singletonList(
                map(
                    "modelInfo",
                    ConditionFactory.containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "modelName", "testModel1_public",
                            "classes", List.of(0L, 1L, 2L),
                            "modelType", "nodeLogisticRegression",
                            "metrics", Map.of(
                                "F1_WEIGHTED", Map.of(
                                    "test", 0.7204968859982255,
                                    "outerTrain", 0.7971014434341788,
                                    "validation", List.of(
                                        Map.of(
                                            "avg", 0.7310144831198069,
                                            "min", 0.6376811503381643,
                                            "max", 0.9999999871739133,
                                            "params", Map.of(
                                                "maxEpochs", 1000,
                                                "minEpochs", 1,
                                                "penalty", 0.0625,
                                                "patience", 1,
                                                "batchSize", 100,
                                                "tolerance", 0.001,
                                                "concurrency", 1
                                            )
                                        )
                                    ),
                                    "train", List.of(
                                        Map.of(
                                            "avg", 0.8814533226428075,
                                            "min", 0.8250227968275875,
                                            "max", 0.9999999930072465,
                                            "params", Map.of(
                                                "maxEpochs", 1000,
                                                "minEpochs", 1,
                                                "penalty", 0.0625,
                                                "patience", 1,
                                                "batchSize", 100,
                                                "tolerance", 0.001,
                                                "concurrency", 1
                                            )
                                        )
                                    )
                                )
                            ),
                            "bestParameters", Map.of(
                                "maxEpochs", 1000,
                                "minEpochs", 1,
                                "penalty", 0.0625,
                                "patience", 1,
                                "batchSize", 100,
                                "tolerance", 0.001,
                                "concurrency", 1
                            )
                        )
                    ),
                    "trainConfig", isA(Map.class),
                    "graphSchema", isA(Map.class),
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", true,
                    "loaded", true,
                    "stored", true
                )
            )
        );
    }

    @Override
    protected void dropStoredModel(String modelName) {
        assertCypherResult(
            "CALL gds.beta.model.drop($modelName)",
            Map.of("modelName", modelName),
            singletonList(
                Map.of(
                    "modelInfo", ConditionFactory.containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "modelName", "testModel1_public",
                            "classes", List.of(0L, 1L, 2L),
                            "modelType", "nodeLogisticRegression",
                            "metrics", Map.of(
                                "F1_WEIGHTED", Map.of(
                                    "test", 0.7204968859982255,
                                    "outerTrain", 0.7971014434341788,
                                    "validation", List.of(
                                        Map.of(
                                            "avg", 0.7310144831198069,
                                            "min", 0.6376811503381643,
                                            "max", 0.9999999871739133,
                                            "params", Map.of(
                                                "maxEpochs", 1000,
                                                "minEpochs", 1,
                                                "penalty", 0.0625,
                                                "patience", 1,
                                                "batchSize", 100,
                                                "tolerance", 0.001,
                                                "concurrency", 1
                                            )
                                        )
                                    ),
                                    "train", List.of(
                                        Map.of(
                                            "avg", 0.8814533226428075,
                                            "min", 0.8250227968275875,
                                            "max", 0.9999999930072465,
                                            "params", Map.of(
                                                "maxEpochs", 1000,
                                                "minEpochs", 1,
                                                "penalty", 0.0625,
                                                "patience", 1,
                                                "batchSize", 100,
                                                "tolerance", 0.001,
                                                "concurrency", 1
                                            )
                                        )
                                    )
                                )
                            ),
                            "bestParameters", Map.of(
                                "maxEpochs", 1000,
                                "minEpochs", 1,
                                "penalty", 0.0625,
                                "patience", 1,
                                "batchSize", 100,
                                "tolerance", 0.001,
                                "concurrency", 1
                            )
                        )
                    ),
                    "creationTime", Matchers.isA(ZonedDateTime.class),
                    "trainConfig", Matchers.isA(Map.class),
                    "loaded", false,
                    "stored", true,
                    "graphSchema", Matchers.isA(Map.class),
                    "shared", true
                )
            )
        );
    }
}
