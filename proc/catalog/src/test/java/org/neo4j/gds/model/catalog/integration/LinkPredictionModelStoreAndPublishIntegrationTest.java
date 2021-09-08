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
package org.neo4j.gds.model.catalog.integration;

import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.assertj.ConditionFactory;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.ml.linkmodels.LinkPredictionTrainProc;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.compat.MapUtil.map;

@GdsEditionTest(Edition.EE)
class LinkPredictionModelStoreAndPublishIntegrationTest extends BaseModelStoreAndPublishIntegrationTest {

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

        "(a)-[:TRAIN {label: 1}]->(b), " +
        "(a)-[:TEST {label: 1}]->(c), " +       // selected for test
        "(a)-[:TRAIN {label: 1}]->(d), " +
        "(b)-[:TRAIN {label: 1}]->(c), " +
        "(b)-[:TEST {label: 1}]->(d), " +       // selected for test
        "(c)-[:TRAIN {label: 1}]->(d), " +

        "(e)-[:TEST {label: 1}]->(f), " +       // selected for test
        "(e)-[:TRAIN {label: 1}]->(g), " +
        "(f)-[:TRAIN {label: 1}]->(g), " +

        "(h)-[:TRAIN {label: 1}]->(i), " +

        "(j)-[:TRAIN {label: 1}]->(k), " +
        "(j)-[:TRAIN {label: 1}]->(l), " +
        "(k)-[:TRAIN {label: 1}]->(l), " +

        "(m)-[:TEST {label: 1}]->(n), " +       // selected for test
        "(m)-[:TEST {label: 1}]->(o), " +       // selected for test
        "(n)-[:TRAIN {label: 1}]->(o), " +
        // 11 false positive TRAIN rels
        "(a)-[:TRAIN {label: 0}]->(e), " +
        "(a)-[:TRAIN {label: 0}]->(o), " +
        "(b)-[:TRAIN {label: 0}]->(e), " +
        "(e)-[:TRAIN {label: 0}]->(i), " +
        "(e)-[:TRAIN {label: 0}]->(o), " +
        "(e)-[:TRAIN {label: 0}]->(n), " +
        "(h)-[:TRAIN {label: 0}]->(k), " +
        "(h)-[:TRAIN {label: 0}]->(m), " +
        "(i)-[:TRAIN {label: 0}]->(j), " +
        "(k)-[:TRAIN {label: 0}]->(m), " +
        "(k)-[:TRAIN {label: 0}]->(o), " +
        // 5 false positive TEST rels
        "(a)-[:TEST {label: 0}]->(f), " +
        "(b)-[:TEST {label: 0}]->(f), " +
        "(i)-[:TEST {label: 0}]->(k), " +
        "(j)-[:TEST {label: 0}]->(o), " +
        "(k)-[:TEST {label: 0}]->(o)";


    @Override
    Class<?> trainProcClass() {
        return LinkPredictionTrainProc.class;
    }

    @Override
    String dbQuery() {
        return GRAPH;
    }

    @Override
    void createGraph() {
        var graphCreateQuery = GdsCypher
            .call()
            .withNodeLabel("N")
            .withNodeProperty("z")
            .withRelationshipType(
                "TRAIN",
                RelationshipProjection.of("TRAIN", UNDIRECTED)
                    .withProperties(PropertyMappings.of(PropertyMapping.of("label")))
            )
            .withRelationshipType(
                "TEST",
                RelationshipProjection.of("TEST", UNDIRECTED)
                    .withProperties(PropertyMappings.of(PropertyMapping.of("label")))
            )
            .graphCreate("g")
            .yields();

        runQuery(graphCreateQuery);
    }

    @Override
    protected void modelToCatalog(String modelName) {
        var query =
            "CALL gds.alpha.ml.linkPrediction.train('g', { " +
            "  trainRelationshipType: 'TRAIN', " +
            "  testRelationshipType: 'TEST', " +
            "  validationFolds: 2, " +
            "  modelName: $modelName, " +
            "  featureProperties: ['z'], " +
            "  negativeClassWeight: 5.5625," +
            "  randomSeed: -1, " +
            "  params: [{penalty: 0.5, maxEpochs: 1}] " +
            "})";

        runQuery(query, Map.of("modelName", modelName));
    }

    @Override
    List<Map<String, Object>> publishModelExpectedResult(String modelName) {
        return modelData(modelName + "_public", true);
    }

    @Override
    List<Map<String, Object>> dropStoreModelExpectedResult(String modelName) {
        return modelData(modelName, false);
    }

    private List<Map<String, Object>> modelData(String expectedModelName, boolean loaded) {
        return singletonList(
            map(
                "modelInfo",
                ConditionFactory.containsExactlyInAnyOrderEntriesOf(
                    Map.of(
                        "modelName", expectedModelName,
                        "modelType", "Link Prediction",
                        "metrics", Map.of(
                            "AUCPR", Map.of(
                                "test", 1.0,
                                "outerTrain", 1.0,
                                "validation", List.of(
                                    Map.of(
                                        "avg", 1.0,
                                        "min", 1.0,
                                        "max", 1.0,
                                        "params", Map.of(
                                            "maxEpochs", 1,
                                            "minEpochs", 1,
                                            "linkFeatureCombiner", "L2",
                                            "penalty", 0.5,
                                            "patience", 1,
                                            "batchSize", 100,
                                            "tolerance", 0.001,
                                            "concurrency", 4
                                        )
                                    )
                                ),
                                "train", List.of(
                                    Map.of(
                                        "avg", 1.0,
                                        "min", 1.0,
                                        "max", 1.0,
                                        "params", Map.of(
                                            "maxEpochs", 1,
                                            "minEpochs", 1,
                                            "linkFeatureCombiner", "L2",
                                            "penalty", 0.5,
                                            "patience", 1,
                                            "batchSize", 100,
                                            "tolerance", 0.001,
                                            "concurrency", 4
                                        )
                                    )
                                )
                            )
                        ),
                        "bestParameters", Map.of(
                            "maxEpochs", 1,
                            "minEpochs", 1,
                            "linkFeatureCombiner", "L2",
                            "penalty", 0.5,
                            "patience", 1,
                            "batchSize", 100,
                            "tolerance", 0.001,
                            "concurrency", 4
                        )
                    )
                ),
                "trainConfig", isA(Map.class),
                "graphSchema", isA(Map.class),
                "creationTime", isA(ZonedDateTime.class),
                "shared", true,
                "loaded", loaded,
                "stored", true
            )
        );
    }
}
