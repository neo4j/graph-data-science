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
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.assertj.ConditionFactory;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.Orientation.UNDIRECTED;

class LinkPredictionTrainProcTest extends BaseProcTest {

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

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionTrainProc.class, GraphCreateProc.class);
        runQuery(GRAPH);

        runQuery(createQuery("g", UNDIRECTED));
    }

    private String createQuery(String graphName, Orientation orientation) {
        return GdsCypher
            .call()
            .withNodeLabel("N")
            .withNodeProperty("z")
            .withRelationshipType(
                "TRAIN",
                RelationshipProjection.of("TRAIN", orientation)
                    .withProperties(PropertyMappings.of(PropertyMapping.of("label")))
            )
            .withRelationshipType(
                "TEST",
                RelationshipProjection.of("TEST", orientation)
                    .withProperties(PropertyMappings.of(PropertyMapping.of("label")))
            )
            .graphCreate(graphName)
            .yields();
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void producesAModel() {
        var query =
            "CALL gds.alpha.ml.linkPrediction.train('g', { " +
            "  trainRelationshipType: 'TRAIN', " +
            "  testRelationshipType: 'TEST', " +
            "  validationFolds: 2, " +
            "  modelName: 'model', " +
            "  featureProperties: ['z'], " +
            "  negativeClassWeight: 5.5625," +
            "  randomSeed: -1, " +
            "  params: [{penalty: 0.5, maxEpochs: 1}, {penalty: 2.0, maxEpochs: 100}] " +
            "})";

        var params1 = LinkLogisticRegressionTrainConfig
            .of(List.of("z"), 4, Map.of("penalty", 0.5, "maxEpochs", 1))
            .toMap();
        var params2 = LinkLogisticRegressionTrainConfig
            .of(List.of("z"), 4, Map.of("penalty", 2.0, "maxEpochs", 100))
            .toMap();

        var expectedModelInfo = Map.of(
            "bestParameters", params1,
            "metrics", Map.of(
                "AUCPR", Map.of(
                    "outerTrain", 1.0,
                    "test", 1.0,
                    "train", List.of(
                        Map.of(
                            "avg", 1.0,
                            "max", 1.0,
                            "min", 1.0,
                            "params", params1
                        ),
                        Map.of(
                            "avg", 1.0,
                            "max", 1.0,
                            "min", 1.0,
                            "params", params2
                        )
                    ),
                    "validation", List.of(
                        Map.of(
                            "avg", 1.0,
                            "max", 1.0,
                            "min", 1.0,
                            "params", params1
                        ),
                        Map.of(
                            "avg", 1.0,
                            "max", 1.0,
                            "min", 1.0,
                            "params", params2
                        )
                    )
                )
            ),
            "name", "model",
            "type", "Link Prediction"
        );
        assertCypherResult(query, List.of(Map.of(
            "trainMillis", greaterThan(0L),
            "modelInfo", ConditionFactory.containsExactlyInAnyOrderEntriesOf(expectedModelInfo),
            "configuration", isA(Map.class)
        )));

        assertTrue(ModelCatalog.exists(getUsername(), "model"));
        var model = ModelCatalog.list(getUsername(), "model");
        assertThat(model.algoType()).isEqualTo("Link Prediction");
        assertThat(model.customInfo().toMap()).containsKeys("metrics", "bestParameters");
    }

    @Test
    void requiresUndirectedGraph() {
        runQuery(createQuery("g2", Orientation.NATURAL));

        var trainQuery =
            "CALL gds.alpha.ml.linkPrediction.train('g2', { " +
            "  trainRelationshipType: 'TRAIN', " +
            "  testRelationshipType: 'TEST', " +
            "  negativeClassWeight: 5.5625," +
            "  validationFolds: 2, " +
            "  modelName: 'model', " +
            "  featureProperties: ['z'], " +
            "  params: [{penalty: 0.5}, {penalty: 2.0}] " +
            "})";

        assertError(trainQuery, "Procedure requires relationship projections to be UNDIRECTED.");
    }

    @Test
    void validateRelTypesExist() {
        runQuery(createQuery("g2", Orientation.NATURAL));

        var query =
            "CALL gds.alpha.ml.linkPrediction.train('g', { " +
            "  trainRelationshipType: 'NOPE', " +
            "  testRelationshipType: 'NIX', " +
            "  negativeClassWeight: 5.5625," +
            "  validationFolds: 2, " +
            "  modelName: 'model', " +
            "  featureProperties: ['z'], " +
            "  params: [{penalty: 0.5}, {penalty: 2.0}] " +
            "})";

        assertErrorRegex(query, ".*No relationships have been loaded for relationship type.*(NOPE|NIX).*");
    }

    @Test
    void shouldNotAcceptEmptyModelCandidates() {
        runQuery(createQuery("g2", Orientation.NATURAL));

        var query =
            "CALL gds.alpha.ml.linkPrediction.train('g', { " +
            "  trainRelationshipType: 'TRAIN', " +
            "  testRelationshipType: 'TEST', " +
            "  negativeClassWeight: 5.5625," +
            "  validationFolds: 2, " +
            "  modelName: 'model', " +
            "  featureProperties: ['z'], " +
            "  params: [] " +
            "})";

        assertError(query, "No model candidates (params) specified, we require at least one");
    }

}
