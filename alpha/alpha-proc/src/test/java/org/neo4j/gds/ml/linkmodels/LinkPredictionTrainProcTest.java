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

    private static final String GRAPH =
        "CREATE " +
        "(a:N {a: 0}), " +
        "(b:N {a: 0}), " +
        "(c:N {a: 0}), " +
        "(d:N {a: 100}), " +
        "(e:N {a: 100}), " +
        "(f:N {a: 100}), " +
        "(g:N {a: 200}), " +
        "(h:N {a: 200}), " +
        "(i:N {a: 200}), " +
        "(j:N {a: 300}), " +
        "(k:N {a: 300}), " +
        "(l:N {a: 300}), " +
        "(m:N {a: 400}), " +
        "(n:N {a: 400}), " +
        "(o:N {a: 400}), " +
        "(d)-[:TRAIN {label: 1}]->(e), " +
        "(f)-[:TRAIN {label: 1}]->(d), " +
        "(m)-[:TRAIN {label: 1}]->(n), " +
        "(a)-[:TRAIN {label: 0}]->(d), " +
        "(b)-[:TRAIN {label: 0}]->(i), " +
        "(i)-[:TRAIN {label: 0}]->(b), " +
        "(a)-[:TEST {label: 1}]->(b)," +
        "(b)-[:TEST {label: 1}]->(c), " +
        "(k)-[:TEST {label: 1}]->(l), " +
        "(g)-[:TEST {label: 0}]->(m), " +
        "(l)-[:TEST {label: 0}]->(f), " +
        "(o)-[:TEST {label: 0}]->(a)";

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
            .withNodeProperty("a")
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
            "  featureProperties: ['a'], " +
            "  randomSeed: -1, " +
            "  params: [{penalty: 0.5}, {penalty: 2.0}] " +
            "})";

        var expectedModelInfo = Map.of(
            "bestParameters", Map.of("penalty", 0.5),
            "metrics", Map.of(
                "F1_SCORE", Map.of(
                    "outerTrain", 0.9999999933333332,
                    "test", 0.9999999933333332,
                    "train", List.of(
                        Map.of("avg", 0.49999999625, "max", 0.9999999925, "min", 0.0, "params", Map.of("penalty", 0.5)),
                        Map.of("avg", 0.49999999625, "max", 0.9999999925, "min", 0.0, "params", Map.of("penalty", 2.0))
                    ),
                    "validation", List.of(
                        Map.of("avg", 0.49999999500000003, "max", 0.9999999900000001, "min", 0.0, "params", Map.of("penalty", 0.5)),
                        Map.of("avg", 0.49999999500000003, "max", 0.9999999900000001, "min", 0.0, "params", Map.of("penalty", 2.0))
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
            "  validationFolds: 2, " +
            "  modelName: 'model', " +
            "  featureProperties: ['a'], " +
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
            "  validationFolds: 2, " +
            "  modelName: 'model', " +
            "  featureProperties: ['a'], " +
            "  randomSeed: -1, " +
            "  params: [{penalty: 0.5}, {penalty: 2.0}] " +
            "})";

        assertErrorRegex(query, ".*No relationships have been loaded for relationship type.*(NOPE|NIX).*");
    }

}