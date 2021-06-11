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
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictStreamProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.gds.ml.nodemodels.NodeClassificationPredictProcTestUtil.addModelWithFeatures;

class NodeClassificationPredictStreamProcTest extends BaseProcTest {

    private static final String MODEL_NAME = "model";

    private  static final String DB_CYPHER =
        "CREATE " +
        "  (n1:N {a: -1.36753705, b:  1.46853155})" +
        ", (n2:N {a: -1.45431768, b: -1.67820474})" +
        ", (n3:N {a: -0.34216825, b: -1.31498086})" +
        ", (n4:N {a: -0.60765016, b:  1.0186564})" +
        ", (n5:N {a: -0.48403364, b: -0.49152604})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, NodeClassificationPredictStreamProc.class);

        runQuery(DB_CYPHER);

        String loadQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withAnyRelationshipType()
            .withNodeProperties(List.of("a", "b"), DefaultValue.of(Double.NaN))
            .graphCreate("g")
            .yields();

        runQuery(loadQuery);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void stream() {
        addModelWithFeatures(getUsername(), MODEL_NAME, List.of("a", "b"));

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification.predict")
            .streamMode()
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(
            // Use MapUtil because Map.of doesn't like nulls
            MapUtil.map(
                "nodeId", 0L,
                "predictedClass", 1L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", 1L,
                "predictedClass", 0L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", 2L,
                "predictedClass", 0L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", 3L,
                "predictedClass", 1L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", 4L,
                "predictedClass", 0L,
                "predictedProbabilities", null
            )
        ));
    }

    @Test
    void streamWithProbabilities() {
        addModelWithFeatures(getUsername(), MODEL_NAME, List.of("a", "b"));

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification.predict")
            .streamMode()
            .addParameter("includePredictedProbabilities", true)
            .addParameter("modelName", MODEL_NAME)
            .yields("nodeId", "predictedClass", "predictedProbabilities")
            + " RETURN nodeId, predictedClass, [x IN predictedProbabilities | floor(x * 1e11) / 1e11] as predictedProbabilities";

        assertCypherResult(query, List.of(
            Map.of(
                "nodeId", 0L,
                "predictedClass", 1L,
                "predictedProbabilities", List.of(0.00962357825, 0.99037642174)
            ), Map.of(
                "nodeId", 1L,
                "predictedClass", 0L,
                "predictedProbabilities", List.of(0.62022348502, 0.37977651497)
            ), Map.of(
                "nodeId", 2L,
                "predictedClass", 0L,
                "predictedProbabilities", List.of(0.91558971961, 0.08441028038)
            ), Map.of(
                "nodeId", 3L,
                "predictedClass", 1L,
                "predictedProbabilities", List.of(0.10339691434, 0.89660308565)
            ), Map.of(
                "nodeId", 4L,
                "predictedClass", 0L,
                "predictedProbabilities", List.of(0.66191855278, 0.33808144721)
            )
        ));
    }

    @Test
    void validateFeaturesExistOnGraph() {
        // c is not in graph
        addModelWithFeatures(getUsername(), MODEL_NAME, List.of("a", "c"));

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification.predict")
            .streamMode()
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertError(query, "The feature properties ['c'] are not present");
    }

    @Test
    void shouldEstimateMemory() {
        addModelWithFeatures(getUsername(), MODEL_NAME, List.of("a", "b"));

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification.predict")
            .estimationMode(GdsCypher.ExecutionModes.STREAM)
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertDoesNotThrow(() -> runQuery(query));
    }
}
