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
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.HashMap;
import java.util.List;

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
            new HashMap<>() {{
                put("nodeId", 0L);
                put("predictedClass", 1L);
                put("predictedProbabilities", null);
            }},
            new HashMap<>() {{
                put("nodeId", 1L);
                put("predictedClass", 0L);
                put("predictedProbabilities", null);
            }},
            new HashMap<>() {{
                put("nodeId", 2L);
                put("predictedClass", 0L);
                put("predictedProbabilities", null);
            }},
            new HashMap<>() {{
                put("nodeId", 3L);
                put("predictedClass", 1L);
                put("predictedProbabilities", null);
            }},
            new HashMap<>() {{
                put("nodeId", 4L);
                put("predictedClass", 0L);
                put("predictedProbabilities", null);
            }}
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
            .yields();

        assertCypherResult(query, List.of(
            new HashMap<>() {{
                put("nodeId", 0L);
                put("predictedClass", 1L);
                put("predictedProbabilities", List.of(0.009623578250764023, 0.9903764217492359));
            }},
            new HashMap<>() {{
                put("nodeId", 1L);
                put("predictedClass", 0L);
                put("predictedProbabilities", List.of(0.6202234850274747, 0.37977651497252507));
            }},
            new HashMap<>() {{
                put("nodeId", 2L);
                put("predictedClass", 0L);
                put("predictedProbabilities", List.of(0.915589719616455, 0.08441028038354483));
            }},
            new HashMap<>() {{
                put("nodeId", 3L);
                put("predictedClass", 1L);
                put("predictedProbabilities", List.of(0.10339691434030411, 0.8966030856596957));
            }},
            new HashMap<>() {{
                put("nodeId", 4L);
                put("predictedClass", 0L);
                put("predictedProbabilities", List.of(0.6619185527831281, 0.3380814472168717));
            }}
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

}
