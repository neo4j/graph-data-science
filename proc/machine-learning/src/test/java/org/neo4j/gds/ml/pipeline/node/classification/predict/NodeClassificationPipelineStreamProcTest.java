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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.GRAPH_NAME;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.TEST_GRAPH_QUERY;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.addPipelineModelWithFeatures;

@Neo4jModelCatalogExtension
class NodeClassificationPipelineStreamProcTest extends BaseProcTest {

    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    private static final String DB_CYPHER = TEST_GRAPH_QUERY;

    @Inject
    private ModelCatalog modelCatalog;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, NodeClassificationPipelineStreamProc.class);

        String loadQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withAnyRelationshipType()
            .withNodeProperties(List.of("a", "b"), DefaultValue.of(Double.NaN))
            .yields();

        runQuery(loadQuery);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void stream() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .streamMode()
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(
            // Use MapUtil because Map.of doesn't like nulls
            MapUtil.map(
                "nodeId", idFunction.of("n1"),
                "predictedClass", 1L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", idFunction.of("n2"),
                "predictedClass", 0L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", idFunction.of("n3"),
                "predictedClass", 0L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", idFunction.of("n4"),
                "predictedClass", 1L,
                "predictedProbabilities", null
            ), MapUtil.map(
                "nodeId", idFunction.of("n5"),
                "predictedClass", 1L,
                "predictedProbabilities", null
            )
        ));
    }

    @Test
    void streamWithProbabilities() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .streamMode()
            .addParameter("includePredictedProbabilities", true)
            .addParameter("modelName", MODEL_NAME)
            .yields("nodeId", "predictedClass", "predictedProbabilities")
            + " RETURN nodeId, predictedClass, [x IN predictedProbabilities | floor(x * 1e11) / 1e11] as predictedProbabilities";

        assertCypherResult(query, List.of(
            Map.of(
                "nodeId", 0L,
                "predictedClass", 1L,
                "predictedProbabilities", List.of(0.01208086561, 0.98791913438)
            ), Map.of(
                "nodeId", 1L,
                "predictedClass", 0L,
                "predictedProbabilities", List.of(0.99980260171, 1.9739828E-4)
            ), Map.of(
                "nodeId", 2L,
                "predictedClass", 0L,
                "predictedProbabilities", List.of(0.93267947552, 0.06732052447)
            ), Map.of(
                "nodeId", 3L,
                "predictedClass", 1L,
                "predictedProbabilities", List.of(0.00352611947, 0.99647388052)
            ), Map.of(
                "nodeId", 4L,
                "predictedClass", 1L,
                "predictedProbabilities", List.of(0.47557912663, 0.52442087336)
            )
        ));
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil#graphNameOrConfigurations")
    void shouldEstimateMemory(Object graphNameOrConfiguration, MemoryRange expected) {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = "CALL gds.beta.pipeline.nodeClassification.predict.stream.estimate(" +
                    "   $graphDefinition, {" +
                    "       modelName: $modelName," +
                    "       includePredictedProbabilities: true" +
                    "})" +
                    "YIELD bytesMin, bytesMax";

        assertCypherResult(
            query,
            Map.of("graphDefinition", graphNameOrConfiguration, "modelName", MODEL_NAME),
            List.of(Map.of("bytesMin", expected.min, "bytesMax", expected.max))
        );
    }
}
