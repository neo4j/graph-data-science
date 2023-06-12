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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.TEST_GRAPH_QUERY;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.addPipelineModelWithFeatures;

@Neo4jModelCatalogExtension
class NodeClassificationPipelineWriteProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    private  static final String DB_CYPHER = TEST_GRAPH_QUERY;

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, NodeClassificationPipelineWriteProc.class);

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
    void write() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .writeMode()
            .addParameter("writeProperty", "class")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 5L,
            "writeMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "preProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        assertCypherResult("CALL db.propertyKeys()", List.of(
            Map.of("propertyKey", "a"),
            Map.of("propertyKey",  "b"),
            Map.of("propertyKey",  "class")
        ));

        assertCypherResult(
            "MATCH (n) WHERE n.class IS NOT NULL RETURN count(n) AS count",
            List.of(Map.of("count", 5L))
        );
    }

    @Test
    void writeWithProbabilities(){
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .writeMode()
            .addParameter("writeProperty", "class")
            .addParameter("predictedProbabilityProperty", "probabilities")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 10L,
            "writeMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "preProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        try (var tx = db.beginTx()) {
            var keys = new HashSet<>();
            for (var key : tx.getAllPropertyKeys()) {
                keys.add(key);
            }
            assertThat(keys).containsExactly("a", "b", "class", "probabilities");

            var classPropertyCount = tx.getAllNodes().stream().filter(n -> n.hasProperty("class")).count();
            assertThat(classPropertyCount).isEqualTo(5L);

            var predictedProbabilitiesPropertyCount = tx.getAllNodes().stream().filter(n -> n.hasProperty("probabilities")).count();
            assertThat(predictedProbabilitiesPropertyCount).isEqualTo(5L);

            double[] probabilities = (double[]) tx.getNodeById(idFunction.of("n1")).getProperty("probabilities");
            assertThat(probabilities).containsExactly(new double[] {0.012080865612605783, 0.9879191343873942}, Offset.offset(1e-6));
        }
    }

    @Test
    void validatePropertyNames() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .writeMode()
            .addParameter("writeProperty", "foo")
            .addParameter("predictedProbabilityProperty", "foo")
            .addParameter("modelName", MODEL_NAME)
            .yields();
        assertError(query, "`writeProperty` and `predictedProbabilityProperty` must be different (both were `foo`)");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil#graphNameOrConfigurations")
    void shouldEstimateMemory(Object graphNameOrConfiguration, MemoryRange expected) {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = "CALL gds.beta.pipeline.nodeClassification.predict.write.estimate(" +
                    "   $graphDefinition, {" +
                    "       modelName: $modelName," +
                    "       writeProperty: 'foo'," +
                    "       predictedProbabilityProperty: 'bar'" +
                    "})" +
                    "YIELD bytesMin, bytesMax, nodeCount, relationshipCount";

        assertCypherMemoryEstimation(
            db,
            query,
            Map.of("graphDefinition", graphNameOrConfiguration, "modelName", MODEL_NAME),
            expected,
            5,
            0
        );
    }
}
