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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.neo4j.gds.AlgoBaseProcTest.TEST_USERNAME;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.GRAPH_NAME;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.TEST_GRAPH_QUERY;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.addPipelineModelWithFeatures;

@Neo4jModelCatalogExtension
class NodeClassificationPipelineMutateProcTest extends BaseProcTest {

    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    private static final String DB_CYPHER = TEST_GRAPH_QUERY;

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, NodeClassificationPipelineMutateProc.class);

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
    void mutate() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "class")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 5L,
            "mutateMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "preProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), GRAPH_NAME).graphStore().getUnion();
        assertThat(mutatedGraph.availableNodeProperties()).isEqualTo(Set.of("a", "b", "class"));
        assertThat(mutatedGraph.nodeProperties("class").nodeCount()).isEqualTo(5);
    }

    @Test
    void mutateWithProbabilities(){
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "class")
            .addParameter("predictedProbabilityProperty", "probabilities")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 10L,
            "mutateMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "preProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), GRAPH_NAME).graphStore().getUnion();
        assertThat(mutatedGraph.availableNodeProperties()).isEqualTo(Set.of("a", "b", "class", "probabilities"));
        assertThat(mutatedGraph.nodeProperties("class").nodeCount()).isEqualTo(5);
        assertThat(mutatedGraph.nodeProperties("probabilities").nodeCount()).isEqualTo(5);
        assertThat(mutatedGraph.nodeProperties("probabilities").doubleArrayValue(0))
            .containsExactly(new double[]{0.012080865612605783, 0.9879191343873942}, Offset.offset(1e-6));
    }

    @Test
    void validatePropertyNames() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .addParameter("predictedProbabilityProperty", "foo")
            .addParameter("modelName", MODEL_NAME)
            .yields();
        assertError(query, "`mutateProperty` and `predictedProbabilityProperty` must be different (both were `foo`)");
    }

    @Test
    void failsOnExistingProbabilityProperty() {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2, List.of("a","b"));

        var firstQuery = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        runQuery(firstQuery);

        var secondQuery = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "bar")
            .addParameter("predictedProbabilityProperty", "foo")
            .addParameter("modelName", MODEL_NAME)
            .yields();


        assertError(secondQuery, "Node property `foo` already exists in the in-memory graph.");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil#graphNameOrConfigurations")
    void shouldEstimateMemory(Object graphNameOrConfiguration, MemoryRange expected) {
        addPipelineModelWithFeatures(modelCatalog, GRAPH_NAME, getUsername(), 2);

        var query = "CALL gds.beta.pipeline.nodeClassification.predict.mutate.estimate(" +
                    "   $graphDefinition, {" +
                    "       modelName: $modelName," +
                    "       mutateProperty: 'foo'," +
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
