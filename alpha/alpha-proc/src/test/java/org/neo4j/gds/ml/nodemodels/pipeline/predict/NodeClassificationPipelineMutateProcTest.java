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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.neo4j.gds.AlgoBaseProcTest.TEST_USERNAME;
import static org.neo4j.gds.ml.nodemodels.pipeline.predict.NodeClassificationPipelinePredictProcTestUtil.addPipelineModelWithFeatures;

@Neo4jModelCatalogExtension
class NodeClassificationPipelineMutateProcTest extends BaseProcTest {

    private static final String MODEL_NAME = "model";

    private  static final String DB_CYPHER =
        "CREATE " +
        "  (n1:N {a: -1.36753705, b:  1.46853155})" +
        ", (n2:N {a: -1.45431768, b: -1.67820474})" +
        ", (n3:N {a: -0.34216825, b: -1.31498086})" +
        ", (n4:N {a: -0.60765016, b:  1.0186564})" +
        ", (n5:N {a: -0.48403364, b: -0.49152604})";

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, NodeClassificationPipelineMutateProc.class);

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
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void mutate() {
        addPipelineModelWithFeatures(modelCatalog, getUsername(), 2);

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "class")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 5L,
            "mutateMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "createMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, db.databaseId(), "g").graphStore().getUnion();
        assertThat(mutatedGraph.availableNodeProperties()).isEqualTo(Set.of("a", "b", "class"));
        assertThat(mutatedGraph.nodeProperties("class").size()).isEqualTo(5);
    }

    @Test
    void mutateWithProbabilities(){
        addPipelineModelWithFeatures(modelCatalog, getUsername(), 2);

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.pipeline.nodeClassification.predict")
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
            "createMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, db.databaseId(), "g").graphStore().getUnion();
        assertThat(mutatedGraph.availableNodeProperties()).isEqualTo(Set.of("a", "b", "class", "probabilities"));
        assertThat(mutatedGraph.nodeProperties("class").size()).isEqualTo(5);
        assertThat(mutatedGraph.nodeProperties("probabilities").size()).isEqualTo(5);
        assertThat(mutatedGraph.nodeProperties("probabilities").doubleArrayValue(0))
            .containsExactly(new double[]{0.012080865612605783, 0.9879191343873942}, Offset.offset(1e-6));
    }

    @Test
    void validatePropertyNames() {
        addPipelineModelWithFeatures(modelCatalog, getUsername(), 2);

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .addParameter("predictedProbabilityProperty", "foo")
            .addParameter("modelName", MODEL_NAME)
            .yields();
        assertError(query, "`mutateProperty` and `predictedProbabilityProperty` must be different (both were `foo`)");
    }

    @Test
    void validateFeaturesExistOnGraph() {
        // c is not in graph
        addPipelineModelWithFeatures(modelCatalog, getUsername(), 3, List.of("a","c"));

        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.pipeline.nodeClassification.predict")
            .mutateMode()
            .addParameter("mutateProperty", "class")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertError(query, "Node properties [c] defined in the feature steps do not exist in the graph or part of the pipeline");
    }
}
