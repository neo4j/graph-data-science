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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

class GraphSageTrainProcTest extends GraphSageBaseProcTest {

    @Test
    void runsTraining() {
        String modelName = "gsModel";
        String graphName = "embeddingsGraph";
        String train = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("featureProperties", List.of("age", "birth_year", "death_year"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 64)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithResultConsumer(train, result -> {
            Map<String, Object> resultRow = result.next();
            assertNotNull(resultRow);
            assertNotNull(resultRow.get("configuration"));
            var modelInfo = (Map<String, Object>) resultRow.get("modelInfo");
            assertNotNull(modelInfo);
            assertEquals(modelName, modelInfo.get(MODEL_NAME_KEY));
            assertEquals(GraphSage.MODEL_TYPE, modelInfo.get(MODEL_TYPE_KEY));
            assertTrue((long) resultRow.get("trainMillis") > 0);
        });

        var model = GraphSageModelResolver.resolveModel(modelCatalog, getUsername(), modelName);
        assertThat(model.gdsVersion()).isEqualTo("Unknown");

        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(1, trainConfig.concurrency());
        assertEquals(List.of("age", "birth_year", "death_year"), trainConfig.featureProperties());
        assertEquals("MEAN", Aggregator.AggregatorType.toString(trainConfig.aggregator()));
        assertEquals("SIGMOID", ActivationFunction.toString(trainConfig.activationFunction()));
        assertEquals(64, trainConfig.embeddingDimension());
    }

    @Test
    void runsTrainingOnMultiLabelGraph() {
        clearDb();
        GraphStoreCatalog.removeAllLoadedGraphs();
        runQuery("CREATE (:A {a1: 1.0, a2: 2.0})-[:REL]->(:B {b1: 42.0, b2: 1337.0})");

        String query = GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel(
                "A",
                NodeProjection
                    .builder()
                    .label("A")
                    .addProperties(PropertyMapping.of("a1"), PropertyMapping.of("a2"))
                    .build()
            ).withNodeLabel(
                "B",
                NodeProjection
                    .builder()
                    .label("B")
                    .addProperties(PropertyMapping.of("b1"), PropertyMapping.of("b2"))
                    .build()
            )
            .withAnyRelationshipType()
            .yields();

        runQuery(query);

        String modelName = "gsModel";
        String train = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("projectedFeatureDimension", 5)
            .addParameter("featureProperties", List.of("a1", "a2", "b1", "b2"))
            .addParameter("embeddingDimension", 64)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithResultConsumer(train, result -> {
            Map<String, Object> resultRow = result.next();
            assertNotNull(resultRow);
            assertNotNull(resultRow.get("configuration"));
            Map<String, Object> modelInfo = (Map<String, Object>) resultRow.get("modelInfo");
            assertNotNull(modelInfo);
            assertEquals(modelName, modelInfo.get(MODEL_NAME_KEY));
            assertEquals(GraphSage.MODEL_TYPE, modelInfo.get(MODEL_TYPE_KEY));
            assertTrue((long) resultRow.get("trainMillis") > 0);
        });

        var model = GraphSageModelResolver.resolveModel(modelCatalog, getUsername(), modelName);

        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(List.of("a1", "a2", "b1", "b2"), trainConfig.featureProperties());
        assertEquals(64, trainConfig.embeddingDimension());
    }

    @Test
    void shouldFailOnMissingNodeProperties() {
        String query = GdsCypher.call("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("featureProperties", List.of("age", "missing_1", "missing_2"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 42)
            .addParameter("modelName", modelName)
            .yields();

        assertThatThrownBy(() -> runQuery(query))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The feature properties ['missing_1', 'missing_2'] are not present for all requested labels")
            .hasMessageContaining("Requested labels: ['King']")
            .hasMessageContaining("Properties available on all requested labels: ['age', 'birth_year', 'death_year']");
    }

    @Test
    void shouldValidateLabelsAndPropertiesWithFeatureDimension() {
        var config = GraphSageTrainConfig.of(
            getUsername(),
            CypherMapWrapper.create(
                Map.of(
                    "modelName", GraphSageBaseProcTest.modelName,
                    "featureProperties", List.of("foo"),
                    "projectedFeatureDimension", 5
                )
            )
        );

        var graphStore = GdlFactory.builder().databaseId(DatabaseId.of(db)).build().build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        )).hasMessageContaining(
            "The feature properties ['foo'] are not present for any of the requested labels. " +
            "Requested labels: ['__ALL__']. Properties available on the requested labels: []"
        );
    }

    @Test
    void shouldValidateLabelsAndPropertiesWithoutFeatureDimension() {
        var config = GraphSageTrainConfig.of(
            getUsername(),
            CypherMapWrapper.create(
                Map.of(
                    "modelName", GraphSageBaseProcTest.modelName,
                    "featureProperties", List.of("foo")
                )
            )
        );
        CSRGraphStore graphStore = GdlFactory.builder().databaseId(DatabaseId.of(db)).build().build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The feature properties ['foo'] are not present for all requested labels. " +
                                  "Requested labels: ['__ALL__']. Properties available on all requested labels: []");
    }

    @Test
    void shouldValidateModelBeforeTraining() {
        var trainConfigParams = Map.of(
            "modelName", GraphSageBaseProcTest.modelName,
            "featureProperties", List.of("age"),
            "sudo", true
        );
        var config = GraphSageTrainConfig.of(
            getUsername(),
            CypherMapWrapper.create(trainConfigParams)
        );
        var model = Model.of(
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            42,
            config,
            GraphSageModelTrainer.GraphSageTrainMetrics.empty()
        );
        modelCatalog.set(model);

        TestProcedureRunner.applyOnProcedure(
            db,
            GraphSageTrainProc.class,
            proc -> assertThatThrownBy(() -> proc.train(GraphSageBaseProcTest.graphName, trainConfigParams))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Model with name `%s` already exists.", GraphSageBaseProcTest.modelName)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void failOnNaNProperties(boolean multiLabel) {
        clearDb();
        runQuery("CREATE ({a: 100.0})-[:REL]->()");
        runQuery(GdsCypher.call("nanGraph").graphProject().withNodeProperty("a").yields());

        var trainCallBuilder = GdsCypher.call("nanGraph")
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("modelName", "myModel")
            .addParameter("featureProperties", List.of("a"));

        var trainQuery = multiLabel
            ? trainCallBuilder.addParameter("projectedFeatureDimension", 42).yields()
            : trainCallBuilder.yields();

        assertError(trainQuery, "invalid feature property value `NaN` for property `a`");
    }


    @Test
    void estimates() {
        String query = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .trainEstimation()
            .addParameter("modelName", modelName)
            .addParameter("featureProperties", List.of("age"))
            .yields("requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "requiredMemory", "[5133 KiB ... 5257 KiB]"
        )));
    }

    @ParameterizedTest
    @ValueSource(ints = {-10, -1, 0})
    void featureDimensionValidation(int projectedFeatureDimension) {
        String query = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .trainEstimation()
            .addParameter("featureProperties", List.of("a"))
            .addParameter("modelName", modelName)
            .addParameter("projectedFeatureDimension", projectedFeatureDimension)
            .yields();

        assertError(query, "Value for `projectedFeatureDimension` was `" + projectedFeatureDimension);
    }
}
