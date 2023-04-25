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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

@Neo4jModelCatalogExtension
class GraphSageTrainProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:King{ name: 'A', age: 20, birth_year: 200, death_year: 300 })" +
        ", (b:King{ name: 'B', age: 12, birth_year: 232, death_year: 300 })" +
        ", (c:King{ name: 'C', age: 67, birth_year: 212, death_year: 300 })" +
        ", (d:King{ name: 'D', age: 78, birth_year: 245, death_year: 300 })" +
        ", (e:King{ name: 'E', age: 32, birth_year: 256, death_year: 300 })" +
        ", (f:King{ name: 'F', age: 32, birth_year: 214, death_year: 300 })" +
        ", (g:King{ name: 'G', age: 35, birth_year: 214, death_year: 300 })" +
        ", (h:King{ name: 'H', age: 56, birth_year: 253, death_year: 300 })" +
        ", (i:King{ name: 'I', age: 62, birth_year: 267, death_year: 300 })" +
        ", (j:King{ name: 'J', age: 44, birth_year: 289, death_year: 300 })" +
        ", (k:King{ name: 'K', age: 89, birth_year: 211, death_year: 300 })" +
        ", (l:King{ name: 'L', age: 99, birth_year: 201, death_year: 300 })" +
        ", (m:King{ name: 'M', age: 99, birth_year: 201, death_year: 300 })" +
        ", (n:King{ name: 'N', age: 99, birth_year: 201, death_year: 300 })" +
        ", (o:King{ name: 'O', age: 99, birth_year: 201, death_year: 300 })" +
        ", (a)-[:REL {weight: 1.0}]->(b)" +
        ", (a)-[:REL {weight: 5.0}]->(c)" +
        ", (b)-[:REL {weight: 42.0}]->(c)" +
        ", (b)-[:REL {weight: 10.0}]->(d)" +
        ", (c)-[:REL {weight: 62.0}]->(e)" +
        ", (d)-[:REL {weight: 1.0}]->(e)" +
        ", (d)-[:REL {weight: 1.0}]->(f)" +
        ", (e)-[:REL {weight: 1.0}]->(f)" +
        ", (e)-[:REL {weight: 4.0}]->(g)" +
        ", (h)-[:REL {weight: 1.0}]->(i)" +
        ", (i)-[:REL {weight: -1.0}]->(j)" +
        ", (j)-[:REL {weight: 1.0}]->(k)" +
        ", (j)-[:REL {weight: -10.0}]->(l)" +
        ", (k)-[:REL {weight: 1.0}]->(l)";

    private static final String GRAPH_NAME = "embeddingsGraph";

    private static final String MODEL_NAME = "graphSageModel";

    @Inject
    protected ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphSageTrainProc.class
        );

        String query = "CALL gds.graph.project($graphName, " +
                       " {" +
                       "    King: {" +
                       "        label: 'King', " +
                       "        properties: {" +
                       "            age: {property: 'age', defaultValue: 1.0}, " +
                       "            birth_year: {property: 'birth_year', defaultValue: 1.0}, " +
                       "            death_year: {property: 'death_year', defaultValue: 1.0}" +
                       "        }" +
                       "    }" +
                       " }, " +
                       " { " +
                       "    R: {" +
                       "        type: '*', orientation: 'UNDIRECTED', properties: 'weight'" +
                       "    }" +
                       "})";

        runQuery(query, Map.of("graphName", GRAPH_NAME));
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

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
            var resultRow = result.next();

            assertThat(resultRow).isNotNull();
            assertThat(resultRow.get("configuration"))
                .isNotNull()
                .isInstanceOf(Map.class);

            assertThat(resultRow.get("modelInfo"))
                .isNotNull()
                .asInstanceOf(MAP)
                .containsEntry(MODEL_NAME_KEY, modelName)
                .containsEntry(MODEL_TYPE_KEY, GraphSage.MODEL_TYPE);

            assertThat(resultRow.get("trainMillis")).asInstanceOf(LONG).isGreaterThan(0);
        });

        var model = GraphSageModelResolver.resolveModel(modelCatalog, getUsername(), modelName);
        assertThat(model.gdsVersion()).isEqualTo("Unknown");

        assertThat(model.name()).isEqualTo(modelName);
        assertThat(model.algoType()).isEqualTo(GraphSage.MODEL_TYPE);

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertThat(trainConfig).isNotNull();
        assertThat(trainConfig.concurrency()).isEqualTo(1);
        assertThat(trainConfig.featureProperties()).containsExactly("age", "birth_year", "death_year");
        assertThat(trainConfig.aggregator()).isEqualTo(Aggregator.AggregatorType.MEAN);
        assertThat(trainConfig.activationFunction()).isEqualTo(ActivationFunction.SIGMOID);
        assertThat(trainConfig.embeddingDimension()).isEqualTo(64);
    }

    @Test
    void runsTrainingOnMultiLabelGraph() {
        clearDb();
        GraphStoreCatalog.removeAllLoadedGraphs();
        runQuery("CREATE (:A {a1: 1.0, a2: 2.0})-[:REL]->(:B {b1: 42.0, b2: 1337.0})");

        String query = GdsCypher.call(GRAPH_NAME)
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
        String train = GdsCypher.call(GRAPH_NAME)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("projectedFeatureDimension", 5)
            .addParameter("featureProperties", List.of("a1", "a2", "b1", "b2"))
            .addParameter("embeddingDimension", 64)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithResultConsumer(train, result -> {
            var resultRow = result.next();

            assertThat(resultRow).isNotNull();
            assertThat(resultRow.get("configuration"))
                .isNotNull()
                .isInstanceOf(Map.class);

            assertThat(resultRow.get("modelInfo"))
                .isNotNull()
                .asInstanceOf(MAP)
                .containsEntry(MODEL_NAME_KEY, modelName)
                .containsEntry(MODEL_TYPE_KEY, GraphSage.MODEL_TYPE);

            assertThat(resultRow.get("trainMillis")).asInstanceOf(LONG).isGreaterThan(0);
        });

        var model = GraphSageModelResolver.resolveModel(modelCatalog, getUsername(), modelName);

        assertThat(model.name()).isEqualTo(modelName);
        assertThat(model.algoType()).isEqualTo(GraphSage.MODEL_TYPE);

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertThat(trainConfig).isNotNull();

        assertThat(trainConfig.featureProperties()).containsExactly("a1", "a2", "b1", "b2");

        assertThat(trainConfig.embeddingDimension()).isEqualTo(64);
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
            .addParameter("modelName", MODEL_NAME)
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
                    "modelName", MODEL_NAME,
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
                    "modelName", MODEL_NAME,
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
            "modelName", MODEL_NAME,
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
            proc -> assertThatThrownBy(() -> proc.train(GRAPH_NAME, trainConfigParams))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Model with name `%s` already exists.", MODEL_NAME)
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
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.beta.graphSage")
            .trainEstimation()
            .addParameter("modelName", MODEL_NAME)
            .addParameter("featureProperties", List.of("age"))
            .yields("requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "requiredMemory", "[5133 KiB ... 5257 KiB]"
        )));
    }

    @ParameterizedTest
    @ValueSource(ints = {-10, -1, 0})
    void featureDimensionValidation(int projectedFeatureDimension) {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.beta.graphSage")
            .trainEstimation()
            .addParameter("featureProperties", List.of("a"))
            .addParameter("modelName", MODEL_NAME)
            .addParameter("projectedFeatureDimension", projectedFeatureDimension)
            .yields();

        assertError(query, "Value for `projectedFeatureDimension` was `" + projectedFeatureDimension);
    }
}
