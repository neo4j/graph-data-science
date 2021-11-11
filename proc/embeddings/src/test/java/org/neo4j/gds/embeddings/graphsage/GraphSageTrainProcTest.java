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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;

class GraphSageTrainProcTest extends GraphSageBaseProcTest {

    @Test
    void runsTraining() {
        String modelName = "gsModel";
        String graphName = "embeddingsGraph";
        String train = GdsCypher.call().explicitCreation(graphName)
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
            assertEquals(graphName, resultRow.get("graphName"));
            var modelInfo = (Map<String, Object>) resultRow.get("modelInfo");
            assertNotNull(modelInfo);
            assertEquals(modelName, modelInfo.get(MODEL_NAME_KEY));
            assertEquals(GraphSage.MODEL_TYPE, modelInfo.get(MODEL_TYPE_KEY));
            assertTrue((long) resultRow.get("trainMillis") > 0);
        });

        var model = modelCatalog.get(
            getUsername(),
            modelName,
            ModelData.class,
            GraphSageTrainConfig.class,
            GraphSageModelTrainer.GraphSageTrainMetrics.class
        );
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

        String query = GdsCypher.call()
            .withNodeLabel(
                "A",
                NodeProjection.of("A", PropertyMappings.of(
                    PropertyMapping.of("a1"),
                    PropertyMapping.of("a2")
                ))
            ).withNodeLabel(
                "B",
                NodeProjection.of("B", PropertyMappings.of(
                    PropertyMapping.of("b1"),
                    PropertyMapping.of("b2")
                ))
            )
            .withAnyRelationshipType()
            .graphCreate(graphName)
            .yields();

        runQuery(query);

        String modelName = "gsModel";
        String train = GdsCypher.call().explicitCreation(graphName)
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
            assertEquals(graphName, resultRow.get("graphName"));
            Map<String, Object> modelInfo = (Map<String, Object>) resultRow.get("modelInfo");
            assertNotNull(modelInfo);
            assertEquals(modelName, modelInfo.get(MODEL_NAME_KEY));
            assertEquals(GraphSage.MODEL_TYPE, modelInfo.get(MODEL_TYPE_KEY));
            assertTrue((long) resultRow.get("trainMillis") > 0);
        });

        var model = modelCatalog.get(
            getUsername(),
            modelName,
            ModelData.class,
            GraphSageTrainConfig.class,
            GraphSageModelTrainer.GraphSageTrainMetrics.class
        );
        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(List.of("a1", "a2", "b1", "b2"), trainConfig.featureProperties());
        assertEquals(64, trainConfig.embeddingDimension());
    }

    @Test
    void runsTrainingOnAnonymousNativeGraph() {
        String train = GdsCypher.call().implicitCreation(
                ImmutableGraphCreateFromStoreConfig.builder()
                    .graphName("implicitWeightedGraph")
                    .nodeProjections(NodeProjections
                        .builder()
                        .putProjection(
                            NodeLabel.of("King"),
                            NodeProjection.of(
                                "King",
                                PropertyMappings.of(
                                    PropertyMapping.of("age", 1.0D),
                                    PropertyMapping.of("birth_year", 1.0D),
                                    PropertyMapping.of("death_year", 1.0D)
                                )
                            )
                        )
                        .build())
                    .relationshipProjections(RelationshipProjections.fromString("REL")
                    ).build()
            )
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("age", "birth_year", "death_year"))
            .addParameter("modelName", modelName)
            .yields();

        assertCypherResult(
            train,
            List.of(
                map(
                    "graphName", null,
                    "modelInfo", aMapWithSize(3),
                    "graphCreateConfig", aMapWithSize(4),
                    "configuration", isA(Map.class),
                    "trainMillis", greaterThan(0L)
                )
            )
        );
    }

    @Test
    void runsTrainingOnAnonymousCypherGraph() {
        var nodeQuery = "MATCH (n:King) RETURN id(n) AS id, n.age AS age, n.birth_year AS birth_year, n.death_year AS death_year";
        var relationshipQuery = "MATCH (n:King)-[:REL]-(k:King) RETURN id(n) AS source, id(k) AS target";

        var train =
            "CALL gds.beta.graphSage.train({" +
            "   modelName: $modelName," +
            "   nodeQuery: $nodeQuery," +
            "   relationshipQuery: $relationshipQuery," +
            "   featureProperties: ['age', 'birth_year', 'death_year']" +
            "})";


        assertCypherResult(
            train,
            Map.of(
                "modelName", modelName,
                "nodeQuery", nodeQuery,
                "relationshipQuery", relationshipQuery
            ),
            List.of(
                map(
                    "graphName", null,
                    "modelInfo", aMapWithSize(3),
                    "graphCreateConfig", aMapWithSize(4),
                    "configuration", isA(Map.class),
                    "trainMillis", greaterThan(0L)
                )
            )
        );
    }

    @Test
    void shouldFailOnMissingNodeProperties() {
        String query = GdsCypher.call().explicitCreation("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("featureProperties", List.of("age", "missing_1", "missing_2"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 42)
            .addParameter("modelName", modelName)
            .yields();

        String expectedFail = "The feature properties ['missing_1', 'missing_2'] are not present for all requested labels. Requested labels: ['King']. Properties available on all requested labels: ['age', 'birth_year', 'death_year']";
        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        assertEquals(expectedFail, throwable.getMessage());
    }

    @Test
    void shouldValidateLabelsAndPropertiesWithFeatureDimension() {
        var proc = new GraphSageTrainProc();
        var config = GraphSageTrainConfig.of(getUsername(), Optional.empty(), Optional.empty(),
            CypherMapWrapper.create(
                Map.of(
                    "modelName", GraphSageBaseProcTest.modelName,
                    "featureProperties", List.of("foo"),
                    "projectedFeatureDimension", 5
                )
            )
        );
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> proc.validateConfigsAfterLoad(
                GdlFactory.builder().namedDatabaseId(db.databaseId()).build().build().graphStore(),
                GraphCreateFromStoreConfig.emptyWithName(getUsername(), graphName),
                config
            )
        );
        assertThat(exception).hasMessage(
            "Each property set in `featureProperties` must exist for at least one label. Missing properties: [foo]"
        );
    }

    @Test
    void shouldValidateLabelsAndPropertiesWithoutFeatureDimension() {
        var proc = new GraphSageTrainProc();
        var config = GraphSageTrainConfig.of(getUsername(), Optional.empty(), Optional.empty(),
            CypherMapWrapper.create(
                Map.of(
                    "modelName", GraphSageBaseProcTest.modelName,
                    "featureProperties", List.of("foo")
                )
            )
        );
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> proc.validateConfigsAfterLoad(
                GdlFactory.builder().namedDatabaseId(db.databaseId()).build().build().graphStore(),
                GraphCreateFromStoreConfig.emptyWithName(getUsername(), graphName),
                config
            )
        );
        assertThat(exception).hasMessage(
            "The following node properties are not present for each label in the graph: [foo]. Properties that exist for each label are []"
        );
    }

    @Test
    void shouldValidateModelBeforeTraining() {
        var trainConfigParams = Map.of(
            "modelName", GraphSageBaseProcTest.modelName,
            "featureProperties", List.of("foo"),
            "sudo", true
        );
        var config = GraphSageTrainConfig.of(
            getUsername(), Optional.empty(), Optional.empty(),
            CypherMapWrapper.create(trainConfigParams)
        );
        var model = Model.of(
            getUsername(),
            GraphSageBaseProcTest.modelName,
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            42,
            config,
            GraphSageModelTrainer.GraphSageTrainMetrics.empty()
        );
        modelCatalog.set(model);

        TestProcedureRunner.applyOnProcedure(db, GraphSageTrainProc.class, proc -> {
            proc.modelCatalog = modelCatalog;
            assertThatThrownBy(() -> proc.train(GraphSageBaseProcTest.graphName, trainConfigParams))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Model with name `%s` already exists.", GraphSageBaseProcTest.modelName);
        });
    }

    @Test
    void estimates() {
        String query = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.graphSage")
            .trainEstimation()
            .addParameter("modelName", modelName)
            .addParameter("featureProperties", List.of("a"))
            .yields("requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "requiredMemory", "[5133 KiB ... 5257 KiB]"
        )));
    }

    @ParameterizedTest
    @ValueSource(ints = {-10, -1, 0})
    void featureDimensionValidation(int projectedFeatureDimension) {
        String query = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.graphSage")
            .trainEstimation()
            .addParameter("featureProperties", List.of("a"))
            .addParameter("modelName", modelName)
            .addParameter("projectedFeatureDimension", projectedFeatureDimension)
            .yields();

        assertError(query, "Value for `projectedFeatureDimension` was `" + projectedFeatureDimension);
    }
}
