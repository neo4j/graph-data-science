/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.similarity.nil.NullGraphStore;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig.PROJECTED_FEATURE_SIZE;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.config.TrainConfig.MODEL_NAME_KEY;
import static org.neo4j.graphalgo.config.TrainConfig.MODEL_TYPE_KEY;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

class GraphSageTrainProcTest extends GraphSageBaseProcTest {

    @Test
    void runsTraining() {
        String modelName = "gsModel";
        String graphName = "embeddingsGraph";
        String train = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.alpha.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("nodePropertyNames", List.of("age", "birth_year", "death_year"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 64)
            .addParameter("degreeAsProperty", true)
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

        Model<ModelData, GraphSageTrainConfig> model = ModelCatalog.get(
            getUsername(),
            modelName,
            ModelData.class,
            GraphSageTrainConfig.class
        );
        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(1, trainConfig.concurrency());
        assertEquals(List.of("age", "birth_year", "death_year"), trainConfig.nodePropertyNames());
        assertEquals("MEAN", Aggregator.AggregatorType.toString(trainConfig.aggregator()));
        assertEquals("SIGMOID", ActivationFunction.toString(trainConfig.activationFunction()));
        assertEquals(64, trainConfig.embeddingDimension());
        assertTrue(trainConfig.degreeAsProperty());
    }

    @Test
    void runsTrainingOnMultiLabelGraph() {
        clearDb();
        GraphStoreCatalog.removeAllLoadedGraphs();
        runQuery(GraphSageTestGraph.GDL);

        String query = GdsCypher.call()
            .withNodeLabel(
                "Restaurant",
                NodeProjection.of("Restaurant", PropertyMappings.of(
                    PropertyMapping.of("numEmployees"),
                    PropertyMapping.of("rating")
                ))
            ).withNodeLabel(
                "Dish",
                NodeProjection.of("Dish", PropertyMappings.of(
                    PropertyMapping.of("numIngredients"),
                    PropertyMapping.of("rating")
                ))
            ).withNodeLabel(
                "Customer",
                NodeProjection.of("Customer", PropertyMappings.of(PropertyMapping.of("numPurchases")))
            )
            .withAnyRelationshipType()
            .graphCreate(graphName)
            .yields();

        runQuery(query);

        String modelName = "gsModel";
        String train = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.alpha.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("projectedFeatureSize", 5)
            .addParameter("nodePropertyNames", List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 64)
            .addParameter("degreeAsProperty", true)
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

        Model<ModelData, GraphSageTrainConfig> model = ModelCatalog.get(
            getUsername(),
            modelName,
            ModelData.class,
            GraphSageTrainConfig.class
        );
        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(1, trainConfig.concurrency());
        assertEquals(List.of("numEmployees", "numIngredients", "rating", "numPurchases"), trainConfig.nodePropertyNames());
        assertEquals("MEAN", Aggregator.AggregatorType.toString(trainConfig.aggregator()));
        assertEquals("SIGMOID", ActivationFunction.toString(trainConfig.activationFunction()));
        assertEquals(64, trainConfig.embeddingDimension());
        assertTrue(trainConfig.degreeAsProperty());
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
            .algo("gds.alpha.graphSage")
            .trainMode()
            .addParameter("nodePropertyNames", List.of("age", "birth_year", "death_year"))
            .addParameter("degreeAsProperty", true)
            .addParameter("modelName", modelName)
            .yields();

        assertCypherResult(
            train,
            List.of(
                map(
                    "graphName", null,
                    "modelInfo", aMapWithSize(2),
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
            "CALL gds.alpha.graphSage.train({" +
            "   modelName: $modelName," +
            "   nodeQuery: $nodeQuery," +
            "   relationshipQuery: $relationshipQuery," +
            "   nodePropertyNames: ['age', 'birth_year', 'death_year']," +
            "   degreeAsProperty: true" +
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
                    "modelInfo", aMapWithSize(2),
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
            .algo("gds.alpha.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("nodePropertyNames", List.of("age", "missing_1", "missing_2"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 42)
            .addParameter("degreeAsProperty", true)
            .addParameter("modelName", modelName)
            .yields();

        String expectedFail = "Node properties [missing_1, missing_2] not found in graph with node properties: [death_year, age, birth_year] in all node labels: ['King']";
        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        assertEquals(expectedFail, throwable.getMessage());
    }

    @ParameterizedTest(name = "projected feature size = {0}")
    @MethodSource("parameters")
    void shouldValidateLabelsAndProperties(int projectedFeatureSize, String expectedMessage) {
        var proc = new GraphSageTrainProc();
        var config = GraphSageTrainConfig.of(getUsername(), Optional.empty(), Optional.empty(),
            CypherMapWrapper.create(
                Map.of(
                    "modelName", GraphSageBaseProcTest.modelName,
                    "nodePropertyNames", List.of("foo"),
                    "projectedFeatureSize", projectedFeatureSize
                )
            )
        );
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> proc.validateConfigsAndGraphStore(
                new NullGraphStore(db.databaseId()),
                GraphCreateFromStoreConfig.emptyWithName(getUsername(), graphName),
                config
            )
        );
        assertThat(exception).hasMessage(expectedMessage);
    }

    static Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                PROJECTED_FEATURE_SIZE,
                "Node properties [foo] not found in graph with node properties: [] in all node labels: ['']"
            ), Arguments.of(
                5,
                "Each property set in `nodePropertyNames` must exist for one label. Missing properties: [foo]"
            )
        );
    }

    @Test
    void estimates() {
        String query = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.alpha.graphSage")
            .trainEstimation()
            .addParameter("degreeAsProperty", true)
            .addParameter("modelName", modelName)
            .yields("requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "requiredMemory", "[5133 KiB ... 5257 KiB]"
        )));
    }
}
