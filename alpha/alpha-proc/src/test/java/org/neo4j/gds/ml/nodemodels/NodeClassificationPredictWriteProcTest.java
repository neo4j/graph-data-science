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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.WritePropertyConfigProcTest;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionResult;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.gds.ml.nodemodels.NodeClassificationPredictProcTestUtil.addModelWithFeatures;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROPERTIES_KEY;

class NodeClassificationPredictWriteProcTest extends BaseProcTest implements AlgoBaseProcTest<NodeClassificationPredict, NodeClassificationPredictWriteConfig, NodeLogisticRegressionResult> {

    private static final String DB_CYPHER =
        "CREATE " +
        "  (n1:N {a: -1.36753705, b:  1.46853155})" +
        ", (n2:N {a: -1.45431768, b: -1.67820474})" +
        ", (n3:N {a: -0.34216825, b: -1.31498086})" +
        ", (n4:N {a: -0.60765016, b:  1.0186564})" +
        ", (n5:N {a: -0.48403364, b: -0.49152604})" +
        ", (n1)-[:R]->(n2)";
    public static final String GRAPH_NAME = "g";
    public static final String MODEL_NAME = "model";

    @TestFactory
    Stream<DynamicTest> configTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, NodeClassificationPredictWriteProc.class);

        runQuery(DB_CYPHER);

        String loadQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withAnyRelationshipType()
            .withNodeProperties(List.of("a", "b"), DefaultValue.of(Double.NaN))
            .graphCreate(GRAPH_NAME)
            .yields();
        addModelWithFeatures(getUsername(), MODEL_NAME, List.of("a", "b"));

        runQuery(loadQuery);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldHaveTheRightOutputs() {
        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification.predict")
            .writeMode()
            .addParameter("writeProperty", "class")
            .addParameter("predictedProbabilityProperty", "probabilities")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 10L,
            "writeMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "createMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Test
    void shouldEstimateMemory() {
        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.nodeClassification.predict")
            .estimationMode(GdsCypher.ExecutionModes.WRITE)
            .addParameter("writeProperty", "class")
            .addParameter("modelName", MODEL_NAME)
            .yields();

        assertDoesNotThrow(() -> runQuery(query));
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper input) {
        if (!input.containsKey("writeProperty")) {
            input = input.withString("writeProperty", "bar");
        }
        return input.withString("modelName", MODEL_NAME);
    }

    @Override
    public CypherMapWrapper createMinimalImplicitConfig(CypherMapWrapper input) {
        CypherMapWrapper updatedMap = AlgoBaseProcTest.super.createMinimalImplicitConfig(input);
        if (updatedMap.containsKey(NODE_PROJECTION_KEY) && !updatedMap.containsKey(NODE_QUERY_KEY)) {
            updatedMap = updatedMap.withEntry(NODE_PROPERTIES_KEY, List.of("a", "b"));
        } else if (!updatedMap.containsKey(NODE_PROJECTION_KEY) && updatedMap.containsKey(NODE_QUERY_KEY)) {
            updatedMap = updatedMap.withString(NODE_QUERY_KEY, "MATCH (n) RETURN id(n) AS id, n.a AS a, n.b AS b");
        }
        return updatedMap;
    }

    @Override
    public Class<? extends AlgoBaseProc<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationPredictWriteConfig>> getProcedureClazz() {
        return NodeClassificationPredictWriteProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public boolean supportsImplicitGraphCreate() {
        return false;
    }

    @Override
    public NodeClassificationPredictWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeClassificationPredictWriteConfig.of(getUsername(), Optional.of(GRAPH_NAME), Optional.empty(), mapWrapper);
    }

    @Override
    public void assertResultEquals(NodeLogisticRegressionResult result1, NodeLogisticRegressionResult result2) {
        assertArrayEquals(result1.predictedClasses().toArray(), result2.predictedClasses().toArray());
        var probabilities1 = result1.predictedProbabilities();
        var probabilities2 = result2.predictedProbabilities();
        if (probabilities1.isPresent()) {
            assertThat(probabilities2)
                .isPresent()
                .get()
                .extracting(HugeObjectArray::toArray, as(InstanceOfAssertFactories.array(double[][].class)))
                .containsExactly(probabilities1.get().toArray());
        } else {
            assertThat(probabilities2).isNotPresent();
        }
    }

    @Override
    public @NotNull GraphLoader graphLoader(GraphCreateConfig graphCreateConfig) {
        GraphCreateConfig configWithNodeProperty = graphCreateConfig instanceof GraphCreateFromStoreConfig
            ? ImmutableGraphCreateFromStoreConfig
            .builder()
            .from(graphCreateConfig)
            .nodeProperties(PropertyMappings.of(PropertyMapping.of("a"), PropertyMapping.of("b")))
            .build()
            : ImmutableGraphCreateFromCypherConfig
                .builder()
                .from(graphCreateConfig)
                .nodeQuery("MATCH (n) RETURN id(n) AS id, n.a AS a, n.b AS b")
                .build();

        return graphLoader(graphDb(), configWithNodeProperty);
    }
}
