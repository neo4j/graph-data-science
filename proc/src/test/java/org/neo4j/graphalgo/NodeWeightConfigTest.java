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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.newapi.NodeWeightConfig;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

public interface NodeWeightConfigTest<CONFIG extends NodeWeightConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<CONFIG, RESULT> {

    NodeProjections MULTI_PROPERTY_NODE_PROJECTION = NodeProjections.builder()
        .putProjection(
            ElementIdentifier.of("Label"),
            NodeProjection.builder()
                .label("Label")
                .properties(
                    PropertyMappings.of(
                        PropertyMapping.of("weight1", 0.0),
                        PropertyMapping.of("weight2", 1.0)
                    )
                )
                .build()
        ).build();

    @Test
    default void testDefaultNodeWeightPropertyIsNull() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.empty();
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.nodeWeightProperty());
    }

    @Test
    default void testNodeWeightPropertyFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("nodeWeightProperty", "weight"));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("weight", config.nodeWeightProperty());
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.AlgoBaseProcTest#emptyStringPropertyValues")
    default void testEmptyNodeWeightPropertyValues(String weightPropertyParameter) {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("nodeWeightProperty", weightPropertyParameter));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.nodeWeightProperty());
    }

    @Test
    default void testNodeWeightPropertyValidation() {
        runQuery(graphDb(), "CREATE (:A {a: 1})");
        Map<String, Object> tempConfig = MapUtil.map(
            "nodeWeightProperty", "foo",
            "nodeProjection", MapUtil.map(
                "A", MapUtil.map(
                    "properties", Arrays.asList("a")
                )
            )
        );

        Map<String, Object> config = createMinimalConfig(CypherMapWrapper.create(tempConfig)).toMap();

        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> {
                applyOnProcedure(proc -> proc.compute(config, Collections.emptyMap()));
            }
        );
        assertThat(e.getMessage(), containsString("foo"));
        assertThat(e.getMessage(), containsString("[a]"));
    }

    @Test
    default void shouldFailWithInvalidNodeWeightProperty() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateFromStoreConfig.emptyWithName("", loadedGraphName);

        applyOnProcedure((proc) -> {
            GraphsByRelationshipType graphs = graphLoader(graphCreateConfig, proc.legacyMode())
                .build(HugeGraphFactory.class)
                .build()
                .graphs();
            GraphCatalog.set(graphCreateConfig, graphs);

            CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
                "nodeWeightProperty",
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimalConfig(mapWrapper).toMap();
            String error = "Node weight property `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found";
            assertMissingProperty(error, () -> proc.compute(
                loadedGraphName,
                configMap
            ));

            assertMissingProperty(error, () -> proc.compute(
                configMap,
                Collections.emptyMap()
            ));
        });
    }

    @ParameterizedTest
    @CsvSource(value = {"weight1, 0.0", "weight2, 1.0"})
    default void testFilteringOnNodePropertiesOnLoadedGraph(String propertyName, double expectedWeight) {
        String graphName = "foo";
        applyOnProcedure((proc) -> {
            loadExplicitGraphWithNodeWeights(graphName, MULTI_PROPERTY_NODE_PROJECTION, proc.legacyMode());

            CypherMapWrapper weightConfig = CypherMapWrapper.create(MapUtil.map("nodeWeightProperty", propertyName));
            CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);
            CONFIG config = proc.newConfig(Optional.of(graphName), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Pair.of(config, Optional.of(graphName));

            Graph graph = proc.createGraph(configAndName);
            graph.forEachNode(nodeId -> {
                assertEquals(expectedWeight, graph.nodeProperties(propertyName).nodeProperty(nodeId));
                return true;
            });

        });
    }

    default void loadExplicitGraphWithNodeWeights(String graphName, NodeProjections nodeProjections, boolean legacyMode) {
        GraphDatabaseAPI db = TestDatabaseCreator.createTestDatabase();

        try {
            Procedures procedures = db
                .getDependencyResolver()
                .resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.ONLY);
            procedures.registerProcedure(GraphCreateProc.class);
        } catch (KernelException ke) {
            ke.printStackTrace();
        }

        String createQuery = "CREATE" +
                             "  (a: Label { weight1: 0.0, weight2: 1.0 })" +
                             ", (b: Label { weight2: 1.0 })" +
                             ", (c: Label { weight1: 0.0 })";

        runQuery(db, createQuery);

        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName(graphName)
            .nodeProjection(nodeProjections)
            .relationshipProjection(RelationshipProjections.empty())
            .build();

        GraphsByRelationshipType graphsByRelationshipType = graphLoader(db, graphCreateConfig, legacyMode)
            .build(HugeGraphFactory.class)
            .build()
            .graphs();

        GraphCatalog.set(graphCreateConfig, graphsByRelationshipType);
        db.shutdown();
    }
}
