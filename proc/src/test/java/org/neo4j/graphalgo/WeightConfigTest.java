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
import org.neo4j.graphalgo.newapi.WeightConfig;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

public interface WeightConfigTest <CONFIG extends WeightConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<CONFIG, RESULT> {
    @Test
    default void testDefaultWeightPropertyIsNull() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.empty();
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.weightProperty());
    }

    @Test
    default void testWeightPropertyFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("weightProperty", "weight"));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("weight", config.weightProperty());
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.AlgoBaseProcTest#emptyStringPropertyValues")
    default void testEmptyWeightPropertyValues(String weightPropertyParameter) {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("weightProperty", weightPropertyParameter));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.weightProperty());
    }

    @Test
    default void testWeightPropertyValidation() {
        List<String> relationshipProperties = Arrays.asList("a");
        Map<String, Object> tempConfig = MapUtil.map(
            "weightProperty", "foo",
            "relationshipProjection", MapUtil.map(
                "A", MapUtil.map(
                    "properties", relationshipProperties
                )
            )
        );

        Map<String, Object> config = createMinimalConfig(CypherMapWrapper.create(tempConfig)).toMap();

        applyOnProcedure(proc -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> proc.compute(config, Collections.emptyMap())
            );
            assertThat(e.getMessage(), containsString("foo"));
            assertThat(e.getMessage(), containsString("[a]"));
        });
    }

    @Test
    default void shouldFailWithInvalidWeightProperty() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateFromStoreConfig.emptyWithName("", loadedGraphName);
        Graph graph = graphLoader(graphCreateConfig)
            .load(HugeGraphFactory.class);

        GraphCatalog.set(graphCreateConfig, GraphsByRelationshipType.of(graph));

        applyOnProcedure((proc) -> {
            CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
                "weightProperty",
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimalConfig(mapWrapper).toMap();
            String error = "Weight property `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found";
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
    @CsvSource(value = { "weight1, 0.0", "weight2, 1.0"})
    default void testFilteringOnPropertiesOnLoadedGraph(String propertyName, double expectedWeight) throws KernelException {
        String graphName = "foo";
        loadExplicitGraph(graphName);

        CypherMapWrapper weightConfig = CypherMapWrapper.create(MapUtil.map("relationshipTypes", Collections.singletonList("*"), "weightProperty", propertyName));
        CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);

        applyOnProcedure((proc) -> {
            CONFIG config = proc.newConfig(Optional.of(graphName), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Pair.of(config, Optional.of(graphName));

            Graph graph = proc.createGraph(configAndName);
            graph.forEachNode(nodeId -> {
                graph.forEachRelationship(nodeId, Direction.OUTGOING, Double.NaN, (s, t, w) -> {
                    assertEquals(expectedWeight, w);
                    return true;
                });
                return true;
            });

        });
    }

    @Test
    default void testFilteringOnRelTypesOnLoadedGraph() {
        String graphName = "foo";
        loadExplicitGraph(graphName);

        CypherMapWrapper weightConfig = CypherMapWrapper.create(MapUtil.map("relationshipTypes", Collections.singletonList("TYPE1"), "weightProperty", "weight1"));
        CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);

        applyOnProcedure((proc) -> {
            CONFIG config = proc.newConfig(Optional.of(graphName), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Pair.of(config, Optional.of(graphName));

            Graph graph = proc.createGraph(configAndName);
            assertGraphEquals(fromGdl("()-[{w1: 0.0}]->(), ()"), graph);
        });
    }

    default void loadExplicitGraph(String graphName) {
        GraphDatabaseAPI db = TestDatabaseCreator.createTestDatabase();

        try {
            Procedures procedures = db
                .getDependencyResolver()
                .resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.ONLY);
            procedures.registerProcedure(GraphCreateProc.class);
        } catch(KernelException ke) {
            ke.printStackTrace();
        }

        String createQuery = "CREATE" +
                             "  (a: Label)" +
                             ", (b: Label)" +
                             ", (c: Label)" +
                             ", (a)-[:TYPE { weight1: 0.0, weight2: 1.0 }]->(b)" +
                             ", (a)-[:TYPE { weight2: 1.0 }]->(c)" +
                             ", (b)-[:TYPE { weight1: 0.0 }]->(c)" +
                             ", (c)-[:TYPE1 { weight1: 0.0 }]->(a)";

        db.execute(createQuery);

        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName(graphName)
            .nodeProjection(NodeProjections.empty())
            .relationshipProjection(RelationshipProjections.builder()
                .putProjection(
                    ElementIdentifier.of("TYPE"),
                    RelationshipProjection.builder()
                        .type("TYPE")
                        .properties(
                            PropertyMappings.of(
                                PropertyMapping.of("weight1", 0.0),
                                PropertyMapping.of("weight2", 1.0)
                            )
                        )
                        .build()
                )
                .putProjection(
                    ElementIdentifier.of("TYPE1"),
                    RelationshipProjection.builder()
                        .type("TYPE1")
                        .build()
                )
                .build()
            )
            .build();

        GraphsByRelationshipType graphsByRelationshipType = graphLoader(db, graphCreateConfig)
            .build(HugeGraphFactory.class)
            .importAllGraphs();

        GraphCatalog.set(graphCreateConfig, graphsByRelationshipType);
        db.shutdown();
    }
}
