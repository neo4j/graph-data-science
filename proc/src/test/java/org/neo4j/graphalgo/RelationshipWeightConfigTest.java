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

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphdb.DependencyResolver;
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
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.compat.MapUtil.map;

public interface RelationshipWeightConfigTest<CONFIG extends RelationshipWeightConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<CONFIG, RESULT> {

    RelationshipProjections MULTI_RELATIONSHIPS_PROJECTION = RelationshipProjections.builder()
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
        .build();

    String CREATE_QUERY = "CREATE" +
                          "  (a: Label)" +
                          ", (b: Label)" +
                          ", (c: Label)" +
                          ", (a)-[:TYPE { weight1: 0.0, weight2: 1.0 }]->(b)" +
                          ", (a)-[:TYPE { weight2: 1.0 }]->(c)" +
                          ", (b)-[:TYPE { weight1: 0.0 }]->(c)" +
                          ", (c)-[:TYPE1 { weight1: 0.0 }]->(a)";

    @Test
    default void testDefaultRelationshipWeightPropertyIsNull() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.empty();
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.relationshipWeightProperty());
    }

    @Test
    default void testRelationshipWeightPropertyFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(map("relationshipWeightProperty", "weight"));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("weight", config.relationshipWeightProperty());
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.AlgoBaseProcTest#emptyStringPropertyValues")
    default void testEmptyRelationshipWeightPropertyValues(String weightPropertyParameter) {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(map("relationshipWeightProperty", weightPropertyParameter));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.relationshipWeightProperty());
    }

    @Test
    default void testRelationshipWeightPropertyValidation() {
        runQuery(graphDb(), "CREATE ()-[:A {a: 1}]->()");
        List<String> relationshipProperties = Arrays.asList("a");
        Map<String, Object> tempConfig = map(
            "relationshipWeightProperty", "foo",
            "relationshipProjection", map(
                "A", map(
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
    default void shouldFailWithInvalidRelationshipWeightProperty() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateFromStoreConfig.emptyWithName("", loadedGraphName);

        applyOnProcedure((proc) -> {
            GraphStore graphStore = graphLoader(graphCreateConfig)
                .build(HugeGraphFactory.class)
                .build()
                .graphStore();

            GraphCatalog.set(graphCreateConfig, graphStore);

            CypherMapWrapper mapWrapper = CypherMapWrapper.create(map(
                "relationshipWeightProperty",
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimalConfig(mapWrapper).toMap();
            String error = "Relationship weight property `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found";
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
    default void testFilteringOnRelationshipPropertiesOnLoadedGraph(String propertyName, double expectedWeight) {
        String graphName = "foo";
        applyOnProcedure((proc) -> {
            loadExplicitGraphWithRelationshipWeights(graphName, MULTI_RELATIONSHIPS_PROJECTION);

            CypherMapWrapper weightConfig = CypherMapWrapper.create(map(
                "relationshipTypes", Collections.singletonList("*"),
                "relationshipWeightProperty", propertyName
                )
            );

            CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);

            CONFIG config = proc.newConfig(Optional.of(graphName), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Tuples.pair(config, Optional.of(graphName));

            Graph graph = proc.createGraph(configAndName);
            graph.forEachNode(nodeId -> {
                graph.forEachRelationship(nodeId, Double.NaN, (s, t, w) -> {
                    assertEquals(expectedWeight, w);
                    return true;
                });
                return true;
            });

        });
    }

    @ParameterizedTest
    @CsvSource(
        delimiter = ';',
        value = {
            "TYPE1; ()-[]->(), ()",
            "TYPE; (c)<--(a)-->(b)-->(c)",
            "*; (a)-->(b)-->(c)-->(a)-->(c)"
        })
    default void testRunUnweightedOnWeightedMultiRelTypeGraph(String relType, String expectedGraph) {
        String weightedGraphName = "weightedGraph";
        applyOnProcedure((proc) -> {
            loadExplicitGraphWithRelationshipWeights(weightedGraphName, MULTI_RELATIONSHIPS_PROJECTION);

            CypherMapWrapper configWithoutRelWeight = CypherMapWrapper.create(map(
                "relationshipTypes",
                Collections.singletonList(relType)
            ));
            CypherMapWrapper algoConfig = createMinimalConfig(configWithoutRelWeight);

            CONFIG config = proc.newConfig(Optional.of(weightedGraphName), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Tuples.pair(config, Optional.of(weightedGraphName));

            Graph graph = proc.createGraph(configAndName);
            assertGraphEquals(fromGdl(expectedGraph), graph);
        });
    }

    @Test
    default void testRunUnweightedOnWeightedNoRelTypeGraph() {
        String noRelGraph = "weightedGraph";

        applyOnProcedure((proc) -> {
            RelationshipProjections relationshipProjections = RelationshipProjections
                .empty()
                .addPropertyMappings(PropertyMappings.of(PropertyMapping.of("weight1", 1.0)));

            loadExplicitGraphWithRelationshipWeights(noRelGraph, relationshipProjections);

            CypherMapWrapper algoConfig = createMinimalConfig(CypherMapWrapper.empty());

            CONFIG config = proc.newConfig(Optional.of(noRelGraph), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Tuples.pair(config, Optional.of(noRelGraph));

            Graph graph = proc.createGraph(configAndName);
            assertGraphEquals(fromGdl("(a)-->(b)-->(c)-->(a)-->(c)"), graph);
        });
    }

    @Test
    default void testRunUnweightedOnWeightedImplicitlyLoadedGraph() {
        runQuery(graphDb(), "MATCH (n) DETACH DELETE n");
        runQuery(graphDb(), CREATE_QUERY);

        CypherMapWrapper weightConfig = CypherMapWrapper.create(map(
            "relationshipProjection", "*",
            "relationshipProperties", "weight1"
        ));
        CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);

        applyOnProcedure((proc) -> {
            CONFIG config = proc.newConfig(Optional.empty(), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Tuples.pair(config, Optional.empty());
            Graph graph = proc.createGraph(configAndName);
            assertGraphEquals(fromGdl("(a)-->(b)-->(c)-->(a)-->(c)"), graph);
        });
    }

    @Test
    default void testFilteringOnRelTypesOnLoadedGraph() {
        String graphName = "foo";
        applyOnProcedure((proc) -> {
            loadExplicitGraphWithRelationshipWeights(graphName, MULTI_RELATIONSHIPS_PROJECTION);

            CypherMapWrapper weightConfig = CypherMapWrapper.create(MapUtil.map(
                "relationshipTypes", Collections.singletonList("TYPE1"),
                "relationshipWeightProperty", "weight1"
            ));
            CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);

            CONFIG config = proc.newConfig(Optional.of(graphName), algoConfig);
            Pair<CONFIG, Optional<String>> configAndName = Tuples.pair(config, Optional.of(graphName));

            Graph graph = proc.createGraph(configAndName);
            assertGraphEquals(fromGdl("()-[{w1: 0.0}]->(), ()"), graph);
        });
    }

    default void loadExplicitGraphWithRelationshipWeights(String graphName, RelationshipProjections relationshipProjections) {
        GraphDatabaseAPI db = TestDatabaseCreator.createTestDatabase();

        try {
            Procedures procedures = db
                .getDependencyResolver()
                .resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.ONLY);
            procedures.registerProcedure(GraphCreateProc.class);
        } catch (Exception ke) {
            ke.printStackTrace();
        }

        runQuery(db, CREATE_QUERY);

        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName(graphName)
            .nodeProjections(NodeProjections.empty())
            .relationshipProjections(relationshipProjections)
            .build();

        GraphStore graphStore = graphLoader(db, graphCreateConfig)
            .build(HugeGraphFactory.class)
            .build()
            .graphStore();

        GraphCatalog.set(graphCreateConfig, graphStore);
        db.shutdown();
    }
}
