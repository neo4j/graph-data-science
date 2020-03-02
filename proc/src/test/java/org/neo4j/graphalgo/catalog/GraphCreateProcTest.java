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
package org.neo4j.graphalgo.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.AbstractNodeProjection.LABEL_KEY;
import static org.neo4j.graphalgo.AbstractRelationshipProjection.AGGREGATION_KEY;
import static org.neo4j.graphalgo.AbstractRelationshipProjection.ORIENTATION_KEY;
import static org.neo4j.graphalgo.AbstractRelationshipProjection.TYPE_KEY;
import static org.neo4j.graphalgo.ElementProjection.PROPERTIES_KEY;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.compat.MapUtil.genericMap;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

class GraphCreateProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (:A {age: 2})-[:REL {weight: 55}]->(:A)";
    private static final String DB_CYPHER_ESTIMATE =
        "CREATE" +
        "  (a:A {id: 0, partition: 42})" +
        ", (b:B {id: 1, partition: 42})" +

        ", (a)-[:X { weight: 1.0 }]->(:A {id: 2,  weight: 1.0, partition: 1})" +
        ", (a)-[:X { weight: 1.0 }]->(:A {id: 3,  weight: 2.0, partition: 1})" +
        ", (a)-[:X { weight: 1.0 }]->(:A {id: 4,  weight: 1.0, partition: 1})" +
        ", (a)-[:Y { weight: 1.0 }]->(:A {id: 5,  weight: 1.0, partition: 1})" +
        ", (a)-[:Z { weight: 1.0 }]->(:A {id: 6,  weight: 8.0, partition: 2})" +

        ", (b)-[:X { weight: 42.0 }]->(:B {id: 7,  weight: 1.0, partition: 1})" +
        ", (b)-[:X { weight: 42.0 }]->(:B {id: 8,  weight: 2.0, partition: 1})" +
        ", (b)-[:X { weight: 42.0 }]->(:B {id: 9,  weight: 1.0, partition: 1})" +
        ", (b)-[:Y { weight: 42.0 }]->(:B {id: 10, weight: 1.0, partition: 1})" +
        ", (b)-[:Z { weight: 42.0 }]->(:B {id: 11, weight: 8.0, partition: 2})";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void createNativeProjection() {
        String graphName = "name";

        assertCypherResult(
            "CALL gds.graph.create($name, 'A', 'REL')",
            map("name", graphName),
            singletonList(map(
                "graphName", graphName,
                NODE_PROJECTION_KEY, map(
                    "A", map(
                        LABEL_KEY, "A",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "REL", map(
                        TYPE_KEY, "REL",
                        ORIENTATION_KEY, Orientation.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(graphName);
    }

    @Test
    void createCypherProjection() {
        String graphName = "name";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", graphName, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY),
            singletonList(map(
                "graphName", graphName,
                NODE_PROJECTION_KEY, map(
                    "*", map(
                        LABEL_KEY, "*",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*", map(
                        TYPE_KEY, "*",
                        ORIENTATION_KEY, Orientation.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(graphName);
    }

    @Test
    void createCypherProjectionWithRelationshipTypes() {
        String name = "name";

        String relationshipQuery = "MATCH (a)-[r:REL]->(b) RETURN id(a) AS source, id(b) AS target, type(r) AS type";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", relationshipQuery),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, map(
                    "*", map(
                        LABEL_KEY, "*",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "REL", map(
                        TYPE_KEY, "REL",
                        ORIENTATION_KEY, Orientation.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @Test
    void createCypherProjectionWithParameters() {
        String graphName = "name";

        String nodeQuery = "MATCH (n) WHERE n.age = $age RETURN id(n) AS id";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { parameters: { age: 2 }})",
            map("name", graphName, "nodeQuery", nodeQuery, "relationshipQuery", ALL_RELATIONSHIPS_QUERY),
            singletonList(map(
                "graphName", graphName,
                NODE_PROJECTION_KEY, map(
                    "*", map(
                        LABEL_KEY, "*",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*", map(
                        TYPE_KEY, "*",
                        ORIENTATION_KEY, Orientation.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", 1L,
                "relationshipCount", 0L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(graphName);
    }

    @Test
    void nodeProjectionWithAsterisk() {
        String query = "CALL gds.graph.create('g', '*', 'REL') YIELD nodeCount";

        runQuery("CREATE (), (:B), (:C:D:E)");
        assertCypherResult(query, singletonList(
            map("nodeCount", 5L)
        ));

        assertGraphExists("g");
    }

    @Test
    void relationshipProjectionWithAsterisk() {
        String query = "CALL gds.graph.create('g', 'A', '*') YIELD relationshipCount";

        runQuery("CREATE (:A)-[:R]->(:A), (:B:A)-[:T]->(:A:B), (cde:C:D:E)-[:SELF]->(cde)");
        assertCypherResult(query, singletonList(
            map("relationshipCount", 3L)
        ));

        assertGraphExists("g");
    }

    @ParameterizedTest(name = "{0}, nodeProjection = {1}")
    @MethodSource("nodeProjectionVariants")
    void nodeProjectionVariants(String description, Object nodeProjection, Map<String, Object> desugaredNodeProjection) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create($name, $nodeProjection, '*')",
            map("name", name, "nodeProjection", nodeProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, desugaredNodeProjection,
                RELATIONSHIP_PROJECTION_KEY, isA(Map.class),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodeProjectionWithProperties(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";
        Map<String, Object> nodeProjection = map("B", map(LABEL_KEY, "A", PROPERTIES_KEY, properties));
        Map<String, Object> expectedNodeProjection = map("B", map(LABEL_KEY, "A", PROPERTIES_KEY, expectedProperties));

        assertCypherResult(
            "CALL gds.graph.create($name, $nodeProjection, '*')",
            map("name", name, "nodeProjection", nodeProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, expectedNodeProjection,
                RELATIONSHIP_PROJECTION_KEY, isA(Map.class),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        assertThat(findLoadedGraph(name).availableNodeProperties(), contains(expectedProperties.keySet().toArray()));
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodeQueryWithProperties(Object nodeProperties, Map<String, Object> expectedProperties) {
        String name = "g";

        Map<String, Object> expectedNodeProjection = map("*", map(LABEL_KEY, "*", PROPERTIES_KEY, expectedProperties));

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { nodeProperties: $nodeProperties })",
            map("name", name,
                "nodeQuery", "RETURN 0 AS id, 1 AS age",
                "relationshipQuery", "RETURN 0 AS source, 0 AS target",
                "nodeProperties", nodeProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, expectedNodeProjection,
                RELATIONSHIP_PROJECTION_KEY, isA(Map.class),
                "nodeCount", 1L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph graph = findLoadedGraph(name);
        assertThat(graph.availableNodeProperties(), contains(expectedProperties.keySet().toArray()));
    }

    @Test
    void nodeQueryWithQueryProperties() {
        String name = "g";

        String nodeQuery = "MATCH (n) RETURN id(n) AS id, n.age AS age";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", nodeQuery, "relationshipQuery", ALL_RELATIONSHIPS_QUERY),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, map("*", map(
                    LABEL_KEY, "*",
                    PROPERTIES_KEY, map("age", map(
                        "property", "age",
                        "defaultValue", Double.NaN
                        )
                    ))
                ),
                RELATIONSHIP_PROJECTION_KEY, isA(Map.class),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "{0}, relProjection = {1}")
    @MethodSource("relationshipProjectionVariants")
    void relationshipProjectionVariants(String description, Object relProjection, Map<String, Object> desugaredRelProjection) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, desugaredRelProjection,
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "projection={0}")
    @MethodSource("relationshipOrientations")
    void relationshipProjectionOrientations(String orientation) {
        String name = "g";

        Long expectedRelationshipCount = orientation.equals("UNDIRECTED") ? 2L : 1L;

        String graphCreate = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType(
                "B",
                RelationshipProjection.builder()
                    .type("REL")
                    .orientation(Orientation.of(orientation))
                    .build()
            )
            .graphCreate(name)
            .yields();

        assertCypherResult(
            graphCreate,
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map("B", genericMap(
                    map("type", "REL", ORIENTATION_KEY, orientation, PROPERTIES_KEY, emptyMap()),
                    AGGREGATION_KEY,
                    Aggregation.DEFAULT.name()
                )),
                "nodeCount", 2L,
                "relationshipCount", expectedRelationshipCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relationshipProjectionWithProperties(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";

        String graphCreate = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType(
                "B", RelationshipProjection.builder()
                    .type("REL")
                    .properties(PropertyMappings.fromObject(properties))
                    .build()
            )
            .graphCreate(name)
            .yields();

        assertCypherResult(graphCreate,
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "B",
                    map("type", "REL",
                        ORIENTATION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, expectedProperties)
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        Graph graph = GraphStoreCatalog.get("", name).getGraph();
        assertGraphEquals(fromGdl("()-[{w:55}]->()"), graph);
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relationshipQueryAndProperties(Object relationshipProperties, Map<String, Object> expectedProperties) {
        String name = "g";

        String relationshipQuery = "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, r.weight AS weight";
        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { relationshipProperties: $relationshipProperties })",
            map("name", name,
                "nodeQuery", ALL_NODES_QUERY,
                "relationshipQuery", relationshipQuery,
                "relationshipProperties", relationshipProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*", map("type", "*",
                        ORIENTATION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, expectedProperties
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        Graph graph = GraphStoreCatalog.get("", name).getGraph();
        assertGraphEquals(fromGdl("()-[{w: 55}]->()"), graph);
    }

    @Test
    void relationshipQueryAndQueryProperties() {
        String name = "g";

        String relationshipQuery = "MATCH (s)-[r]->(t) RETURN id(s) AS source, id(t) AS target, r.weight AS weight";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", relationshipQuery),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*", map(
                        "type", "*",
                        ORIENTATION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, map(
                            "weight", map(
                                "property", "weight",
                                "defaultValue", Double.NaN,
                                AGGREGATION_KEY, "NONE"
                            )
                        )
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph graph = GraphStoreCatalog.get("", name).getGraph();
        assertGraphEquals(fromGdl("()-[{w: 55}]->()"), graph);
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregations")
    void relationshipProjectionPropertyPropagateAggregations(String aggregation) {
        String name = "g";

        Map<String, Object> relationshipProjection = map("B", map(
            "type", "REL",
            "aggregation", aggregation,
            "properties", map("weight", emptyMap())
        ));

        Map<String, Object> relationshipProperties = map("foo", map(
            "property", "weight",
            "aggregation", Optional.of(aggregation)
                .filter(a1 -> a1.equals("NONE"))
                .orElse("MAX")
        ));

        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relationshipProjection, { relationshipProperties: $relationshipProperties })",
            map("name", name, "relationshipProjection", relationshipProjection, "relationshipProperties", relationshipProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "B", map(
                        "type", "REL",
                        ORIENTATION_KEY, "NATURAL",
                        AGGREGATION_KEY, aggregation,
                        PROPERTIES_KEY, map(
                            "weight", map(
                                "property", "weight",
                                AGGREGATION_KEY, aggregation,
                                "defaultValue", Double.NaN
                            ),
                            "foo", map(
                                "property", "weight",
                                AGGREGATION_KEY, Optional.of(aggregation)
                                    .filter(a -> a.equals("NONE"))
                                    .orElse("MAX"),
                                "defaultValue", Double.NaN
                            )
                        )
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph weightGraph = GraphStoreCatalog.get("", name).graphStore().getGraph("B", Optional.of("weight"));
        Graph fooGraph = GraphStoreCatalog.get("", name).graphStore().getGraph("B", Optional.of("foo"));
        assertGraphEquals(fromGdl("(a)-[{w: 55}]->()"), weightGraph);
        assertGraphEquals(fromGdl("(a)-[{w: 55}]->()"), fooGraph);
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregations")
    void relationshipProjectionPropertyAggregations(String aggregation) {
        String name = "g";

        Map<String, Object> relationshipProjection = map("B", map(
            "type", "REL",
            "aggregation", Aggregation.DEFAULT.name(),
            "properties", map("weight", map("aggregation", aggregation))
        ));

        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relationshipProjection)",
            map("name", name, "relationshipProjection", relationshipProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map("B", map(
                    "type", "REL",
                    ORIENTATION_KEY, "NATURAL",
                    AGGREGATION_KEY, "DEFAULT",
                    PROPERTIES_KEY, map("weight", map(
                        "property", "weight",
                        AGGREGATION_KEY, aggregation,
                        "defaultValue", Double.NaN)
                    ))
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph graph = GraphStoreCatalog.get("", name).getGraph();
        assertGraphEquals(fromGdl("()-[{w: 55}]->()"), graph);
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregations")
    void relationshipQueryPropertyAggregations(String aggregation) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { relationshipProperties: $relationshipProperties })",
            map("name", name,
                "nodeQuery", ALL_NODES_QUERY,
                "relationshipQuery", "MATCH (s)-[r]->(t) RETURN id(s) AS source, id(t) AS target, r.weight AS weight",
                "relationshipProperties", map("weight", map("aggregation", aggregation))
            ),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*",
                    map("type", "*",
                        ORIENTATION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, map("weight", map(
                                "property", "weight",
                                AGGREGATION_KEY, aggregation,
                                "defaultValue", Double.NaN
                            )
                        )
                    )
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph graph = GraphStoreCatalog.get("", name).getGraph();
        assertGraphEquals(fromGdl("()-[{w: 55}]->()"), graph);
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregations")
    void relationshipProjectionPropertyAggregationsNativeVsCypher(String aggregation) {

        runQuery(
            " CREATE (p:Person)-[:KNOWS]->(k:Person)," +
            " (p)-[:KNOWS {weight: 1}]->(m:Person)," +
            " (m)-[:KNOWS {weight: 2}]->(m)," +
            " (p)-[:KNOWS {weight: 3}]->(p)," +
            " (p)-[:KNOWS {weight: 4}]->(k)," +
            " (p)-[:KNOWS {weight: -2}]->(k)," +
            " (p)-[:KNOWS {weight: 5}]->(k)"
        );

        String standard = "standard";
        String cypher = "cypher";

        String graphCreateStandard = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType(
                "KNOWS",
                RelationshipProjection.builder()
                    .type("KNOWS")
                    .orientation(Orientation.NATURAL)
                    .addProperty("weight", "weight", Double.NaN, Aggregation.lookup(aggregation))
                    .build()
            )
            .graphCreate(standard)
            .yields();

        AtomicInteger standardNodeCount = new AtomicInteger();
        AtomicInteger standardRelCount = new AtomicInteger();
        runQueryWithRowConsumer(graphCreateStandard,
            row -> {
                standardNodeCount.set(row.getNumber("nodeCount").intValue());
                standardRelCount.set(row.getNumber("relationshipCount").intValue());
            }
        );

        AtomicInteger cypherNodeCount = new AtomicInteger();
        AtomicInteger cypherRelCount = new AtomicInteger();
        runQueryWithRowConsumer(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { relationshipProperties: $relationshipProperties })",
            map("name", cypher,
                "nodeQuery", ALL_NODES_QUERY,
                "relationshipQuery", "MATCH (s)-[r:KNOWS]->(t) RETURN id(s) AS source, id(t) AS target, r.weight AS weight",
                "relationshipProperties", map("weight", map("aggregation", aggregation))
            ),
            row -> {
                cypherNodeCount.set(row.getNumber("nodeCount").intValue());
                cypherRelCount.set(row.getNumber("relationshipCount").intValue());
            }
        );

        assertEquals(standardNodeCount.get(), cypherNodeCount.get());

        int relationshipCountCypher = standardRelCount.get();
        int relationshipsStandard = cypherRelCount.get();

        assertTrue(relationshipsStandard > 0);
        assertTrue(relationshipCountCypher > 0);
        assertEquals(
            relationshipsStandard,
            relationshipCountCypher,
            String.format(
                "Expected %d relationships using `gds.graph.create` to be equal to %d relationships when using `gds.graph.create.cypher`",
                relationshipsStandard,
                relationshipCountCypher
            )
        );
    }

    @Test
    void defaultRelationshipProjectionProperty() {
        assertCypherResult("CALL gds.graph.create('testGraph', '*', $relationshipProjection)",
            singletonMap("relationshipProjection", map(
                "REL", map("properties", map("weight", map("aggregation", "SINGLE"))
            ))),
            singletonList(map(
            "graphName", "testGraph",
            NODE_PROJECTION_KEY, isA(Map.class),
            RELATIONSHIP_PROJECTION_KEY, map(
                "REL", map(
                    "type", "REL",
                    ORIENTATION_KEY, "NATURAL",
                    AGGREGATION_KEY, "DEFAULT",
                    PROPERTIES_KEY,  map(
                    "weight", map(
                        "property", "weight",
                            AGGREGATION_KEY, "SINGLE",
                            "defaultValue", Double.NaN)
                ))
            ),
            "nodeCount", 2L,
            "relationshipCount", 1L,
            "createMillis", instanceOf(Long.class)
        )));
    }

    @Test
    void defaultNodeProjectionProperty() {
        assertCypherResult("CALL gds.graph.create('testGraph', $nodeProjection, '*')",
            singletonMap("nodeProjection", map(
                "A", map("properties", map("age", map("defaultValue", 1)))
            )),
            singletonList(map(
            "graphName", "testGraph",
            NODE_PROJECTION_KEY, map(
                "A", map(
                    "label", "A",
                    "properties", map(
                        "age", map(
                            "defaultValue", 1.0,
                            "property", "age"
                        )
                    )

                )
            ),
            RELATIONSHIP_PROJECTION_KEY, isA(Map.class),
            "nodeCount", 2L,
            "relationshipCount", 1L,
            "createMillis", instanceOf(Long.class)
        )));
    }

    @Test
    void estimateHeapPercentageForNativeProjection() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        Map<String, Object> relProjection = map(
            "B",
            map("type", "REL")
        );
        String query = "CALL gds.graph.create.estimate('*', $relProjection)";
        double expectedPercentage = BigDecimal.valueOf(303504)
            .divide(BigDecimal.valueOf(Runtime.getRuntime().maxMemory()), 1, RoundingMode.UP)
            .doubleValue();

        runQueryWithRowConsumer(localDb, query, map("relProjection", relProjection),
            row -> {
                assertEquals(303504, row.getNumber("bytesMax").longValue());
                assertEquals(303504, row.getNumber("bytesMin").longValue());
                assertEquals(expectedPercentage, row.getNumber("heapPercentageMin").doubleValue());
                assertEquals(expectedPercentage, row.getNumber("heapPercentageMax").doubleValue());
            }
        );
    }

    @Test
    void virtualEstimateHeapPercentage() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        Map<String, Object> relProjection = map(
            "B",
            map("type", "REL")
        );
        String query = "CALL gds.graph.create.estimate('*', $relProjection, {nodeCount: 1000000})";

        double expectedPercentage = BigDecimal.valueOf(30190200L)
            .divide(BigDecimal.valueOf(Runtime.getRuntime().maxMemory()), 1, RoundingMode.UP)
            .doubleValue();

        runQueryWithRowConsumer(localDb, query, map("relProjection", relProjection),
            row -> {
                assertEquals(30190200, row.getNumber("bytesMin").longValue());
                assertEquals(30190200, row.getNumber("bytesMax").longValue());
                assertEquals(expectedPercentage, row.getNumber("heapPercentageMin").doubleValue());
                assertEquals(expectedPercentage, row.getNumber("heapPercentageMax").doubleValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForNativeProjectionWithProperties() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        Map<String, Object> relProjection = map(
            "B",
            map("type", "REL", "properties", "weight")
        );
        String query = "CALL gds.graph.create.estimate('*', $relProjection)";

        runQueryWithRowConsumer(localDb, query, map("relProjection", relProjection),
            row -> {
                assertEquals(573936, row.getNumber("bytesMin").longValue());
                assertEquals(573936, row.getNumber("bytesMax").longValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForCypherProjection() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        String nodeQuery = "MATCH (n) RETURN id(n) AS id";
        String relationshipQuery = "MATCH (n)-[:REL]->(m) RETURN id(n) AS source, id(m) AS target";
        String query = "CALL gds.graph.create.cypher.estimate($nodeQuery, $relationshipQuery)";
        runQueryWithRowConsumer(localDb,
            query,
            map("nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery),
            row -> {
                assertEquals(303504, row.getNumber("bytesMin").longValue());
                assertEquals(303504, row.getNumber("bytesMax").longValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForCypherProjectionWithProperties() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        String nodeQuery = "MATCH (n) RETURN id(n) AS id";
        String relationshipQuery = "MATCH (n)-[r:REL]->(m) RETURN id(n) AS source, id(m) AS target, r.weight AS weight";
        String query = "CALL gds.graph.create.cypher.estimate($nodeQuery, $relationshipQuery, {relationshipProperties: 'weight'})";
        runQueryWithRowConsumer(localDb,
            query,
            map("nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery),
            row -> {
                assertEquals(573936, row.getNumber("bytesMin").longValue());
                assertEquals(573936, row.getNumber("bytesMax").longValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForVirtualGraph() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);

        String query = "CALL gds.graph.create.estimate('*', '*', {nodeCount: 42, relationshipCount: 1337})";
        runQueryWithRowConsumer(localDb, query,
            row -> {
                assertEquals(303744, row.getNumber("bytesMin").longValue());
                assertEquals(303744, row.getNumber("bytesMax").longValue());
                assertEquals(42, row.getNumber("nodeCount").longValue());
                assertEquals(1337, row.getNumber("relationshipCount").longValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForVirtualGraphNonEmptyGraph() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        String query = "CALL gds.graph.create.estimate('*', '*', {nodeCount: 42, relationshipCount: 1337})";
        runQueryWithRowConsumer(localDb, query,
            row -> {
                assertEquals(303744, row.getNumber("bytesMin").longValue());
                assertEquals(303744, row.getNumber("bytesMax").longValue());
                assertEquals(42, row.getNumber("nodeCount").longValue());
                assertEquals(1337, row.getNumber("relationshipCount").longValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForVirtualGraphWithProperties() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);

        String query = "CALL gds.graph.create.estimate('*', {`*`: {type: '', properties: 'weight'}}, {nodeCount: 42, relationshipCount: 1337})";
        runQueryWithRowConsumer(localDb, query,
            row -> {
                assertEquals(574176, row.getNumber("bytesMin").longValue());
                assertEquals(574176, row.getNumber("bytesMax").longValue());
            }
        );
    }

    @Test
    void computeMemoryEstimationForVirtualGraphWithLargeValues() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        String query = "CALL gds.graph.create.estimate('*', '*', {nodeCount: 5000000000, relationshipCount: 20000000000})";
        runQueryWithRowConsumer(localDb, query,
            row -> {
                assertEquals(170836586792L, row.getNumber("bytesMin").longValue());
                assertEquals(230841207440L, row.getNumber("bytesMax").longValue());
                assertEquals(5000000000L, row.getNumber("nodeCount").longValue());
                assertEquals(20000000000L, row.getNumber("relationshipCount").longValue());
            }
        );
    }

    @Test
    void loadGraphWithSaturatedThreadPool() {
        // ensure that we don't drop task that can't be scheduled while importing a graph.

        // TODO: ensure parallel running via batch-size
        String query = "CALL gds.graph.create('g', '*', '*')";

        List<Future<?>> futures = new ArrayList<>();
        // block all available threads
        for (int i = 0; i < AlgoBaseConfig.DEFAULT_CONCURRENCY; i++) {
            futures.add(
                Pools.DEFAULT.submit(() -> LockSupport.parkNanos(Duration.ofSeconds(1).toNanos()))
            );
        }

        try {
            runQueryWithRowConsumer(query,
                row -> {
                    assertEquals(2, row.getNumber("nodeCount").intValue());
                    assertEquals(1, row.getNumber("relationshipCount").intValue());
                }
            );
        } finally {
            ParallelUtil.awaitTermination(futures);
        }
    }

    @Test
    void loadMultipleNodeProperties() {
        String testGraph =
            "CREATE" +
            "  (a: Node { foo: 42, bar: 13.37 })" +
            ", (b: Node { foo: 43, bar: 13.38 })" +
            ", (c: Node { foo: 44, bar: 13.39 })" +
            ", (d: Node { foo: 45 })";

        // TODO: test create.cypher
        runQuery(testGraph, Collections.emptyMap());
        String query = GdsCypher
            .call()
            .withNodeLabel("Node")
            .withAnyRelationshipType()
            .withNodeProperty("fooProp", "foo")
            .withNodeProperty(PropertyMapping.of("barProp", "bar", 19.84))
            .graphCreate("g")
            .yields("nodeCount");

        runQuery(query, map());

        Graph graph = GraphStoreCatalog.get("", "g").getGraph();
        Graph expected = fromGdl("({ fooProp: 42, barProp: 13.37D })" +
                                 "({ fooProp: 43, barProp: 13.38D })" +
                                 "({ fooProp: 44, barProp: 13.39D })" +
                                 "({ fooProp: 45, barProp: 19.84D })");
        assertGraphEquals(expected, graph);
    }

    @Test
    void loadMultipleRelationshipProperties() {
        String testGraph =
            "CREATE" +
            "  (a: Node)" +
            ", (b: Node)" +
            ", (a)-[:TYPE_1 { weight: 42.1, cost: 1 }]->(b)" +
            ", (a)-[:TYPE_1 { weight: 43.2, cost: 2 }]->(b)" +
            ", (a)-[:TYPE_2 { weight: 44.3, cost: 3 }]->(b)" +
            ", (a)-[:TYPE_2 { weight: 45.4, cost: 4 }]->(b)";

        runQuery(testGraph, Collections.emptyMap());

        String query = GdsCypher
            .call()
            .withNodeLabel("Node")
            .withRelationshipProperty(PropertyMapping.of("sumWeight", "weight", 1.0, Aggregation.SUM))
            .withRelationshipProperty(PropertyMapping.of("minWeight", "weight", Aggregation.MIN))
            .withRelationshipProperty(PropertyMapping.of("maxCost", "cost", Aggregation.MAX))
            .withRelationshipType("TYPE_1")
            .graphCreate("aggGraph")
            .yields("relationshipProjection");

        runQueryWithRowConsumer(query, row -> {
            Map<String, Object> relationshipProjections = (Map<String, Object>) row.get("relationshipProjection");
            Map<String, Object> type1Projection = (Map<String, Object>) relationshipProjections.get("TYPE_1");
            Map<String, Object> relProperties = (Map<String, Object>) type1Projection.get("properties");
            assertEquals(3, relProperties.size());

            Map<String, Object> sumWeightParams = (Map<String, Object>) relProperties.get("sumWeight");
            Map<String, Object> minWeightParams = (Map<String, Object>) relProperties.get("minWeight");
            Map<String, Object> maxCostParams = (Map<String, Object>) relProperties.get("maxCost");

            assertEquals("weight", sumWeightParams.get("property").toString());
            assertEquals("SUM", sumWeightParams.get("aggregation").toString());
            assertEquals(1.0, sumWeightParams.get("defaultValue"));

            assertEquals("weight", minWeightParams.get("property").toString());
            assertEquals("MIN", minWeightParams.get("aggregation").toString());

            assertEquals("cost", maxCostParams.get("property").toString());
            assertEquals("MAX", maxCostParams.get("aggregation").toString());
        });

        Graph actual = GraphStoreCatalog.get("", "aggGraph").getGraph();
        Graph expected = fromGdl("(a)-[{w:85.3D}]->(b),(a)-[{w:42.1D}]->(b),(a)-[{w:2.0D}]->(b)");
        assertGraphEquals(expected, actual);
    }

    @Test
    void preferRelationshipPropertiesForCypherLoading() {
        String relationshipQuery = "MATCH (s)-[r]->(t) RETURN id(s) AS source, id(t) AS target " +
                                   " , 23 AS foo, 42 AS bar, 1984 AS baz, r.weight AS weight";

        String query = "CALL gds.graph.create.cypher(" +
                       "    'testGraph', $nodeQuery, $relationshipQuery, {" +
                       "        relationshipProperties: {" +
                       "            foobar : 'foo'," +
                       "            foobaz : 'baz'," +
                       "            raboof : 'weight'" +
                       "        }" +
                       "    }" +
                       ")";

        runQuery(query, map("nodeQuery",
            ALL_NODES_QUERY, "relationshipQuery", relationshipQuery));

        Graph foobarGraph = GraphStoreCatalog.get(getUsername(), "testGraph", "", Optional.of("foobar"));
        Graph foobazGraph = GraphStoreCatalog.get(getUsername(), "testGraph", "", Optional.of("foobaz"));
        Graph raboofGraph = GraphStoreCatalog.get(getUsername(), "testGraph", "", Optional.of("raboof"));

        Graph expectedFoobarGraph = fromGdl("()-[{w: 23.0D}]->()");
        Graph expectedFoobazGraph = fromGdl("()-[{w: 1984.0D}]->()");
        Graph expectedRaboofGraph = fromGdl("()-[{w: 55.0D}]->()");

        assertGraphEquals(expectedFoobarGraph, foobarGraph);
        assertGraphEquals(expectedFoobazGraph, foobazGraph);
        assertGraphEquals(expectedRaboofGraph, raboofGraph);
    }

    @Test
    void multiUseLoadedGraphWithMultipleRelationships() throws Exception {
        String graphName = "foo";

        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class, WccStreamProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        String query = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("X")
            .withRelationshipType("Y")
            .graphCreate(graphName)
            .yields("nodeCount", "relationshipCount", "graphName");

        runQueryWithRowConsumer(localDb, query, map(), resultRow -> {
                assertEquals(12L, resultRow.getNumber("nodeCount"));
                assertEquals(8L, resultRow.getNumber("relationshipCount"));
                assertEquals(graphName, resultRow.getString("graphName"));
            }
        );

        String algoQuery = GdsCypher.call()
            .explicitCreation(graphName)
            .algo("wcc")
            .statsMode()
            .addPlaceholder("relationshipTypes", "relType")
            .yields("componentCount");

        runQueryWithRowConsumer(localDb, algoQuery, singletonMap("relType", Arrays.asList("X", "Y")), resultRow ->
            assertEquals(4L, resultRow.getNumber("componentCount"))
        );

        runQueryWithRowConsumer(localDb, algoQuery, singletonMap("relType", Arrays.asList("X")), resultRow ->
            assertEquals(6L, resultRow.getNumber("componentCount"))
        );

        runQueryWithRowConsumer(localDb, algoQuery, singletonMap("relType", Arrays.asList("Y")), resultRow ->
            assertEquals(10L, resultRow.getNumber("componentCount"))
        );
    }

    // Failure cases

    @ParameterizedTest(name = "projections: {0}")
    @ValueSource(strings = {"'*', {}", "{}, '*'", "'', '*'", "'*', ''", "'', ''"})
    void failsOnEmptyProjection(String projection) {
        String query = "CALL gds.graph.create('g', " + projection + ")";

        assertErrorRegex(
            query,
            ".*An empty ((node)|(relationship)) projection was given; at least one ((node label)|(relationship type)) must be projected."
        );

        assertGraphDoesNotExist("g");
    }

    @Test
    void failsOnInvalidPropertyKey() {
        String name = "g";

        String graphCreate =
            "CALL gds.graph.create(" +
            "$name, " +
            "{" +
            "  A: {" +
            "    label: 'A'," +
            "    properties: {" +
            "      property: 'invalid'" +
            "    }" +
            "  }" +
            "}," +
            "'*')";

        assertError(
            graphCreate,
            map("name", name),
            "Node properties not found: 'invalid'"
        );

        assertGraphDoesNotExist(name);
    }

    @Test
    void failsOnNulls() {
        assertError(
            "CALL gds.graph.create(null, null, null)",
            "`graphName` can not be null or blank, but it was `null`"
        );
        assertError(
            "CALL gds.graph.create('name', null, null)",
            "No value specified for the mandatory configuration parameter `nodeProjection`"
        );
        assertError(
            "CALL gds.graph.create('name', 'A', null)",
            "No value specified for the mandatory configuration parameter `relationshipProjection`"
        );
        assertError(
            "CALL gds.graph.create.cypher(null, null, null)",
            "`graphName` can not be null or blank, but it was `null`"
        );
        assertError(
            "CALL gds.graph.create.cypher('name', null, null)",
            "No value specified for the mandatory configuration parameter `nodeQuery`"
        );
        assertError(
            "CALL gds.graph.create.cypher('name', 'MATCH (n) RETURN id(n) AS id', null)",
            "No value specified for the mandatory configuration parameter `relationshipQuery`"
        );
    }

    @Test
    void failsOnInvalidAggregationCombinations() {
        String query =
            "CALL gds.graph.create('g', '*', " +
            "{" +
            "    B: {" +
            "        type: 'REL'," +
            "        projection: 'NATURAL'," +
            "        aggregation: 'NONE'," +
            "        properties: {" +
            "            weight: {" +
            "                aggregation: 'NONE'" +
            "            }" +
            "        }" +
            "    } " +
            "}, " +
            "{" +
            "    relationshipProperties: {" +
            "        foo: {" +
            "            property: 'weight'," +
            "            aggregation: 'MAX'" +
            "        }" +
            "    }" +
            "})";

        assertError(query, "Conflicting relationship property aggregations, it is not allowed to mix `NONE` with aggregations.");

        assertGraphDoesNotExist("g");
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("org.neo4j.graphalgo.catalog.GraphCreateProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {
        assertError(
            "CALL gds.graph.create($graphName, {}, {})",
            map("graphName", invalidName),
            String.format("`graphName` can not be null or blank, but it was `%s`", invalidName)
        );
        assertError(
            "CALL gds.graph.create.cypher($graphName, $nodeQuery, $relationshipQuery)",
            map("graphName", invalidName, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY),
            String.format("`graphName` can not be null or blank, but it was `%s`", invalidName)
        );

        assertGraphDoesNotExist(invalidName);
    }

    @Test
    void failsOnMissingMandatory() {
        assertError(
            "CALL gds.graph.create()",
            "Procedure call does not provide the required number of arguments"
        );
        assertError(
            "CALL gds.graph.create.cypher()",
            "Procedure call does not provide the required number of arguments"
        );
    }

    @Test
    void failsOnInvalidNeoLabel() {
        String name = "g";
        Map nodeProjection = map("A", map(LABEL_KEY, "INVALID"));

        assertError(
            "CALL gds.graph.create($name, $nodeProjection, '*')",
            map("name", name, "nodeProjection", nodeProjection),
            "Invalid node projection, one or more labels not found: 'INVALID'"
        );

        assertGraphDoesNotExist(name);
    }

    @Test
    void failsOnInvalidAggregation() {
        Map relProjection = map("A", map(TYPE_KEY, "REL", AGGREGATION_KEY, "INVALID"));

        assertError(
            "CALL gds.graph.create('g', '*', $relProjection)",
            map("relProjection", relProjection),
            "Aggregation `INVALID` is not supported."
        );
    }

    @Test
    void failsOnInvalidProjection() {
        Map relProjection = map("A", map(TYPE_KEY, "REL", ORIENTATION_KEY, "INVALID"));

        assertError(
            "CALL gds.graph.create('g', '*', $relProjection)",
            map("relProjection", relProjection),
            "Orientation `INVALID` is not supported."
        );
    }

    @Test
    void failsOnExistingGraphName() {
        String name = "g";
        runQuery("CALL gds.graph.create($name, '*', '*')", map("name", name));
        assertError(
            "CALL gds.graph.create($name, '*', '*')",
            map("name", name),
            String.format("A graph with name '%s' already exists.", name));

        assertError(
            "CALL gds.graph.create.cypher($name, '*', '*')",
            map("name", name),
            String.format("A graph with name '%s' already exists.", name));
    }

    @Test
    void failsOnWriteQuery() {
        String writeQuery = "CREATE (n) RETURN id(n) AS id";
        String query = "CALL gds.graph.create.cypher('dragons', $nodeQuery, $relQuery)";

        assertError(
            query,
            map("relQuery", ALL_RELATIONSHIPS_QUERY, "nodeQuery", writeQuery),
            "Query must be read only. Query: "
        );

        assertError(
            query,
            map("nodeQuery", ALL_NODES_QUERY, "relQuery", writeQuery),
            "Query must be read only. Query: "
        );
    }

    @Test
    void failsOnMissingIdColumn() {
        String query =
            "CALL gds.graph.create.cypher(" +
            "   'cypherGraph', " +
            "   'RETURN 1 AS foo', " +
            "   'RETURN 0 AS source, 1 AS target'" +
            ")";

        assertError(query, "Invalid node query, required column(s) not found: 'id'");
    }

    @ParameterizedTest
    @CsvSource(
        delimiter = ';',
        value = {
            "RETURN 0 AS foo, 1 AS target;source",
            "RETURN 0 AS source, 1 AS foo;target",
            "RETURN 0 AS foo, 1 AS bar;source,target",
        })
    void failsOnMissingSourceAndTargetColumns(String returnClause, String missingColumns) {
        String query = String.format(
            "CALL gds.graph.create.cypher(" +
            "   'cypherGraph', " +
            "   'RETURN 1 AS id', " +
            "   '%s'" +
            ")", returnClause);

        assertError(query, String.format(
            "Invalid relationship query, required column(s) not found: '%s'",
            String.join("', '", missingColumns.split(","))
        ));
    }

    @Test
    void failsOnMismatchOfCypherRelationshipProperty() {
        String query =
            "CALL gds.graph.create.cypher(" +
            "   'cypherGraph', " +
            "   'RETURN 1 AS id', " +
            "   'RETURN 0 AS source, 1 AS target, 2 AS weight, 3 AS bazz', " +
            "   { " +
            "       relationshipProperties: ['foo', 'bar', 'weight'] " +
            "   }" +
            ")";

        assertError(query, "Relationship properties not found: 'foo', 'bar'. Available properties from the relationship query are: 'weight', 'bazz'");
    }

    @Test
    void failsOnMismatchOfCypherNodeProperty() {
        String query =
            "CALL gds.graph.create.cypher(" +
            "   'cypherGraph', " +
            "   'RETURN 1 AS id, 2 AS weight, 3 AS bazz', " +
            "   'RETURN 0 AS source, 1 AS target', " +
            "   { " +
            "       nodeProperties: ['foo', 'bar', 'weight'] " +
            "   }" +
            ")";

        assertError(query, "Node properties not found: 'foo', 'bar'. Available properties from the node query are: 'weight', 'bazz'");

        assertGraphDoesNotExist("cypherGraph");
    }

    @Test
    void failsOnInvalidNeoType() {
        String name = "g";

        String graphCreateQuery =
            "CALL gds.graph.create(" +
            "   $name, " +
            "   '*'," +
            "   {" +
            "       REL: {" +
            "           type: 'INVALID'" +
            "       }" +
            "   }" +
            ")";

        assertError(
            graphCreateQuery,
            map("name", name),
            "Invalid relationship projection, one or more relationship types not found: 'INVALID'"
        );

        assertGraphDoesNotExist(name);
    }

    // Arguments for parameterised tests

    static Stream<Arguments> nodeProjectionVariants() {
        return Stream.of(
            Arguments.of(
                "default neo label",
                singletonMap("A", emptyMap()),
                map("A", map(LABEL_KEY, "A", PROPERTIES_KEY, emptyMap()))
            ),
            Arguments.of(
                "aliased node label",
                map("B", map(LABEL_KEY, "A")),
                map("B", map(LABEL_KEY, "A", PROPERTIES_KEY, emptyMap()))
            ),
            Arguments.of(
                "node projection as list",
                singletonList("A"),
                map("A", map(LABEL_KEY, "A", PROPERTIES_KEY, emptyMap()))
            )
        );
    }

    static Stream<Arguments> nodeProperties() {
        return Stream.of(
            Arguments.of(
                map("age", map("property", "age")),
                map("age", map("property", "age", "defaultValue", Double.NaN))
            ),
            Arguments.of(
                map("weight", map("property", "age", "defaultValue", 3D)),
                map("weight", map("property", "age", "defaultValue", 3D))
            ),
            Arguments.of(
                singletonList("age"),
                map("age", map("property", "age", "defaultValue", Double.NaN))
            ),
            Arguments.of(
                map("weight", "age"),
                map("weight", map("property", "age", "defaultValue", Double.NaN))
            )
        );
    }

    static Stream<Arguments> relationshipProperties() {
        return Stream.of(
            Arguments.of(
                map("weight", map("property", "weight")),
                map(
                    "weight",
                    map("property",
                        "weight",
                        "defaultValue",
                        Double.NaN,
                        AGGREGATION_KEY,
                        "DEFAULT"
                    )
                )
            ),
            Arguments.of(
                map("score", map("property", "weight", "defaultValue", 3D)),
                map(
                    "score",
                    map("property", "weight", "defaultValue", 3D, AGGREGATION_KEY, "DEFAULT")
                )
            ),
            Arguments.of(
                singletonList("weight"),
                map(
                    "weight",
                    map("property",
                        "weight",
                        "defaultValue",
                        Double.NaN,
                        AGGREGATION_KEY,
                        "DEFAULT"
                    )
                )
            ),
            Arguments.of(
                map("score", "weight"),
                map(
                    "score",
                    map("property",
                        "weight",
                        "defaultValue",
                        Double.NaN,
                        AGGREGATION_KEY,
                        "DEFAULT"
                    )
                )
            )
        );
    }

    static Stream<Arguments> relationshipProjectionVariants() {
        return Stream.of(
            Arguments.of(
                "default neo type",
                singletonMap("REL", emptyMap()),
                map(
                    "REL",
                    map(
                        "type",
                        "REL",
                        ORIENTATION_KEY,
                        "NATURAL",
                        AGGREGATION_KEY,
                        "DEFAULT",
                        PROPERTIES_KEY,
                        emptyMap()
                    )
                )
            ),
            Arguments.of(
                "aliased rel type",
                map("CONNECTS", map("type", "REL")),
                map(
                    "CONNECTS",
                    map(
                        "type",
                        "REL",
                        ORIENTATION_KEY,
                        "NATURAL",
                        AGGREGATION_KEY,
                        "DEFAULT",
                        PROPERTIES_KEY,
                        emptyMap()
                    )
                )
            ),
            Arguments.of(
                "rel projection as list",
                singletonList("REL"),
                map(
                    "REL",
                    map(
                        "type",
                        "REL",
                        ORIENTATION_KEY,
                        "NATURAL",
                        AGGREGATION_KEY,
                        "DEFAULT",
                        PROPERTIES_KEY,
                        emptyMap()
                    )
                )
            )
        );
    }

    static Stream<String> relationshipOrientations() {
        return Stream.of(
            "NATURAL",
            "REVERSE",
            "UNDIRECTED"
        );
    }

    static Stream<String> relationshipAggregations() {
        return Stream.of(
            "MAX",
            "MIN",
            "SUM",
            "SINGLE",
            "NONE"
        );
    }

    static Stream<String> invalidGraphNames() {
        return Stream.of("", "   ", "           ", "\r\n\t", null);
    }
}
