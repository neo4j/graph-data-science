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
package org.neo4j.graphalgo.newapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
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
import static org.neo4j.graphalgo.AbstractRelationshipProjection.PROJECTION_KEY;
import static org.neo4j.graphalgo.AbstractRelationshipProjection.TYPE_KEY;
import static org.neo4j.graphalgo.ElementProjection.PROPERTIES_KEY;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

class GraphCreateProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (:A {age: 2})-[:REL {weight: 55}]->(:A)";
    private static final long nodeCount = 2L;
    private static final long relCount = 1L;
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
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void createGraph() {
        assertCypherResult(
            "CALL gds.graph.create('name', 'A', 'REL')",
            singletonList(map(
                "graphName", "name",
                NODE_PROJECTION_KEY, map(
                    "A", map(
                        LABEL_KEY, "A",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "REL", map(
                        TYPE_KEY, "REL",
                        PROJECTION_KEY, Projection.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists("name");
    }

    @Test
    void createCypherGraph() {
        String name = "name";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, map(
                    "*", map(
                        LABEL_KEY, "*",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*", map(
                        TYPE_KEY, "*",
                        PROJECTION_KEY, Projection.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @Test
    void createCypherGraphWithRelationshipTypes() {
        String name = "name";

        String relationshipQuery = "MATCH (a)-[r:REL]->(b) RETURN id(a) AS source, id(b) AS target, type(r) AS type";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", relationshipQuery),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, map("*", map(
                        LABEL_KEY, "*",
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                RELATIONSHIP_PROJECTION_KEY, map("REL", map(
                        TYPE_KEY, "REL",
                        PROJECTION_KEY, Projection.NATURAL.name(),
                        AGGREGATION_KEY, Aggregation.DEFAULT.name(),
                        PROPERTIES_KEY, emptyMap()
                    )
                ),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @Test
    void shouldProjectAllNodesWithSpecialStar() {
        String query = "CALL gds.graph.create('g', '*', 'REL') YIELD nodeCount";

        runQuery("CREATE (), (:B), (:C:D:E)");
        assertCypherResult(query, singletonList(
            map("nodeCount", nodeCount + 3)
        ));

        assertGraphExists("g");
    }

    @Test
    void shouldProjectAllRelationshipsWithSpecialStar() {
        String query = "CALL gds.graph.create('g', 'A', '*') YIELD relationshipCount";

        runQuery("CREATE (:A)-[:R]->(:A), (:B:A)-[:T]->(:A:B), (cde:C:D:E)-[:SELF]->(cde)");
        assertCypherResult(query, singletonList(
            map("relationshipCount", relCount + 2)
        ));

        assertGraphExists("g");
    }

    @ParameterizedTest(name = "projections: {0}")
    @ValueSource(strings = {"'*', {}", "{}, '*'"})
    void emptyProjectionDisallowed(String projection) {
        String query = "CALL gds.graph.create('g', " + projection + ")";

        assertErrorRegex(
            query,
            ".*An empty ((node)|(relationship)) projection was given; at least one ((node label)|(relationship type)) must be projected."
        );

        assertGraphDoesNotExist("g");
    }

    /*
     TODO:
      - node label as LIST OF ANY
      - multiple relationship types in various sugared forms
      - no filter -> load everything
      - different projections
      - type aliasing
      - relationship properties in various sugared forms
      - global definition
      - local definition
      - different aggregations
      - different default values
      - merging of global + local -> positive
      - literal Cypher maps + params as Java maps (borders on Neo responsibilty)
      - uniqueness constraints: -> negative
         - (rel type + projection)s
         - properties in global+local
     */

    @Test
    void failForNulls() {
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

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("org.neo4j.graphalgo.newapi.GraphCreateProcTest#invalidGraphNames")
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
    void failOnMissingMandatory() {
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

    // This test will be removed once we have rich multi-label support
    @ParameterizedTest(name = "argument: {0}")
    @MethodSource("multipleNodeProjections")
    void failsOnMultipleLabelProjections(Object argument) {
        Map<String, Object> nodeProjection = map("A", map(LABEL_KEY, "A"), "B", map(LABEL_KEY, "A"));

        assertError(
            "CALL gds.graph.create('g', $nodeProjection, {})",
            map("nodeProjection", nodeProjection),
            "Multiple node projections are not supported; please use a single projection with a `|` operator to project nodes with different labels into the in-memory graph."
        );

        assertGraphDoesNotExist("g");
    }

    @ParameterizedTest(name = "{0}, nodeProjection = {1}")
    @MethodSource("nodeProjectionVariants")
    void testNodeProjectionVariants(String descr, Object nodeProjection, Map<String, Object> desugarednodeProjection) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create($name, $nodeProjection, '*')",
            map("name", name, "nodeProjection", nodeProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, desugarednodeProjection,
                RELATIONSHIP_PROJECTION_KEY, isA(Map.class),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
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
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph graph = findLoadedGraph(name);
        assertThat(graph.availableNodeProperties(), contains(expectedProperties.keySet().toArray()));
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodeQueryAndProperties(Object nodeProperties, Map<String, Object> expectedProperties) {
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
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
        Graph graph = findLoadedGraph(name);
        assertThat(graph.availableNodeProperties(), contains(expectedProperties.keySet().toArray()));
    }

    @Test
    void nodeQueryAndQueryProperties() {
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
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "{0}, relProjection = {1}")
    @MethodSource("relationshipProjectionVariants")
    void testRelationshipProjectionVariants(String descr, Object relProjection, Map<String, Object> desugaredRelProjection) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, desugaredRelProjection,
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "projection={0}")
    @MethodSource("relationshipProjectionTypes")
    void relationshipProjectionProjections(String projection) {
        String name = "g";

        Long expectedRels = projection.equals("UNDIRECTED") ? relCount * 2 : relCount;

        String graphCreate = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType(
                "B",
                RelationshipProjection.builder()
                    .type("REL")
                    .projection(Projection.of(projection))
                    .build()
            )
            .graphCreate(name)
            .yields();


        // TODO: Validate reverse
        assertCypherResult(
            graphCreate,
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map("B", MapUtil.genericMap(
                    map("type", "REL", PROJECTION_KEY, projection, PROPERTIES_KEY, emptyMap()),
                    AGGREGATION_KEY,
                    Aggregation.DEFAULT.name()
                )),
                "nodeCount", nodeCount,
                "relationshipCount", expectedRels,
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

        // TODO: check property values on graph
        assertCypherResult(graphCreate,
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "B",
                    map("type", "REL",
                        PROJECTION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, expectedProperties)
                ),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relationshipQueryAndProperties(Object relationshipProperties, Map<String, Object> expectedProperties) {
        String name = "g";

        // TODO: check property values on graph
        String nodeQuery = "RETURN 0 AS id";
        String relationshipQuery = "RETURN 0 AS source, 0 AS target, 3 AS weight";
        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { relationshipProperties: $relationshipProperties })",
            map("name", name,
                "nodeQuery", nodeQuery,
                "relationshipQuery", relationshipQuery,
                "relationshipProperties", relationshipProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*", map("type", "*",
                        PROJECTION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, expectedProperties
                    )
                ),
                "nodeCount", 1L,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
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
                        PROJECTION_KEY, "NATURAL",
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
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @Test
    void relationshipProjectionPropertyThrowsAnExceptionWhenNoneIsCombinedWithNotNone() {
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

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregationTypes")
    void relationshipProjectionPropertyPropagateAggregations(String aggregation) {
        String name = "g";

        String extraPropAggregation = Optional.of(aggregation)
            .filter(a -> a.equals("NONE"))
            .orElse("MAX");

        String graphCreate =
            "CALL gds.graph.create($name, '*', " +
            "{" +
            "  B: {" +
            "    type: 'REL', " +
            "    aggregation: $aggregation, " +
            "    properties: {" +
            "      weight: {" +
            "        property: 'weight'" +
            "      }" +
            "    }" +
            "  }" +
            "}," +
            "{ " +
            "  relationshipProperties: {" +
            "    foo: {" +
            "      property: 'weight', " +
            "      aggregation: '" + extraPropAggregation + "'" +
            "    }" +
            "  }" +
            "})";

        assertCypherResult(
            graphCreate,
            map("name", name, "aggregation", aggregation),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "B", map(
                        "type", "REL",
                        PROJECTION_KEY, "NATURAL",
                        AGGREGATION_KEY, aggregation,
                        PROPERTIES_KEY, map(
                            "weight", map(
                                "property", "weight",
                                AGGREGATION_KEY, aggregation,
                                "defaultValue", Double.NaN
                            ),
                            "foo", map(
                                "property", "weight",
                                AGGREGATION_KEY, extraPropAggregation,
                                "defaultValue", Double.NaN
                            )
                        )
                    )
                ),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );

        assertGraphExists(name);
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregationTypes")
    void relationshipProjectionPropertyAggregations(String aggregation) {
        String name = "g";

        String graphCreate =
            "CALL gds.graph.create($name, '*', " +
            "{" +
            "  B: {" +
            "    type: 'REL', " +
            "    aggregation: 'DEFAULT'," +
            "    properties: {" +
            "      weight: {" +
            "        property: 'weight'," +
            "        aggregation: $aggregation" +
            "      }" +
            "    }" +
            "  }" +
            "})";

        // TODO: check property values on graph
        assertCypherResult(
            graphCreate,
            map("name", name, "aggregation", aggregation),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map("B", map(
                    "type", "REL",
                    PROJECTION_KEY, "NATURAL",
                    AGGREGATION_KEY, "DEFAULT",
                    PROPERTIES_KEY, map("weight", map(
                        "property", "weight",
                        AGGREGATION_KEY, aggregation,
                        "defaultValue", Double.NaN)
                    ))
                ),
                "nodeCount", nodeCount,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregationTypes")
    void relationshipQueryPropertyAggregations(String aggregation) {
        String name = "g";

        String graphCreateCypher =
            "CALL gds.graph.create.cypher(" +
            "   $name, " +
            "   $nodeQuery, " +
            "   $relationshipQuery, " +
            "   { " +
            "       relationshipProperties: {" +
            "           weight: {" +
            "               property: 'weight'," +
            "               aggregation: $aggregation" +
            "           }" +
            "       }" +
            "   }" +
            ")";

        // TODO: check property values on graph
        assertCypherResult(
            graphCreateCypher,
            map("name", name,
                "nodeQuery", "RETURN 0 AS id",
                "relationshipQuery", "RETURN 0 AS source, 0 AS target, 1 AS weight",
                "aggregation", aggregation),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, map(
                    "*",
                    map("type", "*",
                        PROJECTION_KEY, "NATURAL",
                        AGGREGATION_KEY, "DEFAULT",
                        PROPERTIES_KEY, map("weight", map(
                                "property", "weight",
                                AGGREGATION_KEY, aggregation,
                                "defaultValue", Double.NaN
                            )
                        )
                    )
                ),
                "nodeCount", 1L,
                "relationshipCount", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relationshipAggregationTypes")
    void relationshipProjectionPropertyAggregationsCypherVsStandard(String aggregation) {

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
                    .projection(Projection.NATURAL)
                    .addProperty("weight", "weight", Double.NaN, Aggregation.valueOf(aggregation))
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

        String graphCreateCypher =
            "CALL gds.graph.create.cypher(" +
            "   $name, " +
            "   $nodeQuery, " +
            "   $relationshipQuery, " +
            "   { " +
            "       relationshipProperties: {" +
            "           weight: {" +
            "               property: 'weight'," +
            "               aggregation: $aggregation" +
            "           }" +
            "       }" +
            "   }" +
            ")";

        AtomicInteger cypherNodeCount = new AtomicInteger();
        AtomicInteger cypherRelCount = new AtomicInteger();
        runQueryWithRowConsumer(
            graphCreateCypher,
            map(
                "name", cypher,
                "nodeQuery", ALL_NODES_QUERY,
                "relationshipQuery", "MATCH (a)-[r:KNOWS]->(b) RETURN id(a) AS source, id(b) AS target, r.weight AS weight",
                "aggregation", aggregation
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
    void shouldDefaultRelationshipProjectionProperty() {
        String graphCreateQuery =
            "CALL gds.graph.create(" +
            "  'testGraph', " +
            "  '*'," +
            "  {" +
            "    REL: {" +
            "      projection: 'NATURAL'," +
            "      properties: {" +
            "        weight: {" +
            "          aggregation: 'SINGLE'" +
            "        }" +
            "      }" +
            "    }" +
            "  }" +
            ");";

        assertCypherResult(graphCreateQuery, singletonList(map(
            "graphName", "testGraph",
            NODE_PROJECTION_KEY, isA(Map.class),
            RELATIONSHIP_PROJECTION_KEY, map(
                "REL", map(
                    "type", "REL",
                    PROJECTION_KEY, "NATURAL",
                    AGGREGATION_KEY, "DEFAULT",
                    PROPERTIES_KEY,  map(
                    "weight", map(
                        "property", "weight",
                            AGGREGATION_KEY, "SINGLE",
                            "defaultValue", Double.NaN)
                ))
            ),
            "nodeCount", nodeCount,
            "relationshipCount", relCount,
            "createMillis", instanceOf(Long.class)
        )));
    }

    @Test
    void shouldDefaultNodeProjectionProperty() {
        String graphCreateQuery =
            "CALL gds.graph.create(" +
            "  'testGraph', " +
            "  {" +
            "    A: {" +
            "      properties: {" +
            "        age: {" +
            "          defaultValue: 1" +
            "        }" +
            "      }" +
            "    }" +
            "  }, " +
            "  '*'" +
            ");";

        assertCypherResult(graphCreateQuery, singletonList(map(
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
            "nodeCount", nodeCount,
            "relationshipCount", relCount,
            "createMillis", instanceOf(Long.class)
        )));
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

    @Test
    void shouldComputeMemoryEstimationForHuge() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);
        runQuery(localDb, DB_CYPHER_ESTIMATE, emptyMap());

        Map<String, Object> relProjection = map(
            "B",
            map("type", "REL")
        );
        String query = "CALL gds.graph.create.estimate('*', $relProjection)";
        runQueryWithRowConsumer(localDb, query, map("relProjection", relProjection),
            row -> {
                assertEquals(303504, row.getNumber("bytesMin").longValue());
                assertEquals(303504, row.getNumber("bytesMax").longValue());
            }
        );
    }

    @Test
    void shouldComputeMemoryEstimationForHugeWithProperties() throws Exception {
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
    void shouldComputeMemoryEstimationForCypher() throws Exception {
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
    void shouldComputeMemoryEstimationForCypherWithProperties() throws Exception {
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
    void shouldComputeMemoryEstimationForVirtualGraph() throws Exception {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, GraphCreateProc.class);

        String query = "CALL gds.graph.create.estimate('*', '*', {nodeCount: 42, relationshipCount: 1337})";
        runQueryWithRowConsumer(localDb, query,
            row -> {
                assertEquals(303744, row.getNumber("bytesMin").longValue());
                assertEquals(303744, row.getNumber("bytesMax").longValue());
            }
        );
    }

    @Test
    void shouldComputeMemoryEstimationForVirtualGraphWithProperties() throws Exception {
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

    // Arguments for parameterised tests

    static Stream<Arguments> multipleNodeProjections() {
        return Stream.of(
            Arguments.of(Arrays.asList("A", "B")),
            Arguments.of(map("A", map(LABEL_KEY, "B"), "B", map(LABEL_KEY, "X")))
        );
    }

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
                        PROJECTION_KEY,
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
                        PROJECTION_KEY,
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
                        PROJECTION_KEY,
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

    static Stream<String> relationshipProjectionTypes() {
        return Stream.of(
            "NATURAL",
            "REVERSE",
            "UNDIRECTED"
        );
    }

    static Stream<String> relationshipAggregationTypes() {
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
