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
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
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
        String name = "name";

        Map<String, Object> nodeProjection = map(
            "A", map(
                LABEL_KEY, "A",
                PROPERTIES_KEY, emptyMap()
            )
        );
        Map<String, Object> relProjection = map(
            "REL", map(
                TYPE_KEY, "REL",
                PROJECTION_KEY, Projection.NATURAL.name(),
                AGGREGATION_KEY, DeduplicationStrategy.DEFAULT.name(),
                PROPERTIES_KEY, emptyMap()
            )
        );

        assertCypherResult(
            "CALL gds.graph.create($name, $nodeProjection, $relProjection)",
            map("name", name, "nodeProjection", nodeProjection, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, nodeProjection,
                RELATIONSHIP_PROJECTION_KEY, relProjection,
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void createCypherGraph() {
        String name = "name";

        String nodeQuery = "MATCH (n:A) RETURN id(n) AS id";
        String relationshipQuery = "MATCH (a)-[r:REL]->(b) RETURN id(a) AS source, id(b) AS target";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, emptyMap(),
                RELATIONSHIP_PROJECTION_KEY, emptyMap(),
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void shouldProjectAllNodesWithSpecialStar() {
        String query = "CALL gds.graph.create('g', '*', 'REL') YIELD nodes";

        runQuery("CREATE (), (:B), (:C:D:E)");
        assertCypherResult(query, singletonList(
            map("nodes", nodeCount + 3)
        ));
    }

    @Test
    void shouldProjectAllRelsWithSpecialStar() {
        String query = "CALL gds.graph.create('g', 'A', '*') YIELD relationships";

        runQuery("CREATE (:A)-[:R]->(:A), (:B:A)-[:T]->(:A:B), (cde:C:D:E)-[:SELF]->(cde)");
        assertCypherResult(query, singletonList(
            map("relationships", relCount + 2)
        ));
    }

    @ParameterizedTest(name = "projections: {0}")
    @ValueSource(strings = {"'*', {}", "{}, '*'"})
    void emptyProjectionDisallowed(String projection) {
        String query = "CALL gds.graph.create('g', " + projection + ")";

        assertErrorRegex(
            query,
            ".*An empty ((node)|(relationship)) projection was given; at least one ((node label)|(relationship type)) must be projected."
        );
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
            "CALL gds.graph.create.cypher('name', 'A', null)",
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
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidPropertyKey() {
        String name = "g";
        Map<String, Object> nodeProjection = singletonMap(
            "A",
            map(LABEL_KEY, "A", PROPERTIES_KEY, map("property", "invalid"))
        );

        assertError(
            "CALL gds.graph.create($name, $nodeProjection, '*')",
            map("name", name, "nodeProjection", nodeProjection),
            "Node properties not found: 'invalid'"
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodePropertiesInNodeProjection(Object properties, Map<String, Object> expectedProperties) {
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
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
        List<Graph> graphs = new ArrayList<>(GraphCatalog.getLoadedGraphs("").values());
        assertThat(graphs, hasSize(1));
        assertThat(graphs.get(0).availableNodeProperties(), contains(expectedProperties.keySet().toArray()));
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodePropertiesAndNodeQuery(Object nodeProperties, Map<String, Object> expectedProperties) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { nodeProperties: $nodeProperties })",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY, "nodeProperties", nodeProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, emptyMap(),
                RELATIONSHIP_PROJECTION_KEY, emptyMap(),
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
        List<Graph> graphs = new ArrayList<>(GraphCatalog.getLoadedGraphs("").values());
        assertThat(graphs, hasSize(1));
        assertThat(graphs.get(0).availableNodeProperties(), contains(expectedProperties.keySet().toArray()));
    }

    @ParameterizedTest(name = "{0}, relProjection = {1}")
    @MethodSource("relProjectionVariants")
    void testRelProjectionVariants(String descr, Object relProjection, Map<String, Object> desugaredRelProjection) {
        String name = "g";

        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, desugaredRelProjection,
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "projection={0}")
    @MethodSource("relProjectionTypes")
    void relProjectionProjections(String projection) {
        String name = "g";
        Map<String, Object> relProjection = map("type", "REL", PROJECTION_KEY, projection, PROPERTIES_KEY, emptyMap());
        Map<String, Object> expectedRelProjection = MapUtil.genericMap(
            new HashMap<>(relProjection),
            AGGREGATION_KEY,
            DeduplicationStrategy.DEFAULT.name()
        );

        Map<String, Object> relProjections = map("B", relProjection);
        Map<String, Object> expectedRelProjections = map("B", expectedRelProjection);

        Long expectedRels = projection.equals("UNDIRECTED") ? relCount * 2 : relCount;

        // TODO: Validate reverse
        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relProjections)",
            map("name", name, "relProjections", relProjections),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, expectedRelProjections,
                "nodes", nodeCount,
                "relationships", expectedRels,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relPropertiesInRelationshipProjection(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";
        Map<String, Object> relProjection = map("B", map("type", "REL", PROPERTIES_KEY, properties));
        Map<String, Object> expectedRelProjection = map(
            "B",
            map("type", "REL", PROJECTION_KEY, "NATURAL", AGGREGATION_KEY, "DEFAULT", PROPERTIES_KEY, expectedProperties)
        );

        // TODO: check property values on graph
        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, expectedRelProjection,
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relPropertiesAndRelationshipQuery(Object relationshipProperties, Map<String, Object> expectedProperties) {
        String name = "g";

        // TODO: check property values on graph
        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { relationshipProperties: $relationshipProperties })",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY, "relationshipProperties", relationshipProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, emptyMap(),
                RELATIONSHIP_PROJECTION_KEY, emptyMap(),
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relAggregationTypes")
    void relationshipProjectionPropertyAggregations(String aggregation) {
        String name = "g";
        Map<String, Object> properties = map(
            "weight",
            map("property", "weight", AGGREGATION_KEY, aggregation, "defaultValue", Double.NaN)
        );
        Map<String, Object> relProjection = map(
            "B",
            map("type", "REL", PROJECTION_KEY, "NATURAL", AGGREGATION_KEY, "DEFAULT", PROPERTIES_KEY, properties)
        );

        // TODO: check property values on graph
        assertCypherResult(
            "CALL gds.graph.create($name, '*', $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, isA(Map.class),
                RELATIONSHIP_PROJECTION_KEY, relProjection,
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relAggregationTypes")
    void relationshipQueryPropertyAggregations(String aggregation) {
        String name = "g";
        Map<String, Object> relationshipProperties = map(
            "weight",
            map("property", "weight", AGGREGATION_KEY, aggregation, "defaultValue", Double.NaN)
        );

        // TODO: check property values on graph
        assertCypherResult(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery, { relationshipProperties: $relationshipProperties })",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY, "relationshipProperties", relationshipProperties),
            singletonList(map(
                "graphName", name,
                NODE_PROJECTION_KEY, emptyMap(),
                RELATIONSHIP_PROJECTION_KEY, emptyMap(),
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidNeoType() {
        String name = "g";
        Map relProjection = map("REL", map("type", "INVALID"));

        assertError(
            "CALL gds.graph.create($name, '*', $relProjection)",
            map("name", name, "relProjection", relProjection),
            "Invalid relationship projection, one or more relationship types not found: 'INVALID'"
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

    static Stream<Arguments> relProjectionVariants() {
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

    static Stream<String> relProjectionTypes() {
        return Stream.of(
            "NATURAL",
            "REVERSE",
            "UNDIRECTED"
        );
    }

    static Stream<String> relAggregationTypes() {
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
