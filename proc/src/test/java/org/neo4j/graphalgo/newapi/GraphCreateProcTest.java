/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.instanceOf;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.helpers.collection.MapUtil.map;

class GraphCreateProcTest extends ProcTestBase {

    private static final String DB_CYPHER = "CREATE (:A {age: 2})-[:REL {weight: 55}]->(:A)";
    private static final long nodeCount = 2L;
    private static final long relCount = 1L;

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCatalogProcs.class);
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    static Stream<Map<String, Object>> nodeFilters() {
        return Stream.of(
            map("A", map("label", "A", "properties", emptyMap())),
            emptyMap()
        );
    }

    static Stream<String> relationshipFilters() {
        return Stream.of("REL", "");
    }

    static Stream<Arguments> allNodesAndRels() {
        return crossArguments(
            () -> nodeFilters().map(Arguments::of),
            () -> relationshipFilters().map(Arguments::of)
        );
    }

    static Stream<Arguments> nodeFilterVariants() {
        return Stream.of(
            Arguments.of(
                "default neo label",
                singletonMap("A", emptyMap()),
                map("A", map("label", "A", "properties", emptyMap()))
            ),
            Arguments.of(
                "aliased node label",
                map("B", map("label", "A")),
                map("B", map("label", "A", "properties", emptyMap()))
            ),
            Arguments.of(
                "node filter as list",
                singletonList("A"),
                map("A", map("label", "A", "properties", emptyMap()))
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

    static  Stream<Arguments> relationshipProperties() {
        return Stream.of(
            Arguments.of(
                map("weight", map("property", "weight")),
                map("weight", map("property", "weight", "defaultValue", Double.NaN, "aggregation", "None"))
            ),
            Arguments.of(
                map("score", map("property", "weight", "defaultValue", 3D)),
                map("score", map("property", "weight", "defaultValue", 3D, "aggregation", "None"))
            ),
            Arguments.of(
                singletonList("weight"),
                map("age", map("property", "weight", "defaultValue", Double.NaN, "aggregation", "None"))
            ),
            Arguments.of(
                map("score", "weight"),
                map("score", map("property", "weight", "defaultValue", Double.NaN, "aggregation", "None"))
            )
        );
    }

    static Stream<Arguments> relFilterVariants() {
        return Stream.of(
            Arguments.of(
                "default neo type",
                singletonMap("REL", emptyMap()),
                map("REL", map("type", "REL", "projection", "NATURAL", "properties", emptyMap()))
            ),
            Arguments.of(
                "aliased rel type",
                map("CONNECTS", map("type", "REL")),
                map("CONNECTS", map("type", "REL", "projection", "NATURAL", "properties", emptyMap()))
            ),
            Arguments.of(
                "rel filter as list",
                singletonList("REL"),
                map("REL", map("type", "REL", "projection", "NATURAL", "properties", emptyMap()))
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

    @ParameterizedTest(name = "nodeFilter = {0}, relFilter = {1}")
    @MethodSource(value = "allNodesAndRels")
    void createGraph(Map<String, Object> nodeFilter, String relFilter) {
        String name = "name";

        assertCypherResult(
            "CALL algo.beta.graph.create($name, $nodeFilter, $relFilter)",
            map("name", name, "nodeFilter", nodeFilter, "relFilter", relFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", nodeFilter,
                "relationshipFilter", anything(),
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
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
        assertError("CALL algo.beta.graph.create(null)", "No value for the key `graphName` was specified");
        //assertError("CALL algo.beta.graph.create('name', null)", "'nodeFilter' cannot be null");
        //assertError("CALL algo.beta.graph.create('name', 'A', null)", "'relationshipFilter' cannot be null");
    }

    @Test
    void failOnMissingMandatory() {
        assertError(
            "CALL algo.beta.graph.create()",
            "Procedure call does not provide the required number of arguments"
        );
    }

    @Test
    void allDefaults() {
        String name = "g";

        assertCypherResult(
            "CALL algo.beta.graph.create($name)",
            map("name", name),
            singletonList(map(
                "graphName", name,
                "nodeFilter", emptyMap(),
                "relationshipFilter", emptyMap(),
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidNeoLabel() {
        String name = "g";
        Map nodeFilter = map("A", map("label", "INVALID"));

        assertError(
            "CALL algo.beta.graph.create($name, $nodeFilter)",
            map("name", name, "nodeFilter", nodeFilter),
            "'INVALID' does not exist in the Neo4j database"
        );
    }

    @Test
    void failsOnDuplicateNeoLabel() {
        String name = "g";
        Map<String, Object> nodeFilter = map("A", map("label", "A"), "B", map("label", "A"));

        assertError(
            "CALL algo.beta.graph.create($name, $nodeFilter)",
            map("name", name, "nodeFilter", nodeFilter),
            "Duplicate node label: 'A'"
        );
    }


    @ParameterizedTest(name = "{0}, nodeFilter = {1}")
    @MethodSource("nodeFilterVariants")
    void testNodeFilterVariants(String descr, Object nodeFilter, Map<String, Object> desugaredNodeFilter) {
        String name = "g";

        assertCypherResult(
            "CALL algo.beta.graph.create($name, $nodeFilter)",
            map("name", name, "nodeFilter", nodeFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", desugaredNodeFilter,
                "relationshipFilter", emptyMap(),
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidPropertKey() {
        String name = "g";
        Map<String, Object> nodeFilter = singletonMap("A", map("label", "A", "properties", map("property", "invalid")));

        assertError(
            "CALL algo.beta.graph.create($name, $nodeFilter)",
            map("name", name, "nodeFilter", nodeFilter),
            "Property key 'invalid' not existing in the Neo4j database"
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodePropertiesInNodeFilter(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";
        Map<String, Object> nodeFilter = map("B", map("label", "A", "properties", properties));
        Map<String, Object> expectedNodeFilter = map("B", map("label", "A", "properties", expectedProperties));


        // TODO: check property values on graph
        assertCypherResult(
            "CALL algo.beta.graph.create($name, $nodeFilter)",
            map("name", name, "nodeFilter", nodeFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", expectedNodeFilter,
                "relationshipFilter", emptyMap(),
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "{0}, relFilter = {1}")
    @MethodSource("relFilterVariants")
    void testRelFilterVariants(String descr, Object relFilter, Map<String, Object> desugaredRelFilter) {
        String name = "g";

        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relFilter)",
            map("name", name, "relFilter", relFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", emptyMap(),
                "relationshipFilter", desugaredRelFilter,
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "projection={0}")
    @MethodSource("relProjectionTypes")
    void relFilterProjections(String projection) {
        String name = "g";
        Map<String, Object> relFilter = map("B", map("type", "REL", "projection", projection, "properties", emptyMap()));

        Long expectedRels = projection.equals("UNDIRECTED") ? relCount * 2 : relCount;

        // TODO: Validate reverse
        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relFilter)",
            map("name", name, "relFilter", relFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", emptyMap(),
                "relationshipFilter", relFilter,
                "nodes", nodeCount,
                "relationships", expectedRels,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relPropertiesInRelFilter(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";
        Map<String, Object> relFilter = map("B", map("type", "REL", "properties", properties));
        Map<String, Object> expectedRelFilter = map("B", map("type", "REL", "projection", "NATURAL", "properties", expectedProperties));


        // TODO: check property values on graph
        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relFilter)",
            map("name", name, "relFilter", relFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", emptyMap(),
                "relationshipFilter", expectedRelFilter,
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relAggregationTypes")
    void relFilterPropertyAggregations(String aggregation) {
        String name = "g";
        Map<String, Object> properties = map("weight", map("property", "weight", "aggregation", aggregation, "defaultValue", Double.NaN));
        Map<String, Object> relFilter = map("B", map("type", "REL", "projection", "NATURAL", "properties", properties));


        // TODO: check property values on graph
        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relFilter)",
            map("name", name, "relFilter", relFilter),
            singletonList(map(
                "graphName", name,
                "nodeFilter", emptyMap(),
                "relationshipFilter", relFilter,
                "nodes", nodeCount,
                "relationships", 2L,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidNeoType() {
        String name = "g";
        Map relFilter = map("REL", map("type", "INVALID"));

        assertError(
            "CALL algo.beta.graph.create($name, {}, $relFilter)",
            map("name", name, "relFilter", relFilter),
            "Relationship type(s) not found: 'INVALID'"
        );
    }

    @Test
    void failsOnDuplicateNeoType() {
        String name = "g";
        Map<String, Object> relFilter = map("REL1", map("type", "REL"), "REL2", map("type", "REL"));

        assertError(
            "CALL algo.beta.graph.create($name, {}, $relFilter)",
            map("name", name, "relFilter", relFilter),
            "Duplicate relationship type: 'REL'"
        );
    }

}
