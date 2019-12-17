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
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.helpers.collection.MapUtil.map;

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

    @ParameterizedTest(name = "nodeProjection = {0}, relProjection = {1}")
    @MethodSource(value = "allNodesAndRels")
    void createGraph(Map<String, Object> nodeProjection, String relProjection) {
        String name = "name";

        assertCypherResult(
            "CALL algo.beta.graph.create($name, $nodeProjection, $relProjection)",
            map("name", name, "nodeProjection", nodeProjection, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                "nodeProjection", nodeProjection,
                "relationshipProjection", anything(),
                "nodes", nodeCount,
                "relationships", relCount,
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
        assertError(
            "CALL algo.beta.graph.create(null, null, null)",
            "`graphName` can not be null or blank, but it was `null`"
        );
        assertError(
            "CALL algo.beta.graph.create('name', null, null)",
            "No value specified for the mandatory configuration parameter `nodeProjection`"
        );
        assertError(
            "CALL algo.beta.graph.create('name', 'A', null)",
            "No value specified for the mandatory configuration parameter `relationshipProjection`"
        );
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("org.neo4j.graphalgo.newapi.GraphCreateProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {
        assertError(
            "CALL algo.beta.graph.create($graphName, {}, {})",
            map("graphName", invalidName),
            String.format("`graphName` can not be null or blank, but it was `%s`", invalidName)
        );
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
            "CALL algo.beta.graph.create($name, {}, {})",
            map("name", name),
            singletonList(map(
                "graphName", name,
                "nodeProjection", emptyMap(),
                "relationshipProjection", emptyMap(),
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidNeoLabel() {
        String name = "g";
        Map nodeProjection = map("A", map("label", "INVALID"));

        assertError(
            "CALL algo.beta.graph.create($name, $nodeProjection, {})",
            map("name", name, "nodeProjection", nodeProjection),
            "Invalid node projection, one or more labels not found: 'INVALID'"
        );
    }

    @Test
    void failsOnMultipleLabelProjections() {
        String name = "g";
        Map<String, Object> nodeProjection = map("A", map("label", "A"), "B", map("label", "A"));

        assertError(
            "CALL algo.beta.graph.create($name, $nodeProjection, {})",
            map("name", name, "nodeProjection", nodeProjection),
            "Only one node projection is supported."
        );
    }


    @ParameterizedTest(name = "{0}, nodeProjection = {1}")
    @MethodSource("nodeProjectionVariants")
    void testnodeProjectionVariants(String descr, Object nodeProjection, Map<String, Object> desugarednodeProjection) {
        String name = "g";

        assertCypherResult(
            "CALL algo.beta.graph.create($name, $nodeProjection, {})",
            map("name", name, "nodeProjection", nodeProjection),
            singletonList(map(
                "graphName", name,
                "nodeProjection", desugarednodeProjection,
                "relationshipProjection", emptyMap(),
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @Test
    void failsOnInvalidPropertKey() {
        String name = "g";
        Map<String, Object> nodeProjection = singletonMap(
            "A",
            map("label", "A", "properties", map("property", "invalid"))
        );

        assertError(
            "CALL algo.beta.graph.create($name, $nodeProjection, {})",
            map("name", name, "nodeProjection", nodeProjection),
            "Node properties not found: 'invalid'"
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "nodeProperties")
    void nodePropertiesInNodeProjection(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";
        Map<String, Object> nodeProjection = map("B", map("label", "A", "properties", properties));
        Map<String, Object> expectedNodeProjection = map("B", map("label", "A", "properties", expectedProperties));

        assertCypherResult(
            "CALL algo.beta.graph.create($name, $nodeProjection, {})",
            map("name", name, "nodeProjection", nodeProjection),
            singletonList(map(
                "graphName", name,
                "nodeProjection", expectedNodeProjection,
                "relationshipProjection", emptyMap(),
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
            "CALL algo.beta.graph.create($name, {}, $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                "nodeProjection", emptyMap(),
                "relationshipProjection", desugaredRelProjection,
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
        Map<String, Object> relProjection = map("type", "REL", "projection", projection, "properties", emptyMap());
        Map<String, Object> expectedRelProjection = MapUtil.genericMap(
            new HashMap<>(relProjection),
            "aggregation",
            DeduplicationStrategy.DEFAULT.name()
        );

        Map<String, Object> relProjections = map("B", relProjection);
        Map<String, Object> expectedRelProjections = map("B", expectedRelProjection);

        Long expectedRels = projection.equals("UNDIRECTED") ? relCount * 2 : relCount;

        // TODO: Validate reverse
        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relProjections)",
            map("name", name, "relProjections", relProjections),
            singletonList(map(
                "graphName", name,
                "nodeProjection", emptyMap(),
                "relationshipProjection", expectedRelProjections,
                "nodes", nodeCount,
                "relationships", expectedRels,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "properties = {0}")
    @MethodSource(value = "relationshipProperties")
    void relPropertiesInRelProjection(Object properties, Map<String, Object> expectedProperties) {
        String name = "g";
        Map<String, Object> relProjection = map("B", map("type", "REL", "properties", properties));
        Map<String, Object> expectedRelProjection = map(
            "B",
            map("type", "REL", "projection", "NATURAL", "aggregation", "DEFAULT", "properties", expectedProperties)
        );

        // TODO: check property values on graph
        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                "nodeProjection", emptyMap(),
                "relationshipProjection", expectedRelProjection,
                "nodes", nodeCount,
                "relationships", relCount,
                "createMillis", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "aggregation={0}")
    @MethodSource("relAggregationTypes")
    void relProjectionPropertyAggregations(String aggregation) {
        String name = "g";
        Map<String, Object> properties = map(
            "weight",
            map("property", "weight", "aggregation", aggregation, "defaultValue", Double.NaN)
        );
        Map<String, Object> relProjection = map(
            "B",
            map("type", "REL", "projection", "NATURAL", "aggregation", "DEFAULT", "properties", properties)
        );


        // TODO: check property values on graph
        assertCypherResult(
            "CALL algo.beta.graph.create($name, {}, $relProjection)",
            map("name", name, "relProjection", relProjection),
            singletonList(map(
                "graphName", name,
                "nodeProjection", emptyMap(),
                "relationshipProjection", relProjection,
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
            "CALL algo.beta.graph.create($name, {}, $relProjection)",
            map("name", name, "relProjection", relProjection),
            "Invalid relationship projection, one or more relationship types not found: 'INVALID'"
        );
    }

    static Stream<Map<String, Object>> nodeProjections() {
        return Stream.of(
            map("A", map("label", "A", "properties", emptyMap())),
            emptyMap()
        );
    }

    static Stream<String> relationshipProjections() {
        return Stream.of("REL", "");
    }

    static Stream<Arguments> allNodesAndRels() {
        return crossArguments(
            () -> nodeProjections().map(Arguments::of),
            () -> relationshipProjections().map(Arguments::of)
        );
    }

    static Stream<Arguments> nodeProjectionVariants() {
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
                "node projection as list",
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

    static Stream<Arguments> relationshipProperties() {
        return Stream.of(
            Arguments.of(
                map("weight", map("property", "weight")),
                map("weight", map("property", "weight", "defaultValue", Double.NaN, "aggregation", "DEFAULT"))
            ),
            Arguments.of(
                map("score", map("property", "weight", "defaultValue", 3D)),
                map("score", map("property", "weight", "defaultValue", 3D, "aggregation", "DEFAULT"))
            ),
            Arguments.of(
                singletonList("weight"),
                map("weight", map("property", "weight", "defaultValue", Double.NaN, "aggregation", "DEFAULT"))
            ),
            Arguments.of(
                map("score", "weight"),
                map("score", map("property", "weight", "defaultValue", Double.NaN, "aggregation", "DEFAULT"))
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
                    map("type", "REL", "projection", "NATURAL", "aggregation", "DEFAULT", "properties", emptyMap())
                )
            ),
            Arguments.of(
                "aliased rel type",
                map("CONNECTS", map("type", "REL")),
                map(
                    "CONNECTS",
                    map("type", "REL", "projection", "NATURAL", "aggregation", "DEFAULT", "properties", emptyMap())
                )
            ),
            Arguments.of(
                "rel projection as list",
                singletonList("REL"),
                map(
                    "REL",
                    map("type", "REL", "projection", "NATURAL", "aggregation", "DEFAULT", "properties", emptyMap())
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
