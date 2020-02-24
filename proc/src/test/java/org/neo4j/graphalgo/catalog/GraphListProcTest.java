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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

class GraphListProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCreateProc.class,
            GraphListProc.class
        );
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void listASingleLabelRelationshipTypeProjection() {
        String name = "name";
        runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL gds.graph.list()", singletonList(
            map(
                "graphName", name,
                "nodeProjection", map(
                    "A", map(
                        "label", "A",
                        "properties", emptyMap()
                    )
                ),
                "relationshipProjection", map(
                    "REL", map(
                        "type", "REL",
                        "orientation", "NATURAL",
                        "aggregation", "DEFAULT",
                        "properties", emptyMap()
                    )
                ),
                "nodeQuery", null,
                "relationshipQuery", null,
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Test
    void listASingleCypherProjection() {
        String name = "name";
        runQuery(
            "CALL gds.graph.create.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY));

        assertCypherResult("CALL gds.graph.list()", singletonList(
            map(
                "graphName", name,
                "nodeProjection", map(
                    "*", map(
                        "label", "*",
                        "properties", emptyMap()
                    )
                ),
                "relationshipProjection", map(
                    "*", map(
                        "type", "*",
                        "orientation", "NATURAL",
                        "aggregation", "DEFAULT",
                        "properties", emptyMap()
                    )
                ),
                "nodeQuery", ALL_NODES_QUERY,
                "relationshipQuery", ALL_RELATIONSHIPS_QUERY,
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Test
    void degreeDistributionComputationIsOptOut() {
        String name = "name";
        runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD graphName, nodeProjection, relationshipProjection, nodeCount, relationshipCount", singletonList(
            map(
                "graphName", name,
                "nodeProjection", map(
                    "A", map(
                        "label", "A",
                        "properties", emptyMap()
                    )
                ),
                "relationshipProjection", map(
                    "REL", map(
                        "type", "REL",
                        "orientation", "NATURAL",
                        "aggregation", "DEFAULT",
                        "properties", emptyMap()
                    )),
                "nodeCount", 2L,
                "relationshipCount", 1L
            )
        ));
    }

    @Test
    void calculateDegreeDistributionForUndirectedNodesWhenAskedTo() {
        String name = "name";
        runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD degreeDistribution", singletonList(
            map(
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Disabled("Disabled until we support REL> syntax for type filter")
    @Test
    void calculateDegreeDistributionForOutgoingRelationshipsWhenAskedTo() {
        String name = "name";
        runQuery("CALL gds.graph.create($name, 'A', 'REL>')", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD degreeDistribution", singletonList(
            map(
                "degreeDistribution", map(
                    "min", 1,
                    "mean", 1,
                    "max", 1,
                    "p50", 1,
                    "p75", 1,
                    "p90", 1,
                    "p95", 1,
                    "p99", 1,
                    "p999", 1
                )
            )
        ));
    }

    @Disabled("Disabled until we support REL> syntax for type filter")
    @Test
    void calculateDegreeDistributionForIncomingRelationshipsWhenAskedTo() {
        String name = "name";
        runQuery("CALL gds.graph.create($name, 'A', '<REL')", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD degreeDistribution", singletonList(
            map(
                "degreeDistribution", map(
                    "min", 1,
                    "mean", 1,
                    "max", 1,
                    "p50", 1,
                    "p75", 1,
                    "p90", 1,
                    "p95", 1,
                    "p99", 1,
                    "p999", 1
                )
            )
        ));
    }

    @ParameterizedTest(name = "name argument: {0}")
    @ValueSource(strings = {"", "null"})
    void listAllGraphsWhenCalledWithoutArgumentOrAnEmptyArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", name));
        }

        List<String> actualNames = runQuery(
            db,
            "CALL gds.graph.list(" + argument + ")",
            Collections.emptyMap(),
            result -> result.<String>columnAs("graphName")
                .stream()
                .collect(toList())
        );

        assertThat(actualNames, containsInAnyOrder(names));
    }

    @Test
    void filterOnExactMatchUsingTheFirstArgument() {
        String[] names = {"b", "bb", "ab", "ba", "B", "ʙ"};
        for (String name : names) {
            runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", name));
        }

        String name = names[0];
        List<String> actualNames = runQuery(
            db,
            "CALL gds.graph.list($name)",
            map("name", name),
            result -> result.<String>columnAs("graphName")
                .stream()
                .collect(toList())
        );

        assertThat(actualNames.size(), is(1));
        assertThat(actualNames, contains(name));
    }

    @Test
    void returnEmptyStreamWhenNoGraphsAreLoaded() {
        long numberOfRows = runQuery("CALL gds.graph.list()", Result::stream).count();
        assertThat(numberOfRows, is(0L));
    }

    @ParameterizedTest(name = "name argument: ''{0}''")
    @ValueSource(strings = {"foobar", "aa"})
    void returnEmptyStreamWhenNoGraphMatchesTheFilterArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", name));
        }

        long numberOfRows = runQuery(
            db,
            "CALL gds.graph.list($argument)",
            map("argument", argument),
            result -> result.stream().count()
        );

        assertThat(numberOfRows, is(0L));
    }

    @Test
    void reverseProjectionForListing() {
        runQuery("CREATE (a:Person), (b:Person), (a)-[:INTERACTS]->(b)");
        runQuery(
            "CALL gds.graph.create('incoming', 'Person', {" +
            "  INTERACTS: {" +
            "    orientation: 'REVERSE'" +
            "  }" +
            "})"
        );
        runQueryWithRowConsumer("CALL gds.graph.list()", row -> {
            assertEquals(2, row.getNumber("nodeCount").intValue());
        });
    }

    @Test
    void listAllAvailableGraphsForUser() {
        String loadQuery = "CALL gds.graph.create(" +
                           "    $name, '', '')";

        runQuery("alice", loadQuery, map("name", "aliceGraph"));
        runQuery("bob", loadQuery, map("name", "bobGraph"));

        String listQuery = "CALL gds.graph.list() YIELD graphName as name";

        runQueryWithRowConsumer("alice", listQuery, resultRow -> Assertions.assertEquals("aliceGraph", resultRow.getString("name")));
        runQueryWithRowConsumer("bob", listQuery, resultRow -> Assertions.assertEquals("bobGraph", resultRow.getString("name")));
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.catalog.GraphCreateProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {
        if (invalidName != null) { // null is not a valid name, but we use it to mean 'list all'
            assertError(
                "CALL gds.graph.list($graphName)",
                map("graphName", invalidName),
                String.format("`graphName` can not be null or blank, but it was `%s`", invalidName)
            );
        }
    }

    @ParameterizedTest(name = "Invalid Graph Name: {0}")
    @ValueSource(strings = {"{ a: 'b' }", "[]", "1", "true", "false", "[1, 2, 3]", "1.4"})
    void failsOnInvalidGraphNameTypeDueToObjectSignature(String graphName) {
        assertError(String.format("CALL gds.graph.list(%s)", graphName), "Type mismatch: expected String but was");
    }
}
