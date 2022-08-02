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
package org.neo4j.gds.catalog;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractIterableAssert;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.core.Is.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.gds.assertj.AssertionsHelper.booleanAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.creationTimeAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.intAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.listAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.stringObjectMapAssertFactory;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphDropProcTest extends BaseProcTest {
    private static final String DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";
    private static final String GRAPH_NAME = "graphNameToDrop";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphExistsProc.class,
            GraphDropProc.class
        );
        registerFunctions(GraphExistsFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void dropGraphFromCatalog() {
        runQuery("CALL gds.graph.project($name, 'A', 'REL')", Map.of("name", GRAPH_NAME));

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of("graphName", GRAPH_NAME, "exists", true)
            )
        );

        assertCypherResult(
            "CALL gds.graph.drop($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.ofEntries(
                    entry("graphName", GRAPH_NAME),
                    entry("configuration",
                    new Condition<>(config -> {
                        assertThat(config)
                            .asInstanceOf(stringObjectMapAssertFactory())
                            .hasSize(9)
                            .containsEntry(
                                "nodeProjection", map(
                                    "A", map(
                                        "label", "A",
                                        "properties", emptyMap()
                                    )
                                )
                            )
                            .containsEntry(
                                "relationshipProjection", map(
                                    "REL", map(
                                        "type", "REL",
                                        "orientation", "NATURAL",
                                        "aggregation", "DEFAULT",
                                        "properties", emptyMap()
                                    )
                                )
                            )
                            .hasEntrySatisfying(
                                "relationshipProperties",
                                listAssertConsumer(AbstractIterableAssert::isEmpty)
                            )
                            .hasEntrySatisfying("nodeProperties", listAssertConsumer(AbstractIterableAssert::isEmpty))
                            .hasEntrySatisfying("creationTime", creationTimeAssertConsumer())
                            .hasEntrySatisfying(
                                "validateRelationships",
                                booleanAssertConsumer(AbstractBooleanAssert::isFalse)
                            )
                            .hasEntrySatisfying(
                                "readConcurrency",
                                intAssertConsumer(readConcurrency -> readConcurrency.isEqualTo(4))
                            )
                            .hasEntrySatisfying("sudo", booleanAssertConsumer(AbstractBooleanAssert::isFalse))
                            .doesNotContainKeys(
                                "username",
                                GraphProjectConfig.NODE_COUNT_KEY,
                                GraphProjectConfig.RELATIONSHIP_COUNT_KEY
                            );

                        return true;
                    }, "Assert native `configuration` map")),
                    entry("nodeCount", 2L),
                    entry("relationshipCount", 1L),
                    entry("creationTime", isA(ZonedDateTime.class)),
                    entry("modificationTime", isA(ZonedDateTime.class)),
                    entry("memoryUsage", ""),
                    entry("sizeInBytes", -1L),
                    entry("schema", Map.of(
                        "nodes", Map.of("A", Map.of()),
                        "relationships", Map.of("REL", Map.of()),
                        "graphProperties", Map.of()
                    )),
                    entry("density", new Condition<>(Double::isFinite, "a finite double")),
                    entry("database", db.databaseName())
                )
            )
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }

    @Test
    void shouldNotReturnDegreeDistribution() {
        runQuery("CALL gds.graph.project($name, 'A', 'REL')", Map.of("name", GRAPH_NAME));

        runQueryWithResultConsumer(
            "CALL gds.graph.drop($graphName)",
            Map.of("graphName", GRAPH_NAME),
            result -> assertFalse(
                result.columns().contains("degreeDistribution"),
                "The result should not contain `degreeDistribution` field"
            )
        );
    }

    @Test
    void removeGraphWithMultipleRelationshipTypes() throws Exception {
        clearDb();
        registerProcedures(GraphListProc.class);

        String testGraph =
            "CREATE" +
            "  (a:A {id: 0, partition: 42})" +
            ", (b:B {id: 1, partition: 42})" +
            ", (a)-[:X { weight: 1.0 }]->(:A {id: 2,  weight: 1.0, partition: 1})" +
            ", (b)-[:Y { weight: 42.0 }]->(:B {id: 10, weight: 1.0, partition: 1})";

        runQuery(testGraph);

        String query = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("X")
            .withRelationshipType("Y")
            .yields();

        runQuery(query);

        List<Map<String, Object>> expectedGraphInfo = List.of(
            Map.of("nodeCount", 4L, "relationshipCount", 2L, "graphName", GRAPH_NAME)
        );

        assertCypherResult(
            "CALL gds.graph.list($name) YIELD nodeCount, relationshipCount, graphName",
            Map.of("name", GRAPH_NAME),
            expectedGraphInfo
        );

        assertCypherResult(
            "CALL gds.graph.drop($name) YIELD nodeCount, relationshipCount, graphName",
            Map.of("name", GRAPH_NAME),
            expectedGraphInfo
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }

    @Test
    void considerFailIfMissingFlag() {
        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of("graphName", GRAPH_NAME, "exists", false)
            )
        );

        assertCypherResult(
            "CALL gds.graph.drop($graphName, false)",
            Map.of("graphName", GRAPH_NAME, "failIfMissing", false),
            Collections.emptyList()
        );
    }

    @Test
    void doNotAcceptUsernameOverrideForNonGdsAdmins() {
        var graphNameParams = Map.<String, Object>of("name", GRAPH_NAME);

        QueryRunner.runQuery(db, "alice", "CALL gds.graph.project($name, 'A', 'REL')", graphNameParams);

        assertThatThrownBy(() -> QueryRunner.runQuery(db, "bob", "CALL gds.graph.drop($name, true, '', 'alice')", graphNameParams))
            .hasMessageContaining("Cannot override the username as a non-admin");

        QueryRunner.runQuery(db, "alice", "CALL gds.graph.exists($name)", graphNameParams, result -> {
            result.accept(row -> {
                assertThat(row.getString("graphName")).isEqualTo(GRAPH_NAME);
                assertThat(row.getBoolean("exists")).isTrue();
                return true;
            });
            return null;
        });
    }

    @Test
    void failsOnNonExistingGraph() {
        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of("graphName", GRAPH_NAME, "exists", false)
            )
        );

        assertError(
            "CALL gds.graph.drop($graphName)",
            Map.of("graphName", GRAPH_NAME),
            formatWithLocale("Graph with name `%s` does not exist on database `neo4j`.", GRAPH_NAME)
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("org.neo4j.gds.catalog.GraphProjectProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {
        var params = new HashMap<String, Object>();
        params.put("graphName", invalidName);
        assertError(
            "CALL gds.graph.drop($graphName)",
            params,
            formatWithLocale("`graphName` can not be null or blank, but it was `%s`", invalidName)
        );
    }

    @Test
    void dropsMultipleGraphsGraphFromCatalogWithListAsArgument() {
        var createQuery = "CALL gds.graph.project($name, '*', '*')";
        runQuery(createQuery, Map.of("name", "g1"));
        runQuery(createQuery, Map.of("name", "g2"));

        assertCypherResult(
            "CALL gds.graph.drop(['g1', 'g2']) YIELD graphName RETURN graphName",
            Map.of(),
            List.of(
                Map.of("graphName", "g1"),
                Map.of("graphName", "g2")
            )
        );

        var existsQuery = "RETURN gds.graph.exists($graphName) AS exists";
        assertCypherResult(
            existsQuery, Map.of("graphName", "g1"),
            List.of(Map.of("exists", false))
        );
        assertCypherResult(
            existsQuery, Map.of("graphName", "g2"),
            List.of(Map.of("exists", false))
        );
    }

    @Test
    void requiresAllGraphsToExistBeforeAnyOneIsDropped() {
        var createQuery = "CALL gds.graph.project($name, '*', '*')";
        var existsQuery = "RETURN gds.graph.exists($graphName) AS exists";

        runQuery(createQuery, Map.of("name", "g1"));

        assertCypherResult(
            existsQuery, Map.of("graphName", "g2"),
            List.of(Map.of("exists", false))
        );

        assertError(
            "CALL gds.graph.drop(['g1', 'g2'])",
            "Graph with name `g2` does not exist on database `neo4j`."
        );

        assertCypherResult(
            existsQuery, Map.of("graphName", "g1"),
            List.of(Map.of("exists", true))
        );
    }

    @Test
    void failsWithAllMissingGraphsInOneCall() {
        var existsQuery = "RETURN gds.graph.exists($graphName) AS exists";

        assertCypherResult(
            existsQuery, Map.of("graphName", "g1"),
            List.of(Map.of("exists", false))
        );

        assertCypherResult(
            existsQuery, Map.of("graphName", "g2"),
            List.of(Map.of("exists", false))
        );

        assertError(
            "CALL gds.graph.drop(['g1', 'g2', 'g3'])",
            "The graphs `g1`, `g2`, and `g3` do not exist on database `neo4j`."
        );
    }

    @Test
    void droppingMultipleGraphsAcceptsTheFailIfMissingFlag() {
        var createQuery = "CALL gds.graph.project($name, '*', '*')";
        runQuery(createQuery, Map.of("name", "g1"));

        assertCypherResult(
            "CALL gds.graph.drop(['g1', 'g2'], false) YIELD graphName RETURN graphName",
            List.of(
                Map.of("graphName", "g1")
            )
        );

        var existsQuery = "RETURN gds.graph.exists($graphName) AS exists";
        assertCypherResult(
            existsQuery, Map.of("graphName", "g1"),
            List.of(Map.of("exists", false))
        );
        assertCypherResult(
            existsQuery, Map.of("graphName", "g2"),
            List.of(Map.of("exists", false))
        );
    }

    @ParameterizedTest
    @MethodSource("invalidInputTypeValues")
    void failOnNonStringOrListInput(Object invalidInput) {
        var params = new HashMap<String, Object>();
        params.put("graphName", invalidInput);
        assertError(
            "CALL gds.graph.drop($graphName)",
            params,
            formatWithLocale(
                "Type mismatch: expected String but was %s",
                invalidInput.getClass().getSimpleName()
            )
        );

        params.put("graphName", List.of(invalidInput));
        assertError(
            "CALL gds.graph.drop($graphName)",
            params,
            formatWithLocale(
                "Type mismatch at index 0: expected String but was %s",
                invalidInput.getClass().getSimpleName()
            )
        );
    }

    static Stream<Object> invalidInputTypeValues() {
        return Stream.of(true, false, 42L, 13.37D);
    }
}
