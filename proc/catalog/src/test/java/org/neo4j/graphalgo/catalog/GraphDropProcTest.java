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
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class GraphDropProcTest extends BaseProcTest {
    private static final String DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";
    private static final String GRAPH_NAME = "graphNameToDrop";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCreateProc.class,
            GraphExistsProc.class,
            GraphDropProc.class
        );

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void dropGraphFromCatalog() {
        runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", GRAPH_NAME));

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", true)
            )
        );

        assertCypherResult(
            "CALL gds.graph.drop($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map(
                    "graphName", GRAPH_NAME,
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
                    ),
                    "creationTime", isA(LocalDateTime.class),
                    "modificationTime", isA(LocalDateTime.class)
                )
            )
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }


    @Test
    void dropWithDegreeDistributionComputationOptOut() {
        runQuery("CALL gds.graph.create($name, 'A', 'REL')", map("name", GRAPH_NAME));

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", true)
            )
        );

        assertCypherResult(
            "CALL gds.graph.drop($graphName) " +
            "YIELD graphName, nodeProjection, relationshipProjection, nodeCount, relationshipCount",
            map("graphName", GRAPH_NAME),
            singletonList(
                map(
                    "graphName", GRAPH_NAME,
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
            )
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }

    @Test
    void removeGraphWithMultipleRelationshipTypes() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class, GraphExistsProc.class, GraphDropProc.class, GraphListProc.class);

        String testGraph =
            "CREATE" +
            "  (a:A {id: 0, partition: 42})" +
            ", (b:B {id: 1, partition: 42})" +
            ", (a)-[:X { weight: 1.0 }]->(:A {id: 2,  weight: 1.0, partition: 1})" +
            ", (b)-[:Y { weight: 42.0 }]->(:B {id: 10, weight: 1.0, partition: 1})";

        runQuery(testGraph, emptyMap());

        String query = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("X")
            .withRelationshipType("Y")
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(query);

        List<Map<String, Object>> expectedGraphInfo = singletonList(
            map("nodeCount", 4L, "relationshipCount", 2L, "graphName", GRAPH_NAME)
        );

        assertCypherResult("CALL gds.graph.list($name) YIELD nodeCount, relationshipCount, graphName",
            singletonMap("name", GRAPH_NAME),
            expectedGraphInfo
        );

        assertCypherResult("CALL gds.graph.drop($name) YIELD nodeCount, relationshipCount, graphName",
            singletonMap("name", GRAPH_NAME),
            expectedGraphInfo
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }

    @Test
    void failsOnNonExistingGraph() {
        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", false)
            )
        );

        assertError(
            "CALL gds.graph.drop($graphName)",
            map("graphName", GRAPH_NAME),
            String.format("Graph with name `%s` does not exist and can't be removed.", GRAPH_NAME)
        );

        assertCypherResult(
            "CALL gds.graph.exists($graphName)",
            map("graphName", GRAPH_NAME),
            singletonList(
                map("graphName", GRAPH_NAME, "exists", false)
            )
        );
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("org.neo4j.graphalgo.catalog.GraphCreateProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {
        assertError(
            "CALL gds.graph.drop($graphName)",
            map("graphName", invalidName),
            String.format("`graphName` can not be null or blank, but it was `%s`", invalidName)
        );
    }

}
