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
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.neo4j.helpers.collection.MapUtil.map;

class GraphDropProcTest extends ProcTestBase {
    private static final String DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCatalogProcs.class,
            GraphExistsProc.class,
            GraphDropProc.class
        );

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void drop() {
        String graphName = "graphNameToDrop";
        runQuery("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", graphName));

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphName),
            singletonList(
                map("graphName", graphName, "exists", true)
            )
        );

        assertCypherResult(
            "CALL algo.beta.graph.drop($graphName)",
            map("graphName", graphName),
            singletonList(
                map(
                    "graphName", graphName,
                    "nodeProjection", map(
                        "A", map(
                            "label", "A",
                            "properties", emptyMap()
                        )
                    ),
                    "relationshipProjection", map(
                        "REL", map(
                            "type", "REL",
                            "projection", "NATURAL",
                            "aggregation", "DEFAULT",
                            "properties", emptyMap()
                        )
                    ),
                    "nodes", 2L,
                    "relationships", 1L,
                    "histogram", map(
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
            )
        );

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphName),
            singletonList(
                map("graphName", graphName, "exists", false)
            )
        );
    }


    @Test
    void dropWithHistogramComputationOptOut() {
        String graphName = "graphNameToDrop";
        runQuery("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", graphName));

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphName),
            singletonList(
                map("graphName", graphName, "exists", true)
            )
        );

        assertCypherResult(
            "CALL algo.beta.graph.drop($graphName) YIELD graphName, nodeProjection, relationshipProjection, nodes, relationships",
            map("graphName", graphName),
            singletonList(
                map(
                    "graphName", graphName,
                    "nodeProjection", map(
                        "A", map(
                            "label", "A",
                            "properties", emptyMap()
                        )
                    ),
                    "relationshipProjection", map(
                        "REL", map(
                            "type", "REL",
                            "projection", "NATURAL",
                            "aggregation", "DEFAULT",
                            "properties", emptyMap()
                        )),
                    "nodes", 2L,
                    "relationships", 1L
                )
            )
        );

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphName),
            singletonList(
                map("graphName", graphName, "exists", false)
            )
        );
    }


    @Test
    void failOnNonExistingGraph() {
        String graphName = "nonExistingGraph";

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphName),
            singletonList(
                map("graphName", graphName, "exists", false)
            )
        );

        assertError(
            "CALL algo.beta.graph.drop($graphName)",
            map("graphName", graphName),
            String.format("Graph with name `%s` does not exist and can't be removed.", graphName)
        );

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphName),
            singletonList(
                map("graphName", graphName, "exists", false)
            )
        );
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("blankStrings")
    void failsOnEmptyGraphName(String graphName) {
        assertError(
            "CALL algo.beta.graph.drop($graphName)",
            map("graphName", graphName),
            String.format("`graphName` can not be null or blank, but it was `%s`", graphName)
        );
    }

    static Stream<String> blankStrings() {
        return Stream.of("", "   ", "           ", "\r\n\t", null);
    }

}
