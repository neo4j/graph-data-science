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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.neo4j.helpers.collection.MapUtil.map;

class GraphExistProcTest extends ProcTestBase {
    private static final String DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCatalogProcs.class
        );
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest(name = "Existing Graphs: {0}, Lookup: {1}, Exists: {2}")
    @MethodSource("graphNameExistsCombinations")
    void existsTest(String graphNameToCreate, String graphNameToCheck, boolean exists) {
        db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", graphNameToCreate));

        assertCypherResult(
            "CALL algo.beta.graph.exists($graphName)",
            map("graphName", graphNameToCheck),
            singletonList(
                map("graphName", graphNameToCheck, "exists", exists)
            )
        );
    }

    static Stream<Arguments> graphNameExistsCombinations() {
        return Stream.of(
            Arguments.of("g", "g", true),
            Arguments.of("graph", "graph", true),
            Arguments.of("graph", "graph1", false),
            Arguments.of("g", "a", false),
            Arguments.of("g", "", false),
            Arguments.of("g", null, false),
            Arguments.of("g", "\n", false),
            Arguments.of("g", "    ", false)
        );
    }
}
