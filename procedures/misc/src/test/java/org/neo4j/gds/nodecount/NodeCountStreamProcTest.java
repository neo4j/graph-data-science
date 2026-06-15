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
package org.neo4j.gds.nodecount;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isA;

class NodeCountStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    @Language("Cypher")
    private static final String DB_CYPHER =
        "CREATE" +
        " (n0:A)" +
        ",(n1:A)" +
        ",(n2:A)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            NodeCountStreamProc.class
        );

        runQuery("CALL gds.graph.project('g', 'A', '*')");
    }

    @Test
    void stream() {
        var query = GdsCypher
            .call("g")
            .algo("gds.nodeCount")
            .streamMode()
            .yields();

        assertCypherResult(query, List.of(Map.of("nodeCount", 3L)));
    }

    @Test
    void estimate() {
        var query = GdsCypher
            .call("g")
            .algo("gds.nodeCount")
            .streamEstimation()
            .yields();

        assertCypherResult(query, List.of(Map.of(
                "mapView", isA(Map.class),
                "treeView", isA(String.class),
                "bytesMax", greaterThanOrEqualTo(0L),
                "heapPercentageMin", greaterThanOrEqualTo(0.0),
                "nodeCount", 3L,
                "relationshipCount", 0L,
                "requiredMemory", isA(String.class),
                "bytesMin", greaterThanOrEqualTo(0L),
                "heapPercentageMax", greaterThanOrEqualTo(0.0)
            ))
        );
    }
}
