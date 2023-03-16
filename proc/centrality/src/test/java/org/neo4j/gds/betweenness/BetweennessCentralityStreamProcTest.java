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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

class BetweennessCentralityStreamProcTest extends BaseProcTest {


    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            BetweennessCentralityStreamProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @Test
    void testStream() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.betweenness")
            .streamMode()
            .yields();

        assertCypherResult(
            query,
            List.of(
                Map.of("nodeId", idFunction.of("a"), "score", 0.0),
                Map.of("nodeId", idFunction.of("b"), "score", 3.0),
                Map.of("nodeId", idFunction.of("c"), "score", 4.0),
                Map.of("nodeId", idFunction.of("d"), "score", 3.0),
                Map.of("nodeId", idFunction.of("e"), "score", 0.0)
            ));
    }

    // This should not be tested here
    @Test
    void shouldValidateSampleSize() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.betweenness")
            .streamMode()
            .addParameter("samplingSize", -42)
            .yields();

        assertError(query, "Configuration parameter 'samplingSize' must be a positive number, got -42.");
    }

}
