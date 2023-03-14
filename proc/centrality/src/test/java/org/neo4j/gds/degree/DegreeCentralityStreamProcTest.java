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
package org.neo4j.gds.degree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DegreeCentralityStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +
        ", (h:Label1)" +
        ", (i:Label1)" +
        ", (j:Label1)" +

        ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +

        ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

        ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 2.0}]->(e)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            DegreeCentralityStreamProc.class,
            GraphProjectProc.class
        );

        String createQuery = GdsCypher.call("dcGraph")
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @Test
    void testStream() {
        String streamQuery = GdsCypher.call("dcGraph")
            .algo("degree")
            .streamMode()
            .yields();

        Map<Long, Double> expected = Map.of(
            idFunction.of("a"), 0.0D,
            idFunction.of("b"), 1.0D,
            idFunction.of("c"), 1.0D,
            idFunction.of("d"), 2.0D,
            idFunction.of("e"), 3.0D,
            idFunction.of("f"), 2.0D,
            idFunction.of("g"), 0.0D,
            idFunction.of("h"), 0.0D,
            idFunction.of("i"), 0.0D,
            idFunction.of("j"), 0.0D
        );
        runQueryWithRowConsumer(streamQuery, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double score = row.getNumber("score").doubleValue();
            assertEquals(expected.get(nodeId), score);
        });

    }
}
