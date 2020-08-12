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
package org.neo4j.graphalgo.beta.pregel.sssp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.beta.pregel.Pregel;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphalgo.TestSupport.mapEquals;

class SingleSourceShortestPathPregelProcTest extends BaseProcTest {

    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        // {J}
        ", (j:Node)" +
        // {A, B, C, D}
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(d)" +
        ", (a)-[:TYPE]->(c)" +
        // {E, F, G}
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(g)" +
        // {H, I}
        ", (i)-[:TYPE]->(h)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(TEST_GRAPH);

        registerProcedures(SingleSourceShortestPathPregelStreamProc.class);
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("example", "pregel", "sssp")
            .streamMode()
            .addParameter("maxIterations", 10)
            .addParameter("startNode", 0)
            .yields("nodeId", "values");

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Double>) r.get("values")).get(Pregel.DEFAULT_NODE_VALUE_KEY)
            );
        });

        var expected = Map.of(
            0L, 0.0D,
            1L, 1.0D,
            2L, 1.0D,
            3L, 2.0D,
            4L, Double.MAX_VALUE,
            5L, Double.MAX_VALUE,
            6L, Double.MAX_VALUE,
            7L, Double.MAX_VALUE,
            8L, Double.MAX_VALUE,
            9L, Double.MAX_VALUE
        );

        assertThat(expected, mapEquals(actual));
    }
}
