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
package org.neo4j.gds.beta.pregel.sssp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.beta.pregel.sssp.SingleSourceShortestPathPregel.DISTANCE;

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

        HashMap<Long, Long> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Long>) r.get("values")).get(DISTANCE)
            );
        });

        var expected = Map.of(
            0L, 0L,
            1L, 1L,
            2L, 1L,
            3L, 2L,
            4L, Long.MAX_VALUE,
            5L, Long.MAX_VALUE,
            6L, Long.MAX_VALUE,
            7L, Long.MAX_VALUE,
            8L, Long.MAX_VALUE,
            9L, Long.MAX_VALUE
        );

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
    }
}
